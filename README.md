# bq2bq-dbt-docker

> **Documentation Technique** : Projet dbt Cloud Run (Architecture Generic Runner)

Ce document détaille l'architecture technique pour l'ingestion de données BigQuery et la notification d'API via Cloud Run.

---

## 📁 Table des matières

1. [Structure et Logique des Données](#1-structure-et-logique-des-données-dbt--sql)
2. [Gestion des Dépendances](#2-gestion-des-dépendances--environnement-uv)
3. [Sécurité & Authentification API](#3-sécurité--authentification-api-secret-token)
4. [Workflow "Generic Runner"](#4-workflow-generic-runner-download-on-start)
5. [Configuration des Requêtes SQL](#5-configuration-des-requêtes-sql)
6. [Wrapper Python (Orchestrateur)](#6-le-wrapper-python-lorchéstrateur)
7. [Conteneurisation Docker](#7-conteneurisation-docker)
8. [Infrastructure Terraform & Cloud Run](#8-infrastructure-terraform--cloud-run)

---

## 1. Structure et Logique des Données (dbt + SQL)

Au lieu de scripts SQL bruts, nous utilisons **dbt** (data build tool) pour gérer la compilation, l'ordre d'exécution (DAG) et la documentation des requêtes.

| Aspect | Description |
|--------|-------------|
| **Organisation** | Chaque requête de création de table est un fichier `.sql` situé dans le dossier `models/` |
| **Configuration Meta** | Pour lier une table SQL à un paramètre spécifique de l'API de gestion (ex: un ID de dataset), nous utilisons la configuration native de dbt directement dans le fichier SQL |

> [!TIP]
> Plus besoin de fichier CSV externe pour la configuration !

---

## 2. Gestion des Dépendances & Environnement (uv)

Le projet utilise [**uv**](https://github.com/astral-sh/uv) pour la gestion des dépendances Python.

```dockerfile
# Installe les dépendances définies dans uv.lock directement dans l'environnement système du conteneur
RUN uv sync --frozen --no-dev --system
```

| Option | Description |
|--------|-------------|
| `--frozen` | Interdit la mise à jour des versions (respect strict du lockfile) |
| `--no-dev` | N'installe pas les outils de test/dev pour alléger l'image |
| `--system` | Installe dans l'environnement système (pas de virtualenv) |

---

## 3. Sécurité & Authentification API (Secret Token)

L'appel à l'API de gestion (pour notifier la création des tables) est sécurisé par un **Bearer Token statique**.

> [!CAUTION]
> Ce token ne doit **jamais** apparaître en clair dans le code ou les fichiers de configuration.

### Flux de gestion du secret

```mermaid
flowchart LR
    A[Google Secret Manager] -->|monte| B[Cloud Run Job]
    B -->|variable env| C[API_TOKEN]
    C -->|utilisé par| D[Script Python]
```

| Étape | Description |
|-------|-------------|
| **Stockage** | Le token est stocké dans Google Secret Manager (géré par Terraform) |
| **Injection** | Le module Terraform configure le Job Cloud Run pour monter ce secret en tant que variable d'environnement `API_TOKEN` |
| **Source** | Secret Manager (version `latest`) |

### Utilisation dans le Script Python

```python
# Extrait du script
token = os.environ.get("API_TOKEN")
headers = {"Authorization": f"apikey {token}"} if token else {}
```

> [!NOTE]
> Si la variable est vide, le script logue un avertissement mais ne plante pas (sauf si un appel est requis).

---

## 4. Workflow "Generic Runner" (Download-on-Start)

L'architecture sépare le **code d'exécution** (Image Docker) des **requêtes métier** (Fichiers SQL).

```mermaid
sequenceDiagram
    participant CR as Cloud Run Job
    participant GCS as GCS Bucket
    participant BQ as BigQuery
    participant API as API de Gestion

    CR->>GCS: 1. Télécharge les fichiers .sql
    CR->>CR: 2. Place dans /app/models
    CR->>BQ: 3. dbt run (compile & exécute)
    CR->>CR: 4. Analyse run_results.json + manifest.json
    CR->>API: 5. Callback pour chaque succès
```

### Étapes détaillées

1. **Démarrage & Téléchargement** : Le script Python se connecte au bucket GCS défini par `GCS_BUCKET_NAME` et télécharge l'intégralité du préfixe `GCS_PREFIX` (défaut `models/`) dans `/app/models`, en préservant les sous-dossiers

2. **Installation des dépendances** : si un `packages.yml` a été expédié, le script lance `dbt deps`. Sinon l'étape est sautée

3. **Exécution dbt** : le script lance `dbt run`, ou `dbt build` si `DBT_COMMAND=build`. dbt compile les fichiers SQL téléchargés et exécute les transformations sur BigQuery

4. **Analyse & Callback** : Le script croise les résultats (`run_results.json`) avec la configuration meta (`manifest.json`). Pour chaque succès, il déclenche l'appel API avec le token sécurisé

### Arborescence attendue dans le bucket

Tout ce qui est sous `GCS_PREFIX` atterrit dans `/app/models`, **sauf** les sous-dossiers propres à dbt et `packages.yml`, qui vont à la racine du projet. C'est ce qui permet d'expédier des macros et des tests sans que dbt tente de les exécuter comme des modèles.

| Chemin dans le bucket | Destination dans le conteneur | Rôle |
|---|---|---|
| `models/mon_modele.sql` | `/app/models/mon_modele.sql` | un modèle |
| `models/staging/stg_x.sql` | `/app/models/staging/stg_x.sql` | un modèle, sous-dossier libre |
| `models/schema.yml` | `/app/models/schema.yml` | tests génériques et documentation de colonnes |
| `models/macros/ma_macro.sql` | `/app/macros/ma_macro.sql` | une macro |
| `models/tests/mon_test.sql` | `/app/tests/mon_test.sql` | un test singulier |
| `models/seeds/ref.csv` | `/app/seeds/ref.csv` | un seed |
| `models/packages.yml` | `/app/packages.yml` | dépendances, par exemple `dbt_utils` |

Les sous-dossiers reconnus sont `macros/`, `tests/`, `seeds/`, `snapshots/` et `analyses/`. Un dépôt existant qui ne contient que des `.sql` à plat garde exactement le comportement précédent.

### Exécuter les tests

`dbt run` ne lance aucun test. Pour que les tests déclarés dans les fichiers `.yml` s'exécutent, passer `DBT_COMMAND=build` : dbt construit les modèles et vérifie leurs tests **dans l'ordre du DAG**, si bien qu'un test en échec interrompt la construction des modèles en aval au lieu de les bâtir sur des données fausses.

La valeur par défaut reste `run`, pour ne rien changer aux pipelines existants.

---

## 5. Configuration des Requêtes SQL

La liaison entre une requête SQL et l'API se fait via la configuration `meta` native de dbt, directement dans le fichier `.sql`.

### Exemple : `models/users.sql`

```sql
{{ config(
    materialized='table',
    meta = {
        "api_trigger_param": "api_param" 
    }
) }}

-- Si api_trigger_param est présent, le script appellera : BASE_URL/api/automation/v1.0/datasets/{api_param}/publish/
SELECT ...
```

---

## 6. Le Wrapper Python (L'Orchestrateur)

Le point d'entrée du conteneur est le script `main.py`.

### Workflow du script

```mermaid
flowchart TD
    A[Démarrage] --> B[dbt run]
    B --> C[Analyse run_results.json + manifest.json]
    C --> D{Table en succès ?}
    D -->|Oui| E[Requête GET vers API]
    D -->|Non| F[Log erreur]
    E --> G[Fin]
    F --> G
```

| Étape | Description |
|-------|-------------|
| **Exécution** | Lance `dbt run` |
| **Analyse** | Croise les fichiers `run_results.json` (statuts) et `manifest.json` (configs meta) |
| **Notification** | Requête GET vers l'API de gestion pour chaque table en succès |
| **Authentification** | Bearer Token lu depuis `API_TOKEN` |

---

## 7. Conteneurisation (Docker)

Le projet est packagé dans une image Docker optimisée pour Cloud Run.

---

## 8. Infrastructure (Terraform & Cloud Run)

Le déploiement est géré par un **module Terraform générique**.

### Ressource principale

- **`google_cloud_run_v2_job`** : Contrairement à un "Service", le "Job" est conçu pour des tâches qui ont un début et une fin (pas d'écoute de port HTTP)

### Variables d'environnement

| Variable | Défaut | Rôle |
|---|---|---|
| `GCS_BUCKET_NAME` | — | bucket des modèles, **requis** |
| `GCS_PREFIX` | `models/` | préfixe téléchargé |
| `DBT_COMMAND` | `run` | `run` ou `build`. `build` exécute aussi les tests |
| `BQ_DBT_TARGET_DATASET` | `dbt_default` | dataset cible du profil, base du médaillon |
| `BQ_LOCATION` | `EU` | localisation BigQuery |
| `API_CALLBACK_URL` | vide | publication data.gouv.nc, facultatif |
| `API_TOKEN_SECRET` | vide | chemin du secret portant le token d'API |


| Variable | Description |
|----------|-------------|
| `API_CALLBACK_URL` | L'URL de base de l'API de gestion |
| `API_TOKEN` | Token d'authentification (injecté depuis Secret Manager) |

### Planification

Un **`google_cloud_scheduler_job`** déclenche le Job Cloud Run à fréquence définie (ex: tous les matins à 6h).

---

## 📂 Structure du projet

```
bq2bq-dbt-docker/
├── src/
│   ├── main.py          # Script orchestrateur Python
│   └── dbt_project.yml  # Configuration dbt
├── Dockerfile           # Image Docker
├── README.md            # Ce fichier
└── ...
```