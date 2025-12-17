CREATE OR REPLACE TABLE sydonia_opendata.comptes_secours AS
WITH csh_cre_last_mvt AS (
  SELECT
    acc_cod,
    ARRAY_AGG(tot_bal ORDER BY instance_id DESC LIMIT 1)[OFFSET(0)] AS last_tot_bal
  FROM sydonia.csh_cre_mvt
  GROUP BY acc_cod
),
csh_gty_last_mvt AS (
  SELECT
    acc_cod,
    ARRAY_AGG(tot_bal ORDER BY instance_id DESC LIMIT 1)[OFFSET(0)] AS last_tot_bal
  FROM sydonia.csh_gty_mvt
  GROUP BY acc_cod
)

SELECT
  cre.acc_cod AS COMPTE,
  cre.acc_typ_dsc AS TYPE,
  CONCAT(
    REPLACE(FORMAT("%'d", CAST(FLOOR(cre.acc_amt) AS INT64)), ",", " "),
    SUBSTR(FORMAT("%.2f", cre.acc_amt - FLOOR(cre.acc_amt)), 2)
  ) AS `MONTANT AUTORISE`,
  CONCAT(
    REPLACE(FORMAT("%'d", CAST(FLOOR(COALESCE(mvt.last_tot_bal, 0)) AS INT64)), ",", " "),
    SUBSTR(FORMAT("%.2f", COALESCE(mvt.last_tot_bal, 0) - FLOOR(COALESCE(mvt.last_tot_bal, 0))), 2)
  ) AS `MONTANT CONSOMME`,
  CONCAT(
    REPLACE(FORMAT("%'d", CAST(FLOOR(cre.acc_amt + COALESCE(mvt.last_tot_bal, 0)) AS INT64)), ",", " "),
    SUBSTR(FORMAT("%.2f", (cre.acc_amt + COALESCE(mvt.last_tot_bal, 0)) - FLOOR(cre.acc_amt + COALESCE(mvt.last_tot_bal, 0))), 2)
  ) AS `SOLDE DISPONIBLE`,
  IFNULL(cre.acc_own_dec, '') AS DECLARANT,
  IF(cre.acc_own_dec IS NOT NULL, cre.acc_own_nam, '') AS `NOM DECLARANT`,
  IFNULL(cre.acc_own_cmp, '') AS ENTREPRISE,
  IF(cre.acc_own_cmp IS NOT NULL, cre.acc_own_nam, '') AS `NOM ENTREPRISE`,
  cre.acc_sta AS STATUT
FROM sydonia.csh_cre cre
LEFT JOIN csh_cre_last_mvt mvt ON mvt.acc_cod = cre.acc_cod
WHERE CURRENT_DATE() BETWEEN cre.acc_dat_beg AND IFNULL(cre.acc_dat_end, CURRENT_DATE())

UNION ALL

SELECT
  cre.acc_cod AS COMPTE,
  cre.acc_typ_dsc AS TYPE,
  CONCAT(
    REPLACE(FORMAT("%'d", CAST(FLOOR(cre.acc_amt) AS INT64)), ",", " "),
    SUBSTR(FORMAT("%.2f", cre.acc_amt - FLOOR(cre.acc_amt)), 2)
  ) AS `MONTANT AUTORISE`,
  CONCAT(
    REPLACE(FORMAT("%'d", CAST(FLOOR(COALESCE(mvt.last_tot_bal, 0)) AS INT64)), ",", " "),
    SUBSTR(FORMAT("%.2f", COALESCE(mvt.last_tot_bal, 0) - FLOOR(COALESCE(mvt.last_tot_bal, 0))), 2)
  ) AS `MONTANT CONSOMME`,
  CONCAT(
    REPLACE(FORMAT("%'d", CAST(FLOOR(cre.acc_amt + COALESCE(mvt.last_tot_bal, 0)) AS INT64)), ",", " "),
    SUBSTR(FORMAT("%.2f", (cre.acc_amt + COALESCE(mvt.last_tot_bal, 0)) - FLOOR(cre.acc_amt + COALESCE(mvt.last_tot_bal, 0))), 2)
  ) AS `SOLDE DISPONIBLE`,
  IFNULL(cre.acc_dec_cod, '') AS DECLARANT,
  IF(cre.acc_dec_cod IS NOT NULL, cre.acc_nam, '') AS `NOM DECLARANT`,
  IFNULL(cre.acc_cmp_cod, '') AS ENTREPRISE,
  IF(cre.acc_cmp_cod IS NOT NULL, cre.acc_nam, '') AS `NOM ENTREPRISE`,
  cre.acc_sta AS STATUT
FROM sydonia.gty_cmp cre
LEFT JOIN csh_gty_last_mvt mvt ON mvt.acc_cod = cre.acc_cod
WHERE CURRENT_DATE() BETWEEN cre.acc_dat_beg AND IFNULL(cre.acc_dat_end, CURRENT_DATE())
