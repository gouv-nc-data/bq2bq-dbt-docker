#!/usr/bin/env python3
"""
bq2bq-dbt-runner: Generic dbt runner for BigQuery

Workflow:
1. Download SQL models from GCS bucket
2. Run dbt transformations
3. Parse results and trigger API callbacks for successful models
"""

import json
import logging
import os
import subprocess
import sys
from pathlib import Path

import requests
import time
from google.cloud import storage

# =============================================================================
# Configuration
# =============================================================================

# GCS Configuration
BUCKET_NAME = os.environ.get("GCS_BUCKET_NAME")
GCS_PREFIX = os.environ.get("GCS_PREFIX", "models/")

# API Configuration
API_CALLBACK_URL = os.environ.get("API_CALLBACK_URL", "")
API_TOKEN = os.environ.get("API_TOKEN", "")

# dbt paths
DBT_PROJECT_DIR = Path(__file__).parent
MODELS_DIR = DBT_PROJECT_DIR / "models"
TARGET_DIR = DBT_PROJECT_DIR / "target"

# =============================================================================
# Logging (GCP structured)
# =============================================================================

import google.cloud.logging
from google.cloud.logging.handlers import StructuredLogHandler

# Initialisation du client de logging (détecte l'environnement Cloud Run)
client = google.cloud.logging.Client()
# Configuration du handler structuré pour envoyer en JSON sur stdout
handler = StructuredLogHandler()
google.cloud.logging.handlers.setup_logging(handler)

logger = logging.getLogger(__name__)


# =============================================================================
# GCS Download
# =============================================================================

def download_models_from_gcs() -> int:
    """Download SQL models from GCS bucket to local models directory."""
    if not BUCKET_NAME:
        logger.error("GCS_BUCKET_NAME environment variable not set")
        sys.exit(1)

    logger.info(f"Downloading models from gs://{BUCKET_NAME}/{GCS_PREFIX}")

    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blobs = list(bucket.list_blobs(prefix=GCS_PREFIX))

    # Filter out directory markers
    files = [b for b in blobs if not b.name.endswith("/")]

    if not files:
        logger.warning(f"No files found in gs://{BUCKET_NAME}/{GCS_PREFIX}")
        return 0

    # Ensure models directory exists
    MODELS_DIR.mkdir(parents=True, exist_ok=True)

    downloaded = 0
    for blob in files:
        # Remove prefix to get relative path
        relative_path = blob.name[len(GCS_PREFIX):] if blob.name.startswith(GCS_PREFIX) else blob.name
        local_path = MODELS_DIR / relative_path

        # Create subdirectories if needed
        local_path.parent.mkdir(parents=True, exist_ok=True)

        blob.download_to_filename(str(local_path))
        logger.info(f"  ✓ {relative_path}")
        downloaded += 1

    logger.info(f"Downloaded {downloaded} file(s)")
    return downloaded


# =============================================================================
# dbt Execution
# =============================================================================

def run_dbt() -> bool:
    """Execute dbt run and return success status."""
    logger.info("Running dbt...")

    result = subprocess.run(
        ["dbt", "run", "--project-dir", str(DBT_PROJECT_DIR)],
        capture_output=False,
        text=True,
    )

    if result.returncode != 0:
        logger.error(f"dbt run failed with exit code {result.returncode}")
        return False

    logger.info("dbt run completed successfully")
    return True


# =============================================================================
# Results Analysis & API Callbacks
# =============================================================================

def load_json_file(path: Path) -> dict | None:
    """Load and parse a JSON file."""
    if not path.exists():
        logger.warning(f"File not found: {path}")
        return None

    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_model_meta(manifest: dict, unique_id: str) -> dict:
    """Extract meta configuration for a model from manifest."""
    node = manifest.get("nodes", {}).get(unique_id, {})
    return node.get("config", {}).get("meta", {})


def trigger_api_callback(api_param: str) -> bool:
    """Send API callback for a successful model with retry logic."""
    if not API_CALLBACK_URL:
        logger.warning("API_CALLBACK_URL not set, skipping callback")
        return False

    url = f"{API_CALLBACK_URL.rstrip('/')}/api/automation/v1.0/datasets/{api_param}/publish/"
    headers = {"Authorization": f"apikey {API_TOKEN}"} if API_TOKEN else {}
    
    delays = [0, 3, 15]  # Impatience: immédiat, 3s, 15s
    max_attempts = len(delays)

    for attempt, delay in enumerate(delays, 1):
        if delay > 0:
            logger.info(f"  ... Retrying in {delay} seconds (attempt {attempt}/{max_attempts})")
            time.sleep(delay)
            
        try:
            response = requests.post(url, headers=headers, timeout=60)
            response.raise_for_status()
            logger.info(f"  ✓ API callback success: {api_param} (at attempt {attempt}/{max_attempts})")
            return True
        except requests.RequestException as e:
            if attempt < max_attempts:
                logger.warning(f"  ⚠ API callback attempt {attempt} failed for {api_param}: {e}")
            else:
                logger.error(f"  ✗ API callback failed for {api_param} after {max_attempts} attempts: {e}")
    
    return False

def process_results() -> tuple[int, int]:
    """
    Analyze dbt results and trigger API callbacks.
    
    Returns:
        Tuple of (successful_callbacks, failed_callbacks)
    """
    run_results = load_json_file(TARGET_DIR / "run_results.json")
    manifest = load_json_file(TARGET_DIR / "manifest.json")

    if not run_results or not manifest:
        logger.error("Could not load dbt result files")
        return 0, 0

    success_count = 0
    fail_count = 0

    for result in run_results.get("results", []):
        unique_id = result.get("unique_id", "")
        status = result.get("status", "")

        # Only process successful model runs
        if status != "success":
            logger.info(f"  ⊘ {unique_id}: status={status}, skipping callback")
            continue

        # Check for api_trigger_param in meta
        meta = get_model_meta(manifest, unique_id)
        api_param = meta.get("api_trigger_param")

        if not api_param:
            logger.info(f"  ⊘ {unique_id}: no api_trigger_param, skipping callback")
            continue

        # Trigger API callback
        if trigger_api_callback(api_param):
            success_count += 1
        else:
            fail_count += 1

    return success_count, fail_count


# =============================================================================
# Main Entry Point
# =============================================================================

def main():
    """Main orchestration workflow."""
    logger.info("=" * 60)
    logger.info("bq2bq-dbt-runner starting")
    logger.info("=" * 60)

    # Step 1: Download models from GCS
    file_count = download_models_from_gcs()
    if file_count == 0:
        logger.warning("No models to process, exiting")
        sys.exit(0)

    # Step 2: Run dbt
    if not run_dbt():
        logger.error("dbt execution failed")
        sys.exit(1)

    # Step 3: Process results and trigger callbacks
    logger.info("Processing results and triggering API callbacks...")
    success, fail = process_results()

    # Summary
    logger.info("=" * 60)
    logger.info(f"Completed: {success} callbacks succeeded, {fail} failed")
    logger.info("=" * 60)

    # Exit with error if any callbacks failed
    if fail > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()