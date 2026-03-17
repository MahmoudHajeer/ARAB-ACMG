# Scripts

This directory now keeps only the active, supported entry points for the current GCS-first workflow.

## 1. Raw Source Freezing
- `verify_gcp.py`
  - Cheap connectivity check for the configured GCS bucket and historical raw BigQuery datasets.
- `manifest_utility.py`
  - Shared manifest builder for source-freeze scripts.
- `ingest_clinvar_cloud.py`
  - Freeze the ClinVar raw VCF into GCS.
- `ingest_gnomad_parquet.py`
  - Freeze the selected gnomAD raw chromosome files into GCS.
- `ingest_gme_cloud.py`
  - Freeze the GME summary table into GCS.
- `freeze_arab_frequency_sources.py`
  - Freeze SHGP and the AVDB workbook, including the AVDB GRCh37 to GRCh38 conversion artifact.
- `verify_arab_frequency_sources.py`
  - Verify the frozen SHGP and AVDB artifacts in GCS.
- `freeze_arab_study_sources.py`
  - Freeze de-identified study supplements from Arab cohort papers.
- `verify_arab_study_sources.py`
  - Verify the frozen study extracts and intake report.

## 2. Harmonization and Checkpoint Build
- `build_brca_normalized_artifacts.py`
  - Main BRCA normalization build. Produces normalized Parquet artifacts, checkpoint tables, manifests, and the frozen review bundle inputs.
- `refresh_supervisor_review_bundle.py`
  - Re-compose the static supervisor bundle from the frozen artifacts without re-running the heavy normalization path.
- `sync_public_gcs_downloads.py`
  - Publish only the safe supervisor download-center objects for anonymous GCS access and verify that downloads start without authentication.
- `verify_brca_normalized_artifacts.py`
  - Validate canonical keys, artifact existence, public downloads, and checkpoint schema relationships.

## 3. Static UI State Builders
- `update_source_review_state.py`
  - Build the scientific source-review JSON from frozen evidence.
- `update_controlled_access_state.py`
  - Build the controlled-access roadmap JSON.
- `update_ui_overview_state.py`
  - Re-bundle overview/progress state for local or deployed UI use.

## Local Build Order
```bash
python3 scripts/build_brca_normalized_artifacts.py
python3 scripts/verify_brca_normalized_artifacts.py
python3 scripts/sync_public_gcs_downloads.py
uvicorn ui.service:app --host 0.0.0.0 --port 8080
```

The removed BigQuery live-registry and raw-layer status scripts remain visible only in Git history and Conductor checkpoints.
