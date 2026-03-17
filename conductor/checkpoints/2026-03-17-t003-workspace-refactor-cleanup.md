# T003 Workspace Refactor Cleanup

## Scope
This pass cleaned the active workspace before the next harmonized-validation phase. The goal was to keep only the current GCS-first runtime surfaces, remove obsolete BigQuery/live-registry code paths, and make the local workflow easier to run from a shared development environment such as Google Studio.

## Active Pipeline Kept
- `scripts/build_brca_normalized_artifacts.py`
- `scripts/refresh_supervisor_review_bundle.py`
- `scripts/verify_brca_normalized_artifacts.py`
- `scripts/update_source_review_state.py`
- `scripts/update_controlled_access_state.py`
- `scripts/update_ui_overview_state.py`
- `scripts/freeze_arab_frequency_sources.py`
- `scripts/verify_arab_frequency_sources.py`
- `scripts/freeze_arab_study_sources.py`
- `scripts/verify_arab_study_sources.py`
- `scripts/ingest_clinvar_cloud.py`
- `scripts/ingest_gnomad_parquet.py`
- `scripts/ingest_gme_cloud.py`
- `scripts/manifest_utility.py`
- `scripts/verify_gcp.py`

## Legacy Paths Removed
- old BigQuery supervisor-registry build scripts
- raw-layer BigQuery load scripts
- raw-layer GE/Data Docs scripts and checked-in expectation artifacts
- old UI helper modules that depended on live BigQuery or legacy registry SQL
- obsolete tests that only covered removed paths
- generated logs and old output folders

## Scientific / Technical Notes
- The active supervisor UI remains static and frozen-data based.
- The build script was corrected so BRCA extraction prefers the frozen raw mirror in GCS instead of a changing provider endpoint.
- A defensive fallback test now covers a stale-manifest case where ClinVar metadata points to an older raw path.

## Verification
- `python3 -m py_compile ui/service.py ui/review_bundle.py ui/source_review.py ui/controlled_access.py ui/overview_data.py ui/traceability.py scripts/build_brca_normalized_artifacts.py scripts/refresh_supervisor_review_bundle.py scripts/update_source_review_state.py scripts/update_controlled_access_state.py scripts/update_ui_overview_state.py scripts/freeze_arab_frequency_sources.py scripts/freeze_arab_study_sources.py scripts/verify_brca_normalized_artifacts.py scripts/verify_arab_frequency_sources.py scripts/verify_arab_study_sources.py scripts/ingest_clinvar_cloud.py scripts/ingest_gnomad_parquet.py scripts/ingest_gme_cloud.py scripts/manifest_utility.py scripts/verify_gcp.py` -> pass
- `node --check ui/app.js` -> pass
- `pytest -q tests` -> `52 passed`
- `pytest -q tests/test_build_brca_normalized_artifacts.py tests/test_refresh_supervisor_review_bundle.py tests/test_verify_brca_normalized_artifacts.py` -> `13 passed`
- local UI smoke test on `http://127.0.0.1:8080` -> pass

## Remaining Caveat
- A full authenticated rerun of `refresh_supervisor_review_bundle.py` / `verify_brca_normalized_artifacts.py` hit transient DNS resolution failures against `oauth2.googleapis.com` in this environment, so the current pass relied on local frozen artifacts plus tests and UI smoke checks.
