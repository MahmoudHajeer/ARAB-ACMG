# ARAB-ACMG Research

## Project Overview
This research project investigates the misclassification of genetic variants in Arab populations (specifically BRCA1/BRCA2) using standard ACMG rules. We aim to quantify the shift in classification when using Arab-enriched frequency models versus global models and propose ancestry-aware adjustments to the ACMG criteria.

## Current Architecture
- **Primary system of record**: GCS frozen artifacts and manifests
- **Historical raw archive only**: BigQuery raw mirrors, kept for audit/reference
- **Active build path**: `scripts/build_brca_normalized_artifacts.py`
- **Static review surface**: FastAPI UI serving frozen JSON and GCS downloads

## Active Workflow
1. Freeze or refresh raw/public source packages into GCS.
2. Build BRCA-focused normalized Parquet and checkpoint artifacts from frozen inputs.
3. Refresh the static supervisor review bundle.
4. Verify the frozen outputs locally before any deploy.

## Project Structure
- `conductor/`: Project management, tracks, and specifications.
- `scripts/`: Active automation scripts. See `scripts/README.md` for the supported entry points.
- `ui/`: Static supervisor review service and frozen review payloads.
- `tests/`: Unit and integration tests for the active low-cost pipeline.
- `src/`: Shared Python package space for future analysis engine code.

## Setup
### Local Environment (Conda)
```bash
conda env create -f environment.yml
conda activate arab-acmg
```

## Local Validation
```bash
python3 scripts/build_brca_normalized_artifacts.py
python3 scripts/verify_brca_normalized_artifacts.py
python3 -m pytest -q tests
uvicorn ui.service:app --host 0.0.0.0 --port 8080
```

## Methodology
The project follows the **Conductor Framework**. All implementation details and roadmaps can be found in the `conductor/` directory.

---
*Principal Investigator: MahmoudHajeer*
