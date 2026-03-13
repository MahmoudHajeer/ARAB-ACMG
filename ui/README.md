# Supervisor UI

## What it shows
- Live Conductor track statuses and progress from `/api/overview`
- Separate workflow pages for `Overview`, `Raw Sources`, `Normalization`, `Pre-GME Review`, `Final Registry`, and `Controlled Access`
- Frozen 10-row evidence samples for raw source packages, normalized per-source artifacts, checkpoint tables, and workflow steps
- A dedicated Arab-aware pre-GME review checkpoint and a final Arab checkpoint kept as frozen review artifacts
- Step-by-step frozen evidence for the BRCA1/BRCA2 normalization workflow
- Build-logic notes explaining how the checkpoints are assembled from normalized source artifacts
- Short scientific notes explaining why the BRCA windows are authoritative across ClinVar, gnomAD genomes, gnomAD exomes, SHGP, and GME
- Required publication-facing columns are shown first; any extra pipeline columns are explicitly marked as extras
- One static final-registry CSV download sourced from Cloud Storage

## Runtime cost policy
- The deployed UI no longer queries BigQuery at runtime for samples, counts, or workflow evidence.
- BigQuery is reserved for the historical raw source-of-truth mirrors only.
- The active T003 workflow is GCS-first and exports normalized Parquet/checkpoint artifacts to GCS, then freezes the review surface into `ui/review_bundle.json`.

## Local run
From repo root:

```bash
python3 -m pip install -r ui/requirements.txt
uvicorn ui.service:app --host 0.0.0.0 --port 8080
```

Then open:

```text
http://localhost:8080/
```

## Build the current BRCA normalization artifacts and refresh the frozen review bundle
Run:

```bash
python3 scripts/build_brca_normalized_artifacts.py
python3 scripts/verify_brca_normalized_artifacts.py
```

This updates:

```text
ui/review_bundle.json
ui/source_review.json
```

It also exports the frozen normalized source artifacts, the Arab pre-GME checkpoint, the final Arab checkpoint, and the final CSV download to GCS so the review surface stays low-cost.

## Refresh the bundled overview state for Cloud Run
Run:

```bash
python3 scripts/update_ui_overview_state.py
```

This updates:

```text
ui/overview_state.json
```

The deployed Cloud Run service uses this bundled overview file because `gcloud run deploy --source ui` packages the `ui/` directory only. Local runs still prefer the live Conductor files first.

## Verify the frozen review surface
Run:

```bash
python3 scripts/verify_brca_normalized_artifacts.py
```
