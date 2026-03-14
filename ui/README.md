# Supervisor UI

## What it shows
- Live Conductor track statuses and progress from `/api/overview`
- Separate workflow pages for `Overview`, `Raw Sources`, `Normalization`, `Legacy Pre-GME`, `Legacy Final`, `Arab Extension`, `Data Downloads`, and `Controlled Access`
- Frozen 10-row evidence samples for raw source packages, normalized per-source artifacts, checkpoint tables, and workflow steps
- A preserved legacy baseline (`pre-GME` + `final`) that stays separate from the new Arab-extension checkpoints
- A structured download center for every derived artifact shown in the dashboard
- Step-by-step frozen evidence for the BRCA1/BRCA2 normalization workflow
- Build-logic notes explaining how the checkpoints are assembled from normalized source artifacts
- Short scientific notes explaining why the BRCA windows are authoritative across ClinVar, gnomAD genomes, gnomAD exomes, SHGP, and GME
- Required publication-facing columns are shown first; any extra pipeline columns are explicitly marked as extras
- Static GCS downloads for normalized artifacts, the legacy baseline tables, and the Arab-extension tables

## Runtime cost policy
- The deployed UI no longer queries BigQuery at runtime for samples, counts, or workflow evidence.
- BigQuery is reserved for the historical raw source-of-truth mirrors only.
- The active T003 workflow is GCS-first and exports normalized Parquet/checkpoint artifacts to GCS, then composes the review surface into `ui/review_bundle.json`.

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

The build now also runs `scripts/refresh_supervisor_review_bundle.py`, which:
- reattaches the historical legacy bundle as the baseline review surface
- publishes CSV downloads for every derived artifact shown in the UI
- keeps the Arab extension in a separate page instead of overwriting the legacy final table

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
