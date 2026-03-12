# Supervisor UI

## What it shows
- Live Conductor track statuses and progress from `/api/overview`
- Separate workflow pages for `Overview`, `Raw Sources`, `Harmonization`, `Pre-GME Review`, and `Final Registry`
- Frozen 10-row evidence samples for raw tables, checkpoint tables, and workflow steps
- A dedicated pre-GME review checkpoint kept as a frozen review artifact
- Step-by-step frozen evidence for the BRCA1/BRCA2 supervisor registry build
- The exact SQL used to build both `supervisor_variant_registry_brca_pre_gme_v1` and `supervisor_variant_registry_brca_v1`
- Short scientific notes explaining why the BRCA windows are authoritative across ClinVar, gnomAD genomes, gnomAD exomes, and GME
- Required publication-facing columns are shown first; any extra pipeline columns are explicitly marked as extras
- One static final-registry CSV download sourced from Cloud Storage

## Runtime cost policy
- The deployed UI no longer queries BigQuery at runtime for samples, counts, or workflow evidence.
- BigQuery is reserved for the raw source-of-truth tables and one-time freeze/export scripts only.
- Processed checkpoint outputs are exported to GCS and frozen into `ui/review_bundle.json`.

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

## Refresh the frozen review bundle
Run:

```bash
python3 scripts/freeze_supervisor_review_bundle.py
python3 scripts/decommission_supervisor_bigquery_outputs.py
```

This updates:

```text
ui/review_bundle.json
```

It also exports the frozen checkpoint artifacts to GCS and removes non-raw BigQuery outputs so the review surface stays low-cost.

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

## Materialize the registry table
Run:

```bash
python3 scripts/build_supervisor_registry.py
python3 scripts/export_pre_gme_registry_xlsx.py
```

This build path reads directly from `arab_acmg_raw` and leaves only these two durable tables in `arab_acmg_harmonized`:

```text
supervisor_variant_registry_brca_pre_gme_v1
supervisor_variant_registry_brca_v1
```

## Verify the frozen review surface
Run:

```bash
python3 scripts/verify_supervisor_registry.py
```
