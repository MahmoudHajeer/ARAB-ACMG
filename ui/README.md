# Supervisor UI

## What it shows
- Live Conductor track statuses and progress from `/api/overview` (not from a baked snapshot)
- Separate workflow pages for `Overview`, `Raw Sources`, `Harmonization`, `Pre-GME Review`, and `Final Registry`
- Page-level lazy loading so raw/checkpoint BigQuery calls only run when the corresponding workflow page is opened
- Live 10-row random samples for raw BigQuery tables and the two BRCA checkpoint tables only
- Full CSV downloads for every live preview surface: raw tables, checkpoint tables, and query-only step evidence
- A dedicated pre-GME review checkpoint with full Excel export modeled on `example.xlsx` style: metadata block first, then review header, then full dataset rows
- Step-by-step evidence queries for the BRCA1/BRCA2 supervisor registry build
- The exact SQL used to build both `supervisor_variant_registry_brca_pre_gme_v1` and `supervisor_variant_registry_brca_v1`
- Short scientific notes explaining why the BRCA windows are authoritative across ClinVar, gnomAD genomes, gnomAD exomes, and GME
- Required publication-facing columns are shown first; any extra pipeline columns are explicitly marked as extras

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

## Legacy static snapshot artifact
Run:

```bash
python3 scripts/update_status_snapshot.py
```

This updates:

```text
ui/status_snapshot.json
```

The dashboard no longer depends on this file for the overview page. It is kept only as a legacy artifact for offline inspection and older handoff compatibility.

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

## Make datasets public
Run:

```bash
python3 scripts/set_bigquery_datasets_public.py
python3 scripts/verify_supervisor_registry.py
```
