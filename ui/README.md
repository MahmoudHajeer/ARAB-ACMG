# Supervisor UI

## What it shows
- Conductor track statuses and progress from `ui/status_snapshot.json`
- Separate workflow pages for `Overview`, `Raw Sources`, `Harmonization`, `Pre-GME Review`, and `Final Registry`
- Live 10-row random samples for raw BigQuery tables and the two BRCA checkpoint tables only
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

## Refresh the static snapshot inputs
Run:

```bash
python3 scripts/update_status_snapshot.py
```

This updates:

```text
ui/status_snapshot.json
```

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
