# Supervisor UI

## What it shows
- Conductor track statuses and progress from `ui/status_snapshot.json`
- Raw table row counts and GCS raw-prefix presence from the frozen snapshot
- Live BigQuery dataset ACL state for the three project datasets
- Live 50-row random samples for every raw dataset
- Step-by-step evidence queries for the first supervisor registry table
- The exact SQL used to build `supervisor_variant_registry_v1`

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
```

## Make datasets public
Run:

```bash
python3 scripts/set_bigquery_datasets_public.py
python3 scripts/verify_supervisor_registry.py
```
