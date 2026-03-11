# Track Journal: T003-Data Harmonization & Normalization

Append-only handoff log for cross-agent continuity.
Do not rewrite previous entries.

## Handoff Log

### Entry 1
- timestamp: `2026-03-02T21:56:34+03:00`
- agent: `Codex`
- task: `meta`
- status: `Completed`
- summary: Initialized T003 Conductor files (liftover + normalization + GE/dbt gates) for cloud-first harmonization.
- files: `conductor/tracks/T003-DataHarmonization/spec.md`, `conductor/tracks/T003-DataHarmonization/plan.md`, `conductor/tracks/T003-DataHarmonization/index.md`, `conductor/data-contracts.md`
- verification: `documentation only (no scripts run)`
- next_action: Start task `1.1` in `conductor/tracks/T003-DataHarmonization/plan.md` after T002 raw layer is complete.

### Entry 2
- Timestamp: `2026-03-10T21:52:06+03:00`
- Agent: `Codex`
- Task ID: `1.1`
- Status: `Started`
- Summary: Starting BRCA1/BRCA2-focused harmonization now that the raw layer is frozen. The immediate goal is to confirm the canonical harmonized schema and `variant_key`, then rebuild the supervisor-facing integration table so it reads from BRCA-only harmonized outputs instead of raw tables.
- Files changed: `conductor/tracks.md`, `conductor/tracks/T003-DataHarmonization/plan.md`, `conductor/tracks/T003-DataHarmonization/index.md`, `conductor/tracks/T002-DataCollection/index.md`
- Verification run + result: `state update only before implementation`
- Next exact action: Inspect BRCA1/BRCA2 extraction rules per source, then implement harmonized source tables and a simplified evidence-first supervisor UI.

### Entry 3
- Timestamp: `2026-03-10T22:33:49+03:00`
- Agent: `Codex`
- Task ID: `1.1`
- Status: `Completed`
- Summary: Confirmed the BRCA-focused harmonized schema and canonical `variant_key` through a seed-backed Ensembl gene-window reference, then materialized BRCA-only harmonized tables plus a supervisor registry rebuilt exclusively from `arab_acmg_harmonized`. The supervisor UI now exposes live source URLs, live coordinate-vs-label audit evidence, and 10-row random samples from the harmonized tables and final registry.
- Files changed: `arab_acmg_dbt/dbt_project.yml`, `arab_acmg_dbt/models/harmonized/brca/*`, `arab_acmg_dbt/seeds/brca_gene_windows_seed.csv`, `scripts/build_supervisor_registry.py`, `scripts/verify_supervisor_registry.py`, `tests/test_registry_queries.py`, `tests/test_ui_catalog.py`, `tests/test_ui_service.py`, `ui/README.md`, `ui/app.js`, `ui/catalog.py`, `ui/index.html`, `ui/registry_queries.py`, `ui/service.py`, `ui/status_snapshot.json`, `ui/styles.css`, `conductor/checkpoints/2026-03-10-t003-brca-harmonization-methodology.md`
- Verification run + result: `python3 -m pytest -q tests (26 passed)`, `python3 -m pytest -q tests/test_registry_queries.py tests/test_ui_catalog.py tests/test_ui_service.py (10 passed)`, `node --check ui/app.js (pass)`, `DBT_PROFILES_DIR=$PWD/arab_acmg_dbt /tmp/arab_acmg_tools/bin/dbt parse --project-dir arab_acmg_dbt (pass)`, `DBT_PROFILES_DIR=$PWD/arab_acmg_dbt /tmp/arab_acmg_tools/bin/dbt seed --project-dir arab_acmg_dbt --select brca_gene_windows_seed (pass)`, `DBT_PROFILES_DIR=$PWD/arab_acmg_dbt /tmp/arab_acmg_tools/bin/dbt run --project-dir arab_acmg_dbt --select h_brca_gene_windows h_brca_clinvar_variants h_brca_gnomad_genomes_variants h_brca_gnomad_exomes_variants h_brca_gme_variants (pass)`, `DBT_PROFILES_DIR=$PWD/arab_acmg_dbt /tmp/arab_acmg_tools/bin/dbt test --project-dir arab_acmg_dbt --select h_brca_gene_windows h_brca_clinvar_variants h_brca_gnomad_genomes_variants h_brca_gnomad_exomes_variants h_brca_gme_variants (pass: 49 tests)`, `python3 scripts/build_supervisor_registry.py (pass: 115,836 rows)`, `python3 scripts/verify_supervisor_registry.py (pass)`, `python3 scripts/verify_bq_health.py (pass)`, `python3 scripts/verify_gcp.py (pass)`, `python3 scripts/update_status_snapshot.py (pass)`, `Playwright browser check on http://127.0.0.1:8082/ (pass: live harmonized samples, registry sample, and scientific evidence rendered)`
- Next exact action: Start task `1.2` in `conductor/tracks/T003-DataHarmonization/plan.md` and formalize standardized chromosome naming across the harmonized BRCA tables before expanding to additional genes or full-source normalization.

### Entry 4
- Timestamp: `2026-03-11T18:25:00+03:00`
- Agent: `Codex`
- Task ID: `1.2`
- Status: `Started`
- Summary: Re-scoping T003 by explicit user request. The harmonized layer will no longer keep per-source BRCA tables/views as durable outputs; instead it will collapse to checkpoint tables only, each honoring the user-mandated publication-facing column floor, with unsupported fields left `NULL` and optional extras clearly marked.
- Files changed: `conductor/tracks/T003-DataHarmonization/spec.md`, `conductor/tracks/T003-DataHarmonization/plan.md`, `conductor/tracks/T003-DataHarmonization/index.md`
- Verification run + result: `state update only before implementation`
- Next exact action: Build raw-to-checkpoint SQL directly from `arab_acmg_raw`, update the UI/export to distinguish required vs extra columns, then delete obsolete tables/views from `arab_acmg_harmonized`.

### Entry 5
- Timestamp: `2026-03-11T22:35:00+03:00`
- Agent: `Codex`
- Task ID: `1.2, 4.1, 4.2`
- Status: `Completed`
- Summary: Rebuilt the BRCA harmonized layer as two checkpoint tables only (`pre-GME`, `final-with-GME`) with the user-mandated publication-facing header as the minimum schema, and removed every durable per-source BRCA harmonized output. The supervisor UI and Excel export now label required columns versus extras explicitly, and Cloud Run was redeployed after bundling the frozen BRCA gene-window seed into the `ui/` build context.
- Files changed: `ui/schema_columns.py`, `ui/registry_queries.py`, `ui/catalog.py`, `ui/service.py`, `ui/export_workbook.py`, `ui/app.js`, `ui/index.html`, `ui/styles.css`, `ui/README.md`, `ui/brca_gene_windows_seed.csv`, `scripts/build_supervisor_registry.py`, `scripts/verify_supervisor_registry.py`, `tests/test_registry_queries.py`, `tests/test_ui_catalog.py`, `tests/test_ui_service.py`, `tests/test_export_workbook.py`, `arab_acmg_dbt/models/harmonized/brca/*`, `ui/status_snapshot.json`, `conductor/checkpoints/2026-03-11-t003-checkpoint-only-registry.md`
- Verification run + result: `python3 scripts/build_supervisor_registry.py (pass: pre-GME=116,067 rows; final=116,087 rows)`, `python3 scripts/verify_supervisor_registry.py (pass)`, `python3 scripts/export_pre_gme_registry_xlsx.py (pass)`, `python3 -m pytest -q tests (42 passed)`, `python3 -m pytest -q tests/test_registry_queries.py tests/test_ui_catalog.py tests/test_ui_service.py tests/test_export_workbook.py (25 passed)`, `python3 -m py_compile ui/registry_queries.py ui/catalog.py ui/service.py (pass)`, `node --check ui/app.js (pass)`, `python3 scripts/update_status_snapshot.py (pass)`, `Playwright CLI browser check on http://127.0.0.1:8082/ (pass: workflow pages rendered after loading)`, `gcloud run deploy supervisor-ui --source ui --region europe-west1 --project genome-services-platform --allow-unauthenticated --quiet (pass: revision supervisor-ui-00010-qql)`, `curl -s https://supervisor-ui-wrx363kqnq-ew.a.run.app/api/health (pass)`, `curl -s https://supervisor-ui-wrx363kqnq-ew.a.run.app/api/pre-gme (pass)`, `curl -s https://supervisor-ui-wrx363kqnq-ew.a.run.app/api/registry (pass)`, `curl -s 'https://supervisor-ui-wrx363kqnq-ew.a.run.app/api/pre-gme/sample?limit=10' (pass)`, `curl -s 'https://supervisor-ui-wrx363kqnq-ew.a.run.app/api/registry/sample?limit=10' (pass)`
- Next exact action: Start task `1.3` in `conductor/tracks/T003-DataHarmonization/plan.md` and define explicit transformation metadata fields for liftover/normalization status before any non-GRCh38 or normalized-source expansion.
