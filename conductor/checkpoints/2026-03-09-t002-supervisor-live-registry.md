# Checkpoint: T002 Supervisor Live Registry

## Scope
Closed task `5.4` by turning the supervisor dashboard into a live BigQuery explorer and by materializing the first joined supervisor registry table.

## What Changed
- Added a FastAPI backend under `ui/` so dataset sample requests hit BigQuery live.
- Added fixed per-column explanations for every raw dataset.
- Built `genome-services-platform.arab_acmg_results.supervisor_variant_registry_v1`.
- Added stepwise registry evidence queries so the supervisor can inspect scope selection, allele splitting, frequency parsing, and the final join.
- Shared `arab_acmg_raw`, `arab_acmg_harmonized`, and `arab_acmg_results` to `allAuthenticatedUsers` at the dataset ACL layer.

## GME Assessment
- Workspace file: `/Users/macbookpro/Desktop/storage/raw/gme/hg38_gme.txt.gz`
- Observed rows: `699,496`
- Observed chromosome coverage: `1-22` and `X`
- Not observed in the local file: `Y`, `MT`
- Practical interpretation for the project: this is a usable Arab/Middle Eastern summary-frequency source, but not a native full-fidelity GRCh38 raw VCF stream.

## Registry Table Notes
- Table: `genome-services-platform.arab_acmg_results.supervisor_variant_registry_v1`
- Row count after build: `58,769,720`
- Join key: `chrom:pos:ref:alt`
- Scope: chromosomes `13` and `17`
- `AC_eur` / `AF_eur` are not exposed directly by the loaded raw gnomAD v4.1 site tables, so the registry stores documented Europe proxies from `NFE + FIN + ASJ`.
- `grpmax_faf95` is populated from the raw `faf95` tag while `grpmax` keeps the corresponding population label.
- `Depth` stays null for now because the loaded raw site files do not expose `Depth` or `DP`.

## Verification
- `python3 -m pytest -q tests` -> `20 passed`
- `python3 scripts/verify_bq_health.py` -> pass
- `python3 scripts/build_supervisor_registry.py` -> pass
- `python3 scripts/set_bigquery_datasets_public.py` -> pass
- `python3 scripts/verify_supervisor_registry.py` -> pass
- `node --check ui/app.js` -> pass
- Playwright local browser validation -> pass
