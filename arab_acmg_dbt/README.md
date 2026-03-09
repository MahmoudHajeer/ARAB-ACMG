# ARAB-ACMG dbt Project

This dbt project materializes typed staging views on top of the frozen raw BigQuery tables.

Current T002 scope:
- `stg_clinvar_variants`
- `stg_gnomad_genomes_variants`
- `stg_gnomad_exomes_variants`

Conventions:
- Raw tables stay in `arab_acmg_raw`.
- Staging views materialize into `arab_acmg_harmonized`.
- The current BigQuery datasets live in the `US` location, so dbt uses `DBT_BIGQUERY_LOCATION=US` by default.
- `variant_key` uses `chrom:pos:ref:alt` with any leading `chr` removed.
- gnomAD `AC_eur/AF_eur` are exposed as `*_eur_proxy` because the raw INFO payload stores `nfe`, `fin`, and `asj` separately rather than direct `eur` fields.

Typical local commands:
- `DBT_PROFILES_DIR=$PWD/arab_acmg_dbt /tmp/arab_acmg_tools/bin/dbt parse --project-dir arab_acmg_dbt`
- `DBT_PROFILES_DIR=$PWD/arab_acmg_dbt /tmp/arab_acmg_tools/bin/dbt run --project-dir arab_acmg_dbt --select stg_clinvar_variants stg_gnomad_genomes_variants stg_gnomad_exomes_variants`
- `DBT_PROFILES_DIR=$PWD/arab_acmg_dbt /tmp/arab_acmg_tools/bin/dbt test --project-dir arab_acmg_dbt --select stg_clinvar_variants stg_gnomad_genomes_variants stg_gnomad_exomes_variants`
