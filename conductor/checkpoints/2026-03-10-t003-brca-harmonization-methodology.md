# Checkpoint: T003 BRCA Harmonization Methodology

- Timestamp: `2026-03-10T22:20:00+03:00`
- Agent: `Codex`
- Scope: BRCA1/BRCA2-only harmonization and supervisor integration

## Source of Truth

- BRCA1 target window:
  - Ensembl gene: `ENSG00000012048`
  - GRCh38 window: `chr17:43044295-43170245`
  - Frozen artifact: `arab_acmg_dbt/seeds/brca_gene_windows_seed.csv`
  - Source URL: `https://www.ensembl.org/Homo_sapiens/Gene/Summary?g=ENSG00000012048`
  - Access date: `2026-03-10`
- BRCA2 target window:
  - Ensembl gene: `ENSG00000139618`
  - GRCh38 window: `chr13:32315086-32400268`
  - Frozen artifact: `arab_acmg_dbt/seeds/brca_gene_windows_seed.csv`
  - Source URL: `https://www.ensembl.org/Homo_sapiens/Gene/Summary?g=ENSG00000139618`
  - Access date: `2026-03-10`

## Extraction Rules

- ClinVar:
  - Primary rule: coordinate overlap with the frozen BRCA window.
  - Audit signal: `GENEINFO` agreement with the target gene.
  - Reason: `GENEINFO` is useful evidence but cannot be the cross-source source of truth because some BRCA-labeled ClinVar rows sit outside the strict Ensembl windows in the frozen snapshot.
- gnomAD genomes and exomes:
  - Primary rule: coordinate overlap with the same frozen BRCA window.
  - Scientific note: genomes and exomes share the same GRCh38 coordinates. Differences come from cohort design and coverage, not coordinate semantics.
- GME hg38 summary:
  - Primary rule: coordinate overlap on the provided hg38 `start` coordinate.
  - Scientific note: this source contributes summary-frequency evidence and does not provide gene labels comparable to ClinVar.

## Harmonized Outputs

- `genome-services-platform.arab_acmg_harmonized.h_brca_gene_windows`
- `genome-services-platform.arab_acmg_harmonized.h_brca_clinvar_variants`
- `genome-services-platform.arab_acmg_harmonized.h_brca_gnomad_genomes_variants`
- `genome-services-platform.arab_acmg_harmonized.h_brca_gnomad_exomes_variants`
- `genome-services-platform.arab_acmg_harmonized.h_brca_gme_variants`
- `genome-services-platform.arab_acmg_harmonized.supervisor_variant_registry_brca_v1`

## Validation

- dbt harmonized model tests: canonical keys, enum fields, positive positions, AF range where applicable.
- UI scientific-method panel: live row counts, live ClinVar gene-label mismatch audit, and source URLs for the frozen BRCA window definitions.

## Live Evidence Snapshot

- Harmonized row counts on `2026-03-10`:
  - `h_brca_clinvar_variants`: `36,046`
  - `h_brca_gnomad_genomes_variants`: `54,331`
  - `h_brca_gnomad_exomes_variants`: `45,028`
  - `h_brca_gme_variants`: `218`
  - `supervisor_variant_registry_brca_v1`: `115,836`
- ClinVar coordinate-vs-label audit on `2026-03-10`:
  - Inside frozen BRCA1 window: `15,018` harmonized rows; BRCA1-labeled rows outside the strict Ensembl window in staging: `16`
  - Inside frozen BRCA2 window: `21,028` harmonized rows; BRCA2-labeled rows outside the strict Ensembl window in staging: `3`
