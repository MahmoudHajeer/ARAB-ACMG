# T003 BRCA Normalization Pass

## Scope
Closed T003 Phase 3 for the ready GRCh38-capable sources:
- ClinVar GRCh38 VCF
- gnomAD v4.1 genomes (chr13 + chr17)
- gnomAD v4.1 exomes (chr13 + chr17)
- SHGP Saudi allele-frequency table
- GME hg38 summary table

## What was built
1. Per-source normalized Parquet artifacts in GCS
2. One Arab-aware BRCA pre-GME checkpoint (`ClinVar + gnomAD + SHGP`)
3. One final Arab-aware BRCA checkpoint (`pre-GME + GME`)
4. A normalization report and checkpoint report in GCS
5. A refreshed static supervisor review bundle with raw -> normalized -> checkpoint evidence

## Scientific implementation notes
- Normalization engine: `bcftools norm`
- Reference: GRCh38 `chr13` + `chr17` FASTA built from UCSC hg38 chromosome FASTA files
- Public VCF sources were subsetted only to BRCA windows to keep the run low-cost
- SHGP and GME were converted to minimal VCF with explicit `SRC_*` provenance tags before normalization
- GME deletion rows that used `-` were anchored on the previous GRCh38 base to produce legal VCF alleles
- `GNOMAD_ALL_AF` and `GNOMAD_MID_AF` in the checkpoint are derived from explicit genomes/exomes counts used in this project; they are not copied from a single upstream field
- The final checkpoint intentionally keeps only Arab-relevant GME extras (`GME_AF`, `GME_NWA`, `GME_NEA`, `GME_AP`, `GME_SD`)

## Row counts
- `clinvar_normalized_brca`: `36,253`
- `gnomad_genomes_normalized_brca`: `54,331`
- `gnomad_exomes_normalized_brca`: `45,340`
- `shgp_normalized_brca`: `1,607`
- `gme_normalized_brca`: `218`
- `supervisor_variant_registry_brca_arab_pre_gme_v2`: `116,398`
- `supervisor_variant_registry_brca_arab_v2`: `116,413`

## Key artifact URIs
- Normalization report:
  - `gs://mahmoud-arab-acmg-research-data/frozen/harmonized/normalization_report/snapshot_date=2026-03-14/brca_normalization_report.json`
- Checkpoint report:
  - `gs://mahmoud-arab-acmg-research-data/frozen/harmonized/checkpoint=supervisor_variant_registry_brca_arab_v2/snapshot_date=2026-03-14/supervisor_variant_registry_brca_arab_v2_report.json`
- Pre-GME checkpoint:
  - `gs://mahmoud-arab-acmg-research-data/frozen/harmonized/checkpoint=supervisor_variant_registry_brca_arab_pre_gme_v2/snapshot_date=2026-03-14/supervisor_variant_registry_brca_arab_pre_gme_v2.parquet`
- Final checkpoint:
  - `gs://mahmoud-arab-acmg-research-data/frozen/harmonized/checkpoint=supervisor_variant_registry_brca_arab_v2/snapshot_date=2026-03-14/supervisor_variant_registry_brca_arab_v2.parquet`
- Final CSV download:
  - `https://storage.googleapis.com/mahmoud-arab-acmg-research-data/frozen/results/checkpoint=supervisor_variant_registry_brca_arab_v2/snapshot_date=2026-03-14/supervisor_variant_registry_brca_arab_v2.csv`

## Verification
- `python3 scripts/build_brca_normalized_artifacts.py` -> pass
- `python3 scripts/verify_brca_normalized_artifacts.py` -> pass
- `python3 -m pytest -q tests` -> `75 passed`
- Local UI verification -> pass (`Raw`, `Normalization`, `Pre-GME`, and `Final` pages rendered the frozen artifacts and samples)

## Commit
- Implementation SHA: `3d88305`
