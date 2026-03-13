# Checkpoint: SHGP Freeze and AVDB GRCh37-to-GRCh38 LiftOver

- Timestamp: `2026-03-13T17:20:00+03:00`
- Track: `T003-DataHarmonization`
- Task: `2.2` and `2.3`
- Agent: `Codex`

## What Was Finalized
- `SHGP Saudi allele-frequency table` was frozen into the raw vault with provenance and checksum evidence.
- `AVDB Emirati workbook` was frozen into the raw vault and then passed through a real `GRCh37 -> GRCh38` coordinate-conversion checkpoint.
- The supervisor UI now explains, for every active source:
  - build and coordinate status
  - current project use (`Adopted 100%`, `Supporting`, `Reference only`, `Demo only`, `Blocked`)
  - why that decision was made
  - frozen provenance artifacts
  - `AVDB` liftover method and result counts

## SHGP Outcome
- Raw source: `Saudi_Arabian_Allele_Frequencies.txt`
- Official source: Figshare article `A list of Saudi Arabian variants and their allele frequencies`
- DOI: `10.6084/m9.figshare.28059686.v1`
- Published date captured from provider metadata: `2024-12-19T05:46:10Z`
- Final row count recorded in the frozen manifest: `25,488,989`
- Scientific use decision: `Adopted 100%`
- Why: genome-wide, GRCh38, `CHROM/POS/REF/ALT + AC/AN`, and overlaps the BRCA1/BRCA2 windows with `1,607` rows

## AVDB LiftOver Outcome
- Raw source: `avdb_uae.xlsx`
- Official source page: `https://avdb-arabgenome.ae/downloads`
- Workbook-created timestamp retained from workbook properties: `2025-06-27T10:05:29`
- Source coordinate field used: `HGVS_Genomic_GRCh37`
- Why liftover was required:
  - the project canonical build is `GRCh38`
  - direct joins against `ClinVar`, `gnomAD`, `SHGP`, and `GME` would be unsafe if `AVDB` remained on `GRCh37`

## AVDB Method
1. Parse `HGVS_Genomic_GRCh37` into RefSeq accession, interval, and event type.
2. Map the RefSeq accession to chromosome using the official NCBI `GRCh37` assembly report.
3. Lift the genomic interval to `GRCh38` with the official Ensembl `GRCh37 -> GRCh38` assembly map endpoint.
4. Preserve both the original `GRCh37` interval and the mapped `GRCh38` interval.
5. Record failures explicitly instead of dropping them.

## AVDB Result Counts
- Total rows: `801`
- Parsed rows: `799`
- Parse / liftover non-success rows: `2`
- Liftover success rows: `799`
- BRCA rows in current workbook release: `0`
- Scientific use decision: `Reference only`

Interpretation:
- The coordinate conversion itself is valid and frozen.
- The current workbook is still too small and too indirect for the main BRCA frequency layer.
- Therefore the lifted checkpoint is retained for audit and Emirati context, not as a primary BRCA input.

## Frozen Artifacts
- SHGP raw freeze:
  - `gs://mahmoud-arab-acmg-research-data/raw/sources/shgp_saudi_af/version=figshare-28059686-v1/snapshot_date=2026-03-13/`
- AVDB raw freeze:
  - `gs://mahmoud-arab-acmg-research-data/raw/sources/avdb_uae/version=workbook-created-2025-06-27/build=GRCh37/snapshot_date=2026-03-13/`
- AVDB liftover checkpoint:
  - `gs://mahmoud-arab-acmg-research-data/frozen/harmonized/source=avdb_uae/version=workbook-created-2025-06-27/stage=liftover/build=GRCh37_to_GRCh38/snapshot_date=2026-03-13/`

## Supervisor-Facing Guarantees
- The harmonization page remains static at runtime and does not query BigQuery.
- The UI now shows the exact scientific reason each source is:
  - fully adopted
  - supporting only
  - reference only
  - demo only
  - blocked
- `AVDB` now has an auditable liftover explanation in the UI, including official mapping references and failure examples.

## Next Exact Action
- Start `T003 / 3.1`: normalize the ready GRCh38-capable sources (`ClinVar`, `gnomAD`, `SHGP`, `GME`) and decide whether the `UAE PMC12011969` subset is robust enough to enter the same normalization pass.
