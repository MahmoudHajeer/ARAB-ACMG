# Implementation Plan: T002-Data Ingestion & QC

## Phase 1: Data Contracts & Storage Layout
- [ ] 1.1 Define a canonical variant key for raw tables (chr/pos/ref/alt/build/source).
- [ ] 1.2 Define BigQuery raw-layer schemas (ClinVar, gnomAD, each Arab-frequency source).
- [ ] 1.3 Define GCS folder layout and a provenance manifest format (version, access date, checksum).

## Phase 2: ClinVar Ingestion (Classification Layer)
- [ ] 2.1 Select ClinVar export format (VCF vs XML) and record version/access date.
- [ ] 2.2 Download/upload raw ClinVar snapshot to GCS with checksum.
- [ ] 2.3 Parse BRCA1/2 variants into `raw_clinvar` BigQuery table.
- [ ] 2.4 QC: star review distribution, conflict flags, counts by gene, missingness report.

## Phase 3: gnomAD Ingestion (Global + Ancestry Frequencies)
- [ ] 3.1 Freeze gnomAD version and dataset choice (v4 exomes, genomes, or combined).
- [ ] 3.2 Extract BRCA1/2 variants with AF/AN/hom counts/coverage metrics.
- [ ] 3.3 Load to BigQuery `raw_gnomad` table(s).
- [ ] 3.4 QC: AF ranges, AN distributions, missingness, duplicates on variant key.

## Phase 4: Arab / Middle Eastern Frequency Sources
- [ ] 4.1 Enumerate accessible sources and document licenses (GME, Qatar, gnomAD ME subset).
- [ ] 4.2 Ingest each source into `raw_<source>` BigQuery tables with provenance captured.
- [ ] 4.3 QC: sample size/AN coverage, missingness, duplicates, schema validation.

## Phase 5: QC Gate + Deliverables
- [ ] 5.1 Implement automated data validation checks (schema + null rates + duplicates).
- [ ] 5.2 Publish a data dictionary and a provenance table (versions, dates, checksums).
- [ ] 5.3 Freeze a "raw snapshot" tag for reproducible downstream harmonization (T003).

---
**Track Status**: `[ ]`
**Checkpoint SHA**: `[checkpoint: TBA]`
