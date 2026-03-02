# Specification: T002-Data Ingestion & QC

## Goal
Automate the acquisition of the three primary data layers (ClinVar, gnomAD, Arab-specific frequencies) into Cloud Storage, with initial validation and standardized formatting.

## Scope
- **Genes**: BRCA1, BRCA2.
- **Build**: GRCh38.
- **Data Layers**:
  1. **ClinVar**: Classification data (VCF/XML).
  2. **gnomAD**: Global frequency data (v4).
  3. **Arab-Enriched**: GME Variome, Qatar Genome Program, and gnomAD Middle Eastern subset.

## Requirements

### Ingestion Pipeline
- **Automated Download**: Use Python to fetch bulk data from source FTPs/URLs.
- **Gene Filtering**: Early-stage filtering to restrict data to BRCA1 and BRCA2.
- **Cloud Storage**: Transfer all raw downloads directly to `gs://mahmoud-arab-acmg-research-data/raw/`.

### Quality Control (QC)
- **Integrity Checks**: Verify file sizes and checksums.
- **Format Validation**: Ensure ClinVar/gnomAD data follows the VCF 4.2+ specification.
- **Column Checks**: Confirm presence of critical fields (AF, AN, ClinicalSignificance).

## Success Criteria
- [ ] Raw ClinVar (BRCA1/2) VCF in GCS.
- [ ] Raw gnomAD (BRCA1/2) VCF in GCS.
- [ ] Arab-specific summary tables in GCS.
- [ ] Automated QC report generated for each ingested file.
