# ARAB-ACMG Research: Project Walkthrough

This document provides a detailed overview of the ARAB-ACMG research project, its technical architecture, and the automated pipelines implemented to study genetic variant misclassification in Arab populations.

## 1. Project Goal
The primary objective is to quantify how ancestry-dependent denominator distortions in genetic data (specifically BRCA1/BRCA2) affect ACMG classification (e.g., PM2 and BS1 criteria) when applied to Arab cohorts.

## 2. Technical Architecture
The project follows a **Cloud-First, Pipeline-Style** architecture using Google Cloud Platform (GCP).

### Core Components:
- **Language**: Python 3.10+ (utilizing minimalist pipeline patterns).
- **Storage**: Google Cloud Storage (GCS) for raw VCFs and Parquet snapshots.
- **Warehouse**: BigQuery for structured data layers (`raw`, `harmonized`, `results`).
- **Quality Control**: Great Expectations (GE) for data validation and dbt for BigQuery testing.
- **Workflow**: Governed by the **Conductor Framework** (see `conductor/`).

## 3. Data Pipeline Overview

### Phase 1: Infrastructure (T001)
- Provisioned GCS bucket: `mahmoud-arab-acmg-research-data`
- Initialized BigQuery datasets: `arab_acmg_raw`, `arab_acmg_harmonized`, `arab_acmg_results`.
- Established Conda environment and project scaffolding.

### Phase 2: Ingestion & QC (T002)
Automated scripts (`scripts/`) manage the acquisition of three primary data layers:
1.  **ClinVar (Classification)**: `ingest_clinvar_cloud.py` fetches the latest GRCh38 VCF and uploads it to GCS with a provenance manifest.
2.  **gnomAD (Global Frequency)**: `ingest_gnomad_parquet.py` extracts BRCA1/2 subsets from public gnomAD v4.1 data directly into GCS.
3.  **Arab-Enriched Frequency**: `ingest_arab_enriched.py` collects data from GME Variome and other regional sources.

**Loading to BigQuery**:
- `load_clinvar_to_bq.py` and `load_gnomad_to_bq.py` transfer filtered variants into the `arab_acmg_raw` layer.

### Phase 3: Harmonization (T003 - Upcoming)
- Standardization of all variants to GRCh38 coordinates using `bcftools` and `vt`.
- Normalization of variant representations (multiallelic splitting and left-alignment).

## 4. Operational Procedures

### Environment Setup
```bash
conda env create -f environment.yml
conda activate arab-acmg
```

### Running the Ingestion Pipeline
To re-run the full ingestion and load sequence:
```bash
# 1. Verify GCP Access
python scripts/verify_gcp.py

# 2. Ingest Data to GCS
python scripts/ingest_clinvar_cloud.py
python scripts/ingest_gnomad_parquet.py
python scripts/ingest_arab_enriched.py

# 3. Load to BigQuery
python scripts/load_clinvar_to_bq.py
python scripts/load_gnomad_to_bq.py

# 4. Generate Reports
python scripts/generate_inventory_report.py
python scripts/generate_data_dictionary.py
```

## 5. Coding Standards
- **Pipeline Architecture**: Logic is structured as sequential stages within class methods.
- **AI-Signed Comments**: Every major logical block is documented by the AI agent (e.g., `[AI-Agent: Gemini 2.0 Flash]`).
- **Provenance**: Every raw artifact in GCS is accompanied by a `manifest.json` generated via `scripts/manifest_utility.py`.

---
*Last Updated: March 2, 2026*
