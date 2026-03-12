# Tracks Registry: ARAB-ACMG Research

## Tracks

| Track ID | Name | Description | Status | Folder |
| :--- | :--- | :--- | :--- | :--- |
| `T001` | **Infrastructure & GCP Setup** | Local environment configuration, GCP resource provisioning (GCS, BigQuery), and Docker/Conda scaffolding. | `[x]` | `conductor/tracks/T001-Infrastructure/` |
| `T002` | **Data Ingestion & QC** | Automated collection of ClinVar, gnomAD, and Arab-specific datasets to GCS. Perform initial QC and data-type validation. | `[x]` | `conductor/tracks/T002-DataCollection/` |
| `T003` | **Data Harmonization & Normalization** | Building GCS-hosted Parquet harmonization pipelines (liftover, normalization, provenance) and canonical variant snapshots. | `[~]` | `conductor/tracks/T003-DataHarmonization/` |
| `T004` | **Master Dataset & Analysis Engine** | Building the harmonized master dataset from frozen Parquet artifacts and implementing the ACMG rule-based engine (PM2, BS1, etc.) without managed-query dependencies. | `[ ]` | `conductor/tracks/T004-AnalysisEngine/` |
| `T005` | **Statistical Evaluation & Results** | Performing statistical analysis of classification shifts from frozen artifacts and generating publication-grade figures/tables with low-cost local or GCS-backed tooling. | `[ ]` | `conductor/tracks/T005-StatsResults/` |

---
**AI Agent Note:** Tracks are to be executed in sequential order.
