# Tracks Registry: ARAB-ACMG Research

## Tracks

| Track ID | Name | Description | Status | Folder |
| :--- | :--- | :--- | :--- | :--- |
| `T001` | **Infrastructure & GCP Setup** | Local environment configuration, GCP resource provisioning (GCS, BigQuery), and Docker/Conda scaffolding. | `[x]` | `conductor/tracks/T001-Infrastructure/` |
| `T002` | **Data Ingestion & QC** | Automated collection of ClinVar, gnomAD, and Arab-specific datasets to GCS. Perform initial QC and data-type validation. | `[x]` | `conductor/tracks/T002-DataCollection/` |
| `T003` | **Data Harmonization & Normalization** | Implementing pipelines (bcftools, vt) for variant normalization and lifting over all data to GRCh38. | `[ ]` | `conductor/tracks/T003-DataHarmonization/` |
| `T004` | **Master Dataset & Analysis Engine** | Building the harmonized Master Dataset in BigQuery and implementing the ACMG rule-based engine (PM2, BS1, etc.). | `[ ]` | `conductor/tracks/T004-AnalysisEngine/` |
| `T005` | **Statistical Evaluation & Results** | Performing statistical analysis of classification shifts and generating high-quality research visualizations in Vertex AI. | `[ ]` | `conductor/tracks/T005-StatsResults/` |

---
**AI Agent Note:** Tracks are to be executed in sequential order.
