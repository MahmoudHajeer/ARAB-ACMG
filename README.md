# ARAB-ACMG Research

## Project Overview
This research project investigates the misclassification of genetic variants in Arab populations (specifically BRCA1/BRCA2) using standard ACMG rules. We aim to quantify the shift in classification when using Arab-enriched frequency models versus global models and propose ancestry-aware adjustments to the ACMG criteria.

## Tech Stack
- **Language**: Python 3.14+
- **Cloud**: Google Cloud Platform (GCS, BigQuery, Vertex AI)
- **Bioinformatics**: bcftools, vt, Ensembl VEP, cyvcf2, pysam
- **Data Analysis**: Pandas, NumPy, Dask, SciPy, Statsmodels
- **Visualization**: Matplotlib, Seaborn, Plotly

## Project Structure
- `conductor/`: Project management, tracks, and specifications.
- `src/`: Core Python package for variant analysis logic.
- `tests/`: Unit and integration tests.
- `scripts/`: Automation scripts for data ingestion and processing.
- `data/`: Local storage for data samples (gitignored).

## Setup
### Local Environment (Conda)
```bash
conda env create -f environment.yml
conda activate arab-acmg
```

## Methodology
The project follows the **Conductor Framework**. All implementation details and roadmaps can be found in the `conductor/` directory.

---
*Principal Investigator: MahmoudHajeer*
