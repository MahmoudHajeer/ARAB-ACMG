# Specification: T001-Infrastructure & GCP Setup

## Goal
Establish a robust, reproducible development and analysis environment, both locally and on Google Cloud Platform (GCP).

## Requirements

### Local Environment
- Python 3.10+ environment using Conda or Poetry.
- Essential bioinformatics CLI tools installed (bcftools, vt).
- Git configuration for the project.

### Cloud Infrastructure (GCP)
- **Project Setup**: Enable necessary APIs (Cloud Storage, BigQuery, Vertex AI, Batch).
- **Storage**: Create a GCS bucket for variant data (e.g., `gs://arab-acmg-data`).
- **Data Warehouse**: Initialize BigQuery datasets for raw and harmonized variants.
- **Compute**: Configure Vertex AI Notebooks or instances for analysis.

### Repository Structure
```
/
├── conductor/        # Management & tracking
├── data/             # Local data samples (gitignored)
├── scripts/          # Automation & processing scripts
├── src/              # Core analysis Python package
├── tests/            # Unit and integration tests
├── environment.yml   # Conda environment definition
└── README.md         # Project overview
```

## Success Criteria
- [ ] `conda env create` successfully builds the environment.
- [ ] `gcloud` is configured and can access the project.
- [ ] GCS bucket and BigQuery dataset are provisioned.
- [ ] A "hello world" script can read/write to GCS from the local environment.
