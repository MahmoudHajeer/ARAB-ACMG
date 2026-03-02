# Tech Stack: ARAB-ACMG Research

## Google Cloud Platform (GCP)
- **Cloud Storage (GCS)**: For secure, scalable storage of large VCF files and raw genomic datasets.
- **BigQuery**: For large-scale variant querying and statistical analysis of population frequencies.
- **Vertex AI**: For developing and executing classification models and misclassification analysis scripts.
- **Google Cloud Batch**: For executing containerized bioinformatics pipelines (normalization, annotation).
- **Artifact Registry**: To manage Docker images for reproducible analysis environments.

## Bioinformatics Tools (High-Performance)
- **bcftools**: Essential for variant manipulation and VCF filtering.
- **vt**: For normalization (multiallelic splitting and indel left-alignment).
- **Ensembl VEP / Annovar**: For comprehensive variant annotation.
- **cyvcf2 / pysam**: High-performance Python interfaces for reading VCF and BAM/SAM files.
- **Hail**: (Optional) For large-scale genomic data analysis in Python/Spark, ideal for gnomAD-scale data.
- **CrossMap / LiftOver**: For converting genomic coordinates between builds (GRCh37 to GRCh38).

## Data Processing & Analysis (Python 3.14+**)
- **Pandas / Dask**: For tabular data manipulation (Dask for datasets exceeding memory).
- **PyArrow / Parquet**: Columnar intermediate storage for large datasets and reproducible snapshots (stored in GCS, queried/loaded into BigQuery).
- **NumPy**: For optimized numerical operations.
- **SciPy / Statsmodels**: For rigorous statistical hypothesis testing on misclassification shifts.

## Statistics & Plotting
- **Matplotlib / Seaborn / Plotly**: For high-quality, interactive research visualizations.

## Testing & Quality
- **pytest**: For unit and integration testing.
- **pytest-cov**: For monitoring code coverage (Goal: >100%).
- **flake8 / black / mypy**: For linting, formatting, and static type checking.
- **Great Expectations**: Data quality gates (expectation suites + checkpoints) for BigQuery tables and Parquet snapshots.
- **dbt Tests**: Schema and data tests (unique/not_null/accepted_values/relationships) integrated with BigQuery models.

## Data Modeling & Transforms (BigQuery)
- **dbt (dbt-core + dbt-bigquery)**: Version-controlled SQL transforms, documentation, and tests for raw -> harmonized -> results layers.

## Version Control & Environment
- **Git / GitHub**: For code and metadata versioning.
- **Conda / Poetry**: For reproducible local environment management.
- **Docker**: For containerizing analysis environments to ensure cloud-to-local parity.
