# Tech Stack: ARAB-ACMG Research

## Google Cloud Platform (GCP)
- **Cloud Storage (GCS)**: Primary system of record for raw snapshots, manifests, frozen extracts, harmonized Parquet, and final results bundles.
- **BigQuery (legacy raw archive only)**: Existing raw mirrors may remain for audit/reference, but no new downstream workflows should depend on BigQuery.
- **Google Cloud Batch**: Optional execution layer for heavy one-off bioinformatics jobs that cannot be run cheaply elsewhere.
- **Artifact Registry**: To manage Docker images for reproducible analysis environments.

## Bioinformatics Tools (High-Performance)
- **bcftools**: Essential for variant manipulation and VCF filtering.
- **bcftools norm**: Default normalization engine for T003 because it is already available in the workstation/runtime and covers multiallelic splitting, left-alignment, and parsimonious trimming reproducibly.
- **vt**: Optional fallback normalization tool only if a later source exposes a case that `bcftools norm` cannot handle cleanly.
- **Ensembl VEP / Annovar**: For comprehensive variant annotation.
- **cyvcf2 / pysam**: High-performance Python interfaces for reading VCF and BAM/SAM files.
- **Hail**: (Optional) For large-scale genomic data analysis in Python/Spark, ideal for gnomAD-scale data.
- **CrossMap / LiftOver**: For converting genomic coordinates between builds (GRCh37 to GRCh38).

## Data Processing & Analysis (Python 3.14+**)
- **Pandas**: For tabular extraction, sheet-level cleanup, and result packaging.
- **PyArrow / Parquet**: Columnar storage for immutable extracts, harmonized snapshots, and results bundles stored in GCS.
- **DuckDB**: Primary low-cost query engine for harmonization, master-dataset assembly, and statistical marts over Parquet artifacts.
- **openpyxl / xlrd**: Spreadsheet readers for supplementary `.xlsx`/`.xls` source packages that commonly appear in Arab cohort publications.
- **NumPy**: For optimized numerical operations.
- **SciPy / Statsmodels**: For rigorous statistical hypothesis testing on misclassification shifts.

## Statistics & Plotting
- **Matplotlib / Seaborn / Plotly**: For high-quality, interactive research visualizations.

## Testing & Quality
- **pytest**: For unit and integration testing.
- **pytest-cov**: For monitoring code coverage (Goal: >100%).
- **flake8 / black / mypy**: For linting, formatting, and static type checking.
- **Great Expectations**: Data quality gates (expectation suites + checkpoints) for Parquet/CSV snapshots.
- **DuckDB SQL + pytest assertions**: Default schema, uniqueness, and accepted-values checks without managed-query cost.

## Version Control & Environment
- **Git / GitHub**: For code and metadata versioning.
- **Conda / Poetry**: For reproducible local environment management.
- **Docker**: For containerizing analysis environments to ensure cloud-to-local parity.

## Supervisor Runtime Interface
- **FastAPI**: Lightweight backend for the supervisor UI, now serving frozen JSON/GCS artifacts instead of live BigQuery queries.
- **Uvicorn**: ASGI runtime for local testing and Cloud Run deployment of the supervisor service.
- **Cloud Storage static artifacts**: Frozen review bundle JSON, Parquet checkpoints, and the final CSV download are published to GCS so the UI does not consume managed-query quota at runtime.
