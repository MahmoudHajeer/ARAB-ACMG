# Product Guidelines: ARAB-ACMG Research

## Research Standards

### Reproducibility
- All data analysis pipelines must be documented and repeatable.
- Code should be written in a modular and clear manner.
- Data sources and versions (e.g., gnomAD version, access date) must be explicitly recorded.
- Every threshold and parameter must be pre-defined and documented.

### Data Integrity & Quality Control (QC)
- Maintain raw data in a read-only state.
- All transformations and cleaning steps must be scripted.
- **Variant Inclusion Criteria**: 
    - Must have PASS filter in VCF.
    - Minimum coverage (e.g., ≥ 20x).
    - Allele Number (AN) must be reported.
    - Exclude variants with low-complexity masking flags.

### Data Harmonization
- Genome build: **All data must be lifted over to GRCh38.**
- Variant normalization: Use standard tools (e.g., `bcftools norm`).
- Representation: Split multiallelic variants into biallelic; standardize indel left alignment.
- Annotation: Use consistent tools (e.g., Ensembl VEP).

### Ethical Considerations
- Ensure all datasets are properly de-identified.
- Adhere to international and regional regulations regarding genetic data privacy.
- Acknowledge all data sources correctly.

## Technical Guidelines

### Code Quality
- Use Python for data processing and analysis.
- Follow PEP 8 style guidelines.
- Add docstrings to all functions.
- Write unit tests for data transformation logic.

### Documentation
- Use Markdown for all documentation.
- Maintain a detailed README.md at the root.
- Document every track's purpose and outcome in `conductor/tracks.md`.

## User Experience (UX) for Research Deliverables
- Tables and plots should be clear, high-resolution, and self-explanatory.
- Provide a clear data dictionary for the master dataset.
- Any software tools should be simple to install and run.

## Communication
- Use Conventional Commits.
- Use Git notes for task summaries.
