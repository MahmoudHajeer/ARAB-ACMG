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

### Code Quality & Style
- **Minimalist Pipeline Style**: Write code in a functional, pipeline-like structure (e.g., method chaining or clear sequential data flows) to make the data transformation steps obvious.
- **Python 3.14+ Features**: Proactively use features from Python 3.14 (and the latest available versions). When a 3.14+ feature is used, it must be explicitly mentioned in a comment along with its specific effect on performance or readability.
- **AI-Signed Descriptive Comments**: Every major logical block or step must have a concise, high-value comment describing **what** it does and **why** (its effect). These comments must be signed by the AI agent (e.g., `# [AI-Agent]: ...`).
- **Follow the Data**: Comments should make it easy to follow how data is being transformed from one state to another.

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
