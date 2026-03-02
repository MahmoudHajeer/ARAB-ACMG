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

### Code Quality & Style (The "Pipeline" Protocol)
- **Minimalist Pipeline Architecture**: Code must be written as a series of discrete, sequential stages. Data transformations should be linear and obvious, prioritizing readability over complex abstractions.
- **AI-Signed Documentation**: Every major logic step or function must be preceded by a descriptive comment block. These comments must follow the format: `[AI-Agent: Gemini 2.0 Flash]: <Explanation>`.
- **"What & Why" Comments**: Comments must explain both the action (what) and the intended effect on the data (why/result).
- **Modern Python 3.14+**: Always utilize the latest Python features (e.g., improved type hinting, deferred evaluations). When using 3.14+ specific features, explicitly document their performance or architectural benefits.

### Example Signature
```python
# [AI-Agent: Gemini 2.0 Flash]: Stage 1 - Filter variants by BRCA1/2 coordinates.
# Effect: Reduces dataset size from millions to a few thousand targeted variants.
```

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
