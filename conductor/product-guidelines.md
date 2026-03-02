# Product Guidelines: ARAB-ACMG Research

## Research Standards

### Reproducibility
- All data analysis pipelines must be documented and repeatable.
- Code should be written in a modular and clear manner.
- Data sources and versions must be explicitly stated.

### Data Integrity
- Maintain raw data in a read-only state.
- All transformations and cleaning steps must be scripted (no manual Excel edits).
- Use version control for all code and metadata.

### Ethical Considerations
- Ensure all datasets are properly de-identified (if applicable).
- Adhere to international and regional regulations regarding genetic data privacy.
- Acknowledge all data sources correctly.

## Technical Guidelines

### Code Quality
- Use Python for data processing and analysis.
- Follow PEP 8 style guidelines.
- Add docstrings to all major functions.
- Write unit tests for data transformation logic.

### Documentation
- Use Markdown for all documentation.
- Maintain a detailed README.md at the root.
- Document every track's purpose and outcome in `conductor/tracks.md`.

## User Experience (UX) for Research Deliverables
- Tables and plots should be clear, high-resolution, and self-explanatory.
- Any software tools should be simple to install and run (e.g., using `pip` or `conda`).
- Provide clear error messages for data formatting issues.

## Communication
- Use Git commit messages that follow the Conventional Commits specification.
- Use Git notes to summarize major task completions.
