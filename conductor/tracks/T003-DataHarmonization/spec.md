# Specification: T003-Data Harmonization & Normalization

## Goal
Standardize all ingested datasets (ClinVar, gnomAD, Arab frequency) into a consistent genomic representation to enable accurate downstream integration and misclassification analysis.

## Scope
- **Genome Build**: Standardize all datasets to **GRCh38**.
- **Normalization**: Ensure all variants are parsimonious and left-aligned.
- **Multiallelic Handling**: Split all multiallelic variants into biallelic records.
- **Target Genes**: BRCA1, BRCA2.

## Requirements

### LiftOver Pipeline
- **Coordinate Conversion**: Use `bcftools` or `CrossMap` to lift over any legacy datasets (e.g., GME or older ClinVar) to GRCh38.
- **Verification**: Confirm that lifted-over variants align with the GRCh38 reference genome.

### Variant Normalization
- **bcftools norm**: Use `bcftools norm -m -any` to split multiallelics.
- **vt normalize**: Use `vt normalize` to left-align indels and ensure parsimony.
- **Ref/Alt Consistency**: Verify that REF alleles match the reference genome at the specified positions.

### Cloud Storage Integration
- **Output Storage**: Save harmonized datasets to `gs://mahmoud-arab-acmg-research-data/harmonized/`.
- **Naming Convention**: Follow a clear suffix pattern (e.g., `*_grch38_norm.vcf.gz`).

## Success Criteria
- [ ] All datasets are in GRCh38 coordinates.
- [ ] No multiallelic records remain in the harmonized files.
- [ ] Indels are consistently left-aligned.
- [ ] Automated harmonization report generated for each dataset.