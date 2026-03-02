# Product Definition: ARAB-ACMG Research

## Vision
To investigate and quantify the misclassification of genetic variants in Arab populations using standard ACMG (American College of Medical Genetics and Genomics) rules, specifically focusing on **HBOC (BRCA1/BRCA2)**, and to propose data-driven improvements or population-specific considerations.

## Core Problem
ACMG rules were largely developed using data from Western/European populations. Applying these rules to Arab populations without considering regional allele frequencies and genetic diversity leads to high rates of "Variants of Uncertain Significance" (VUS) or misclassification of benign variants as pathogenic (and vice versa). denominator distortions in ancestry-dependent frequency data can significantly impact PM2 and BS1 criteria.

## Objectives
1. **Data Collection & Harmonization**: 
    - Genes: **BRCA1, BRCA2**.
    - Genome Build: **GRCh38 only**.
    - Variant Types: **SNVs and small indels**.
    - Sources: ClinVar (Classification), gnomAD (Global Frequency), and Arab-enriched frequency model (GME Variome, Qatar Genome Program, gnomAD Middle Eastern).
2. **ACMG Classification**: Apply standard ACMG rules (PM2, BS1, etc.) to the collected variants using both global and Arab-enriched frequency models.
3. **Misclassification Analysis**: Quantify the % of variants that shift classification and the direction/severity of the shift.
4. **Impact Assessment**: Evaluate how sensitive ACMG frequency rules are to ancestry-dependent denominator distortions.
5. **Recommendations**: Develop a framework for applying ACMG rules more accurately in the Arab context, specifically defining PM2/BS1 operational thresholds.

## Target Audience
- Geneticists and clinicians working with Arab patients.
- Researchers in population genetics.
- Diagnostic laboratories in the MENA region.

## Key Features / Deliverables
- **Master Dataset**: A single harmonized table (Variant, ClinVar class, gnomAD global AF, gnomAD ME AF, GME AF, Qatar AF, etc.).
- **Comparative Analysis Report**: Analysis of classification shifts (Scenario A: Global vs Scenario B: Arab-enriched).
- **Methodological Framework**: Pre-defined thresholds for PM2/BS1 and guidelines for Arab data.
