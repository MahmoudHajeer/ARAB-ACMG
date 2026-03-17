# Data Collection Phase (Operational Roadmap)

This project is computational. "Data collection" means dataset acquisition, versioning, quality control (QC), and preparation for harmonization.

## Phase 1: Freeze Scope (Do This First)
- Genes: BRCA1, BRCA2
- Preferred genome build: GRCh38
- Variant types: SNVs 
 (exclude SVs unless explicitly modeled)

Why this matters: without a frozen scope, the dataset becomes unbounded and hard to reproduce.

## Phase 2: Primary Data Layers To Collect

### 1) ClinVar (Classification Layer)
Collect:
- Clinical significance (Pathogenic/Likely Pathogenic/VUS/Likely Benign/Benign)
- Review status (stars) and submitter count
- Conflicts and interpretation details
- Stable identifiers (VariationID, RCV/SCV when available)

Recommended ingestion approach:
- Use the official ClinVar bulk downloads (VCF or XML)
- Filter to germline BRCA1/2 variants
- Record version and access date

### 2) gnomAD (Global Frequency Layer)
Collect:
- Global AF (allele frequency)
- AF by ancestry group (including Middle Eastern subset if available)
- AN (allele number) and AC (allele count)
- Homozygote counts (when available)
- Coverage metrics (where available)

Recommended ingestion approach:
- Use GRCh38 sources and freeze the exact gnomAD version (for example v4)
- Record whether exomes, genomes, or combined were used
- Record version and access date

### 3) Arab / Middle Eastern Frequency Layer
Goal: build an Arab-enriched frequency model from summary sources that are accessible and license-permitted.

Candidate sources (depending on access):
- GME Variome (summary frequencies)
- Qatar Genome Program (published summary AF tables when accessible)
- gnomAD Middle Eastern ancestry subset (if present and sample size is sufficient)

Important:
- Always track sample size / AN. Small AN can make AF unstable and must be handled explicitly later.

## Phase 3: Define "High Quality Data" (QC Gate)
Define QC rules operationally and apply them consistently across sources:
- PASS filter (for VCF sources)
- Coverage threshold policy (document the threshold and how it is measured)
- AN must be reported for any AF used in modeling
- Exclude low-complexity / masked regions if a source flags them

QC outputs to keep:
- Row counts by gene and by source
- Missingness report for critical fields (chr/pos/ref/alt, AF, AN)
- Duplicate report under the chosen variant key

## Phase 4: Harmonization (Handled In Conductor Track T003)
You must harmonize:
- Build differences: liftover GRCh37 -> GRCh38 (with explicit failure tracking)
- Variant representation: normalize (split multiallelics, left-align indels, trim common bases)

Recommended tools:
- `bcftools norm`
- `vt normalize`
- CrossMap/LiftOver (if needed)

## Phase 5: Master Dataset (Handled In Conductor Track T004)
Build a single master table with one row per canonical variant (GRCh38 chr/pos/ref/alt), including:
- ClinVar classification + review/conflict metadata
- gnomAD global AF + ancestry AF + AN/AC
- Arab/Middle Eastern AF + AN/AC (per source)

This becomes the working dataset for the ACMG engine.

## Phase 6: Misclassification Model (Tracks T004/T005)
Pre-declare your comparison scenarios:
- Scenario A (Global): apply frequency rules using global AF thresholds
- Scenario B (Arab-enriched): apply frequency rules using Arab/Middle Eastern AF thresholds

Measure:
- Percent of variants that shift classification
- Direction of shift
- Stratification by gene (BRCA1 vs BRCA2) and ClinVar review confidence

## Phase 7: Documentation & Reproducibility
For every source, record:
- Version, access date, and license notes
- Checksums for raw files
- Exact ingestion command/script versions

Also produce:
- Data dictionary for raw and harmonized tables
- Provenance table that links each BigQuery table to its raw GCS snapshot

## Decisions To Freeze Early (Avoid Post-hoc Choices)
- AF threshold(s) for BS1/BA1 and the definition of "absent from controls" for PM2
- Whether to weight by AN and how to handle small AN
- ClinVar inclusion filter (for example: stars >= 2)
- How to handle conflicting ClinVar submissions

## Expected Volume (Rough)
- ClinVar BRCA1/BRCA2 variants: typically thousands (order of 5,000-10,000)
- After QC + harmonization: typically a smaller, manageable subset

## Suggested Timeline (Example)
- Months 1-2: ClinVar ingestion + QC
- Months 2-3: gnomAD ingestion + QC
- Months 3-4: Arab/Middle Eastern layer compilation
- Months 4-5: Harmonization + normalization
- Months 5-6: Master dataset build + pre-registered scenario definitions

## Mapping To Conductor Tracks
- T002 (Data Ingestion & QC): acquire sources, store raw snapshots, initial QC + schemas
- T003 (Harmonization & Normalization): liftover + normalization + canonical variant key
- T004 (Master Dataset & Analysis Engine): join master table + implement frequency-rule evaluation
- T005 (Stats & Results): quantify shifts, sensitivity analyses, and produce figures/tables
