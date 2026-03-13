# Source Readiness Review

Scientific review of the currently frozen sources before T003 liftover/normalization work.

## Review Scale
- `Ready`: source already exposes genomic coordinates in a directly usable GRCh38-oriented form.
- `Partial`: source has useful genomic signals but still needs row-level parsing/validation before canonical-key construction.
- `Blocked`: source does not yet expose genomic coordinates in a form that can enter liftover/normalization directly.

## Source Matrix

| Source | Role | Source Build | Coordinate Readiness | Liftover Decision | BRCA Relevance | Review Status | Evidence | Next Action |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `clinvar` | Global clinical classification anchor | `GRCh38` | Genomic coordinates already present in VCF (`CHROM/POS/REF/ALT`) | `not_needed` | Direct | `Ready` | Upstream path is `vcf_GRCh38`; raw table preserves VCF coordinates and INFO tags for `ALLELEID`/`CLNSIG`. | Split multiallelic rows and normalize alleles. |
| `gnomad_v4.1_genomes_chr13/chr17` | Global genome frequency baseline | `GRCh38` | Genomic coordinates already present in VCF (`CHROM/POS/REF/ALT`) | `not_needed` | Direct | `Ready` | gnomAD v4.1 raw snapshots are GRCh38 VCF files for BRCA chromosomes. | Parse INFO tags, split multiallelic rows, normalize alleles. |
| `gnomad_v4.1_exomes_chr13/chr17` | Global exome frequency baseline | `GRCh38` | Genomic coordinates already present in VCF (`CHROM/POS/REF/ALT`) | `not_needed` | Direct | `Ready` | gnomAD v4.1 exome raw snapshots are GRCh38 VCF files for BRCA chromosomes. | Parse INFO tags, split multiallelic rows, normalize alleles. |
| `gme_hg38` | Arab/Middle Eastern frequency summary layer | `GRCh38` summary table | Start/end/ref/alt columns exist but the file is not a native VCF | `not_needed` | Direct | `Partial` | Frozen raw file is explicitly `hg38`; notes already state it behaves like a summary-frequency resource rather than a native VCF. | Build stable variant keys from `chrom/start/end/ref/alt` and document summary-table assumptions. |
| `saudi_breast_cancer_pmc10474689` | Arab breast-cancer publication supplement | `unknown` at genomic-coordinate level | No genomic coordinate column in extracted Table S5; only transcript/protein HGVS strings are present | `cannot_assess_until_mapping` | Indirect until BRCA rows and genomic mapping are confirmed | `Blocked` | Current downstream extract contains `gene`, `pathogenic_variant_type`, `HGVS Codon Change`, and `HGVS Protein Change`, but not genomic coordinates. | Confirm whether BRCA rows exist in the retained sheet and map transcript HGVS to genomic coordinates before harmonization. |
| `uae_brca_pmc12011969` | Arab BRCA cohort supplement | `GRCh38` claimed at column level | Mutation-positive subset includes a `Chr location (hg38)` column, but coordinates still need row-level parsing and validation | `not_needed` for valid hg38 rows | Direct | `Partial` | Extracted rows keep `Mutations`, `HGVS`, and `Chr location (hg38)` from the source workbook. | Parse and validate the `Chr location (hg38)` field, then normalize alleles from the mutation-positive rows. |

## Scientific Findings
- The active BRCA harmonization inputs that are immediately usable are `ClinVar`, `gnomAD genomes`, and `gnomAD exomes`.
- `GME hg38` is usable without liftover, but it is a summary table and must be treated differently from VCF-style inputs during canonical-key construction.
- `UAE BRCA PMC12011969` is scientifically relevant and likely usable without liftover for populated rows, but it still requires coordinate parsing and row-level validation.
- `Saudi PMC10474689` is frozen correctly as an Arab study source, but it is not yet harmonization-ready because the retained sheet does not expose genomic coordinates.

## Next Exact Action
- Execute `T003 / 2.2` only after the row-level parsing strategy is fixed for `gme_hg38` and `uae_brca_pmc12011969`, while `saudi_breast_cancer_pmc10474689` remains in a blocked mapping-review state.
