from __future__ import annotations

from dataclasses import dataclass
from typing import Final

try:  # pragma: no cover - import path differs between local package and Cloud Run container
    from ui.export_workbook import PRE_GME_EXPORT_FILENAME, export_header_columns, export_metadata_lines
    from ui.registry_queries import (
        CLINVAR_RAW_TABLE_REF,
        CLINVAR_TABLE_REF,
        FINAL_REGISTRY_TABLE,
        GENE_WINDOWS_TABLE_REF,
        GME_RAW_TABLE_REF,
        GME_TABLE_REF,
        GNOMAD_EXOMES_CHR13_RAW_TABLE_REF,
        GNOMAD_EXOMES_CHR17_RAW_TABLE_REF,
        GNOMAD_EXOMES_TABLE_REF,
        GNOMAD_GENOMES_CHR13_RAW_TABLE_REF,
        GNOMAD_GENOMES_CHR17_RAW_TABLE_REF,
        GNOMAD_GENOMES_TABLE_REF,
        PRE_GME_REGISTRY_TABLE,
        PRE_GME_REGISTRY_TABLE_REF,
        REGISTRY_TABLE_REF,
        build_final_registry_sql,
        build_pre_gme_registry_sql,
    )
except ModuleNotFoundError:  # pragma: no cover - runtime fallback inside the ui/ build context
    from export_workbook import PRE_GME_EXPORT_FILENAME, export_header_columns, export_metadata_lines
    from registry_queries import (  # type: ignore[no-redef]
        CLINVAR_RAW_TABLE_REF,
        CLINVAR_TABLE_REF,
        FINAL_REGISTRY_TABLE,
        GENE_WINDOWS_TABLE_REF,
        GME_RAW_TABLE_REF,
        GME_TABLE_REF,
        GNOMAD_EXOMES_CHR13_RAW_TABLE_REF,
        GNOMAD_EXOMES_CHR17_RAW_TABLE_REF,
        GNOMAD_EXOMES_TABLE_REF,
        GNOMAD_GENOMES_CHR13_RAW_TABLE_REF,
        GNOMAD_GENOMES_CHR17_RAW_TABLE_REF,
        GNOMAD_GENOMES_TABLE_REF,
        PRE_GME_REGISTRY_TABLE,
        PRE_GME_REGISTRY_TABLE_REF,
        REGISTRY_TABLE_REF,
        build_final_registry_sql,
        build_pre_gme_registry_sql,
    )


@dataclass(frozen=True)
class DatasetCatalogEntry:
    key: str
    title: str
    table_ref: str
    sample_percent: float
    simple_summary: str
    notes: tuple[str, ...]
    columns: tuple[tuple[str, str], ...]


WORKFLOW_PAGES: Final[tuple[dict[str, str], ...]] = (
    {
        "id": "overview",
        "title": "Overview",
        "summary": "Track progress and workflow navigation for the supervisor.",
    },
    {
        "id": "raw",
        "title": "Raw Sources",
        "summary": "Untouched upstream tables as frozen in BigQuery before harmonization.",
    },
    {
        "id": "harmonization",
        "title": "Harmonization",
        "summary": "BRCA gene-window rules and source-specific harmonized tables.",
    },
    {
        "id": "pre-gme",
        "title": "Pre-GME Review",
        "summary": "Supervisor review checkpoint after ClinVar + gnomAD integration and before adding GME.",
    },
    {
        "id": "final",
        "title": "Final Registry",
        "summary": "Final BRCA registry after adding GME evidence to the harmonized integration.",
    },
)

RAW_DATASETS: Final[dict[str, DatasetCatalogEntry]] = {
    "clinvar_raw_vcf": DatasetCatalogEntry(
        key="clinvar_raw_vcf",
        title="ClinVar raw VCF table",
        table_ref=CLINVAR_RAW_TABLE_REF,
        sample_percent=0.8,
        simple_summary="Untouched ClinVar raw rows loaded into BigQuery before any BRCA-specific extraction or harmonization.",
        notes=(
            "This table stays raw-as-is with the original VCF slots: CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO.",
            "Scientific interpretation happens later in staging and harmonization. This page exists to prove the untouched upstream structure.",
        ),
        columns=(
            ("chrom", "Original chromosome label from the raw ClinVar VCF row."),
            ("pos", "Original VCF POS value."),
            ("id", "Original ClinVar VCF identifier field."),
            ("ref", "Original VCF reference allele."),
            ("alt", "Original VCF alternate allele string."),
            ("qual", "Original VCF QUAL field."),
            ("filter", "Original VCF FILTER field."),
            ("info", "Original VCF INFO payload before staging extraction."),
        ),
    ),
    "gnomad_v4_1_genomes_chr13_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_genomes_chr13_raw",
        title="gnomAD genomes chr13 raw",
        table_ref=GNOMAD_GENOMES_CHR13_RAW_TABLE_REF,
        sample_percent=0.02,
        simple_summary="Untouched gnomAD genomes chr13 rows as frozen from the v4.1 source file.",
        notes=(
            "This is the raw-as-is genomes stream for chr13, kept before any BRCA extraction or INFO parsing.",
            "The large INFO payload is preserved intact so the supervisor can verify that derived fields come from traceable raw source tags.",
        ),
        columns=(
            ("chrom", "Original chromosome label from the gnomAD raw VCF row."),
            ("pos", "Original VCF POS value."),
            ("id", "Original gnomAD ID field."),
            ("ref", "Original VCF reference allele."),
            ("alt", "Original VCF alternate allele string."),
            ("qual", "Original VCF QUAL field."),
            ("filter", "Original VCF FILTER field."),
            ("info", "Original VCF INFO payload before parsing into AC/AN/AF and cohort metrics."),
        ),
    ),
    "gnomad_v4_1_genomes_chr17_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_genomes_chr17_raw",
        title="gnomAD genomes chr17 raw",
        table_ref=GNOMAD_GENOMES_CHR17_RAW_TABLE_REF,
        sample_percent=0.02,
        simple_summary="Untouched gnomAD genomes chr17 rows as frozen from the v4.1 source file.",
        notes=(
            "This is the raw-as-is genomes stream for chr17, which includes the BRCA1 locus.",
            "No harmonization or BRCA-specific filtering has happened yet on this page.",
        ),
        columns=(
            ("chrom", "Original chromosome label from the gnomAD raw VCF row."),
            ("pos", "Original VCF POS value."),
            ("id", "Original gnomAD ID field."),
            ("ref", "Original VCF reference allele."),
            ("alt", "Original VCF alternate allele string."),
            ("qual", "Original VCF QUAL field."),
            ("filter", "Original VCF FILTER field."),
            ("info", "Original VCF INFO payload before parsing into cohort metrics."),
        ),
    ),
    "gnomad_v4_1_exomes_chr13_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_exomes_chr13_raw",
        title="gnomAD exomes chr13 raw",
        table_ref=GNOMAD_EXOMES_CHR13_RAW_TABLE_REF,
        sample_percent=0.08,
        simple_summary="Untouched gnomAD exomes chr13 rows as frozen from the v4.1 source file.",
        notes=(
            "Exomes and genomes are kept separate at the raw layer because they represent different upstream cohorts.",
            "This page proves that any later cohort-specific interpretation started from separate untouched inputs.",
        ),
        columns=(
            ("chrom", "Original chromosome label from the gnomAD raw VCF row."),
            ("pos", "Original VCF POS value."),
            ("id", "Original gnomAD ID field."),
            ("ref", "Original VCF reference allele."),
            ("alt", "Original VCF alternate allele string."),
            ("qual", "Original VCF QUAL field."),
            ("filter", "Original VCF FILTER field."),
            ("info", "Original VCF INFO payload before parsing into exome cohort metrics."),
        ),
    ),
    "gnomad_v4_1_exomes_chr17_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_exomes_chr17_raw",
        title="gnomAD exomes chr17 raw",
        table_ref=GNOMAD_EXOMES_CHR17_RAW_TABLE_REF,
        sample_percent=0.05,
        simple_summary="Untouched gnomAD exomes chr17 rows as frozen from the v4.1 source file.",
        notes=(
            "This raw table covers the BRCA1 chromosome in the exome cohort before any staging or harmonization.",
            "The raw page stays intentionally simple so source provenance remains obvious.",
        ),
        columns=(
            ("chrom", "Original chromosome label from the gnomAD raw VCF row."),
            ("pos", "Original VCF POS value."),
            ("id", "Original gnomAD ID field."),
            ("ref", "Original VCF reference allele."),
            ("alt", "Original VCF alternate allele string."),
            ("qual", "Original VCF QUAL field."),
            ("filter", "Original VCF FILTER field."),
            ("info", "Original VCF INFO payload before parsing into exome cohort metrics."),
        ),
    ),
    "gme_hg38_raw": DatasetCatalogEntry(
        key="gme_hg38_raw",
        title="GME hg38 raw summary table",
        table_ref=GME_RAW_TABLE_REF,
        sample_percent=100.0,
        simple_summary="Untouched local GME hg38 summary table loaded into BigQuery as the raw Arab/Middle Eastern source.",
        notes=(
            "GME arrives as a summary-frequency table, not a VCF callset, so its raw schema differs from ClinVar and gnomAD.",
            "This raw table is displayed separately so the supervisor can see exactly what was added when the Arab-specific layer entered the workflow.",
        ),
        columns=(
            ("chrom", "Chromosome label in the frozen GME source file."),
            ("start", "1-based start coordinate in hg38."),
            ("end", "1-based end coordinate in hg38."),
            ("ref", "Reference allele."),
            ("alt", "Alternate allele."),
            ("gme_af", "Overall GME alternate-allele frequency."),
            ("gme_nwa", "North West Africa subgroup frequency."),
            ("gme_nea", "North East Africa subgroup frequency."),
            ("gme_ap", "Arabian Peninsula subgroup frequency."),
            ("gme_israel", "Israel/Jewish subgroup frequency."),
            ("gme_sd", "Syrian Desert subgroup frequency."),
            ("gme_tp", "Turkish Peninsula subgroup frequency."),
            ("gme_ca", "Central Asia subgroup frequency."),
        ),
    ),
}

HARMONIZED_DATASETS: Final[dict[str, DatasetCatalogEntry]] = {
    "h_brca_gene_windows": DatasetCatalogEntry(
        key="h_brca_gene_windows",
        title="BRCA target windows",
        table_ref=GENE_WINDOWS_TABLE_REF,
        sample_percent=100.0,
        simple_summary="Two reference rows define the exact GRCh38 windows used to extract BRCA1 and BRCA2 variants from every source.",
        notes=(
            "BRCA1 uses chr17:43044295-43170245 and BRCA2 uses chr13:32315086-32400268.",
            "These windows come from Ensembl GRCh38 gene summaries and are the primary cross-source extraction rule.",
        ),
        columns=(
            ("gene_symbol", "Target gene name used in the BRCA-focused workflow."),
            ("ensembl_gene_id", "Stable Ensembl gene identifier for the target gene."),
            ("chrom38", "Canonical GRCh38 chromosome label with chr prefix."),
            ("chrom_nochr", "Same chromosome label without the chr prefix for joining against staging tables."),
            ("start_pos38", "Inclusive GRCh38 start coordinate of the target window."),
            ("end_pos38", "Inclusive GRCh38 end coordinate of the target window."),
            ("coordinate_source", "Reference source used for the target coordinates."),
            ("coordinate_source_url", "Reference URL for the coordinate source."),
        ),
    ),
    "h_brca_clinvar_variants": DatasetCatalogEntry(
        key="h_brca_clinvar_variants",
        title="ClinVar BRCA harmonized",
        table_ref=CLINVAR_TABLE_REF,
        sample_percent=15.0,
        simple_summary="ClinVar alleles restricted to BRCA1/BRCA2 windows and collapsed to one row per canonical allele key.",
        notes=(
            "ClinVar extraction uses coordinate overlap first, then keeps a GENEINFO audit signal so label-vs-window mismatches stay visible.",
            "The scientific-method panel reports live mismatch counts from the current frozen snapshot so label-vs-window drift is visible without hardcoded numbers in the UI.",
        ),
        columns=(
            ("gene_symbol", "Target gene assigned from the BRCA window table."),
            ("ensembl_gene_id", "Stable Ensembl gene identifier for the target gene."),
            ("source_name", "Source stream name for this harmonized table."),
            ("source_build", "Genome build declared for the upstream source."),
            ("liftover_status", "Whether liftover was needed before harmonization."),
            ("norm_status", "Whether harmonization succeeded for the canonical allele representation."),
            ("source_version", "Upstream source version copied from staging."),
            ("snapshot_date", "Frozen snapshot date used for this harmonized table."),
            ("variant_key", "Canonical allele key built as chrom:pos:ref:alt."),
            ("chrom38", "Canonical GRCh38 chromosome label."),
            ("pos38", "Canonical GRCh38 position."),
            ("ref_norm", "Reference allele in canonical form."),
            ("alt_norm", "Alternate allele in canonical form."),
            ("clinvar_record_count", "How many staged ClinVar rows collapsed into this canonical allele."),
            ("clinvar_ids", "Distinct ClinVar IDs observed for this allele."),
            ("clinvar_significance_values", "Distinct clinical significance labels observed for this allele."),
            ("clinvar_review_status_values", "Distinct ClinVar review-status labels observed for this allele."),
            ("gene_info_match_count", "How many contributing ClinVar rows also named the same BRCA gene in GENEINFO."),
            ("gene_info_mismatch_count", "How many contributing ClinVar rows landed inside the BRCA window without a matching GENEINFO label."),
            ("source_record_keys", "Auditable list of staged source record keys that collapsed into this allele."),
            ("extraction_rule", "Short explanation of whether the allele passed by coordinate only or by coordinate plus GENEINFO agreement."),
        ),
    ),
    "h_brca_gnomad_genomes_variants": DatasetCatalogEntry(
        key="h_brca_gnomad_genomes_variants",
        title="gnomAD genomes BRCA harmonized",
        table_ref=GNOMAD_GENOMES_TABLE_REF,
        sample_percent=8.0,
        simple_summary="Allele-level gnomAD genomes evidence restricted to BRCA1/BRCA2 windows and collapsed to one row per canonical allele key.",
        notes=(
            "Genomes and exomes use the same BRCA coordinates. The scientific difference is cohort coverage and discovery, not genomic position.",
            "This table carries the genomes cohort metrics only, so it can be compared side-by-side with the exomes cohort without mixing them.",
        ),
        columns=(
            ("gene_symbol", "Target gene assigned from the BRCA window table."),
            ("ensembl_gene_id", "Stable Ensembl gene identifier for the target gene."),
            ("source_name", "Source stream name for this harmonized table."),
            ("cohort", "gnomAD cohort represented by this harmonized table."),
            ("source_build", "Genome build declared for the upstream source."),
            ("liftover_status", "Whether liftover was needed before harmonization."),
            ("norm_status", "Whether harmonization succeeded for the canonical allele representation."),
            ("source_version", "Upstream source version copied from staging."),
            ("snapshot_date", "Frozen snapshot date used for this harmonized table."),
            ("variant_key", "Canonical allele key built as chrom:pos:ref:alt."),
            ("chrom38", "Canonical GRCh38 chromosome label."),
            ("pos38", "Canonical GRCh38 position."),
            ("ref_norm", "Reference allele in canonical form."),
            ("alt_norm", "Alternate allele in canonical form."),
            ("source_row_count", "How many staged rows contributed to this canonical allele."),
            ("ac", "gnomAD genomes allele count for this canonical allele."),
            ("an", "gnomAD genomes allele number for this canonical allele."),
            ("af", "gnomAD genomes allele frequency for this canonical allele."),
            ("grpmax_population", "gnomAD population label with the highest observed frequency."),
            ("grpmax_faf95", "faf95 value stored for the genomes cohort."),
            ("depth", "Depth slot from staging. It remains null when the raw source does not carry a depth tag."),
            ("ac_afr", "African-ancestry allele count from gnomAD genomes."),
            ("af_afr", "African-ancestry allele frequency from gnomAD genomes."),
            ("ac_eur_proxy", "Europe proxy allele count built from NFE + FIN + ASJ."),
            ("af_eur_proxy", "Europe proxy allele frequency built from the same NFE + FIN + ASJ components."),
            ("extraction_rule", "Extraction method used for the BRCA slice."),
        ),
    ),
    "h_brca_gnomad_exomes_variants": DatasetCatalogEntry(
        key="h_brca_gnomad_exomes_variants",
        title="gnomAD exomes BRCA harmonized",
        table_ref=GNOMAD_EXOMES_TABLE_REF,
        sample_percent=12.0,
        simple_summary="Allele-level gnomAD exomes evidence restricted to BRCA1/BRCA2 windows and collapsed to one row per canonical allele key.",
        notes=(
            "Exome rows use the same BRCA windows as genomes. What changes is capture breadth and cohort composition, not coordinate definition.",
            "Keeping exomes separate prevents coverage-driven differences from being hidden inside a single pooled gnomAD number.",
        ),
        columns=(
            ("gene_symbol", "Target gene assigned from the BRCA window table."),
            ("ensembl_gene_id", "Stable Ensembl gene identifier for the target gene."),
            ("source_name", "Source stream name for this harmonized table."),
            ("cohort", "gnomAD cohort represented by this harmonized table."),
            ("source_build", "Genome build declared for the upstream source."),
            ("liftover_status", "Whether liftover was needed before harmonization."),
            ("norm_status", "Whether harmonization succeeded for the canonical allele representation."),
            ("source_version", "Upstream source version copied from staging."),
            ("snapshot_date", "Frozen snapshot date used for this harmonized table."),
            ("variant_key", "Canonical allele key built as chrom:pos:ref:alt."),
            ("chrom38", "Canonical GRCh38 chromosome label."),
            ("pos38", "Canonical GRCh38 position."),
            ("ref_norm", "Reference allele in canonical form."),
            ("alt_norm", "Alternate allele in canonical form."),
            ("source_row_count", "How many staged rows contributed to this canonical allele."),
            ("ac", "gnomAD exomes allele count for this canonical allele."),
            ("an", "gnomAD exomes allele number for this canonical allele."),
            ("af", "gnomAD exomes allele frequency for this canonical allele."),
            ("grpmax_population", "gnomAD population label with the highest observed frequency."),
            ("grpmax_faf95", "faf95 value stored for the exomes cohort."),
            ("depth", "Depth slot from staging. It remains null when the raw source does not carry a depth tag."),
            ("ac_afr", "African-ancestry allele count from gnomAD exomes."),
            ("af_afr", "African-ancestry allele frequency from gnomAD exomes."),
            ("ac_eur_proxy", "Europe proxy allele count built from NFE + FIN + ASJ."),
            ("af_eur_proxy", "Europe proxy allele frequency built from the same NFE + FIN + ASJ components."),
            ("extraction_rule", "Extraction method used for the BRCA slice."),
        ),
    ),
    "h_brca_gme_variants": DatasetCatalogEntry(
        key="h_brca_gme_variants",
        title="GME BRCA harmonized",
        table_ref=GME_TABLE_REF,
        sample_percent=100.0,
        simple_summary="GME hg38 summary rows restricted to BRCA1/BRCA2 windows and collapsed to one row per canonical allele key.",
        notes=(
            "GME has no explicit gene label in the frozen file, so BRCA extraction is purely coordinate-based.",
            "This source remains a summary-frequency stream, not a VCF cohort callset, so it contributes frequency evidence but not ClinVar-style labels or gnomAD-style cohort structure.",
        ),
        columns=(
            ("gene_symbol", "Target gene assigned from the BRCA window table."),
            ("ensembl_gene_id", "Stable Ensembl gene identifier for the target gene."),
            ("source_name", "Source stream name for this harmonized table."),
            ("source_build", "Genome build declared for the upstream source."),
            ("liftover_status", "Whether liftover was needed before harmonization."),
            ("norm_status", "Whether harmonization succeeded for the canonical allele representation."),
            ("source_version", "Upstream source version label for the frozen GME file."),
            ("snapshot_date", "Frozen snapshot date used for this harmonized table."),
            ("variant_key", "Canonical allele key built as chrom:pos:ref:alt."),
            ("chrom38", "Canonical GRCh38 chromosome label."),
            ("pos38", "Canonical GRCh38 position."),
            ("ref_norm", "Reference allele in canonical form."),
            ("alt_norm", "Alternate allele in canonical form."),
            ("source_row_count", "How many raw GME rows contributed to this canonical allele."),
            ("gme_af", "Overall GME alternate-allele frequency."),
            ("gme_nwa", "North West Africa subgroup frequency."),
            ("gme_nea", "North East Africa subgroup frequency."),
            ("gme_ap", "Arabian Peninsula subgroup frequency."),
            ("gme_israel", "Israel/Jewish subgroup frequency."),
            ("gme_sd", "Syrian Desert subgroup frequency."),
            ("gme_tp", "Turkish Peninsula subgroup frequency."),
            ("gme_ca", "Central Asia subgroup frequency."),
            ("extraction_rule", "Extraction method used for the BRCA slice."),
        ),
    ),
}

PRE_GME_REGISTRY_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("gene_symbol", "BRCA target gene assigned from the harmonized gene-window table."),
    ("variant_key", "Canonical cross-source join key built as chrom:pos:ref:alt."),
    ("chrom", "Canonical GRCh38 chromosome label used in the pre-GME review table."),
    ("pos", "Canonical GRCh38 position used in the pre-GME review table."),
    ("ref", "Canonical reference allele."),
    ("alt", "Canonical alternate allele."),
    ("present_in_clinvar", "True when the harmonized ClinVar BRCA table contains the allele."),
    ("present_in_gnomad_genomes", "True when the harmonized gnomAD genomes BRCA table contains the allele."),
    ("present_in_gnomad_exomes", "True when the harmonized gnomAD exomes BRCA table contains the allele."),
    ("clinvar_ids", "Distinct ClinVar IDs observed for this allele."),
    ("clinvar_significance_values", "Distinct ClinVar significance labels observed for this allele."),
    ("clinvar_review_status_values", "Distinct ClinVar review-status labels observed for this allele."),
    ("clinvar_record_count", "How many ClinVar harmonized rows contributed to this final allele row."),
    ("clinvar_gene_info_match_count", "How many ClinVar source rows agreed with the target BRCA gene in GENEINFO."),
    ("clinvar_gene_info_mismatch_count", "How many ClinVar source rows required coordinate-only extraction review."),
    ("gnomad_genomes_ac", "gnomAD genomes allele count."),
    ("gnomad_genomes_an", "gnomAD genomes allele number."),
    ("gnomad_genomes_af", "gnomAD genomes allele frequency."),
    ("gnomad_genomes_grpmax", "gnomAD genomes population label with the highest observed frequency."),
    ("gnomad_genomes_grpmax_faf95", "gnomAD genomes faf95 value carried into the pre-GME review table."),
    ("gnomad_genomes_depth", "Depth slot from genomes staging."),
    ("gnomad_genomes_ac_afr", "African-ancestry allele count from gnomAD genomes."),
    ("gnomad_genomes_af_afr", "African-ancestry allele frequency from gnomAD genomes."),
    ("gnomad_genomes_ac_eur_proxy", "Europe proxy allele count from gnomAD genomes."),
    ("gnomad_genomes_af_eur_proxy", "Europe proxy allele frequency from gnomAD genomes."),
    ("gnomad_exomes_ac", "gnomAD exomes allele count."),
    ("gnomad_exomes_an", "gnomAD exomes allele number."),
    ("gnomad_exomes_af", "gnomAD exomes allele frequency."),
    ("gnomad_exomes_grpmax", "gnomAD exomes population label with the highest observed frequency."),
    ("gnomad_exomes_grpmax_faf95", "gnomAD exomes faf95 value carried into the pre-GME review table."),
    ("gnomad_exomes_depth", "Depth slot from exomes staging."),
    ("gnomad_exomes_ac_afr", "African-ancestry allele count from gnomAD exomes."),
    ("gnomad_exomes_af_afr", "African-ancestry allele frequency from gnomAD exomes."),
    ("gnomad_exomes_ac_eur_proxy", "Europe proxy allele count from gnomAD exomes."),
    ("gnomad_exomes_af_eur_proxy", "Europe proxy allele frequency from gnomAD exomes."),
    ("source_count", "How many non-GME source streams support the exact allele."),
    ("last_refresh_date", "Date when the pre-GME review table was rebuilt."),
)

FINAL_REGISTRY_COLUMNS: Final[tuple[tuple[str, str], ...]] = PRE_GME_REGISTRY_COLUMNS + (
    ("present_in_gme", "True when the harmonized GME BRCA table contains the allele."),
    ("gme_af", "Overall GME alternate-allele frequency."),
    ("gme_nwa", "North West Africa subgroup frequency from GME."),
    ("gme_nea", "North East Africa subgroup frequency from GME."),
    ("gme_ap", "Arabian Peninsula subgroup frequency from GME."),
    ("gme_israel", "Israel/Jewish subgroup frequency from GME."),
    ("gme_sd", "Syrian Desert subgroup frequency from GME."),
    ("gme_tp", "Turkish Peninsula subgroup frequency from GME."),
    ("gme_ca", "Central Asia subgroup frequency from GME."),
)

HARMONIZATION_STEPS: Final[tuple[dict[str, str], ...]] = (
    {
        "id": "gene_windows",
        "title": "Step 1: Freeze the BRCA windows",
        "simple": "The workflow starts by freezing the GRCh38 BRCA1 and BRCA2 gene windows from Ensembl so every source is sliced by the same coordinates.",
        "technical": "The gene-window table is small on purpose: it is the frozen scientific source of truth used to control all later joins and audits.",
    },
    {
        "id": "clinvar_brca",
        "title": "Step 2: Harmonize ClinVar into BRCA windows",
        "simple": "ClinVar rows are kept when they land inside the BRCA window. GENEINFO remains an audit signal, not the cross-source rule.",
        "technical": "This preserves the coordinate-first logic while keeping a live audit trail for label-vs-window disagreements.",
    },
    {
        "id": "gnomad_genomes_brca",
        "title": "Step 3: Harmonize gnomAD genomes",
        "simple": "gnomAD genomes rows are reduced to BRCA alleles inside the same frozen windows.",
        "technical": "Genomes are kept separate because whole-genome coverage and cohort structure differ from exomes.",
    },
    {
        "id": "gnomad_exomes_brca",
        "title": "Step 4: Harmonize gnomAD exomes",
        "simple": "gnomAD exomes rows are reduced to BRCA alleles inside the same frozen windows.",
        "technical": "Exomes use the same coordinates but represent a different upstream capture design and cohort.",
    },
)

FINAL_STEPS: Final[tuple[dict[str, str], ...]] = (
    {
        "id": "pre_gme_registry",
        "title": "Step 5: Build the pre-GME review table",
        "simple": "Before adding GME, ClinVar + gnomAD genomes + gnomAD exomes are joined into a supervisor review checkpoint.",
        "technical": "This step exists so the supervisor can review the global+clinical integration and export the full artifact to Excel before Arab-specific frequencies are introduced.",
    },
    {
        "id": "gme_brca",
        "title": "Step 6: Harmonize GME into BRCA windows",
        "simple": "GME BRCA rows are added after the pre-GME review checkpoint so the Arab-specific contribution is visible as a distinct workflow step.",
        "technical": "GME remains a summary-frequency source, so it is layered into the final registry after the pre-GME table has been inspected.",
    },
    {
        "id": "registry_final",
        "title": "Step 7: Build the final registry",
        "simple": "The final registry adds GME evidence to the already reviewed pre-GME integration.",
        "technical": "A union of harmonized canonical keys is left-joined to ClinVar, gnomAD genomes, gnomAD exomes, and GME so every observed BRCA allele remains visible.",
    },
)


def raw_dataset_catalog_payload() -> list[dict[str, object]]:
    return [_dataset_payload(entry) for entry in RAW_DATASETS.values()]


def dataset_catalog_payload() -> list[dict[str, object]]:
    return [_dataset_payload(entry) for entry in HARMONIZED_DATASETS.values()]


def _dataset_payload(entry: DatasetCatalogEntry) -> dict[str, object]:
    return {
        "key": entry.key,
        "title": entry.title,
        "table_ref": entry.table_ref,
        "row_count": None,
        "sample_percent": entry.sample_percent,
        "simple_summary": entry.simple_summary,
        "notes": list(entry.notes),
        "columns": [
            {"name": name, "description": description}
            for name, description in entry.columns
        ],
    }


def pre_gme_catalog_payload() -> dict[str, object]:
    return {
        "title": PRE_GME_REGISTRY_TABLE,
        "table_ref": PRE_GME_REGISTRY_TABLE_REF,
        "scope_note": "This is the review checkpoint built after BRCA harmonization of ClinVar + gnomAD genomes + gnomAD exomes, and before adding GME.",
        "accuracy_notes": [
            "The pre-GME table excludes GME on purpose so the supervisor can inspect the global+clinical integration independently.",
            "Rows are built only from harmonized BRCA tables, not from raw joins.",
            "The downloadable Excel artifact mirrors this table one-for-one and keeps a metadata block ahead of the header.",
        ],
        "scientific_notes": [
            "This checkpoint exists to surface errors before Arab-specific frequency evidence changes the interpretation context.",
            "The Excel export uses a VCF-style metadata block inspired by the provided example workbook so review remains traceable and human-readable.",
            "The exported header is fully described in-app before download.",
        ],
        "columns": [
            {"name": name, "description": description}
            for name, description in PRE_GME_REGISTRY_COLUMNS
        ],
        "build_sql": build_pre_gme_registry_sql(),
        "export_filename": PRE_GME_EXPORT_FILENAME,
        "export_metadata_preview": export_metadata_lines(created_at="DD/MM/YYYY HH:MM"),
        "export_header_columns": export_header_columns(),
    }


def registry_catalog_payload() -> dict[str, object]:
    return {
        "title": FINAL_REGISTRY_TABLE,
        "table_ref": REGISTRY_TABLE_REF,
        "scope_note": "The final supervisor table is BRCA-only and is rebuilt from harmonized BRCA source tables after the GME layer is added.",
        "accuracy_notes": [
            "All current sources in this workspace already use GRCh38 coordinates, so liftover is not needed for the BRCA workflow.",
            "Genomes and exomes share the same BRCA coordinates; the difference is cohort coverage and capture design, not position.",
            "ClinVar GENEINFO is preserved as an audit signal, but coordinate windows are the authoritative cross-source extraction rule.",
            "GME is added only after the pre-GME checkpoint, so the Arab-specific contribution remains auditable as a separate step.",
        ],
        "scientific_notes": [
            "BRCA1 window and BRCA2 window definitions are frozen from an Ensembl-backed seed artifact, not ad-hoc literals in the UI logic.",
            "ClinVar GENEINFO disagreement is reported from live harmonized counts rather than fixed explanatory numbers.",
            "The final registry is intentionally downstream of the pre-GME review checkpoint to reduce silent integration mistakes.",
        ],
        "columns": [
            {"name": name, "description": description}
            for name, description in FINAL_REGISTRY_COLUMNS
        ],
        "steps": list(FINAL_STEPS),
        "build_sql": build_final_registry_sql(),
    }
