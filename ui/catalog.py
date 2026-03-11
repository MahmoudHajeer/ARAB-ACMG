from __future__ import annotations

from dataclasses import dataclass
from typing import Final

try:  # pragma: no cover
    from ui.export_workbook import PRE_GME_EXPORT_FILENAME, export_header_columns, export_metadata_lines
    from ui.registry_queries import (
        CLINVAR_RAW_TABLE_REF,
        GME_RAW_TABLE_REF,
        GNOMAD_EXOMES_CHR13_RAW_TABLE_REF,
        GNOMAD_EXOMES_CHR17_RAW_TABLE_REF,
        GNOMAD_GENOMES_CHR13_RAW_TABLE_REF,
        GNOMAD_GENOMES_CHR17_RAW_TABLE_REF,
        PRE_GME_REGISTRY_TABLE,
        PRE_GME_REGISTRY_TABLE_REF,
        REGISTRY_TABLE_REF,
        FINAL_REGISTRY_TABLE,
        build_final_registry_sql,
        build_pre_gme_registry_sql,
        gene_windows_payload,
    )
    from ui.schema_columns import column_payload, final_columns, pre_gme_columns
except ModuleNotFoundError:  # pragma: no cover
    from export_workbook import PRE_GME_EXPORT_FILENAME, export_header_columns, export_metadata_lines
    from registry_queries import (  # type: ignore[no-redef]
        CLINVAR_RAW_TABLE_REF,
        GME_RAW_TABLE_REF,
        GNOMAD_EXOMES_CHR13_RAW_TABLE_REF,
        GNOMAD_EXOMES_CHR17_RAW_TABLE_REF,
        GNOMAD_GENOMES_CHR13_RAW_TABLE_REF,
        GNOMAD_GENOMES_CHR17_RAW_TABLE_REF,
        PRE_GME_REGISTRY_TABLE,
        PRE_GME_REGISTRY_TABLE_REF,
        REGISTRY_TABLE_REF,
        FINAL_REGISTRY_TABLE,
        build_final_registry_sql,
        build_pre_gme_registry_sql,
        gene_windows_payload,
    )
    from schema_columns import column_payload, final_columns, pre_gme_columns


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
    {"id": "overview", "title": "Overview", "summary": "Track progress and workflow navigation for the supervisor."},
    {"id": "raw", "title": "Raw Sources", "summary": "Untouched upstream tables as frozen in BigQuery before checkpoint extraction."},
    {"id": "harmonization", "title": "Checkpoint Design", "summary": "Direct raw-to-checkpoint BRCA extraction with the mandated publication-facing header."},
    {"id": "pre-gme", "title": "Pre-GME Review", "summary": "Unified BRCA checkpoint before adding GME."},
    {"id": "final", "title": "Final Registry", "summary": "Unified BRCA checkpoint after adding GME."},
)

RAW_DATASETS: Final[dict[str, DatasetCatalogEntry]] = {
    "clinvar_raw_vcf": DatasetCatalogEntry(
        key="clinvar_raw_vcf",
        title="ClinVar raw VCF table",
        table_ref=CLINVAR_RAW_TABLE_REF,
        sample_percent=0.8,
        simple_summary="Untouched ClinVar raw rows before BRCA checkpoint extraction.",
        notes=(
            "This raw table preserves the original VCF fields: CHROM, POS, ID, REF, ALT, QUAL, FILTER, INFO.",
            "Checkpoint tables now parse directly from this raw source instead of depending on durable per-source harmonized tables.",
        ),
        columns=(
            ("chrom", "Original chromosome label from ClinVar raw."),
            ("pos", "Original VCF POS value."),
            ("id", "Original raw identifier field."),
            ("ref", "Original reference allele."),
            ("alt", "Original alternate allele string."),
            ("qual", "Original QUAL field."),
            ("filter", "Original FILTER field."),
            ("info", "Original INFO payload used for ALLELEID, CLNSIG, and other source-backed tags."),
        ),
    ),
    "gnomad_v4_1_genomes_chr13_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_genomes_chr13_raw",
        title="gnomAD genomes chr13 raw",
        table_ref=GNOMAD_GENOMES_CHR13_RAW_TABLE_REF,
        sample_percent=0.02,
        simple_summary="Untouched gnomAD genomes chr13 rows before BRCA checkpoint extraction.",
        notes=(
            "INFO tags are parsed directly from this raw table for AF, HOM, AFR, MID, and Europe-proxy metrics.",
            "No durable h_* genomes table is kept anymore; only checkpoint outputs remain in arab_acmg_harmonized.",
        ),
        columns=(("chrom", "Original chromosome label."), ("pos", "Original POS."), ("id", "Original ID."), ("ref", "Reference allele."), ("alt", "Alternate allele."), ("qual", "QUAL field."), ("filter", "FILTER field."), ("info", "INFO payload.")),
    ),
    "gnomad_v4_1_genomes_chr17_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_genomes_chr17_raw",
        title="gnomAD genomes chr17 raw",
        table_ref=GNOMAD_GENOMES_CHR17_RAW_TABLE_REF,
        sample_percent=0.02,
        simple_summary="Untouched gnomAD genomes chr17 rows before BRCA checkpoint extraction.",
        notes=("This raw table covers the BRCA1 chromosome.",),
        columns=(("chrom", "Original chromosome label."), ("pos", "Original POS."), ("id", "Original ID."), ("ref", "Reference allele."), ("alt", "Alternate allele."), ("qual", "QUAL field."), ("filter", "FILTER field."), ("info", "INFO payload.")),
    ),
    "gnomad_v4_1_exomes_chr13_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_exomes_chr13_raw",
        title="gnomAD exomes chr13 raw",
        table_ref=GNOMAD_EXOMES_CHR13_RAW_TABLE_REF,
        sample_percent=0.08,
        simple_summary="Untouched gnomAD exomes chr13 rows before BRCA checkpoint extraction.",
        notes=("Exomes remain separate from genomes at the raw layer and are only unified inside the checkpoint tables.",),
        columns=(("chrom", "Original chromosome label."), ("pos", "Original POS."), ("id", "Original ID."), ("ref", "Reference allele."), ("alt", "Alternate allele."), ("qual", "QUAL field."), ("filter", "FILTER field."), ("info", "INFO payload.")),
    ),
    "gnomad_v4_1_exomes_chr17_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_exomes_chr17_raw",
        title="gnomAD exomes chr17 raw",
        table_ref=GNOMAD_EXOMES_CHR17_RAW_TABLE_REF,
        sample_percent=0.05,
        simple_summary="Untouched gnomAD exomes chr17 rows before BRCA checkpoint extraction.",
        notes=("This raw table covers the BRCA1 chromosome in the exome cohort.",),
        columns=(("chrom", "Original chromosome label."), ("pos", "Original POS."), ("id", "Original ID."), ("ref", "Reference allele."), ("alt", "Alternate allele."), ("qual", "QUAL field."), ("filter", "FILTER field."), ("info", "INFO payload.")),
    ),
    "gme_hg38_raw": DatasetCatalogEntry(
        key="gme_hg38_raw",
        title="GME hg38 raw summary table",
        table_ref=GME_RAW_TABLE_REF,
        sample_percent=100.0,
        simple_summary="Untouched GME summary rows before the final checkpoint adds Arab-specific frequencies.",
        notes=(
            "GME is not mixed into the pre-GME checkpoint by design.",
            "The final checkpoint adds GME only after the minimum publication-facing header has already been materialized and reviewed.",
        ),
        columns=(("chrom", "Chromosome label."), ("start", "Start coordinate in hg38."), ("end", "End coordinate in hg38."), ("ref", "Reference allele."), ("alt", "Alternate allele."), ("gme_af", "Overall GME AF."), ("gme_nwa", "NWA subgroup frequency."), ("gme_nea", "NEA subgroup frequency."), ("gme_ap", "Arabian Peninsula subgroup frequency."), ("gme_israel", "Israel/Jewish subgroup frequency."), ("gme_sd", "Syrian Desert subgroup frequency."), ("gme_tp", "Turkish Peninsula subgroup frequency."), ("gme_ca", "Central Asia subgroup frequency.")),
    ),
}

HARMONIZED_DATASETS: Final[dict[str, DatasetCatalogEntry]] = {
    "pre_gme_registry": DatasetCatalogEntry(
        key="pre_gme_registry",
        title="Pre-GME unified checkpoint",
        table_ref=PRE_GME_REGISTRY_TABLE_REF,
        sample_percent=100.0,
        simple_summary="Single BRCA checkpoint table built directly from raw ClinVar + gnomAD raw sources with the mandated publication-facing header as the minimum schema.",
        notes=(
            "Unsupported requested fields remain NULL instead of being guessed.",
            "Extra columns appear only after the required header and are marked as pipeline extras in the UI/export.",
        ),
        columns=pre_gme_columns(),
    ),
    "final_registry": DatasetCatalogEntry(
        key="final_registry",
        title="Final unified checkpoint with GME",
        table_ref=REGISTRY_TABLE_REF,
        sample_percent=100.0,
        simple_summary="Single BRCA checkpoint table after the GME layer is added.",
        notes=(
            "This is the only durable final checkpoint after GME integration inside arab_acmg_harmonized.",
            "GME-specific columns are treated as extras after the required header floor.",
        ),
        columns=final_columns(),
    ),
}

HARMONIZATION_STEPS: Final[tuple[dict[str, str], ...]] = (
    {
        "id": "clinvar_raw_brca",
        "title": "Step 1: Extract BRCA ClinVar rows directly from raw",
        "simple": "ClinVar raw rows are filtered by the frozen BRCA windows, split by ALT, and aggregated to the checkpoint key.",
        "technical": "ALLELEID, CLNSIG, CLNREVSTAT, and MC-derived effect labels come directly from raw INFO tags.",
    },
    {
        "id": "gnomad_genomes_raw_brca",
        "title": "Step 2: Extract BRCA gnomAD genomes rows directly from raw",
        "simple": "gnomAD genomes raw INFO is parsed directly for AF, HOM, AFR, MID, and Europe-proxy metrics.",
        "technical": "No durable per-source harmonized genomes table is kept after the checkpoint is built.",
    },
    {
        "id": "gnomad_exomes_raw_brca",
        "title": "Step 3: Extract BRCA gnomAD exomes rows directly from raw",
        "simple": "gnomAD exomes are handled separately at extraction time and only unified inside the checkpoint table.",
        "technical": "Combined GNOMAD_ALL_* and GNOMAD_MID_* fields are derived from the loaded genomes + exomes cohorts.",
    },
)

FINAL_STEPS: Final[tuple[dict[str, str], ...]] = (
    {
        "id": "pre_gme_checkpoint",
        "title": "Step 4: Build the pre-GME unified checkpoint",
        "simple": "The required publication-facing header is materialized before any GME addition.",
        "technical": "This table is the supervisor review checkpoint and the source for the Excel export.",
    },
    {
        "id": "gme_raw_brca",
        "title": "Step 5: Extract BRCA GME rows directly from raw",
        "simple": "GME enters only after the minimum header table already exists and has been reviewed.",
        "technical": "GME remains a summary-frequency source, so it adds Arab-specific frequency extras rather than ClinVar-style labels.",
    },
    {
        "id": "final_checkpoint",
        "title": "Step 6: Build the final unified checkpoint",
        "simple": "The final checkpoint keeps the required header first and adds GME extras after it.",
        "technical": "Obsolete h_* and staging-derived harmonized outputs are removed from arab_acmg_harmonized after this build succeeds.",
    },
)


def _dataset_payload(entry: DatasetCatalogEntry) -> dict[str, object]:
    return {
        "key": entry.key,
        "title": entry.title,
        "table_ref": entry.table_ref,
        "row_count": None,
        "sample_percent": entry.sample_percent,
        "simple_summary": entry.simple_summary,
        "notes": list(entry.notes),
        "columns": column_payload(entry.columns),
    }


def raw_dataset_catalog_payload() -> list[dict[str, object]]:
    return [_dataset_payload(entry) for entry in RAW_DATASETS.values()]


def dataset_catalog_payload() -> list[dict[str, object]]:
    return [_dataset_payload(entry) for entry in HARMONIZED_DATASETS.values()]


def pre_gme_catalog_payload() -> dict[str, object]:
    return {
        "title": PRE_GME_REGISTRY_TABLE,
        "table_ref": PRE_GME_REGISTRY_TABLE_REF,
        "scope_note": "This checkpoint is built directly from raw ClinVar + gnomAD inputs and enforces the requested publication-facing header as the minimum schema.",
        "accuracy_notes": [
            "Fields without a loaded source remain NULL instead of being guessed.",
            "Required columns are shown first; extras are marked separately in the UI and export.",
            "No durable per-source harmonized BRCA tables remain after this checkpoint design is applied.",
        ],
        "scientific_notes": [
            "BRCA windows still come from the frozen Ensembl-backed seed, but they are now used inside the checkpoint SQL rather than as a durable BigQuery table.",
            "ClinVar and gnomAD are parsed directly from raw INFO payloads so the checkpoint remains traceable to the untouched upstream sources.",
            "The Excel export mirrors this checkpoint schema one-for-one.",
        ],
        "columns": column_payload(pre_gme_columns()),
        "build_sql": build_pre_gme_registry_sql(),
        "export_filename": PRE_GME_EXPORT_FILENAME,
        "export_metadata_preview": export_metadata_lines(created_at="DD/MM/YYYY HH:MM"),
        "export_header_columns": export_header_columns(),
        "gene_windows": gene_windows_payload(),
    }


def registry_catalog_payload() -> dict[str, object]:
    return {
        "title": FINAL_REGISTRY_TABLE,
        "table_ref": REGISTRY_TABLE_REF,
        "scope_note": "This final checkpoint preserves the required header floor and adds GME-derived extras after it.",
        "accuracy_notes": [
            "The first columns stay aligned to the requested publication-facing header.",
            "GME is added only after the pre-GME checkpoint exists and can be reviewed independently.",
            "Extra columns remain visually distinguished from the required floor.",
        ],
        "scientific_notes": [
            "GNOMAD_ALL_AF and GNOMAD_MID_AF are derived from the currently loaded genomes + exomes cohorts, not from a hidden external merged table.",
            "Fields such as TOPMED, ALFA, OMIM, JGP, and in-silico predictors remain NULL until those sources are added explicitly.",
            "The final checkpoint is the only durable harmonized output after GME integration.",
        ],
        "columns": column_payload(final_columns()),
        "steps": list(FINAL_STEPS),
        "build_sql": build_final_registry_sql(),
        "gene_windows": gene_windows_payload(),
    }
