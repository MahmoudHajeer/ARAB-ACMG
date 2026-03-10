from __future__ import annotations

from dataclasses import dataclass
from typing import Final

try:  # pragma: no cover - import path differs between local package and Cloud Run container
    from ui.registry_queries import (
        CLINVAR_TABLE_REF,
        GENE_WINDOWS_TABLE_REF,
        GME_TABLE_REF,
        GNOMAD_EXOMES_TABLE_REF,
        GNOMAD_GENOMES_TABLE_REF,
        REGISTRY_TABLE_REF,
        build_registry_sql,
    )
except ModuleNotFoundError:  # pragma: no cover - runtime fallback inside the ui/ build context
    from registry_queries import (  # type: ignore[no-redef]
        CLINVAR_TABLE_REF,
        GENE_WINDOWS_TABLE_REF,
        GME_TABLE_REF,
        GNOMAD_EXOMES_TABLE_REF,
        GNOMAD_GENOMES_TABLE_REF,
        REGISTRY_TABLE_REF,
        build_registry_sql,
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

REGISTRY_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("gene_symbol", "BRCA target gene assigned from the harmonized gene-window table."),
    ("variant_key", "Canonical cross-source join key built as chrom:pos:ref:alt."),
    ("chrom", "Canonical GRCh38 chromosome label used in the final join."),
    ("pos", "Canonical GRCh38 position used in the final join."),
    ("ref", "Canonical reference allele used in the final join."),
    ("alt", "Canonical alternate allele used in the final join."),
    ("present_in_clinvar", "True when the harmonized ClinVar BRCA table contains the allele."),
    ("present_in_gnomad_genomes", "True when the harmonized gnomAD genomes BRCA table contains the allele."),
    ("present_in_gnomad_exomes", "True when the harmonized gnomAD exomes BRCA table contains the allele."),
    ("present_in_gme", "True when the harmonized GME BRCA table contains the allele."),
    ("clinvar_ids", "Distinct ClinVar IDs observed for this allele after harmonized collapse."),
    ("clinvar_significance_values", "Distinct ClinVar significance labels observed for this allele."),
    ("clinvar_review_status_values", "Distinct ClinVar review-status labels observed for this allele."),
    ("clinvar_record_count", "How many ClinVar harmonized rows contributed to this final allele row."),
    ("clinvar_gene_info_match_count", "How many ClinVar source rows agreed with the target BRCA gene in GENEINFO."),
    ("clinvar_gene_info_mismatch_count", "How many ClinVar source rows required coordinate-only extraction review."),
    ("gnomad_genomes_ac", "gnomAD genomes allele count."),
    ("gnomad_genomes_an", "gnomAD genomes allele number."),
    ("gnomad_genomes_af", "gnomAD genomes allele frequency."),
    ("gnomad_genomes_grpmax", "gnomAD genomes population label with the highest observed frequency."),
    ("gnomad_genomes_grpmax_faf95", "gnomAD genomes faf95 value carried into the supervisor table."),
    ("gnomad_genomes_depth", "Depth slot from genomes staging. Null when the raw source does not expose depth."),
    ("gnomad_genomes_ac_afr", "African-ancestry allele count from gnomAD genomes."),
    ("gnomad_genomes_af_afr", "African-ancestry allele frequency from gnomAD genomes."),
    ("gnomad_genomes_ac_eur_proxy", "Europe proxy allele count from gnomAD genomes."),
    ("gnomad_genomes_af_eur_proxy", "Europe proxy allele frequency from gnomAD genomes."),
    ("gnomad_exomes_ac", "gnomAD exomes allele count."),
    ("gnomad_exomes_an", "gnomAD exomes allele number."),
    ("gnomad_exomes_af", "gnomAD exomes allele frequency."),
    ("gnomad_exomes_grpmax", "gnomAD exomes population label with the highest observed frequency."),
    ("gnomad_exomes_grpmax_faf95", "gnomAD exomes faf95 value carried into the supervisor table."),
    ("gnomad_exomes_depth", "Depth slot from exomes staging. Null when the raw source does not expose depth."),
    ("gnomad_exomes_ac_afr", "African-ancestry allele count from gnomAD exomes."),
    ("gnomad_exomes_af_afr", "African-ancestry allele frequency from gnomAD exomes."),
    ("gnomad_exomes_ac_eur_proxy", "Europe proxy allele count from gnomAD exomes."),
    ("gnomad_exomes_af_eur_proxy", "Europe proxy allele frequency from gnomAD exomes."),
    ("gme_af", "Overall GME alternate-allele frequency."),
    ("gme_nwa", "North West Africa subgroup frequency from GME."),
    ("gme_nea", "North East Africa subgroup frequency from GME."),
    ("gme_ap", "Arabian Peninsula subgroup frequency from GME."),
    ("gme_israel", "Israel/Jewish subgroup frequency from GME."),
    ("gme_sd", "Syrian Desert subgroup frequency from GME."),
    ("gme_tp", "Turkish Peninsula subgroup frequency from GME."),
    ("gme_ca", "Central Asia subgroup frequency from GME."),
    ("source_count", "How many harmonized source streams support the exact allele."),
    ("last_refresh_date", "Date when the BRCA supervisor registry table was rebuilt."),
)

REGISTRY_STEPS: Final[tuple[dict[str, str], ...]] = (
    {
        "id": "gene_windows",
        "title": "Step 1: Fix the BRCA windows",
        "simple": "First we freeze the exact GRCh38 windows for BRCA1 and BRCA2, because every source has to be sliced by the same coordinates.",
        "technical": "The workflow uses Ensembl GRCh38 coordinates for BRCA1 and BRCA2 and stores them in a small harmonized reference table.",
    },
    {
        "id": "clinvar_brca",
        "title": "Step 2: Extract ClinVar BRCA alleles",
        "simple": "ClinVar rows are kept when they land inside the BRCA window. GENEINFO is kept as an audit signal, not as the primary cross-source rule.",
        "technical": "Coordinate overlap is the authoritative extraction rule for cross-source joins. The live scientific-evidence panel reports current ClinVar GENEINFO agreement and disagreement counts from BigQuery rather than embedding fixed numbers in the UI.",
    },
    {
        "id": "gnomad_genomes_brca",
        "title": "Step 3: Extract gnomAD genomes BRCA alleles",
        "simple": "The genomes cohort is sliced by the same BRCA windows and kept as its own harmonized evidence stream.",
        "technical": "Genome-wide sequencing covers the same GRCh38 positions as exomes; the difference is discovery breadth and cohort composition, not genomic coordinates.",
    },
    {
        "id": "gnomad_exomes_brca",
        "title": "Step 4: Extract gnomAD exomes BRCA alleles",
        "simple": "The exomes cohort uses the same BRCA windows, but its evidence mainly reflects targeted exome capture rather than whole-genome coverage.",
        "technical": "Positions do not shift between genomes and exomes. What changes is which alleles are observed because exomes concentrate on captured coding regions and nearby splice context.",
    },
    {
        "id": "gme_brca",
        "title": "Step 5: Extract GME BRCA alleles",
        "simple": "GME has no gene label column in the frozen hg38 file, so BRCA rows are selected purely by coordinate overlap.",
        "technical": "The GME stream stays summary-style: it contributes frequency evidence only and does not try to mimic ClinVar labels or gnomAD cohort structure.",
    },
    {
        "id": "registry_final",
        "title": "Step 6: Join the harmonized BRCA tables",
        "simple": "The final supervisor table now reads only from harmonized BRCA tables, so sampling it is a direct table read rather than a complex raw parse-and-join query.",
        "technical": "A union of harmonized canonical keys is left-joined to the ClinVar, gnomAD genomes, gnomAD exomes, and GME BRCA tables so every observed BRCA allele stays visible.",
    },
)


def dataset_catalog_payload() -> list[dict[str, object]]:
    payload: list[dict[str, object]] = []
    for entry in HARMONIZED_DATASETS.values():
        payload.append(
            {
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
        )
    return payload


def registry_catalog_payload() -> dict[str, object]:
    return {
        "title": "supervisor_variant_registry_brca_v1",
        "table_ref": REGISTRY_TABLE_REF,
        "scope_note": "The final supervisor table is now BRCA-only and is rebuilt from harmonized BRCA source tables inside arab_acmg_harmonized.",
        "accuracy_notes": [
            "All current sources in this workspace already use GRCh38 coordinates, so liftover is not needed for the BRCA workflow.",
            "Genomes and exomes share the same BRCA coordinates; the difference is cohort coverage and capture design, not position.",
            "ClinVar GENEINFO is preserved as an audit signal, but coordinate windows are the authoritative cross-source extraction rule.",
        ],
        "scientific_notes": [
            "BRCA1 window and BRCA2 window definitions are frozen from an Ensembl-backed seed artifact, not ad-hoc literals in the UI logic.",
            "ClinVar GENEINFO disagreement is reported from live harmonized counts rather than fixed explanatory numbers.",
            "Step 1 exposes the exact source URLs and access date used for the BRCA target definition.",
        ],
        "columns": [
            {"name": name, "description": description}
            for name, description in REGISTRY_COLUMNS
        ],
        "steps": list(REGISTRY_STEPS),
        "build_sql": build_registry_sql(),
    }
