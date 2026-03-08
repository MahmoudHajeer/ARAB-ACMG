from __future__ import annotations

import json
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path
from typing import Final

try:  # pragma: no cover - import path differs between local package and Cloud Run container
    from ui.registry_queries import REGISTRY_TABLE_REF, build_registry_sql
except ModuleNotFoundError:  # pragma: no cover - runtime fallback inside the ui/ build context
    from registry_queries import REGISTRY_TABLE_REF, build_registry_sql

ROOT: Final[Path] = Path(__file__).resolve().parent
SNAPSHOT_FILE: Final[Path] = ROOT / "status_snapshot.json"


@dataclass(frozen=True)
class DatasetCatalogEntry:
    key: str
    title: str
    table_ref: str
    sample_percent: float
    simple_summary: str
    notes: tuple[str, ...]
    columns: tuple[tuple[str, str], ...]


RAW_DATASETS: Final[dict[str, DatasetCatalogEntry]] = {
    "clinvar_raw_vcf": DatasetCatalogEntry(
        key="clinvar_raw_vcf",
        title="ClinVar raw VCF",
        table_ref="genome-services-platform.arab_acmg_raw.clinvar_raw_vcf",
        sample_percent=0.3,
        simple_summary="Untouched ClinVar GRCh38 VCF rows loaded into BigQuery with the eight canonical VCF columns.",
        notes=(
            "Every row is one raw VCF record exactly as it came from ClinVar after header skipping.",
            "The info column is still a raw key-value string; clinical fields are parsed later in evidence views.",
        ),
        columns=(
            ("chrom", "Chromosome label from the original ClinVar VCF record."),
            ("pos", "1-based genomic position of the raw record on GRCh38."),
            ("id", "ClinVar record identifier carried in the VCF ID column."),
            ("ref", "Reference allele exactly as written in the source VCF."),
            ("alt", "Alternate allele string from the source VCF; comma-separated if the raw row is multi-allelic."),
            ("qual", "Raw QUAL field from the VCF. It stays untouched in the raw layer."),
            ("filter", "Raw FILTER status from the VCF record."),
            ("info", "Untouched INFO payload from ClinVar. This is where labels like CLNSIG and CLNREVSTAT live before parsing."),
        ),
    ),
    "gnomad_v4_1_genomes_chr13_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_genomes_chr13_raw",
        title="gnomAD v4.1 genomes chr13 raw",
        table_ref="genome-services-platform.arab_acmg_raw.gnomad_v4_1_genomes_chr13_raw",
        sample_percent=0.1,
        simple_summary="Untouched gnomAD v4.1 genomes site-level rows for chromosome 13.",
        notes=(
            "Rows are still site-level raw VCF records. Multi-allelic ALT strings are not split in this raw table.",
            "Frequency evidence such as AC, AN, AF, grpmax, and faf95 still lives inside the raw info string.",
        ),
        columns=(
            ("chrom", "Chromosome label from the original gnomAD VCF record, for example chr13."),
            ("pos", "1-based genomic position from the raw site record."),
            ("id", "Raw gnomAD record ID field."),
            ("ref", "Reference allele from the source site record."),
            ("alt", "Alternate allele string from the source site record; comma-separated when the site is multi-allelic."),
            ("qual", "Raw QUAL field from gnomAD."),
            ("filter", "Raw FILTER value from gnomAD."),
            ("info", "Untouched INFO payload where cohort and ancestry frequencies are stored before allele-level parsing."),
        ),
    ),
    "gnomad_v4_1_genomes_chr17_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_genomes_chr17_raw",
        title="gnomAD v4.1 genomes chr17 raw",
        table_ref="genome-services-platform.arab_acmg_raw.gnomad_v4_1_genomes_chr17_raw",
        sample_percent=0.1,
        simple_summary="Untouched gnomAD v4.1 genomes site-level rows for chromosome 17.",
        notes=(
            "This is the raw site table, not a filtered BRCA subset and not an allele-split table.",
            "The info field is preserved exactly so later parsing can be audited against the original source string.",
        ),
        columns=(
            ("chrom", "Chromosome label from the original gnomAD VCF record, for example chr17."),
            ("pos", "1-based genomic position from the raw site record."),
            ("id", "Raw gnomAD record ID field."),
            ("ref", "Reference allele from the source site record."),
            ("alt", "Alternate allele string from the source site record; comma-separated when the site is multi-allelic."),
            ("qual", "Raw QUAL field from gnomAD."),
            ("filter", "Raw FILTER value from gnomAD."),
            ("info", "Untouched INFO payload where cohort and ancestry frequencies are stored before allele-level parsing."),
        ),
    ),
    "gnomad_v4_1_exomes_chr13_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_exomes_chr13_raw",
        title="gnomAD v4.1 exomes chr13 raw",
        table_ref="genome-services-platform.arab_acmg_raw.gnomad_v4_1_exomes_chr13_raw",
        sample_percent=0.2,
        simple_summary="Untouched gnomAD v4.1 exomes site-level rows for chromosome 13.",
        notes=(
            "This table captures the exome cohort separately from genomes so the supervisor can compare both evidence streams.",
            "Allele-specific counts are extracted later from the raw info string in the registry build step.",
        ),
        columns=(
            ("chrom", "Chromosome label from the original gnomAD VCF record, for example chr13."),
            ("pos", "1-based genomic position from the raw site record."),
            ("id", "Raw gnomAD record ID field."),
            ("ref", "Reference allele from the source site record."),
            ("alt", "Alternate allele string from the source site record; comma-separated when the site is multi-allelic."),
            ("qual", "Raw QUAL field from gnomAD."),
            ("filter", "Raw FILTER value from gnomAD."),
            ("info", "Untouched INFO payload where cohort and ancestry frequencies are stored before allele-level parsing."),
        ),
    ),
    "gnomad_v4_1_exomes_chr17_raw": DatasetCatalogEntry(
        key="gnomad_v4_1_exomes_chr17_raw",
        title="gnomAD v4.1 exomes chr17 raw",
        table_ref="genome-services-platform.arab_acmg_raw.gnomad_v4_1_exomes_chr17_raw",
        sample_percent=0.1,
        simple_summary="Untouched gnomAD v4.1 exomes site-level rows for chromosome 17.",
        notes=(
            "The table is kept raw-as-is so every later statistic can be traced back to the exact original VCF row.",
            "The registry view later splits ALT alleles so each row becomes one allele-level record for joining.",
        ),
        columns=(
            ("chrom", "Chromosome label from the original gnomAD VCF record, for example chr17."),
            ("pos", "1-based genomic position from the raw site record."),
            ("id", "Raw gnomAD record ID field."),
            ("ref", "Reference allele from the source site record."),
            ("alt", "Alternate allele string from the source site record; comma-separated when the site is multi-allelic."),
            ("qual", "Raw QUAL field from gnomAD."),
            ("filter", "Raw FILTER value from gnomAD."),
            ("info", "Untouched INFO payload where cohort and ancestry frequencies are stored before allele-level parsing."),
        ),
    ),
    "gme_hg38_raw": DatasetCatalogEntry(
        key="gme_hg38_raw",
        title="GME hg38 raw summary",
        table_ref="genome-services-platform.arab_acmg_raw.gme_hg38_raw",
        sample_percent=0.5,
        simple_summary="The local GME hg38 file frozen into BigQuery as a raw summary-frequency table.",
        notes=(
            "Current workspace evidence shows this file covers chromosomes 1-22 and X only, with 699,496 rows.",
            "This is a legacy summary-style resource, not a native raw GRCh38 VCF. Public documentation describes the hg38 form as a liftover of the original GME release.",
        ),
        columns=(
            ("chrom", "Chromosome label in the supplied GME summary file."),
            ("start", "1-based start coordinate from the source summary file."),
            ("end", "End coordinate from the source summary file."),
            ("ref", "Reference allele from the source summary file."),
            ("alt", "Alternate allele from the source summary file."),
            ("gme_af", "Overall GME alternate-allele frequency."),
            ("gme_nwa", "North West Africa frequency."),
            ("gme_nea", "North East Africa frequency."),
            ("gme_ap", "Arabian Peninsula frequency."),
            ("gme_israel", "Frequency for the Israel/Jewish subgroup column supplied by the source file."),
            ("gme_sd", "Syrian Desert subgroup frequency."),
            ("gme_tp", "Turkish Peninsula subgroup frequency."),
            ("gme_ca", "Central Asia subgroup frequency."),
        ),
    ),
}

REGISTRY_COLUMNS: Final[tuple[tuple[str, str], ...]] = (
    ("variant_key", "Stable join key built as chrom:pos:ref:alt after chromosome normalization and allele splitting."),
    ("chrom", "Normalized chromosome label used to join all source tables."),
    ("pos", "1-based genomic position used across all sources."),
    ("ref", "Reference allele used in the cross-source join key."),
    ("alt", "Single alternate allele after splitting raw multi-allelic rows."),
    ("present_in_clinvar", "True when the exact allele appears in ClinVar raw data."),
    ("present_in_gnomad_genomes", "True when the exact allele appears in gnomAD genomes for chr13/chr17."),
    ("present_in_gnomad_exomes", "True when the exact allele appears in gnomAD exomes for chr13/chr17."),
    ("present_in_gme", "True when the exact allele appears in the frozen GME summary table."),
    ("clinvar_id", "ClinVar identifier copied from the raw VCF ID field."),
    ("clinvar_significance", "Clinical significance label parsed from the raw ClinVar info field."),
    ("clinvar_review_status", "ClinVar review status parsed from the raw ClinVar info field."),
    ("gnomad_genomes_ac", "Allele count parsed for the genomes cohort after ALT splitting."),
    ("gnomad_genomes_an", "Allele number parsed for the genomes cohort."),
    ("gnomad_genomes_af", "Allele frequency parsed for the genomes cohort."),
    ("gnomad_genomes_grpmax", "Population label reported by raw gnomAD as the group with the highest observed frequency."),
    ("gnomad_genomes_grpmax_faf95", "Requested grpmax_faf95 slot. In raw gnomAD v4.1 the available tag is faf95, so that raw value is stored here and grpmax names the population."),
    ("gnomad_genomes_depth", "Requested depth slot. The current raw gnomAD site files loaded here do not expose a Depth/DP tag, so this stays null until a depth-bearing source is introduced."),
    ("gnomad_genomes_ac_afr", "African ancestry allele count from raw gnomAD genomes info."),
    ("gnomad_genomes_af_afr", "African ancestry allele frequency from raw gnomAD genomes info."),
    ("gnomad_genomes_ac_eur_proxy", "European proxy allele count built from NFE + FIN + ASJ because raw gnomAD v4.1 does not expose AC_eur directly."),
    ("gnomad_genomes_af_eur_proxy", "European proxy allele frequency built from the same NFE + FIN + ASJ components."),
    ("gnomad_exomes_ac", "Allele count parsed for the exomes cohort after ALT splitting."),
    ("gnomad_exomes_an", "Allele number parsed for the exomes cohort."),
    ("gnomad_exomes_af", "Allele frequency parsed for the exomes cohort."),
    ("gnomad_exomes_grpmax", "Population label reported by raw gnomAD exomes as the group with the highest observed frequency."),
    ("gnomad_exomes_grpmax_faf95", "Requested grpmax_faf95 slot filled from the raw faf95 tag for exomes."),
    ("gnomad_exomes_depth", "Requested depth slot. Null for the current raw site files because Depth/DP is not present in the loaded info payload."),
    ("gnomad_exomes_ac_afr", "African ancestry allele count from raw gnomAD exomes info."),
    ("gnomad_exomes_af_afr", "African ancestry allele frequency from raw gnomAD exomes info."),
    ("gnomad_exomes_ac_eur_proxy", "European proxy allele count built from NFE + FIN + ASJ because raw gnomAD v4.1 does not expose AC_eur directly."),
    ("gnomad_exomes_af_eur_proxy", "European proxy allele frequency built from the same NFE + FIN + ASJ components."),
    ("gme_af", "Overall GME alternate-allele frequency."),
    ("gme_nwa", "North West Africa frequency from GME."),
    ("gme_nea", "North East Africa frequency from GME."),
    ("gme_ap", "Arabian Peninsula frequency from GME."),
    ("gme_israel", "Israel/Jewish subgroup frequency from GME."),
    ("gme_sd", "Syrian Desert subgroup frequency from GME."),
    ("gme_tp", "Turkish Peninsula subgroup frequency from GME."),
    ("gme_ca", "Central Asia subgroup frequency from GME."),
    ("source_count", "How many source streams support the exact allele in this registry row."),
    ("last_refresh_date", "Date when the registry table was rebuilt."),
)

REGISTRY_STEPS: Final[tuple[dict[str, str], ...]] = (
    {
        "id": "scope",
        "title": "Step 1: Limit the scope",
        "simple": "Only chromosomes 13 and 17 are kept here because that is the current gnomAD raw scope and the BRCA work lives on those chromosomes.",
        "technical": "The build filters ClinVar and GME to chr13/chr17 and unions the four frozen gnomAD raw tables already loaded in BigQuery.",
    },
    {
        "id": "gnomad_alleles",
        "title": "Step 2: Split multi-allelic raw rows",
        "simple": "A raw VCF row can carry more than one ALT allele. This step turns one raw row into one row per ALT allele so the join key is exact.",
        "technical": "UNNEST(SPLIT(alt, ',')) WITH OFFSET keeps allele order so AC/AF arrays from info can be aligned with the correct ALT allele.",
    },
    {
        "id": "gnomad_metrics",
        "title": "Step 3: Parse frequency evidence",
        "simple": "The raw gnomAD info text is cracked open into clear columns like AC, AN, AF, African frequencies, and the requested Europe proxy.",
        "technical": "REGEXP_EXTRACT pulls each info token, SPLIT aligns per-allele arrays, and SAFE_CAST prevents parsing crashes when a token is missing.",
    },
    {
        "id": "clinvar_labels",
        "title": "Step 4: Parse ClinVar labels",
        "simple": "Clinical meaning is brought in from ClinVar so the same allele can be seen with both disease labels and population frequencies.",
        "technical": "CLNSIG and CLNREVSTAT are extracted from the raw ClinVar info field after ALT splitting to the same allele-level key.",
    },
    {
        "id": "registry_final",
        "title": "Step 5: Join everything into the first supervisor registry",
        "simple": "All sources meet on one exact allele key so the supervisor can see which source supports each allele and what evidence each source contributes.",
        "technical": "A union of all distinct keys is left-joined to parsed ClinVar, gnomAD, and GME evidence so no available allele is dropped from the final table.",
    },
)


@lru_cache(maxsize=1)
def load_snapshot() -> dict[str, object]:
    if not SNAPSHOT_FILE.exists():
        return {}
    return json.loads(SNAPSHOT_FILE.read_text(encoding="utf-8"))


@lru_cache(maxsize=1)
def snapshot_row_counts() -> dict[str, int]:
    snapshot = load_snapshot()
    metrics = snapshot.get("bigquery_metrics", {})
    tables = metrics.get("tables", []) if isinstance(metrics, dict) else []
    counts: dict[str, int] = {}
    for table in tables:
        if isinstance(table, dict):
            counts[str(table.get("table", ""))] = int(table.get("rows", 0) or 0)
    return counts


def dataset_catalog_payload() -> list[dict[str, object]]:
    counts = snapshot_row_counts()
    payload: list[dict[str, object]] = []
    for entry in RAW_DATASETS.values():
        payload.append(
            {
                "key": entry.key,
                "title": entry.title,
                "table_ref": entry.table_ref,
                "row_count": counts.get(entry.key),
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
        "title": "supervisor_variant_registry_v1",
        "table_ref": REGISTRY_TABLE_REF,
        "scope_note": "The first registry table is allele-level and currently limited to chromosomes 13 and 17 because those are the gnomAD raw chromosomes frozen in this workspace.",
        "accuracy_notes": [
            "Every final row uses one exact allele key: chrom:pos:ref:alt.",
            "Raw multi-allelic VCF rows are split before joining so ALT-specific frequencies stay aligned.",
            "Raw gnomAD v4.1 does not expose AC_eur/AF_eur or grpmax_faf95 or Depth exactly by those names. The table therefore stores Europe proxies and a documented faf95-backed grpmax_faf95 slot instead of inventing unsupported raw fields.",
        ],
        "columns": [
            {"name": name, "description": description}
            for name, description in REGISTRY_COLUMNS
        ],
        "steps": list(REGISTRY_STEPS),
        "build_sql": build_registry_sql(),
    }
