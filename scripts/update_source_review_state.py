"""Build a frozen scientific source-review payload for the supervisor UI.

The output is a static JSON file bundled with the UI so the supervisor can
review source readiness, scientific evidence, and transformation strategy
without triggering live data queries.
"""

from __future__ import annotations

import datetime as dt
import io
import json
import math
from pathlib import Path
from typing import Any, Final

import pandas as pd
from google.cloud import storage

try:
    from scripts.freeze_arab_study_sources import STUDY_SOURCES, apply_extract_spec
except ModuleNotFoundError:
    from freeze_arab_study_sources import STUDY_SOURCES, apply_extract_spec

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
UI_FILE: Final[Path] = ROOT / "ui" / "source_review.json"
SOURCE_FREEZE_FILE: Final[Path] = ROOT / "conductor" / "source-freeze.md"
REVIEW_BUNDLE_FILE: Final[Path] = ROOT / "ui" / "review_bundle.json"
SHGP_LOCAL_FILE: Final[Path] = Path("/Users/macbookpro/Desktop/storage/raw/shgp/Saudi_Arabian_Allele_Frequencies.txt")
AVDB_LOCAL_FILE: Final[Path] = Path("/Users/macbookpro/Desktop/storage/raw/uae/avdb_uae.xlsx")
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
AVDB_LIFTOVER_OBJECT: Final[str] = (
    "frozen/harmonized/source=avdb_uae/version=workbook-created-2025-06-27/"
    "stage=liftover/build=GRCh37_to_GRCh38/snapshot_date=2026-03-13/avdb_uae_liftover.parquet"
)
AVDB_REPORT_OBJECT: Final[str] = (
    "frozen/harmonized/source=avdb_uae/version=workbook-created-2025-06-27/"
    "stage=liftover/build=GRCh37_to_GRCh38/snapshot_date=2026-03-13/avdb_uae_liftover_report.json"
)

WORKFLOW_CATEGORIES: Final[tuple[dict[str, object], ...]] = (
    {
        "id": "raw_freeze",
        "title": "Stage 1: Freeze the raw source package",
        "purpose": "Preserve the exact upstream bytes before any filtering, de-identification, or normalization logic.",
        "evidence_types": [
            "raw GCS prefix",
            "snapshot date",
            "manifest checksum",
            "upstream URL",
        ],
        "output": "A raw source-of-truth artifact under raw/sources/... with a manifest.",
    },
    {
        "id": "deidentify_extract",
        "title": "Stage 2: Build a de-identified working extract when needed",
        "purpose": "Remove direct identifiers from workbook-style cohort sources while preserving enough scientific detail for variant review.",
        "evidence_types": [
            "source sheet name",
            "source row number",
            "source record locator",
            "retained column list",
        ],
        "output": "A frozen extract under frozen/arab_variant_evidence/... ready for source review.",
    },
    {
        "id": "build_review",
        "title": "Stage 3: Review build and coordinate readiness",
        "purpose": "Decide whether a source can enter harmonization directly, needs liftover, or needs a different mapping strategy.",
        "evidence_types": [
            "explicit GRCh38 labels",
            "presence/absence of genomic coordinates",
            "VCF CHROM/POS/REF/ALT structure",
            "HGVS-only versus genomic-coordinate evidence",
        ],
        "output": "A scientific readiness decision with a next exact action.",
    },
    {
        "id": "liftover_decision",
        "title": "Stage 4: Decide liftover versus direct normalization",
        "purpose": "Avoid unnecessary coordinate conversion and keep liftover only for sources that truly require it.",
        "evidence_types": [
            "source build assessment",
            "coordinate field semantics",
            "row-level coordinate validity",
        ],
        "output": "A per-source liftover decision (`not_needed`, `required`, or `blocked pending mapping`).",
    },
    {
        "id": "normalization_entry",
        "title": "Stage 5: Enter normalization only when the source is coordinate-ready",
        "purpose": "Keep normalization restricted to sources that can actually construct a genomic variant key.",
        "evidence_types": [
            "REF/ALT presence",
            "table-style versus VCF-style structure",
            "canonical-key prerequisites",
        ],
        "output": "A clear normalization strategy or a justified block.",
    },
)

USE_TIER_META: Final[dict[str, dict[str, str]]] = {
    "adopted_100": {
        "label": "Adopted 100%",
        "summary": "Core input",
    },
    "adopted_secondary": {
        "label": "Supporting source",
        "summary": "Used as supporting evidence",
    },
    "reference_only": {
        "label": "Reference only",
        "summary": "Kept for context and audit",
    },
    "demo_only": {
        "label": "Demo only",
        "summary": "Useful for walkthroughs, not a core evidence stream",
    },
    "blocked": {
        "label": "Blocked",
        "summary": "Frozen only until scientific gaps are resolved",
    },
}

WORKFLOW_POSITION_RULES: Final[dict[str, dict[str, object]]] = {
    "clinvar": {
        "raw_stage": "Frozen raw sample is shown on the Raw Evidence page.",
        "brca_stage": "Direct BRCA1/BRCA2 extraction from GRCh38 VCF rows.",
        "final_stage": "Included in the baseline tables and in the Arab extension tables.",
        "included_in_current_final": True,
    },
    "gnomad_genomes": {
        "raw_stage": "Frozen raw chr13 and chr17 genome samples are shown on the Raw Evidence page.",
        "brca_stage": "Direct BRCA1/BRCA2 extraction with INFO parsing from GRCh38 genome VCF rows.",
        "final_stage": "Included in the baseline tables and in the Arab extension tables.",
        "included_in_current_final": True,
    },
    "gnomad_exomes": {
        "raw_stage": "Frozen raw chr13 and chr17 exome samples are shown on the Raw Evidence page.",
        "brca_stage": "Direct BRCA1/BRCA2 extraction with INFO parsing from GRCh38 exome VCF rows.",
        "final_stage": "Included in the baseline tables and in the Arab extension tables.",
        "included_in_current_final": True,
    },
    "gme_hg38": {
        "raw_stage": "Frozen raw summary-table sample is shown on the Raw Evidence page.",
        "brca_stage": "Converted to canonical BRCA rows after handling chrom/start/end/ref/alt.",
        "final_stage": "Included only in the baseline final table and the Arab final table.",
        "included_in_current_final": True,
    },
    "shgp_saudi_af": {
        "raw_stage": "Frozen raw Saudi frequency sample is shown on the Raw Evidence page.",
        "brca_stage": "Converted to canonical BRCA rows with direct CHROM/POS/REF/ALT parsing.",
        "final_stage": "Included in the Arab draft table and the Arab final table.",
        "included_in_current_final": True,
    },
    "avdb_uae": {
        "raw_stage": "Frozen workbook and lifted sample are shown on the build-conversion page.",
        "brca_stage": "Liftover completed, but the current release has zero BRCA rows and remains reference-only.",
        "final_stage": "Excluded from the current baseline and Arab extension tables.",
        "included_in_current_final": False,
    },
    "saudi_breast_cancer_pmc10474689": {
        "raw_stage": "Frozen de-identified workbook extract is shown on the Raw Evidence page.",
        "brca_stage": "Blocked before BRCA normalization because genomic coordinates are still missing.",
        "final_stage": "Excluded from the current baseline and Arab extension tables.",
        "included_in_current_final": False,
    },
    "uae_brca_pmc12011969": {
        "raw_stage": "Frozen de-identified BRCA cohort sample is shown on the Raw Evidence page.",
        "brca_stage": "Awaiting row-level coordinate parsing before any BRCA normalization decision.",
        "final_stage": "Excluded from the current baseline and Arab extension tables; may enter a later Arab-specific table if parsing succeeds.",
        "included_in_current_final": False,
    },
}

SCIENTIFIC_RULES: Final[dict[str, dict[str, object]]] = {
    "clinvar": {
        "display_name": "ClinVar GRCh38 VCF",
        "category": "Global clinical classification anchor",
        "source_kind": "VCF",
        "source_build": "GRCh38",
        "coordinate_readiness": "Genomic coordinates ready",
        "liftover_decision": "not_needed",
        "normalization_decision": "Split multiallelic alleles and normalize VCF representation",
        "brca_relevance": "Direct",
        "review_status": "ready",
        "evidence": [
            "Upstream path is the GRCh38 ClinVar VCF (`vcf_GRCh38`).",
            "Raw rows preserve CHROM, POS, REF, ALT, and INFO tags required for downstream parsing.",
        ],
        "project_fit": "adopted_100",
        "project_fit_note": "Primary clinical truth source. Keep as a mandatory anchor for downstream BRCA interpretation.",
        "next_action": "Carry ClinVar rows directly into BRCA filtering and allele normalization.",
        "sample_keys": ["clinvar_raw_brca_window"],
    },
    "gnomad_genomes": {
        "display_name": "gnomAD v4.1 genomes (chr13 + chr17)",
        "category": "Global genome frequency baseline",
        "source_kind": "VCF",
        "source_build": "GRCh38",
        "coordinate_readiness": "Genomic coordinates ready",
        "liftover_decision": "not_needed",
        "normalization_decision": "Split multiallelic alleles and parse INFO tags into canonical frequency fields",
        "brca_relevance": "Direct",
        "review_status": "ready",
        "evidence": [
            "Frozen sources are gnomAD v4.1 GRCh38 VCFs for BRCA chromosomes chr13 and chr17.",
            "Genome and exome cohorts remain separate until harmonization logic combines them deliberately.",
        ],
        "project_fit": "adopted_100",
        "project_fit_note": "Primary global population baseline. Keep as a mandatory comparator for allele-frequency interpretation.",
        "next_action": "Normalize chr13 and chr17 genome rows after BRCA window filtering.",
        "sample_keys": ["gnomad_genomes_raw_brca_window"],
    },
    "gnomad_exomes": {
        "display_name": "gnomAD v4.1 exomes (chr13 + chr17)",
        "category": "Global exome frequency baseline",
        "source_kind": "VCF",
        "source_build": "GRCh38",
        "coordinate_readiness": "Genomic coordinates ready",
        "liftover_decision": "not_needed",
        "normalization_decision": "Split multiallelic alleles and parse INFO tags into canonical frequency fields",
        "brca_relevance": "Direct",
        "review_status": "ready",
        "evidence": [
            "Frozen sources are gnomAD v4.1 GRCh38 VCFs for BRCA chromosomes chr13 and chr17.",
            "Exome rows must remain distinguishable from genome rows until the final checkpoint logic merges them.",
        ],
        "project_fit": "adopted_100",
        "project_fit_note": "Primary global exome baseline. Keep alongside genomes so exome-specific frequency signals remain visible.",
        "next_action": "Normalize chr13 and chr17 exome rows after BRCA window filtering.",
        "sample_keys": ["gnomad_exomes_raw_brca_window"],
    },
    "gme_hg38": {
        "display_name": "GME hg38 summary table",
        "category": "Arab / Middle Eastern summary-frequency layer",
        "source_kind": "Summary table",
        "source_build": "GRCh38",
        "coordinate_readiness": "Coordinates present but table-style, not VCF-style",
        "liftover_decision": "not_needed",
        "normalization_decision": "Construct canonical keys from chrom/start/end/ref/alt and document summary-table assumptions",
        "brca_relevance": "Direct",
        "review_status": "partial",
        "evidence": [
            "The frozen source is explicitly labeled hg38.",
            "The raw table includes chrom/start/end/ref/alt plus GME subgroup frequencies rather than a native VCF INFO structure.",
        ],
        "project_fit": "adopted_secondary",
        "project_fit_note": (
            "Keep as a secondary Arab/MENA frequency layer. It is usable and relevant, "
            "but it is a summary table and not the strongest primary population baseline."
        ),
        "next_action": "Keep as the supporting Arab/MENA layer that is added only after the draft table is frozen.",
        "sample_keys": ["gme_raw_brca_window"],
    },
    "shgp_saudi_af": {
        "display_name": "SHGP Saudi allele-frequency table",
        "category": "Arab population-frequency baseline",
        "source_kind": "TSV frequency table",
        "source_build": "GRCh38",
        "coordinate_readiness": "Genomic coordinates ready",
        "liftover_decision": "not_needed",
        "normalization_decision": "Parse CHROM/POS/REF/ALT directly and normalize alleles in Phase 3",
        "brca_relevance": "Direct",
        "review_status": "ready",
        "evidence": [
            "Local SHGP file matched the official Figshare MD5 exactly during the 2026-03-13 freeze run.",
            "The source exposes CHROM/POS/REF/ALT plus AC/AN and contains 1,607 rows inside the BRCA1/BRCA2 windows.",
        ],
        "project_fit": "adopted_100",
        "project_fit_note": (
            "Use as a primary Arab frequency source. It is large, genome-wide, GRCh38, and directly relevant to BRCA windows."
        ),
        "next_action": "Keep in the active Arab extension workflow and carry it forward into downstream master-dataset work.",
        "sample_mode": "shgp_raw",
    },
    "avdb_uae": {
        "display_name": "AVDB Emirati workbook",
        "category": "Arab curated clinical-frequency workbook",
        "source_kind": "Workbook",
        "source_build": "GRCh37 workbook with GRCh38 liftover checkpoint",
        "coordinate_readiness": "Genomic HGVS on GRCh37 parsed and lifted to GRCh38",
        "liftover_decision": "required_and_completed",
        "normalization_decision": "Liftover stage is complete; allele normalization remains a later step",
        "brca_relevance": "Indirect",
        "review_status": "partial",
        "evidence": [
            "799 of 801 data rows were parsed and lifted to GRCh38 successfully; the 2 non-success rows are workbook footer noise rather than biological variants.",
            "The workbook contains zero BRCA1/BRCA2 rows, so it does not materially affect the current BRCA-focused pipeline.",
        ],
        "project_fit": "reference_only",
        "project_fit_note": (
            "Keep as secondary/reference evidence only. The liftover is scientifically valid, "
            "but the workbook is small, curated, and not BRCA-relevant in the current cohort."
        ),
        "next_action": "Retain the lifted checkpoint for audit/reference and reconsider only if future work needs non-BRCA Emirati context.",
        "sample_mode": "avdb_liftover",
    },
    "saudi_breast_cancer_pmc10474689": {
        "display_name": "Saudi breast cancer supplement (PMC10474689)",
        "category": "Arab clinical publication supplement",
        "source_kind": "Supplementary workbook",
        "source_build": "unknown at genomic-coordinate level",
        "coordinate_readiness": "HGVS-only in retained sheet; no genomic coordinates",
        "liftover_decision": "blocked pending transcript-to-genome mapping",
        "normalization_decision": "Do not normalize until genomic coordinates can be resolved",
        "brca_relevance": "Indirect until BRCA rows are confirmed in the retained table",
        "review_status": "blocked",
        "evidence": [
            "The retained Table S5 extract contains gene and transcript/protein HGVS fields but no genomic coordinate column.",
            "Current sample rows show cross-gene pathogenic carriers, so BRCA-specific relevance still needs confirmation inside the retained sheet.",
        ],
        "project_fit": "blocked",
        "project_fit_note": "Keep frozen as source evidence only. Do not use it downstream until transcript-to-genome mapping is justified.",
        "next_action": "Review Table S5 for BRCA-specific rows and map transcript HGVS to genomic coordinates before inclusion in harmonization.",
        "extract_key": "saudi_variant_carriers",
    },
    "uae_brca_pmc12011969": {
        "display_name": "UAE BRCA supplement (PMC12011969)",
        "category": "Arab BRCA cohort supplement",
        "source_kind": "Supplementary workbook",
        "source_build": "GRCh38 claimed at column level",
        "coordinate_readiness": "Mutation-positive rows retain a `Chr location (hg38)` field",
        "liftover_decision": "not_needed for valid hg38 rows",
        "normalization_decision": "Parse `Chr location (hg38)` and mutation fields into genomic alleles before normalization",
        "brca_relevance": "Direct",
        "review_status": "partial",
        "evidence": [
            "Both extracted UAE sheets retain `Mutations`, `HGVS`, and `Chr location (hg38)`.",
            "Only mutation-positive rows are carried forward into the de-identified extracts, so the working set is narrower than the raw workbook.",
        ],
        "project_fit": "demo_only",
        "project_fit_note": (
            "Keep as small targeted BRCA cohort evidence. It can help case-context review, "
            "but it is not a population-frequency baseline."
        ),
        "next_action": "Validate the `Chr location (hg38)` field row-by-row and resolve allele syntax before canonical-key construction.",
        "extract_key": "uae_mutation_positive_rows",
    },
}


def parse_source_freeze_register(markdown: str) -> dict[str, dict[str, str]]:
    """Read the source-freeze markdown table into a keyed lookup."""
    entries: dict[str, dict[str, str]] = {}
    for line in markdown.splitlines():
        if not line.startswith("| ") or line.startswith("| :---"):
            continue
        columns = [column.strip() for column in line.split("|")[1:-1]]
        if columns[0] == "Source":
            continue
        if len(columns) != 6:
            continue
        entries[columns[0]] = {
            "source": columns[0],
            "source_version": columns[1],
            "snapshot_date": columns[2],
            "upstream_url": columns[3],
            "raw_vault_prefix": columns[4].strip("`"),
            "notes": columns[5],
        }
    return entries


def clean_value(value: Any) -> Any:
    """Normalize pandas/JSON values so the frozen UI payload stays readable."""
    if value is None:
        return None
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, pd.Timestamp):
        return value.isoformat()
    return value


def compact_rows(frame: pd.DataFrame, limit: int = 10) -> dict[str, object]:
    """Serialize a small preview table for the supervisor UI."""
    preview = frame.head(limit).copy()
    preview = preview.where(pd.notna(preview), None)
    columns = list(preview.columns)
    rows = [
        {column: clean_value(value) for column, value in row.items()}
        for row in preview.to_dict(orient="records")
    ]
    return {"columns": columns, "rows": rows}


def load_review_bundle() -> dict[str, object]:
    return json.loads(REVIEW_BUNDLE_FILE.read_text(encoding="utf-8"))


def build_raw_lookup(review_bundle: dict[str, object]) -> dict[str, dict[str, object]]:
    """Index the frozen raw samples and counts already approved for UI display."""
    return {
        entry["key"]: entry
        for entry in review_bundle["raw_datasets"]["datasets"]
    }


def build_arab_extract_samples(limit: int = 10) -> dict[str, dict[str, object]]:
    """Generate small evidence previews from the frozen Arab study extracts."""
    samples: dict[str, dict[str, object]] = {}

    for source in STUDY_SOURCES:
        if source.slug == "saudi_breast_cancer_pmc10474689":
            spec = next(spec for spec in source.extracts if spec.output_slug == "variant_carriers")
            frame = pd.read_excel(source.local_source, sheet_name=spec.sheet_name)
            extracted = apply_extract_spec(frame, spec)
            samples["saudi_variant_carriers"] = {
                "title": "Saudi Table S5 de-identified extract",
                "sample": compact_rows(extracted, limit=limit),
                "row_count": int(len(extracted)),
            }
        if source.slug == "uae_brca_pmc12011969":
            frames = []
            for spec in source.extracts:
                frame = pd.read_excel(source.local_source, sheet_name=spec.sheet_name)
                extracted = apply_extract_spec(frame, spec).copy()
                extracted.insert(0, "extract_slug", spec.output_slug)
                frames.append(extracted)
            combined = pd.concat(frames, ignore_index=True)
            samples["uae_mutation_positive_rows"] = {
                "title": "UAE mutation-positive de-identified extracts",
                "sample": compact_rows(combined, limit=limit),
                "row_count": int(len(combined)),
            }
    return samples


def shgp_profile(limit: int = 10) -> dict[str, object]:
    sample_frame = pd.read_csv(
        SHGP_LOCAL_FILE,
        sep="\t",
        skiprows=lambda index: index < 7,
        nrows=limit,
    ).rename(columns={"#CHROM": "CHROM"})
    brca_counts = {"BRCA1_window_rows": 0, "BRCA2_window_rows": 0}
    with SHGP_LOCAL_FILE.open("r", encoding="utf-8") as handle:
        next(handle)
        for line in handle:
            columns = line.rstrip("\n").split("\t")
            if len(columns) < 2:
                continue
            chrom, pos_text = columns[0], columns[1]
            if pos_text == "POS" or not pos_text.isdigit():
                continue
            pos = int(pos_text)
            if chrom in {"13", "chr13"} and 32315086 <= pos <= 32400268:
                brca_counts["BRCA2_window_rows"] += 1
            if chrom in {"17", "chr17"} and 43044295 <= pos <= 43170245:
                brca_counts["BRCA1_window_rows"] += 1
    row_count = sum(1 for _ in SHGP_LOCAL_FILE.open("r", encoding="utf-8")) - 1
    return {
        "row_count": row_count,
        "brca_counts": brca_counts,
        "sample": compact_rows(sample_frame, limit=limit),
    }


def load_avdb_liftover_assets(limit: int = 10) -> dict[str, object]:
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    report = json.loads(bucket.blob(AVDB_REPORT_OBJECT).download_as_text())
    parquet_bytes = bucket.blob(AVDB_LIFTOVER_OBJECT).download_as_bytes()
    frame = pd.read_parquet(io.BytesIO(parquet_bytes))
    return {
        "report": report,
        "sample": compact_rows(
            frame.loc[frame["liftover_status"].eq("success"), [
                "gene_symbol",
                "hgvs_genomic_grch37",
                "chrom37",
                "start37",
                "end37",
                "chrom38",
                "start38",
                "end38",
                "liftover_status",
            ]],
            limit=limit,
        ),
        "artifacts": {
            "parquet_uri": f"gs://{BUCKET_NAME}/{AVDB_LIFTOVER_OBJECT}",
            "report_uri": f"gs://{BUCKET_NAME}/{AVDB_REPORT_OBJECT}",
        },
    }


def build_raw_manifest_uri(snapshot_entry: dict[str, str] | None) -> str | None:
    if snapshot_entry is None:
        return None
    raw_prefix = snapshot_entry.get("raw_vault_prefix")
    if not raw_prefix:
        return None
    return raw_prefix.rstrip("/") + "/manifest.json"


def storage_console_url(uri: str) -> str:
    if not uri.startswith("gs://"):
        return uri
    bucket_and_path = uri.removeprefix("gs://")
    if "/" not in bucket_and_path:
        return f"https://storage.cloud.google.com/{bucket_and_path}"
    bucket_name, object_name = bucket_and_path.split("/", 1)
    return f"https://storage.cloud.google.com/{bucket_name}/{object_name}"


def build_source_decision_summary(entries: list[dict[str, object]]) -> list[dict[str, object]]:
    summary: list[dict[str, object]] = []
    ordered_tiers = [
        "adopted_100",
        "adopted_secondary",
        "reference_only",
        "demo_only",
        "blocked",
    ]
    for tier in ordered_tiers:
        members = [entry["display_name"] for entry in entries if entry["use_tier"] == tier]
        if not members:
            continue
        summary.append(
            {
                "tier": tier,
                "label": USE_TIER_META[tier]["label"],
                "summary": USE_TIER_META[tier]["summary"],
                "count": len(members),
                "members": members,
            }
        )
    return summary


def build_source_entry(
    source_key: str,
    freeze_lookup: dict[str, dict[str, str]],
    raw_lookup: dict[str, dict[str, object]],
    arab_extract_samples: dict[str, dict[str, object]],
) -> dict[str, object]:
    """Compose one review-ready source record with evidence and the next exact action."""
    rule = SCIENTIFIC_RULES[source_key]

    snapshot_entry: dict[str, str] | None = None
    row_count = None
    sample = None
    extra_notes: list[str] = []
    extra_links: list[dict[str, str]] = []
    lift_details: dict[str, object] | None = None

    if source_key == "gnomad_genomes":
        row_count = sum(int(raw_lookup[key]["row_count"]) for key in rule["sample_keys"])
        snapshot_entry = freeze_lookup["gnomad_v4.1_genomes_chr13"]
    elif source_key == "gnomad_exomes":
        row_count = sum(int(raw_lookup[key]["row_count"]) for key in rule["sample_keys"])
        snapshot_entry = freeze_lookup["gnomad_v4.1_exomes_chr13"]
    elif source_key in {"clinvar", "gme_hg38"}:
        freeze_key = source_key
        snapshot_entry = freeze_lookup[freeze_key]
        row_count = int(raw_lookup[rule["sample_keys"][0]]["row_count"])
        sample = raw_lookup[rule["sample_keys"][0]]["sample"]
    elif source_key == "shgp_saudi_af":
        snapshot_entry = freeze_lookup[source_key]
        shgp = shgp_profile()
        row_count = int(shgp["row_count"])
        sample = shgp["sample"]
        extra_notes.append(
            f"BRCA window rows in SHGP: BRCA1={shgp['brca_counts']['BRCA1_window_rows']}, "
            f"BRCA2={shgp['brca_counts']['BRCA2_window_rows']}."
        )
    elif source_key == "avdb_uae":
        snapshot_entry = freeze_lookup[source_key]
        avdb = load_avdb_liftover_assets(limit=10)
        row_count = int(avdb["report"]["counts"]["total_rows"])
        sample = avdb["sample"]
        extra_notes.extend(
            [
                f"AVDB liftover success={avdb['report']['counts']['liftover_success_rows']} / total={avdb['report']['counts']['total_rows']}.",
                f"AVDB BRCA rows={avdb['report']['counts']['brca_rows']}.",
                avdb["report"]["workflow_summary"],
            ]
        )
        lift_details = {
            "why_needed": (
                "AVDB stores genomic coordinates as HGVS on GRCh37. The project canonical build is GRCh38, "
                "so keeping AVDB on GRCh37 would make direct joins with ClinVar, gnomAD, SHGP, and GME unsafe."
            ),
            "how_it_worked": [
                "Use `HGVS_Genomic_GRCh37` as the row-level source-of-truth coordinate field.",
                "Parse each genomic HGVS string into RefSeq accession, interval, and event type.",
                "Map the RefSeq accession to a chromosome with the official NCBI GRCh37 assembly report.",
                "Lift the genomic interval to GRCh38 with the official Ensembl GRCh37-to-GRCh38 assembly map endpoint.",
                "Keep both the original GRCh37 interval and the mapped GRCh38 interval, and mark failures explicitly instead of dropping them.",
            ],
            "counts": avdb["report"]["counts"],
            "workflow_summary": avdb["report"]["workflow_summary"],
            "official_sources": [
                avdb["report"]["official_sources"]["avdb_downloads_page"],
                avdb["report"]["official_sources"]["ncbi_grch37_assembly_report"],
                avdb["report"]["official_sources"]["ncbi_grch38_assembly_report"],
                avdb["report"]["official_sources"]["ensembl_map_api_template"],
            ],
            "report_uri": avdb["artifacts"]["report_uri"],
            "parquet_uri": avdb["artifacts"]["parquet_uri"],
            "failure_examples": avdb["report"]["failure_examples"],
        }
    else:
        snapshot_entry = freeze_lookup[source_key]
        extract_payload = arab_extract_samples[rule["extract_key"]]
        row_count = int(extract_payload["row_count"])
        sample = extract_payload["sample"]

    manifest_uri = build_raw_manifest_uri(snapshot_entry)
    if snapshot_entry and snapshot_entry.get("upstream_url"):
        extra_links.append({"label": "Upstream source", "url": storage_console_url(snapshot_entry["upstream_url"])})
    if snapshot_entry and snapshot_entry.get("raw_vault_prefix"):
        extra_links.append({"label": "Raw vault prefix", "url": storage_console_url(snapshot_entry["raw_vault_prefix"])})
    if manifest_uri:
        extra_links.append({"label": "Raw manifest", "url": storage_console_url(manifest_uri)})
    if lift_details is not None:
        extra_links.append({"label": "Liftover report", "url": storage_console_url(str(lift_details["report_uri"]))})
        extra_links.append({"label": "Lifted Parquet", "url": storage_console_url(str(lift_details["parquet_uri"]))})

    use_tier = str(rule["project_fit"])
    tier_meta = USE_TIER_META[use_tier]
    workflow_position = WORKFLOW_POSITION_RULES[source_key]

    entry = {
        "source_key": source_key,
        "display_name": rule["display_name"],
        "category": rule["category"],
        "source_kind": rule["source_kind"],
        "source_build": rule["source_build"],
        "coordinate_readiness": rule["coordinate_readiness"],
        "liftover_decision": rule["liftover_decision"],
        "normalization_decision": rule["normalization_decision"],
        "brca_relevance": rule["brca_relevance"],
        "review_status": rule["review_status"],
        "project_fit": rule["project_fit"],
        "project_fit_note": rule["project_fit_note"],
        "use_tier": use_tier,
        "use_tier_label": tier_meta["label"],
        "use_tier_summary": tier_meta["summary"],
        "snapshot_date": snapshot_entry["snapshot_date"] if snapshot_entry else None,
        "source_version": snapshot_entry["source_version"] if snapshot_entry else None,
        "upstream_url": snapshot_entry["upstream_url"] if snapshot_entry else None,
        "raw_vault_prefix": snapshot_entry["raw_vault_prefix"] if snapshot_entry else None,
        "raw_manifest_uri": manifest_uri,
        "row_count": row_count,
        "notes": [*rule["evidence"], *extra_notes, snapshot_entry["notes"]] if snapshot_entry else [*rule["evidence"], *extra_notes],
        "artifact_links": extra_links,
        "workflow_position": workflow_position,
        "next_action": rule["next_action"],
    }
    if lift_details is not None:
        entry["liftover_method"] = lift_details
    if sample:
        entry["sample"] = sample
    return entry


def build_source_review_payload() -> dict[str, object]:
    """Assemble the full frozen scientific-review payload."""
    freeze_lookup = parse_source_freeze_register(SOURCE_FREEZE_FILE.read_text(encoding="utf-8"))
    raw_lookup = build_raw_lookup(load_review_bundle())
    arab_extract_samples = build_arab_extract_samples(limit=10)

    sources = [
        build_source_entry("clinvar", freeze_lookup, raw_lookup, arab_extract_samples),
        build_source_entry("gnomad_genomes", freeze_lookup, raw_lookup, arab_extract_samples),
        build_source_entry("gnomad_exomes", freeze_lookup, raw_lookup, arab_extract_samples),
        build_source_entry("gme_hg38", freeze_lookup, raw_lookup, arab_extract_samples),
        build_source_entry("shgp_saudi_af", freeze_lookup, raw_lookup, arab_extract_samples),
        build_source_entry("avdb_uae", freeze_lookup, raw_lookup, arab_extract_samples),
        build_source_entry("saudi_breast_cancer_pmc10474689", freeze_lookup, raw_lookup, arab_extract_samples),
        build_source_entry("uae_brca_pmc12011969", freeze_lookup, raw_lookup, arab_extract_samples),
    ]

    return {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "workflow_categories": list(WORKFLOW_CATEGORIES),
        "decision_summary": build_source_decision_summary(sources),
        "sources": sources,
    }


def main() -> None:
    # [AI-Agent: Codex]: Review Stage 1 - build a supervisor-facing scientific review from frozen evidence only.
    payload = build_source_review_payload()
    UI_FILE.write_text(json.dumps(payload, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    print(
        f"Wrote {UI_FILE} "
        f"(generated_at={payload['generated_at']}, sources={len(payload['sources'])})"
    )


if __name__ == "__main__":
    main()
