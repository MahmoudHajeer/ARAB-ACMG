"""Build a frozen scientific source-review payload for the supervisor UI.

The output is a static JSON file bundled with the UI so the supervisor can
review source readiness, scientific evidence, and transformation strategy
without triggering live data queries.
"""

from __future__ import annotations

import datetime as dt
import json
import math
from pathlib import Path
from typing import Any, Final

import pandas as pd

try:
    from scripts.freeze_arab_study_sources import STUDY_SOURCES, apply_extract_spec
except ModuleNotFoundError:
    from freeze_arab_study_sources import STUDY_SOURCES, apply_extract_spec

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
UI_FILE: Final[Path] = ROOT / "ui" / "source_review.json"
SOURCE_FREEZE_FILE: Final[Path] = ROOT / "conductor" / "source-freeze.md"
REVIEW_BUNDLE_FILE: Final[Path] = ROOT / "ui" / "review_bundle.json"

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
        "next_action": "Carry ClinVar rows directly into BRCA filtering and allele normalization.",
        "sample_keys": ["clinvar_raw_vcf"],
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
        "next_action": "Normalize chr13 and chr17 genome rows after BRCA window filtering.",
        "sample_keys": ["gnomad_v4_1_genomes_chr13_raw", "gnomad_v4_1_genomes_chr17_raw"],
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
        "next_action": "Normalize chr13 and chr17 exome rows after BRCA window filtering.",
        "sample_keys": ["gnomad_v4_1_exomes_chr13_raw", "gnomad_v4_1_exomes_chr17_raw"],
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
        "next_action": "Validate how `start`/`end` map onto the canonical key and then normalize as a summary-frequency input.",
        "sample_keys": ["gme_hg38_raw"],
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


def compact_rows(frame: pd.DataFrame, limit: int = 5) -> dict[str, object]:
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


def build_arab_extract_samples(limit: int = 5) -> dict[str, dict[str, object]]:
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
    else:
        snapshot_entry = freeze_lookup[source_key]
        extract_payload = arab_extract_samples[rule["extract_key"]]
        row_count = int(extract_payload["row_count"])
        sample = extract_payload["sample"]

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
        "snapshot_date": snapshot_entry["snapshot_date"] if snapshot_entry else None,
        "source_version": snapshot_entry["source_version"] if snapshot_entry else None,
        "upstream_url": snapshot_entry["upstream_url"] if snapshot_entry else None,
        "raw_vault_prefix": snapshot_entry["raw_vault_prefix"] if snapshot_entry else None,
        "row_count": row_count,
        "notes": [*rule["evidence"], snapshot_entry["notes"]] if snapshot_entry else list(rule["evidence"]),
        "next_action": rule["next_action"],
    }
    if sample:
        entry["sample"] = sample
    return entry


def build_source_review_payload() -> dict[str, object]:
    """Assemble the full frozen scientific-review payload."""
    freeze_lookup = parse_source_freeze_register(SOURCE_FREEZE_FILE.read_text(encoding="utf-8"))
    raw_lookup = build_raw_lookup(load_review_bundle())
    arab_extract_samples = build_arab_extract_samples(limit=5)

    return {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "workflow_categories": list(WORKFLOW_CATEGORIES),
        "sources": [
            build_source_entry("clinvar", freeze_lookup, raw_lookup, arab_extract_samples),
            build_source_entry("gnomad_genomes", freeze_lookup, raw_lookup, arab_extract_samples),
            build_source_entry("gnomad_exomes", freeze_lookup, raw_lookup, arab_extract_samples),
            build_source_entry("gme_hg38", freeze_lookup, raw_lookup, arab_extract_samples),
            build_source_entry("saudi_breast_cancer_pmc10474689", freeze_lookup, raw_lookup, arab_extract_samples),
            build_source_entry("uae_brca_pmc12011969", freeze_lookup, raw_lookup, arab_extract_samples),
        ],
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
