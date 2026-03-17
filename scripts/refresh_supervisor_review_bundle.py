"""Compose the supervisor review bundle from frozen artifacts only.

This stage is intentionally cheaper than the main normalization build. It does
not recompute ClinVar/gnomAD/SHGP/GME artifacts. Instead it:
1. Loads the current T003 frozen bundle written by the normalization pipeline.
2. Loads the historical legacy bundle that predates the Arab extension.
3. Publishes CSV downloads for every derived artifact that is shown in the UI.
4. Rewrites the supervisor bundle so the baseline final table stays separate
   from the Arab-extension checkpoint tables.

The result is a supervisor-facing review surface that reads like a scientific
logbook rather than a developer console.
"""

from __future__ import annotations

import json
import logging
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Final

import pandas as pd
from google.cloud import storage

try:
    from scripts.gcs_public_policy import (
        BUCKET_NAME,
        attachment_header_value,
        default_action_label,
        gcs_access_profile,
        is_public_safe_gcs_uri,
        object_public_url,
        parse_gs_uri,
        public_url_for_gs_uri,
    )
    from scripts.runtime_config import project_id
except ModuleNotFoundError:
    from gcs_public_policy import (  # type: ignore[no-redef]
        BUCKET_NAME,
        attachment_header_value,
        default_action_label,
        gcs_access_profile,
        is_public_safe_gcs_uri,
        object_public_url,
        parse_gs_uri,
        public_url_for_gs_uri,
    )
    from runtime_config import project_id  # type: ignore[no-redef]

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
UI_DIR: Final[Path] = ROOT / "ui"
PROJECT_ID: Final[str] = project_id()
CURRENT_BUNDLE_PATH: Final[Path] = UI_DIR / "review_bundle.json"
LEGACY_BUNDLE_URI: Final[str] = (
    "gs://mahmoud-arab-acmg-research-data/"
    "frozen/supervisor_review/snapshot_date=2026-03-12/review_bundle.json"
)
REVIEW_BUNDLE_OBJECT: Final[str] = "frozen/review_bundle/snapshot_date=2026-03-14/review_bundle.json"

LOGGER = logging.getLogger("refresh_supervisor_review_bundle")

RAW_SOURCE_LINK_MAP: Final[dict[str, str]] = {
    "clinvar_raw_brca_window": "clinvar",
    "gnomad_genomes_raw_brca_window": "gnomad_genomes",
    "gnomad_exomes_raw_brca_window": "gnomad_exomes",
    "shgp_raw_brca_window": "shgp_saudi_af",
    "gme_raw_brca_window": "gme_hg38",
}

RAW_SOURCE_FILES: Final[dict[str, list[dict[str, str]]]] = {
    "clinvar_raw_brca_window": [
        {"label": "Raw VCF", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/clinvar/lastmod-20260302/snapshot_date=2026-03-03/clinvar.vcf.gz", "kind": "source_data"},
        {"label": "Index", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/clinvar/lastmod-20260302/snapshot_date=2026-03-03/clinvar.vcf.gz.tbi", "kind": "index"},
        {"label": "Manifest", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/clinvar/lastmod-20260302/snapshot_date=2026-03-03/manifest.json", "kind": "manifest"},
    ],
    "gnomad_genomes_raw_brca_window": [
        {"label": "chr13 VCF.BGZ", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr13/snapshot_date=2026-03-03/gnomad.genomes.v4.1.sites.chr13.vcf.bgz", "kind": "source_data"},
        {"label": "chr13 index", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr13/snapshot_date=2026-03-03/gnomad.genomes.v4.1.sites.chr13.vcf.bgz.tbi", "kind": "index"},
        {"label": "chr13 manifest", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr13/snapshot_date=2026-03-03/manifest.json", "kind": "manifest"},
        {"label": "chr17 VCF.BGZ", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr17/snapshot_date=2026-03-03/gnomad.genomes.v4.1.sites.chr17.vcf.bgz", "kind": "source_data"},
        {"label": "chr17 index", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr17/snapshot_date=2026-03-03/gnomad.genomes.v4.1.sites.chr17.vcf.bgz.tbi", "kind": "index"},
        {"label": "chr17 manifest", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=genomes/chrom=chr17/snapshot_date=2026-03-03/manifest.json", "kind": "manifest"},
    ],
    "gnomad_exomes_raw_brca_window": [
        {"label": "chr13 VCF.BGZ", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr13/snapshot_date=2026-03-03/gnomad.exomes.v4.1.sites.chr13.vcf.bgz", "kind": "source_data"},
        {"label": "chr13 index", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr13/snapshot_date=2026-03-03/gnomad.exomes.v4.1.sites.chr13.vcf.bgz.tbi", "kind": "index"},
        {"label": "chr13 manifest", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr13/snapshot_date=2026-03-03/manifest.json", "kind": "manifest"},
        {"label": "chr17 VCF.BGZ", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr17/snapshot_date=2026-03-03/gnomad.exomes.v4.1.sites.chr17.vcf.bgz", "kind": "source_data"},
        {"label": "chr17 index", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr17/snapshot_date=2026-03-03/gnomad.exomes.v4.1.sites.chr17.vcf.bgz.tbi", "kind": "index"},
        {"label": "chr17 manifest", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gnomad_v4.1/release=4.1/cohort=exomes/chrom=chr17/snapshot_date=2026-03-03/manifest.json", "kind": "manifest"},
    ],
    "shgp_raw_brca_window": [
        {"label": "Raw frequency table", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/shgp_saudi_af/version=figshare-28059686-v1/snapshot_date=2026-03-13/Saudi_Arabian_Allele_Frequencies.txt", "kind": "source_data"},
        {"label": "Manifest", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/shgp_saudi_af/version=figshare-28059686-v1/snapshot_date=2026-03-13/manifest.json", "kind": "manifest"},
    ],
    "gme_raw_brca_window": [
        {"label": "Raw summary table", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gme/release=20161025-hg38/build=hg38/snapshot_date=2026-03-08/hg38_gme.txt.gz", "kind": "source_data"},
        {"label": "Manifest", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gme/release=20161025-hg38/build=hg38/snapshot_date=2026-03-08/manifest.json", "kind": "manifest"},
    ],
}

AVDB_STANDARDIZATION_FILES: Final[list[dict[str, str]]] = [
    {"label": "Raw workbook", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/avdb_uae/version=workbook-created-2025-06-27/build=GRCh37/snapshot_date=2026-03-13/avdb_uae.xlsx", "kind": "workbook"},
    {"label": "Raw manifest", "uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/avdb_uae/version=workbook-created-2025-06-27/build=GRCh37/snapshot_date=2026-03-13/manifest.json", "kind": "manifest"},
    {"label": "Lifted parquet", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/harmonized/source=avdb_uae/version=workbook-created-2025-06-27/stage=liftover/build=GRCh37_to_GRCh38/snapshot_date=2026-03-13/avdb_uae_liftover.parquet", "kind": "parquet"},
    {"label": "Liftover report", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/harmonized/source=avdb_uae/version=workbook-created-2025-06-27/stage=liftover/build=GRCh37_to_GRCh38/snapshot_date=2026-03-13/avdb_uae_liftover_report.json", "kind": "report"},
]

REFERENCE_STUDY_FILES: Final[dict[str, list[dict[str, str]]]] = {
    "saudi_breast_cancer_pmc10474689": [
        {"label": "De-identified CSV", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=saudi_breast_cancer_pmc10474689/version=moesm1/snapshot_date=2026-03-12/variant_carriers.csv", "kind": "csv"},
        {"label": "De-identified parquet", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=saudi_breast_cancer_pmc10474689/version=moesm1/snapshot_date=2026-03-12/variant_carriers.parquet", "kind": "parquet"},
        {"label": "Extract manifest", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=saudi_breast_cancer_pmc10474689/version=moesm1/snapshot_date=2026-03-12/variant_carriers.manifest.json", "kind": "manifest"},
    ],
    "uae_brca_pmc12011969": [
        {"label": "Family-screening CSV", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/family_screening_variant_rows.csv", "kind": "csv"},
        {"label": "Family-screening parquet", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/family_screening_variant_rows.parquet", "kind": "parquet"},
        {"label": "Family-screening manifest", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/family_screening_variant_rows.manifest.json", "kind": "manifest"},
        {"label": "Cancer-cohort CSV", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/cancer_cohort_variant_rows.csv", "kind": "csv"},
        {"label": "Cancer-cohort parquet", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/cancer_cohort_variant_rows.parquet", "kind": "parquet"},
        {"label": "Cancer-cohort manifest", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/cancer_cohort_variant_rows.manifest.json", "kind": "manifest"},
    ],
}

REVIEW_DOCUMENT_FILES: Final[list[dict[str, str]]] = [
    {"label": "Supervisor review bundle", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/supervisor_review/snapshot_date=2026-03-12/review_bundle.json", "kind": "bundle"},
    {"label": "Supervisor review manifest", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/supervisor_review/snapshot_date=2026-03-12/manifest.json", "kind": "manifest"},
    {"label": "Normalization report", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/harmonized/normalization_report/snapshot_date=2026-03-15/brca_normalization_report.json", "kind": "report"},
    {"label": "Arab intake report", "uri": "gs://mahmoud-arab-acmg-research-data/frozen/arab_variant_evidence/snapshot_date=2026-03-12/intake_report.json", "kind": "report"},
]

FILE_KIND_LABELS: Final[dict[str, str]] = {
    "source_data": "SOURCE",
    "index": "INDEX",
    "manifest": "MANIFEST",
    "parquet": "PARQUET",
    "csv": "CSV",
    "workbook": "WORKBOOK",
    "report": "REPORT",
    "bundle": "BUNDLE",
    "document": "DOC",
}
def json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def download_json_from_gcs(storage_client: storage.Client, uri: str) -> dict[str, Any]:
    bucket_name, object_name = parse_gs_uri(uri)
    return json.loads(storage_client.bucket(bucket_name).blob(object_name).download_as_text())


def download_parquet_frame(storage_client: storage.Client, uri: str) -> pd.DataFrame:
    bucket_name, object_name = parse_gs_uri(uri)
    with tempfile.TemporaryDirectory(prefix="arab_acmg_bundle_") as tmpdir:
        parquet_path = Path(tmpdir) / Path(object_name).name
        storage_client.bucket(bucket_name).blob(object_name).download_to_filename(parquet_path)
        return pd.read_parquet(parquet_path)


def upload_public_csv(storage_client: storage.Client, frame: pd.DataFrame, object_name: str) -> dict[str, str]:
    with tempfile.TemporaryDirectory(prefix="arab_acmg_csv_") as tmpdir:
        csv_path = Path(tmpdir) / Path(object_name).name
        frame.to_csv(csv_path, index=False)
        blob = storage_client.bucket(BUCKET_NAME).blob(object_name)
        blob.upload_from_filename(str(csv_path), content_type="text/csv")
        blob.content_disposition = attachment_header_value(f"gs://{BUCKET_NAME}/{object_name}")
        blob.patch()
        blob.make_public()
    return {"gs_uri": f"gs://{BUCKET_NAME}/{object_name}", "public_url": public_url_for_gs_uri(f"gs://{BUCKET_NAME}/{object_name}")}


def csv_object_from_parquet_uri(parquet_uri: str) -> str:
    bucket_name, object_name = parse_gs_uri(parquet_uri)
    if bucket_name != BUCKET_NAME:
        raise ValueError(f"Unexpected bucket for CSV export: {parquet_uri}")
    if not object_name.endswith(".parquet"):
        raise ValueError(f"Expected a parquet artifact URI, got: {parquet_uri}")
    return object_name[:-8] + ".csv"


def ensure_public_csv(storage_client: storage.Client, parquet_uri: str) -> dict[str, str]:
    frame = download_parquet_frame(storage_client, parquet_uri)
    return upload_public_csv(storage_client, frame, csv_object_from_parquet_uri(parquet_uri))


def infer_file_kind(uri: str) -> str:
    name = Path(uri).name.lower()
    if name.endswith(".manifest.json") or name == "manifest.json":
        return "manifest"
    if name.endswith("_report.json") or name.endswith("report.json"):
        return "report"
    if name.endswith("review_bundle.json"):
        return "bundle"
    if name.endswith(".parquet"):
        return "parquet"
    if name.endswith(".csv"):
        return "csv"
    if name.endswith(".xlsx") or name.endswith(".xls"):
        return "workbook"
    if name.endswith(".tbi"):
        return "index"
    if any(name.endswith(suffix) for suffix in (".vcf.gz", ".vcf.bgz", ".txt", ".txt.gz")):
        return "source_data"
    return "document"


def public_gs_uri(storage_client: storage.Client | None, uri: str) -> str:
    public_url = public_url_for_gs_uri(uri)
    bucket_name, object_name = parse_gs_uri(uri)
    if not is_public_safe_gcs_uri(uri):
        return ""
    if storage_client is None or bucket_name != BUCKET_NAME:
        return public_url
    blob = storage_client.bucket(bucket_name).blob(object_name)
    if not blob.exists():
        LOGGER.warning("Missing GCS object in artifact catalog: %s", uri)
        return public_url
    attachment = attachment_header_value(uri)
    blob.reload()
    changed = False
    if blob.content_disposition != attachment:
        blob.content_disposition = attachment
        changed = True
    if changed:
        blob.patch()
    blob.make_public()
    return public_url


def storage_file(label: str, uri: str, *, kind: str | None = None) -> dict[str, str]:
    return {"label": label, "uri": uri, "kind": kind or infer_file_kind(uri)}


def publish_storage_files(storage_client: storage.Client | None, files: list[dict[str, str]]) -> list[dict[str, str]]:
    published: list[dict[str, str]] = []
    for file_item in files:
        uri = file_item["uri"]
        if uri.startswith("gs://"):
            access_profile = gcs_access_profile(uri)
            public_url = public_gs_uri(storage_client, uri) if access_profile["access"] == "public" else ""
            published.append(
                {
                    "label": file_item["label"],
                    "kind": file_item["kind"],
                    "kind_label": FILE_KIND_LABELS.get(file_item["kind"], file_item["kind"].upper()),
                    "gs_uri": uri,
                    "public_url": public_url,
                    "filename": Path(uri).name,
                    "access": access_profile["access"],
                    "access_label": access_profile["access_label"],
                    "access_reason": access_profile["access_reason"],
                    "action_label": default_action_label(file_item["kind"], access=access_profile["access"]),
                }
            )
            continue
        published.append(
            {
                "label": file_item["label"],
                "kind": file_item["kind"],
                "kind_label": FILE_KIND_LABELS.get(file_item["kind"], file_item["kind"].upper()),
                "gs_uri": "",
                "public_url": uri,
                "filename": Path(uri).name or uri,
                "access": "external",
                "access_label": "External",
                "access_reason": "Resolved outside the project bucket.",
                "action_label": default_action_label(file_item["kind"], access="public"),
            }
        )
    return published


def gs_uri_from_public_url(url: str) -> str | None:
    prefix = f"https://storage.googleapis.com/{BUCKET_NAME}/"
    if not url.startswith(prefix):
        return None
    return f"gs://{BUCKET_NAME}/{url.removeprefix(prefix)}"


def sibling_uri(parquet_uri: str, suffix: str) -> str:
    bucket_name, object_name = parse_gs_uri(parquet_uri)
    if not object_name.endswith(".parquet"):
        raise ValueError(f"Expected parquet URI, got: {parquet_uri}")
    return f"gs://{bucket_name}/{object_name[:-8]}{suffix}"


def normalize_review_entry(entry: dict[str, Any], *, table_label: str, scope_note: str) -> dict[str, Any]:
    normalized = deepcopy(entry)
    normalized["review_label"] = table_label
    normalized["scope_note"] = scope_note
    return normalized


def schema_lineage_summary(*, baseline_entry: dict[str, Any], current_entry: dict[str, Any], added_label: str) -> dict[str, object]:
    baseline_columns = [column["name"] for column in baseline_entry.get("columns", [])]
    current_columns = [column["name"] for column in current_entry.get("columns", [])]
    preserved = [name for name in baseline_columns if name in current_columns]
    added = [name for name in current_columns if name not in baseline_columns]
    missing = [name for name in baseline_columns if name not in current_columns]
    return {
        "baseline_column_count": len(baseline_columns),
        "current_column_count": len(current_columns),
        "preserved_column_count": len(preserved),
        "missing_columns": missing,
        "added_columns": added,
        "added_label": added_label,
    }


def raw_source_catalog_entries(
    storage_client: storage.Client,
    raw_payload: dict[str, Any],
    source_review: dict[str, Any],
) -> list[dict[str, Any]]:
    source_lookup = {
        source["source_key"]: source
        for source in source_review.get("sources", [])
        if source.get("source_key")
    }

    entries = []
    for dataset in raw_payload["datasets"]:
        source_key = RAW_SOURCE_LINK_MAP.get(str(dataset["key"]))
        source = source_lookup.get(source_key or "", {})
        links = []
        if source.get("upstream_url") and not str(source["upstream_url"]).startswith("file://"):
            links.append({"label": "Official source", "url": source["upstream_url"]})
        files = publish_storage_files(storage_client, RAW_SOURCE_FILES.get(str(dataset["key"]), []))
        entries.append(
            {
                "key": dataset["key"],
                "title": dataset["title"],
                "group": "raw_public_sources",
                "stage": "Raw source freeze",
                "overview": dataset["simple_summary"],
                "row_count": dataset["row_count"],
                "files": files,
                "links": links,
                "references": [*dataset.get("notes", [])[:1]],
                "download_note": "Displayed rows are the frozen BRCA-window preview. The file list below is the full frozen source package used to build that preview.",
            }
        )
    return entries


def derived_storage_files(
    *,
    parquet_uri: str,
    csv_public_url: str | None = None,
    include_manifest: bool,
    include_report: bool,
    extra_files: list[dict[str, str]] | None = None,
) -> list[dict[str, str]]:
    files = [storage_file("Parquet artifact", parquet_uri, kind="parquet")]
    csv_uri = gs_uri_from_public_url(csv_public_url) if csv_public_url else None
    if csv_uri:
        files.insert(0, storage_file("CSV export", csv_uri, kind="csv"))
    if include_manifest:
        files.append(storage_file("Artifact manifest", sibling_uri(parquet_uri, "_manifest.json"), kind="manifest"))
    if include_report:
        files.append(storage_file("Artifact report", sibling_uri(parquet_uri, "_report.json"), kind="report"))
    if extra_files:
        files.extend(extra_files)
    return files


def derived_catalog_entry(
    *,
    storage_client: storage.Client,
    key: str,
    title: str,
    stage: str,
    overview: str,
    row_count: int,
    files: list[dict[str, str]],
    review_label: str,
    links: list[dict[str, str]] | None = None,
    references: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "group": stage,
        "stage": review_label,
        "overview": overview,
        "row_count": row_count,
        "files": publish_storage_files(storage_client, files),
        "links": links or [],
        "references": references or [],
        "download_note": "",
    }


def build_artifact_catalog(
    *,
    storage_client: storage.Client,
    legacy_pre_gme: dict[str, Any],
    legacy_registry: dict[str, Any],
    arab_pre_gme: dict[str, Any],
    arab_registry: dict[str, Any],
    normalized_datasets: list[dict[str, Any]],
    raw_datasets: dict[str, Any],
    source_review: dict[str, Any],
) -> dict[str, Any]:
    source_lookup = {
        source["source_key"]: source
        for source in source_review.get("sources", [])
        if source.get("source_key")
    }
    avdb_source = source_lookup.get("avdb_uae", {})
    uae_source = source_lookup.get("uae_brca_pmc12011969", {})
    saudi_source = source_lookup.get("saudi_breast_cancer_pmc10474689", {})

    normalized_entries = []
    for entry in normalized_datasets:
        normalized_entries.append(
            {
                "key": entry["key"],
                "title": entry["title"],
                "group": "normalized_artifacts",
                "stage": "Normalized artifact",
                "overview": entry["simple_summary"],
                "row_count": entry["row_count"],
                "files": publish_storage_files(
                    storage_client,
                    derived_storage_files(
                        parquet_uri=entry["table_ref"],
                        csv_public_url=entry.get("download_url"),
                        include_manifest=True,
                        include_report=True,
                    ),
                ),
                "links": [],
                "references": [],
                "download_note": "",
            }
        )

    groups = [
        {
            "id": "raw_public_sources",
            "title": "Stage 1 outputs: raw source freezes",
            "summary": "Exact upstream source packages stored in GCS before any liftover, filtering, or BRCA normalization.",
            "entries": raw_source_catalog_entries(storage_client, raw_datasets, source_review),
        },
        {
            "id": "standardization_artifacts",
            "title": "Stage 2 outputs: build conversion evidence",
            "summary": "Files produced only for sources that needed explicit GRCh37 to GRCh38 handling.",
            "entries": [
                derived_catalog_entry(
                    storage_client=storage_client,
                    key="avdb_grch37_to_grch38",
                    title="AVDB UAE build-conversion package",
                    stage="standardization_artifacts",
                    overview="Raw AVDB workbook plus the frozen liftover parquet/report that document the GRCh37 to GRCh38 conversion.",
                    row_count=int(avdb_source.get("row_count", 0)),
                    files=AVDB_STANDARDIZATION_FILES,
                    review_label="Build conversion package",
                    links=([{"label": "Official source", "url": avdb_source["upstream_url"]}] if avdb_source.get("upstream_url") else []),
                    references=["Reference-only source: valid build conversion, but no BRCA rows in the frozen release."],
                )
            ],
        },
        {
            "id": "normalized_artifacts",
            "title": "Stage 3 outputs: BRCA-normalized source artifacts",
            "summary": "Per-source BRCA artifacts after canonical coordinate normalization, each with CSV, parquet, manifest, and report.",
            "entries": normalized_entries,
        },
        {
            "id": "legacy_checkpoint_artifacts",
            "title": "Stage 4-5 outputs: baseline draft and final tables",
            "summary": "Historical baseline tables kept unchanged. These remain the comparison reference for the Arab extension.",
            "entries": [
                derived_catalog_entry(
                    storage_client=storage_client,
                    key="legacy_pre_gme",
                    title=legacy_pre_gme["title"],
                    stage="legacy_checkpoint_artifacts",
                    overview=legacy_pre_gme["scope_note"],
                    row_count=legacy_pre_gme["row_count"],
                    files=derived_storage_files(
                        parquet_uri=legacy_pre_gme["table_ref"],
                        csv_public_url=legacy_pre_gme["csv_download_url"],
                        include_manifest=False,
                        include_report=False,
                        extra_files=[storage_file("Review workbook", legacy_pre_gme["table_ref"].replace(".parquet", ".xlsx"), kind="workbook")],
                    ),
                    review_label="Baseline draft table",
                    references=["This table stays frozen exactly as the historical baseline checkpoint."],
                ),
                derived_catalog_entry(
                    storage_client=storage_client,
                    key="legacy_registry",
                    title=legacy_registry["title"],
                    stage="legacy_checkpoint_artifacts",
                    overview=legacy_registry["scope_note"],
                    row_count=legacy_registry["row_count"],
                    files=derived_storage_files(
                        parquet_uri=legacy_registry["table_ref"],
                        csv_public_url=legacy_registry["csv_download_url"],
                        include_manifest=False,
                        include_report=False,
                    ),
                    review_label="Baseline final table",
                    references=["Use this frozen final table as the baseline comparison point."],
                ),
            ],
        },
        {
            "id": "arab_extension_artifacts",
            "title": "Stage 6 outputs: Arab extension tables",
            "summary": "Arab-source checkpoints kept separate from the unchanged baseline so the scientific delta stays explicit.",
            "entries": [
                derived_catalog_entry(
                    storage_client=storage_client,
                    key="arab_pre_gme",
                    title=arab_pre_gme["title"],
                    stage="arab_extension_artifacts",
                    overview=arab_pre_gme["scope_note"],
                    row_count=arab_pre_gme["row_count"],
                    files=derived_storage_files(
                        parquet_uri=arab_pre_gme["table_ref"],
                        csv_public_url=arab_pre_gme["csv_download_url"],
                        include_manifest=False,
                        include_report=False,
                    ),
                    review_label="Arab draft table",
                    references=["SHGP enters here before GME is added as a supporting layer."],
                ),
                derived_catalog_entry(
                    storage_client=storage_client,
                    key="arab_registry",
                    title=arab_registry["title"],
                    stage="arab_extension_artifacts",
                    overview=arab_registry["scope_note"],
                    row_count=arab_registry["row_count"],
                    files=derived_storage_files(
                        parquet_uri=arab_registry["table_ref"],
                        csv_public_url=arab_registry["csv_download_url"],
                        include_manifest=False,
                        include_report=True,
                    ),
                    review_label="Arab final table",
                    references=["This is the current Arab-aware final table, separate from the unchanged baseline."],
                ),
            ],
        },
        {
            "id": "reference_study_artifacts",
            "title": "Reference study packages",
            "summary": "Reference-only workbook sources and their frozen extracts kept for audit and scientific review, not active frequency aggregation.",
            "entries": [
                derived_catalog_entry(
                    storage_client=storage_client,
                    key="uae_brca_reference",
                    title="UAE BRCA study review package",
                    stage="reference_study_artifacts",
                    overview="De-identified extract files used to review the UAE BRCA study as supporting evidence only. The raw workbook remains private in GCS.",
                    row_count=int(uae_source.get("row_count", 0)),
                    files=REFERENCE_STUDY_FILES["uae_brca_pmc12011969"],
                    review_label="Reference-only study package",
                    links=([{"label": "Publication", "url": uae_source["upstream_url"]}] if uae_source.get("upstream_url") else []),
                    references=["The raw workbook is retained privately in GCS because it originated as a patient-level study supplement."],
                ),
                derived_catalog_entry(
                    storage_client=storage_client,
                    key="saudi_brca_reference",
                    title="Saudi breast-cancer study review package",
                    stage="reference_study_artifacts",
                    overview="De-identified extract files used to review the Saudi BRCA study. The raw workbook remains private in GCS and the source stays blocked for normalization.",
                    row_count=int(saudi_source.get("row_count", 0)),
                    files=REFERENCE_STUDY_FILES["saudi_breast_cancer_pmc10474689"],
                    review_label="Blocked study package",
                    links=([{"label": "Publication", "url": saudi_source["upstream_url"]}] if saudi_source.get("upstream_url") else []),
                    references=["The raw workbook is retained privately in GCS because it originated as a patient-level study supplement."],
                ),
            ],
        },
        {
            "id": "review_documents",
            "title": "Workflow documents and reports",
            "summary": "Supervisor-facing documents that describe the frozen review state, normalization outputs, and intake decisions.",
            "entries": [
                derived_catalog_entry(
                    storage_client=storage_client,
                    key="workflow_documents",
                    title="Review bundle and workflow reports",
                    stage="review_documents",
                    overview="Top-level bundle/report files that document the current static review surface.",
                    row_count=0,
                    files=REVIEW_DOCUMENT_FILES,
                    review_label="Workflow documents",
                )
            ],
        },
    ]
    return {"groups": groups}


def refresh_bundle() -> dict[str, Any]:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    storage_client = storage.Client(project=PROJECT_ID)

    # [AI-Agent: Codex]: Stage 1 / Load the current and historical frozen bundles so the UI can show baseline and Arab extension separately.
    LOGGER.info("Loading current review bundle from %s", CURRENT_BUNDLE_PATH)
    current_bundle = json.loads(CURRENT_BUNDLE_PATH.read_text(encoding="utf-8"))
    source_review = json.loads((UI_DIR / "source_review.json").read_text(encoding="utf-8"))
    LOGGER.info("Loading legacy review bundle from %s", LEGACY_BUNDLE_URI)
    legacy_bundle = download_json_from_gcs(storage_client, LEGACY_BUNDLE_URI)

    # [AI-Agent: Codex]: Stage 2 / Publish one CSV per derived artifact so the supervisor can download exactly what is displayed.
    LOGGER.info("Publishing CSV downloads for normalized artifacts and current Arab checkpoints")
    for dataset in current_bundle["datasets"]["datasets"]:
        dataset["download_url"] = ensure_public_csv(storage_client, dataset["table_ref"])["public_url"]

    current_arab_pre = current_bundle.get("arab_pre_gme", current_bundle["pre_gme"])
    current_arab_final = current_bundle.get("arab_registry", current_bundle["registry"])

    arab_pre_gme = normalize_review_entry(
        current_arab_pre,
        table_label="Arab extension before GME",
        scope_note="Arab-aware checkpoint from normalized ClinVar + gnomAD + SHGP before GME is added.",
    )
    arab_pre_gme["csv_download_url"] = ensure_public_csv(storage_client, arab_pre_gme["table_ref"])["public_url"]

    arab_registry = normalize_review_entry(
        current_arab_final,
        table_label="Arab extension final",
        scope_note="Arab-aware checkpoint after GME is added as a supporting Arab/MENA layer.",
    )
    arab_registry["csv_download_url"] = current_arab_final.get("csv_download_url") or ensure_public_csv(storage_client, arab_registry["table_ref"])["public_url"]

    legacy_pre_gme = normalize_review_entry(
        legacy_bundle["pre_gme"],
        table_label="Legacy baseline pre-GME",
        scope_note="Historical checkpoint frozen before the Arab extension work began.",
    )
    legacy_pre_gme["csv_download_url"] = ensure_public_csv(storage_client, legacy_pre_gme["table_ref"])["public_url"]

    legacy_registry = normalize_review_entry(
        legacy_bundle["registry"],
        table_label="Legacy baseline final",
        scope_note="Historical final BRCA checkpoint frozen before adding the new Arab frequency datasets.",
    )
    legacy_registry["csv_download_url"] = legacy_bundle["registry"]["csv_download_url"]
    arab_pre_gme["schema_lineage"] = schema_lineage_summary(
        baseline_entry=legacy_pre_gme,
        current_entry=arab_pre_gme,
        added_label="New Arab pre-GME additions",
    )
    arab_registry["schema_lineage"] = schema_lineage_summary(
        baseline_entry=legacy_registry,
        current_entry=arab_registry,
        added_label="New Arab-final additions",
    )

    # [AI-Agent: Codex]: Stage 3 / Reframe the bundle so the legacy baseline stays under the original final-table routes and Arab work moves to its own page.
    LOGGER.info("Composing updated review bundle structure")
    bundle = deepcopy(current_bundle)
    bundle["workflow"]["pages"] = [
        {"id": "overview", "title": "Overview", "summary": "Track progress and move through the scientific workflow stage by stage."},
        {"id": "raw", "title": "Raw Freeze", "summary": "Exact upstream files and manifests preserved before any processing."},
        {"id": "harmonization", "title": "BRCA Normalization", "summary": "Per-source BRCA artifacts after canonical coordinate normalization."},
        {"id": "pre-gme", "title": "Baseline Draft Integration", "summary": "ClinVar plus gnomAD combined before the GME support layer."},
        {"id": "final", "title": "Baseline Final Integration", "summary": "Baseline final table after GME is added as support."},
        {"id": "arab-extension", "title": "Arab Extension", "summary": "New Arab-source checkpoints kept separate from the unchanged baseline."},
        {"id": "artifacts", "title": "GCS Download Center", "summary": "Public frozen files grouped by workflow stage and dataset."},
        {"id": "access", "title": "Controlled Access", "summary": "Official acquisition paths for restricted Arab datasets still outside the active workflow."},
    ]
    bundle["workflow"]["legacy_final_steps"] = legacy_bundle["workflow"]["final_steps"]
    bundle["workflow"]["arab_extension_steps"] = current_bundle["workflow"]["final_steps"]
    bundle["legacy_step_samples"] = legacy_bundle.get("step_samples", {})
    bundle["arab_step_samples"] = current_bundle.get("step_samples", {})
    bundle["pre_gme"] = legacy_pre_gme
    bundle["registry"] = legacy_registry
    bundle["arab_pre_gme"] = arab_pre_gme
    bundle["arab_registry"] = arab_registry
    bundle["artifact_catalog"] = build_artifact_catalog(
        storage_client=storage_client,
        legacy_pre_gme=legacy_pre_gme,
        legacy_registry=legacy_registry,
        arab_pre_gme=arab_pre_gme,
        arab_registry=arab_registry,
        normalized_datasets=bundle["datasets"]["datasets"],
        raw_datasets=bundle["raw_datasets"],
        source_review=source_review,
    )
    bundle["artifacts"]["legacy_bundle_uri"] = LEGACY_BUNDLE_URI
    bundle["artifacts"]["legacy_final_csv_public_url"] = legacy_registry["csv_download_url"]
    bundle["artifacts"]["arab_pre_gme_csv_public_url"] = arab_pre_gme["csv_download_url"]
    bundle["artifacts"]["arab_final_csv_public_url"] = arab_registry["csv_download_url"]

    # [AI-Agent: Codex]: Stage 4 / Persist the refreshed bundle locally and back to GCS so Cloud Run serves the exact same static state.
    LOGGER.info("Writing updated bundle to %s", CURRENT_BUNDLE_PATH)
    json_dump(CURRENT_BUNDLE_PATH, bundle)
    upload_blob = storage_client.bucket(BUCKET_NAME).blob(REVIEW_BUNDLE_OBJECT)
    upload_blob.upload_from_string(json.dumps(bundle, indent=2), content_type="application/json")
    upload_blob.content_disposition = attachment_header_value(f"gs://{BUCKET_NAME}/{REVIEW_BUNDLE_OBJECT}")
    upload_blob.patch()
    upload_blob.make_public()

    return bundle


def main() -> None:
    bundle = refresh_bundle()
    print(
        json.dumps(
            {
                "status": "ok",
                "legacy_final_title": bundle["registry"]["title"],
                "arab_final_title": bundle["arab_registry"]["title"],
                "normalized_downloads": len(bundle["datasets"]["datasets"]),
                "artifact_groups": len(bundle["artifact_catalog"]["groups"]),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
