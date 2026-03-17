"""Central policy for which GCS objects may be published anonymously.

This project keeps some frozen study workbooks in GCS for audit only. Those
objects must never become publicly downloadable from the supervisor surface.
Only the explicitly safe raw public-source packages and frozen derived
artifacts may be published without authentication.
"""

from __future__ import annotations

from pathlib import Path
from typing import Final

try:
    from scripts.runtime_config import bucket_name
except ModuleNotFoundError:
    from runtime_config import bucket_name  # type: ignore[no-redef]

BUCKET_NAME: Final[str] = bucket_name()

PUBLIC_GCS_PREFIXES: Final[tuple[str, ...]] = (
    f"gs://{BUCKET_NAME}/raw/sources/clinvar/",
    f"gs://{BUCKET_NAME}/raw/sources/gnomad_v4.1/",
    f"gs://{BUCKET_NAME}/raw/sources/gme/",
    f"gs://{BUCKET_NAME}/raw/sources/shgp_saudi_af/",
    f"gs://{BUCKET_NAME}/raw/sources/avdb_uae/",
    f"gs://{BUCKET_NAME}/frozen/harmonized/",
    f"gs://{BUCKET_NAME}/frozen/results/",
    f"gs://{BUCKET_NAME}/frozen/supervisor_review/",
    f"gs://{BUCKET_NAME}/frozen/review_bundle/",
    f"gs://{BUCKET_NAME}/frozen/arab_variant_evidence/source=saudi_breast_cancer_pmc10474689/",
    f"gs://{BUCKET_NAME}/frozen/arab_variant_evidence/source=uae_brca_pmc12011969/",
    f"gs://{BUCKET_NAME}/frozen/arab_variant_evidence/snapshot_date=",
)

RESTRICTED_GCS_PREFIXES: Final[tuple[str, ...]] = (
    f"gs://{BUCKET_NAME}/raw/sources/saudi_breast_cancer_pmc10474689/",
    f"gs://{BUCKET_NAME}/raw/sources/uae_brca_pmc12011969/",
)

PUBLIC_FILE_KINDS: Final[frozenset[str]] = frozenset(
    {"source_data", "index", "manifest", "parquet", "csv", "workbook", "report", "bundle", "document"}
)


def parse_gs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Unsupported GCS URI: {uri}")
    bucket_name, object_name = uri[5:].split("/", 1)
    return bucket_name, object_name


def object_public_url(bucket_name: str, object_name: str) -> str:
    return f"https://storage.googleapis.com/{bucket_name}/{object_name}"


def public_url_for_gs_uri(uri: str) -> str:
    bucket_name, object_name = parse_gs_uri(uri)
    return object_public_url(bucket_name, object_name)


def is_public_safe_gcs_uri(uri: str) -> bool:
    if not uri.startswith(f"gs://{BUCKET_NAME}/"):
        return False
    if any(uri.startswith(prefix) for prefix in RESTRICTED_GCS_PREFIXES):
        return False
    return any(uri.startswith(prefix) for prefix in PUBLIC_GCS_PREFIXES)


def gcs_access_profile(uri: str) -> dict[str, str]:
    if is_public_safe_gcs_uri(uri):
        return {
            "access": "public",
            "access_label": "Public",
            "access_reason": "Anonymous download allowed.",
            "public_url": public_url_for_gs_uri(uri),
        }
    return {
        "access": "restricted",
        "access_label": "Private",
        "access_reason": "Retained privately in GCS for audit only.",
        "public_url": "",
    }


def default_action_label(kind: str, *, access: str) -> str:
    if access != "public":
        return "Restricted"
    if kind in {"manifest", "report", "bundle", "document"}:
        return "Open file"
    return "Download"


def attachment_header_value(uri: str) -> str:
    return f'attachment; filename="{Path(parse_gs_uri(uri)[1]).name}"'
