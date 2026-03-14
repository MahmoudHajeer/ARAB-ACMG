"""Verify the frozen BRCA normalization artifacts and review bundle.

The checks in this file stay artifact-centric and reproducible:
1. Required local frozen bundle files exist.
2. Referenced GCS objects exist.
3. Public download URLs respond.
4. Canonical variant keys match the displayed genomic columns.
5. Checkpoints are unique on canonical keys.
6. Source-level duplicates are allowed only when they are exact duplicates that
   are intentionally preserved for provenance review.
"""

from __future__ import annotations

import json
import subprocess
import sys
import tempfile
from pathlib import Path
from typing import Final

import pandas as pd
from google.cloud import storage

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
UI_DIR: Final[Path] = ROOT / "ui"
PROJECT_ID: Final[str] = "genome-services-platform"
GME_DUPLICATE_IGNORED_COLUMNS: Final[tuple[str, ...]] = (
    "source_id",
    "source_row_number",
    "source_record_locator",
)


def parse_gs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Unsupported URI: {uri}")
    bucket, object_name = uri[5:].split("/", 1)
    return bucket, object_name


def require(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def blob_exists(client: storage.Client, uri: str) -> bool:
    bucket_name, object_name = parse_gs_uri(uri)
    return client.bucket(bucket_name).blob(object_name).exists(client)


def public_url_exists(url: str) -> bool:
    result = subprocess.run(["curl", "-I", "-sS", url], capture_output=True, text=True, check=True)
    status_line = result.stdout.splitlines()[0] if result.stdout else ""
    return "200" in status_line


def canonical_variant_key(
    frame: pd.DataFrame,
    *,
    chrom_column: str,
    pos_column: str,
    ref_column: str,
    alt_column: str,
) -> pd.Series:
    return (
        frame[chrom_column].astype(str)
        + ":"
        + frame[pos_column].astype(int).astype(str)
        + ":"
        + frame[ref_column].astype(str)
        + ":"
        + frame[alt_column].astype(str)
    )


def duplicate_key_count(frame: pd.DataFrame, *, key_column: str) -> int:
    return int(frame.duplicated(subset=[key_column]).sum())


def duplicate_groups_are_exact(
    frame: pd.DataFrame,
    *,
    key_column: str,
    ignored_columns: tuple[str, ...] = (),
) -> tuple[bool, list[str]]:
    duplicate_rows = frame[frame.duplicated(subset=[key_column], keep=False)]
    if duplicate_rows.empty:
        return True, []

    comparable_columns = [column for column in duplicate_rows.columns if column not in {key_column, *ignored_columns}]
    failures: list[str] = []

    for variant_key, group in duplicate_rows.groupby(key_column, sort=True):
        normalized_group = group[comparable_columns].fillna("<NA>").drop_duplicates()
        if len(normalized_group) > 1:
            failures.append(str(variant_key))

    return not failures, failures


def download_parquet_frame(client: storage.Client, uri: str) -> pd.DataFrame:
    bucket_name, object_name = parse_gs_uri(uri)
    with tempfile.TemporaryDirectory(prefix="arab_acmg_verify_") as tmpdir:
        parquet_path = Path(tmpdir) / Path(object_name).name
        client.bucket(bucket_name).blob(object_name).download_to_filename(parquet_path)
        return pd.read_parquet(parquet_path)


def validate_canonical_keys(
    frame: pd.DataFrame,
    *,
    title: str,
    key_column: str | None,
    chrom_column: str,
    pos_column: str,
    ref_column: str,
    alt_column: str,
    require_unique_keys: bool,
    allow_exact_duplicates: bool = False,
    ignored_duplicate_columns: tuple[str, ...] = (),
) -> dict[str, int]:
    expected = canonical_variant_key(
        frame,
        chrom_column=chrom_column,
        pos_column=pos_column,
        ref_column=ref_column,
        alt_column=alt_column,
    )
    if key_column is not None:
        require(key_column in frame.columns, f"{title}: missing key column {key_column}")
        mismatches = int((frame[key_column].astype(str) != expected.astype(str)).sum())
        require(mismatches == 0, f"{title}: canonical key mismatch count was {mismatches}")
        null_keys = int(frame[key_column].isna().sum())
        require(null_keys == 0, f"{title}: canonical key contains NULL values")
        duplicate_frame = frame
        duplicate_column = key_column
    else:
        mismatches = 0
        null_keys = int(expected.isna().sum())
        require(null_keys == 0, f"{title}: computed canonical key contains NULL values")
        duplicate_frame = frame.assign(__computed_variant_key=expected)
        duplicate_column = "__computed_variant_key"

    duplicates = duplicate_key_count(duplicate_frame, key_column=duplicate_column)
    if require_unique_keys:
        require(duplicates == 0, f"{title}: duplicate canonical keys were found ({duplicates})")
    elif allow_exact_duplicates and duplicates:
        exact_ok, failures = duplicate_groups_are_exact(
            duplicate_frame,
            key_column=duplicate_column,
            ignored_columns=ignored_duplicate_columns,
        )
        require(exact_ok, f"{title}: duplicate keys were not exact duplicates for {failures[:5]}")

    return {
        "rows": int(len(frame)),
        "null_keys": null_keys,
        "duplicate_keys": duplicates,
        "canonical_mismatches": mismatches,
    }


def main() -> None:
    bundle = json.loads((UI_DIR / "review_bundle.json").read_text(encoding="utf-8"))
    source_review = json.loads((UI_DIR / "source_review.json").read_text(encoding="utf-8"))

    require(bundle["pre_gme"]["row_count"] > 0, "Legacy pre-GME checkpoint must have rows.")
    require(bundle["registry"]["row_count"] > 0, "Legacy final checkpoint must have rows.")
    require(bundle["arab_pre_gme"]["row_count"] > 0, "Arab pre-GME checkpoint must have rows.")
    require(bundle["arab_registry"]["row_count"] > 0, "Arab final checkpoint must have rows.")
    require(
        bundle["arab_registry"]["row_count"] >= bundle["arab_pre_gme"]["row_count"],
        "Arab final checkpoint must not be smaller than Arab pre-GME checkpoint.",
    )

    storage_client = storage.Client(project=PROJECT_ID)
    gcs_uris = {
        bundle["artifacts"]["normalization_report_uri"],
        bundle["artifacts"]["checkpoint_report_uri"],
        bundle["pre_gme"]["table_ref"],
        bundle["registry"]["table_ref"],
        bundle["arab_pre_gme"]["table_ref"],
        bundle["arab_registry"]["table_ref"],
    }
    for dataset in bundle["datasets"]["datasets"]:
        gcs_uris.add(dataset["table_ref"])
    for source in source_review["sources"]:
        for link in source.get("artifact_links", []):
            url = str(link.get("url", ""))
            if url.startswith("gs://"):
                gcs_uris.add(url)

    missing = sorted(uri for uri in gcs_uris if not blob_exists(storage_client, uri))
    require(not missing, f"Missing GCS artifacts: {missing}")

    public_urls = {
        bundle["pre_gme"]["csv_download_url"],
        bundle["registry"]["csv_download_url"],
        bundle["arab_pre_gme"]["csv_download_url"],
        bundle["arab_registry"]["csv_download_url"],
        *(dataset["download_url"] for dataset in bundle["datasets"]["datasets"]),
    }
    failed_public_urls = sorted(url for url in public_urls if not public_url_exists(url))
    require(not failed_public_urls, f"Public artifact URLs failed: {failed_public_urls}")

    validation_summary: dict[str, dict[str, int]] = {}
    for dataset in bundle["datasets"]["datasets"]:
        frame = download_parquet_frame(storage_client, dataset["table_ref"])
        allow_exact_duplicates = dataset["key"] == "gme_normalized_brca"
        ignored_columns = GME_DUPLICATE_IGNORED_COLUMNS if allow_exact_duplicates else ()
        validation_summary[dataset["key"]] = validate_canonical_keys(
            frame,
            title=dataset["title"],
            key_column="variant_key",
            chrom_column="chrom38",
            pos_column="pos38",
            ref_column="ref_norm",
            alt_column="alt_norm",
            require_unique_keys=not allow_exact_duplicates,
            allow_exact_duplicates=allow_exact_duplicates,
            ignored_duplicate_columns=ignored_columns,
        )

    for payload_key, label in [
        ("pre_gme", "Legacy pre-GME checkpoint"),
        ("registry", "Legacy final checkpoint"),
        ("arab_pre_gme", "Arab pre-GME checkpoint"),
        ("arab_registry", "Arab final checkpoint"),
    ]:
        frame = download_parquet_frame(storage_client, bundle[payload_key]["table_ref"])
        validation_summary[payload_key] = validate_canonical_keys(
            frame,
            title=label,
            key_column=("VARIANT_KEY" if payload_key.startswith("arab_") else None),
            chrom_column="CHROM",
            pos_column="POS",
            ref_column="REF",
            alt_column="ALT",
            require_unique_keys=True,
        )

    print(
        json.dumps(
            {
                "status": "ok",
                "legacy_final_row_count": bundle["registry"]["row_count"],
                "arab_final_row_count": bundle["arab_registry"]["row_count"],
                "normalized_artifacts": len(bundle["datasets"]["datasets"]),
                "verified_gcs_artifacts": len(gcs_uris),
                "validated_public_urls": len(public_urls),
                "canonical_key_validation": validation_summary,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:  # pragma: no cover - script entrypoint
        print(f"verify_brca_normalized_artifacts failed: {exc}", file=sys.stderr)
        raise
