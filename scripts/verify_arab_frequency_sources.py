"""Verify the frozen SHGP and AVDB artifacts created for T003."""

from __future__ import annotations

import json

from google.cloud import storage

BUCKET_NAME = "mahmoud-arab-acmg-research-data"
SHGP_MANIFEST_OBJECT = (
    "raw/sources/shgp_saudi_af/version=figshare-28059686-v1/snapshot_date=2026-03-13/manifest.json"
)
AVDB_RAW_MANIFEST_OBJECT = (
    "raw/sources/avdb_uae/version=workbook-created-2025-06-27/build=GRCh37/snapshot_date=2026-03-13/manifest.json"
)
AVDB_REPORT_OBJECT = (
    "frozen/harmonized/source=avdb_uae/version=workbook-created-2025-06-27/"
    "stage=liftover/build=GRCh37_to_GRCh38/snapshot_date=2026-03-13/avdb_uae_liftover_report.json"
)


def load_json(bucket: storage.Bucket, object_name: str) -> dict[str, object]:
    return json.loads(bucket.blob(object_name).download_as_text())


def main() -> None:
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)

    shgp_manifest = load_json(bucket, SHGP_MANIFEST_OBJECT)
    avdb_raw_manifest = load_json(bucket, AVDB_RAW_MANIFEST_OBJECT)
    avdb_report = load_json(bucket, AVDB_REPORT_OBJECT)

    assert shgp_manifest["source"] == "shgp_saudi_af"
    assert shgp_manifest["row_count"] == 25488989
    assert shgp_manifest["local_md5"] == shgp_manifest["upstream_md5"]

    assert avdb_raw_manifest["source"] == "avdb_uae"
    assert avdb_raw_manifest["row_count"] == 801

    assert avdb_report["source"] == "avdb_uae"
    assert avdb_report["counts"]["total_rows"] == 801
    assert avdb_report["counts"]["liftover_success_rows"] == 799
    assert avdb_report["counts"]["liftover_failure_rows"] == 2
    assert avdb_report["use_decision"]["label"] == "reference_only"

    print("shgp_manifest=pass")
    print("avdb_raw_manifest=pass")
    print("avdb_liftover_report=pass")


if __name__ == "__main__":
    main()
