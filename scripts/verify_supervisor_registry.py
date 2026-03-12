"""Verify the frozen supervisor-review artifacts and the raw-only BigQuery posture."""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Final

from google.cloud import bigquery, storage

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
UI_BUNDLE: Final[Path] = ROOT / "ui" / "review_bundle.json"
PROJECT_ID: Final[str] = "genome-services-platform"
REQUIRED_RAW_TABLES: Final[tuple[str, ...]] = (
    "clinvar_raw_vcf",
    "gnomad_v4_1_genomes_chr13_raw",
    "gnomad_v4_1_genomes_chr17_raw",
    "gnomad_v4_1_exomes_chr13_raw",
    "gnomad_v4_1_exomes_chr17_raw",
    "gme_hg38_raw",
)


def main() -> None:
    ok = True

    print("--- Frozen Supervisor Verification ---")
    if not UI_BUNDLE.exists():
        print(f"review_bundle_missing={UI_BUNDLE}")
        sys.exit(1)

    bundle = json.loads(UI_BUNDLE.read_text(encoding="utf-8"))
    artifacts = bundle.get("artifacts", {})
    print(f"bundle_generated_at={bundle.get('generated_at')}")
    print(f"freeze_prefix={artifacts.get('freeze_prefix')}")

    bq_client = bigquery.Client(project=PROJECT_ID)
    raw_tables = {table.table_id for table in bq_client.list_tables(f"{PROJECT_ID}.arab_acmg_raw")}
    print(f"raw_tables={sorted(raw_tables)}")
    if raw_tables != set(REQUIRED_RAW_TABLES):
        ok = False

    for dataset_id in ("arab_acmg_harmonized", "arab_acmg_results"):
        remaining = [table.table_id for table in bq_client.list_tables(f"{PROJECT_ID}.{dataset_id}")]
        print(f"dataset={dataset_id} remaining_tables={remaining}")
        if remaining:
            ok = False

    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(artifacts["bucket"])
    for key in ("pre_gme_parquet_uri", "pre_gme_xlsx_uri", "final_parquet_uri", "final_csv_uri", "bundle_uri", "manifest_uri"):
        uri = artifacts.get(key)
        if not uri or not uri.startswith("gs://"):
            print(f"artifact_uri_invalid[{key}]={uri}")
            ok = False
            continue
        _, remainder = uri.split("gs://", 1)
        bucket_name, object_name = remainder.split("/", 1)
        if bucket_name != artifacts["bucket"]:
            print(f"artifact_bucket_mismatch[{key}]={uri}")
            ok = False
            continue
        blob = bucket.blob(object_name)
        exists = blob.exists()
        print(f"artifact[{key}] exists={exists} uri={uri}")
        if not exists:
            ok = False

    public_url = artifacts.get("final_csv_public_url")
    print(f"final_csv_public_url={public_url}")
    if not public_url or not public_url.startswith("https://storage.googleapis.com/"):
        ok = False

    if ok:
        print("✅ Frozen supervisor verification passed.")
        sys.exit(0)

    print("❌ Frozen supervisor verification failed.")
    sys.exit(1)


if __name__ == "__main__":
    main()
