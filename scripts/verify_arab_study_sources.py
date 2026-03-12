"""Verify frozen Arab study source packages and their de-identified extracts in GCS."""

from __future__ import annotations

import datetime as dt
import sys
from typing import Final

from google.cloud import storage

try:
    from scripts.freeze_arab_study_sources import BUCKET_NAME, PROJECT_ID, STUDY_SOURCES
except ModuleNotFoundError:
    from freeze_arab_study_sources import BUCKET_NAME, PROJECT_ID, STUDY_SOURCES

SNAPSHOT_DATE: Final[str] = dt.date.today().isoformat()


def main() -> None:
    storage_client = storage.Client(project=PROJECT_ID)
    bucket = storage_client.bucket(BUCKET_NAME)
    ok = True

    report_blob = bucket.blob(
        f"frozen/arab_variant_evidence/snapshot_date={SNAPSHOT_DATE}/intake_report.json"
    )
    print(f"intake_report exists={report_blob.exists()}")
    ok &= report_blob.exists()

    for source in STUDY_SOURCES:
        raw_blob = bucket.blob(
            f"{source.raw_vault_prefix(SNAPSHOT_DATE)}/{source.local_source.name}"
        )
        raw_manifest_blob = bucket.blob(f"{source.raw_vault_prefix(SNAPSHOT_DATE)}/manifest.json")
        print(f"{source.slug} raw_workbook exists={raw_blob.exists()}")
        print(f"{source.slug} raw_manifest exists={raw_manifest_blob.exists()}")
        ok &= raw_blob.exists()
        ok &= raw_manifest_blob.exists()

        for spec in source.extracts:
            extract_prefix = source.frozen_extract_prefix(SNAPSHOT_DATE)
            csv_blob = bucket.blob(f"{extract_prefix}/{spec.output_slug}.csv")
            parquet_blob = bucket.blob(f"{extract_prefix}/{spec.output_slug}.parquet")
            manifest_blob = bucket.blob(f"{extract_prefix}/{spec.output_slug}.manifest.json")
            print(f"{source.slug}/{spec.output_slug} csv exists={csv_blob.exists()}")
            print(f"{source.slug}/{spec.output_slug} parquet exists={parquet_blob.exists()}")
            print(f"{source.slug}/{spec.output_slug} manifest exists={manifest_blob.exists()}")
            ok &= csv_blob.exists()
            ok &= parquet_blob.exists()
            ok &= manifest_blob.exists()

    if ok:
        print("✅ Arab study source freeze verification passed.")
        sys.exit(0)

    print("❌ Arab study source freeze verification failed.")
    sys.exit(1)


if __name__ == "__main__":
    main()
