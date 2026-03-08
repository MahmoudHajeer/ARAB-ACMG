"""Ingest the locally provided GME raw file into the GCS raw vault.

This keeps the upstream file untouched and records provenance for later QC and
staging work.
"""

from __future__ import annotations

import datetime as dt
import gzip
import os
import sys
from email.utils import format_datetime
from pathlib import Path
from typing import Final

from google.cloud import storage

try:
    from scripts.manifest_utility import ManifestGenerator
except ModuleNotFoundError:
    from manifest_utility import ManifestGenerator

PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
SOURCE_NAME: Final[str] = "gme"
SOURCE_VERSION: Final[str] = "20161025-hg38"
SOURCE_BUILD: Final[str] = "hg38"
LOCAL_SOURCE: Final[Path] = Path("/Users/macbookpro/Desktop/storage/raw/gme/hg38_gme.txt.gz")
LOCAL_WORK_DIR: Final[Path] = Path("data/raw/gme")
UPSTREAM_URL: Final[str] = f"file://{LOCAL_SOURCE}"


class GMEIngestionPipeline:
    def __init__(self) -> None:
        self.snapshot_date = dt.date.today().isoformat()
        self.local_manifest = LOCAL_WORK_DIR / "manifest.json"
        self.row_count = 0
        self.last_modified = "unknown"

    def raw_vault_prefix(self) -> str:
        return (
            f"raw/sources/{SOURCE_NAME}/release={SOURCE_VERSION}/build={SOURCE_BUILD}/"
            f"snapshot_date={self.snapshot_date}"
        )

    def check_local_source(self) -> "GMEIngestionPipeline":
        print(f"--- [GME Stage 1]: Checking local source {LOCAL_SOURCE} ---")
        if not LOCAL_SOURCE.exists():
            print("❌ [Stage 1 Effect]: GME raw source file is missing.")
            sys.exit(1)
        LOCAL_WORK_DIR.mkdir(parents=True, exist_ok=True)
        with gzip.GzipFile(filename=str(LOCAL_SOURCE), mode="rb") as handle:
            handle.peek(1)
            if handle.mtime:
                self.last_modified = format_datetime(
                    dt.datetime.fromtimestamp(handle.mtime, tz=dt.timezone.utc)
                )
        print("✅ [Stage 1 Effect]: GME raw source found.")
        return self

    def count_rows(self) -> "GMEIngestionPipeline":
        print("--- [GME Stage 2]: Counting data rows ---")
        with gzip.open(LOCAL_SOURCE, "rt", encoding="utf-8") as handle:
            total_lines = sum(1 for _ in handle)
        self.row_count = max(total_lines - 1, 0)
        print(f"✅ [Stage 2 Effect]: Counted {self.row_count} data rows.")
        return self

    def generate_manifest(self) -> "GMEIngestionPipeline":
        print("--- [GME Stage 3]: Generating provenance manifest ---")
        manifest_json = ManifestGenerator.create_manifest(
            source=SOURCE_NAME,
            source_version=SOURCE_VERSION,
            upstream_url=UPSTREAM_URL,
            local_file_path=str(LOCAL_SOURCE),
            gcs_uri=f"gs://{BUCKET_NAME}/{self.raw_vault_prefix()}/{LOCAL_SOURCE.name}",
            row_count=self.row_count,
            license_notes="Local file provided by user; verify external reuse terms before publication.",
            notes=f"archive_last_modified={self.last_modified}; build={SOURCE_BUILD}",
        )
        self.local_manifest.write_text(manifest_json, encoding="utf-8")
        print(f"✅ [Stage 3 Effect]: Manifest created at {self.local_manifest}")
        return self

    def write_source_freeze_doc(self) -> "GMEIngestionPipeline":
        print("--- [GME Stage 4]: Updating source freeze register ---")
        freeze_file = Path("conductor/source-freeze.md")
        row = (
            f"| {SOURCE_NAME}_{SOURCE_BUILD} | {SOURCE_VERSION} | {self.snapshot_date} | {UPSTREAM_URL} | "
            f"`gs://{BUCKET_NAME}/{self.raw_vault_prefix()}/` | "
            f"local raw file supplied by user; archive_last_modified={self.last_modified} |\n"
        )
        current_lines = freeze_file.read_text(encoding="utf-8").splitlines()
        filtered_lines = [
            line
            for line in current_lines
            if not line.startswith(f"| {SOURCE_NAME}_{SOURCE_BUILD} | {SOURCE_VERSION} | {self.snapshot_date} |")
        ]
        freeze_file.write_text("\n".join(filtered_lines) + "\n" + row, encoding="utf-8")
        print(f"✅ [Stage 4 Effect]: Freeze register updated at {freeze_file}")
        return self

    def upload_to_gcs(self) -> "GMEIngestionPipeline":
        print(f"--- [GME Stage 5]: Uploading raw snapshot to gs://{BUCKET_NAME} ---")
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.bucket(BUCKET_NAME)

            raw_blob = bucket.blob(f"{self.raw_vault_prefix()}/{LOCAL_SOURCE.name}")
            raw_blob.upload_from_filename(str(LOCAL_SOURCE))
            print(f"✅ [Stage 5 Effect]: Uploaded gs://{BUCKET_NAME}/{raw_blob.name}")

            manifest_blob = bucket.blob(f"{self.raw_vault_prefix()}/manifest.json")
            manifest_blob.upload_from_filename(str(self.local_manifest))
            print(f"✅ [Stage 5 Effect]: Uploaded gs://{BUCKET_NAME}/{manifest_blob.name}")
        except Exception as exc:
            print(f"❌ [Stage 5 Effect]: GCS upload failed. Error: {exc}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        print("🎉 [Final Effect]: GME raw snapshot ingestion completed successfully.")


if __name__ == "__main__":
    (
        GMEIngestionPipeline()
        .check_local_source()
        .count_rows()
        .generate_manifest()
        .write_source_freeze_doc()
        .upload_to_gcs()
        .finalize()
    )
