# [AI-Agent: Gemini 2.0 Flash]: This script automates ClinVar VCF ingestion to GCS.
# It implements a cloud-first workflow with automated provenance manifest generation.

import os
import sys
import datetime
from email.utils import parsedate_to_datetime
from pathlib import Path
import requests
from typing import Final, Self
from google.cloud import storage

try:
    # Supports `python -m scripts.ingest_clinvar_cloud`
    from scripts.manifest_utility import ManifestGenerator
except ModuleNotFoundError:
    # Supports `python scripts/ingest_clinvar_cloud.py`
    from manifest_utility import ManifestGenerator

# [AI-Agent: Gemini 2.0 Flash]: Pipeline Configuration.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
CLINVAR_URL: Final[str] = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz"
CLINVAR_INDEX_URL: Final[str] = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz.tbi"
SOURCE_NAME: Final[str] = "clinvar"

class ClinVarIngestionPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the ClinVar data ingestion to GCS.
    Goal: Capture raw ClinVar VCF and its provenance metadata in the research bucket.
    """

    def __init__(self) -> None:
        self.local_dir: Final[str] = "data/raw/clinvar"
        self.local_file: str = os.path.join(self.local_dir, "clinvar_raw.vcf.gz")
        self.local_index: str = os.path.join(self.local_dir, "clinvar_raw.vcf.gz.tbi")
        self.manifest_file: str = os.path.join(self.local_dir, "manifest.json")
        self.snapshot_date: str = datetime.date.today().isoformat()
        self.source_version: str = f"latest-{self.snapshot_date}"
        self.upstream_last_modified: str = "unknown"
        self.upstream_etag: str = "unknown"
        os.makedirs(self.local_dir, exist_ok=True)

    def gcs_prefix(self) -> str:
        return f"raw/{SOURCE_NAME}/{self.source_version}/snapshot_date={self.snapshot_date}"

    def raw_vault_prefix(self) -> str:
        return f"raw/sources/{SOURCE_NAME}/{self.source_version}/snapshot_date={self.snapshot_date}"

    def download_vcf(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - HTTP Ingestion.
        Effect: Downloads the bulk ClinVar VCF from NCBI to local staging.
        """
        print(f"--- [ClinVar Stage 1]: Downloading from {CLINVAR_URL} ---")
        try:
            with requests.get(CLINVAR_URL, stream=True) as r:
                r.raise_for_status()
                self.upstream_last_modified = r.headers.get("Last-Modified", "unknown")
                self.upstream_etag = r.headers.get("ETag", "unknown")

                # Freeze upstream version from Last-Modified when available.
                if self.upstream_last_modified != "unknown":
                    try:
                        dt = parsedate_to_datetime(self.upstream_last_modified)
                        self.source_version = f"lastmod-{dt.strftime('%Y%m%d')}"
                    except Exception:
                        self.source_version = f"latest-{self.snapshot_date}"

                with open(self.local_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print(f"✅ [Stage 1 Effect]: ClinVar VCF downloaded to {self.local_file}")

            # Keep the index as-is for reproducible raw snapshots and fast region queries.
            with requests.get(CLINVAR_INDEX_URL, stream=True) as r:
                r.raise_for_status()
                with open(self.local_index, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print(f"✅ [Stage 1 Effect]: ClinVar index downloaded to {self.local_index}")
        except Exception as e:
            print(f"❌ [Stage 1 Effect]: Download failed. Error: {e}")
            sys.exit(1)
        return self

    def generate_manifest(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 2 - Provenance Recording.
        Effect: Generates a metadata manifest for the downloaded artifact.
        """
        print("--- [ClinVar Stage 2]: Generating Provenance Manifest ---")
        gcs_uri = f"gs://{BUCKET_NAME}/{self.gcs_prefix()}/clinvar_raw.vcf.gz"
        manifest_json = ManifestGenerator.create_manifest(
            source=SOURCE_NAME,
            source_version=self.source_version,
            upstream_url=CLINVAR_URL,
            local_file_path=self.local_file,
            gcs_uri=gcs_uri,
            notes=(
                "Bulk ClinVar GRCh38 download for ARAB-ACMG research. "
                f"last_modified={self.upstream_last_modified}; etag={self.upstream_etag}"
            ),
        )
        with open(self.manifest_file, "w") as f:
            f.write(manifest_json)
        print(f"✅ [Stage 2 Effect]: Manifest created at {self.manifest_file}")
        return self

    def write_source_freeze_doc(self) -> Self:
        """
        Pipeline Stage 2.5 - Freeze Documentation.
        Effect: Appends/maintains a source freeze record in conductor docs.
        """
        freeze_file = Path("conductor/source-freeze.md")
        freeze_file.parent.mkdir(parents=True, exist_ok=True)
        if not freeze_file.exists():
            freeze_file.write_text(
                "# Source Freeze Register\n\n"
                "| Source | Source Version | Snapshot Date | Upstream URL | Raw Vault Prefix | Notes |\n"
                "| :--- | :--- | :--- | :--- | :--- | :--- |\n",
                encoding="utf-8",
            )

        row = (
            f"| {SOURCE_NAME} | {self.source_version} | {self.snapshot_date} | {CLINVAR_URL} | "
            f"`gs://{BUCKET_NAME}/{self.raw_vault_prefix()}/` | "
            f"last_modified={self.upstream_last_modified}; etag={self.upstream_etag} |\n"
        )
        current = freeze_file.read_text(encoding="utf-8")
        if row not in current:
            freeze_file.write_text(current + row, encoding="utf-8")
        print(f"✅ [Stage 2.5 Effect]: Freeze record updated at {freeze_file}")
        return self

    def upload_to_gcs(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 3 - Cloud Persistence.
        Effect: Uploads the VCF and manifest to the research bucket in GCS.
        """
        print(f"--- [ClinVar Stage 3]: Uploading to GCS: {BUCKET_NAME} ---")
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            
            # Upload parsed-raw artifact bundle used by downstream T002 steps.
            parsed_bundle = [
                (self.local_file, "clinvar_raw.vcf.gz"),
                (self.local_index, "clinvar_raw.vcf.gz.tbi"),
                (self.manifest_file, "manifest.json"),
            ]
            for local_p, gcs_n in parsed_bundle:
                blob = bucket.blob(f"{self.gcs_prefix()}/{gcs_n}")
                blob.upload_from_filename(local_p)
                print(f"✅ [Stage 3 Effect]: Uploaded to gs://{BUCKET_NAME}/{blob.name}")

            # Persist untouched raw-as-is snapshot in the dedicated raw vault path.
            raw_bundle = [
                (self.local_file, "clinvar.vcf.gz"),
                (self.local_index, "clinvar.vcf.gz.tbi"),
                (self.manifest_file, "manifest.json"),
            ]
            for local_p, gcs_n in raw_bundle:
                blob = bucket.blob(f"{self.raw_vault_prefix()}/{gcs_n}")
                blob.upload_from_filename(local_p)
                print(f"✅ [Stage 3 Effect]: Raw-as-is snapshot uploaded to gs://{BUCKET_NAME}/{blob.name}")
        except Exception as e:
            print(f"❌ [Stage 3 Effect]: GCS upload failed. Error: {e}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Status Reporting.
        Effect: Confirms the completion of the cloud-first ingestion stage.
        """
        print("🎉 [Final Effect]: ClinVar Cloud Ingestion completed successfully!")

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Start the ingestion pipeline.
    (
        ClinVarIngestionPipeline()
        .download_vcf()
        .generate_manifest()
        .write_source_freeze_doc()
        .upload_to_gcs()
        .finalize()
    )
