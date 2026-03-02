# [AI-Agent: Gemini 2.0 Flash]: This script automates ClinVar VCF ingestion to GCS.
# It implements a cloud-first workflow with automated provenance manifest generation.

import os
import sys
import datetime
import requests
from typing import Final, Self
from google.cloud import storage
from scripts.manifest_utility import ManifestGenerator

# [AI-Agent: Gemini 2.0 Flash]: Pipeline Configuration.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
CLINVAR_URL: Final[str] = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz"
SOURCE_NAME: Final[str] = "clinvar"
SOURCE_VERSION: Final[str] = "2026-03-02" # [AI-Agent: Gemini 2.0 Flash]: Recorded snapshot version.

class ClinVarIngestionPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the ClinVar data ingestion to GCS.
    Goal: Capture raw ClinVar VCF and its provenance metadata in the research bucket.
    """

    def __init__(self) -> None:
        self.local_dir: Final[str] = "data/raw/clinvar"
        self.local_file: str = os.path.join(self.local_dir, "clinvar_raw.vcf.gz")
        self.manifest_file: str = os.path.join(self.local_dir, "manifest.json")
        self.gcs_prefix: str = f"raw/{SOURCE_NAME}/{SOURCE_VERSION}/snapshot_date={datetime.date.today().isoformat()}"
        os.makedirs(self.local_dir, exist_ok=True)

    def download_vcf(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - HTTP Ingestion.
        Effect: Downloads the bulk ClinVar VCF from NCBI to local staging.
        """
        print(f"--- [ClinVar Stage 1]: Downloading from {CLINVAR_URL} ---")
        try:
            with requests.get(CLINVAR_URL, stream=True) as r:
                r.raise_for_status()
                with open(self.local_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print(f"✅ [Stage 1 Effect]: ClinVar VCF downloaded to {self.local_file}")
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
        gcs_uri = f"gs://{BUCKET_NAME}/{self.gcs_prefix}/clinvar_raw.vcf.gz"
        manifest_json = ManifestGenerator.create_manifest(
            source=SOURCE_NAME,
            source_version=SOURCE_VERSION,
            upstream_url=CLINVAR_URL,
            local_file_path=self.local_file,
            gcs_uri=gcs_uri,
            notes="Bulk ClinVar GRCh38 download for ARAB-ACMG research."
        )
        with open(self.manifest_file, "w") as f:
            f.write(manifest_json)
        print(f"✅ [Stage 2 Effect]: Manifest created at {self.manifest_file}")
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
            
            # [AI-Agent: Gemini 2.0 Flash]: Upload both data and manifest.
            for local_p, gcs_n in [(self.local_file, "clinvar_raw.vcf.gz"), (self.manifest_file, "manifest.json")]:
                blob = bucket.blob(f"{self.gcs_prefix}/{gcs_n}")
                blob.upload_from_filename(local_p)
                print(f"✅ [Stage 3 Effect]: Uploaded to gs://{BUCKET_NAME}/{blob.name}")
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
    ClinVarIngestionPipeline() 
        .download_vcf() 
        .generate_manifest() 
        .upload_to_gcs() 
        .finalize()
