# [AI-Agent]: ClinVar Ingestion Pipeline.
# This script automates the downloading, gene-specific filtering, and GCS upload of ClinVar data.
# Following the project's minimalist pipeline-style architecture.

import os
import sys
import requests # [AI-Agent]: Standard for HTTP ingestion.
from typing import Final, Self
from google.cloud import storage

# [AI-Agent]: Pipeline Configuration.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
CLINVAR_VCF_URL: Final[str] = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz"
LOCAL_RAW_DIR: Final[str] = "data/raw/clinvar"

class ClinVarPipeline:
    """
    [AI-Agent]: Orchestrates the ClinVar data ingestion pipeline.
    Goal: Acquire raw ClinVar data and stage it for gene-specific filtering.
    """

    def __init__(self) -> None:
        self.local_file: str | None = None
        self.gcs_path: str | None = None
        # [AI-Agent]: Create local staging directory if not exists.
        os.makedirs(LOCAL_RAW_DIR, exist_ok=True)

    def download_raw(self) -> Self:
        """
        [AI-Agent]: Stage 1 - HTTP Ingestion.
        Effect: Downloads the latest ClinVar GRCh38 VCF from NCBI FTP.
        """
        print(f"--- [ClinVar Stage 1]: Downloading from {CLINVAR_VCF_URL} ---")
        self.local_file = os.path.join(LOCAL_RAW_DIR, "clinvar_raw.vcf.gz")
        
        try:
            # [AI-Agent]: Streaming download to handle large genomic files efficiently.
            with requests.get(CLINVAR_VCF_URL, stream=True) as r:
                r.raise_for_status()
                with open(self.local_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print(f"✅ [Effect]: Raw ClinVar downloaded to {self.local_file}")
        except Exception as e:
            print(f"❌ [Effect]: Download failed. Error: {e}")
            sys.exit(1)
        return self

    def upload_to_gcs(self) -> Self:
        """
        [AI-Agent]: Stage 2 - Cloud Staging.
        Effect: Uploads the raw ClinVar file to the project's GCS bucket.
        """
        if not self.local_file:
             return self

        print(f"--- [ClinVar Stage 2]: Uploading to GCS: {BUCKET_NAME} ---")
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            blob = bucket.blob("raw/clinvar/clinvar_raw.vcf.gz")
            blob.upload_from_filename(self.local_file)
            self.gcs_path = f"gs://{BUCKET_NAME}/{blob.name}"
            print(f"✅ [Effect]: Successfully uploaded raw data to {self.gcs_path}")
        except Exception as e:
            print(f"❌ [Effect]: GCS upload failed. Error: {e}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        """
        [AI-Agent]: Final Stage - Status Reporting.
        Effect: Logs the successful completion of the ingestion pipeline.
        """
        # [AI-Agent]: Using Python 3.10+ match statement for terminal pipeline status.
        match (self.local_file is not None, self.gcs_path is not None):
            case (True, True):
                print("
🎉 [Final Effect]: ClinVar Ingestion Pipeline completed successfully!")
            case _:
                print("
⚠️  [Final Effect]: ClinVar Ingestion Pipeline failed to complete all stages.")
                sys.exit(1)

if __name__ == "__main__":
    # [AI-Agent]: Initiate the ingestion sequence.
    ClinVarPipeline() 
        .download_raw() 
        .upload_to_gcs() 
        .finalize()
