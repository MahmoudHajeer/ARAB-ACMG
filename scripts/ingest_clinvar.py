# [AI-Agent: Gemini 2.0 Flash]: This script automates ClinVar VCF ingestion.
# Each class method is a stage in the data acquisition pipeline.

import os
import sys
import requests
from typing import Final, Self
from google.cloud import storage

# [AI-Agent: Gemini 2.0 Flash]: Configuration for the ingestion pipeline.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
CLINVAR_VCF_URL: Final[str] = "https://ftp.ncbi.nlm.nih.gov/pub/clinvar/vcf_GRCh38/clinvar.vcf.gz"
LOCAL_RAW_DIR: Final[str] = "data/raw/clinvar"

class ClinVarPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the ClinVar data ingestion pipeline.
    The goal is to acquire the latest raw ClinVar data and stage it in GCS.
    """

    def __init__(self) -> None:
        self.local_file: str | None = None
        self.gcs_path: str | None = None
        os.makedirs(LOCAL_RAW_DIR, exist_ok=True)

    def download_raw(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - HTTP Download.
        Effect: Downloads the latest ClinVar GRCh38 VCF from NCBI to local staging.
        """
        print(f"--- [ClinVar Stage 1]: Downloading from {CLINVAR_VCF_URL} ---")
        self.local_file = os.path.join(LOCAL_RAW_DIR, "clinvar_raw.vcf.gz")
        
        try:
            # [AI-Agent: Gemini 2.0 Flash]: Use streaming download to manage memory for large VCFs.
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
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 2 - Cloud Staging.
        Effect: Persists the raw ClinVar file into our research GCS bucket.
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
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Status Reporting.
        Effect: Verifies completion and logs the successful end of the ingestion pipeline.
        """
        # [AI-Agent: Gemini 2.0 Flash]: Utilize match/case for clean final status flow.
        match (self.local_file is not None, self.gcs_path is not None):
            case (True, True):
                print("\n🎉 [Final Effect]: ClinVar Ingestion Pipeline completed successfully!")
            case _:
                print("\n⚠️  [Final Effect]: ClinVar Ingestion Pipeline failed to complete all stages.")
                sys.exit(1)

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Execute the pipeline sequence.
    ClinVarPipeline() \
        .download_raw() \
        .upload_to_gcs() \
        .finalize()