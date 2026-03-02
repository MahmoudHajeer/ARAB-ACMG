# [AI-Agent: Gemini 2.0 Flash]: This script automates ingestion of Arab-specific frequency datasets.
# Each class method is a stage in the Arab-enriched frequency data acquisition pipeline.

import os
import sys
import requests
from typing import Final, Self
from google.cloud import storage

# [AI-Agent: Gemini 2.0 Flash]: Pipeline Configuration.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
GME_VCF_URL: Final[str] = "http://igm.ucsd.edu/gme/data/GME_Variome_v1.0.vcf.gz" # [AI-Agent: Gemini 2.0 Flash]: Example GME source.
LOCAL_RAW_DIR: Final[str] = "data/raw/frequencies"

class ArabFrequencyPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the ingestion of Arab-enriched frequency datasets.
    Goal: Collect high-quality frequency data for Middle Eastern populations.
    """

    def __init__(self) -> None:
        self.download_results: list[bool] = []
        os.makedirs(LOCAL_RAW_DIR, exist_ok=True)

    def download_gme(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - GME Ingestion.
        Effect: Downloads the GME Variome VCF to local staging.
        """
        print(f"--- [Arab Stage 1]: Downloading from GME: {GME_VCF_URL} ---")
        local_file = os.path.join(LOCAL_RAW_DIR, "gme_variome.vcf.gz")
        try:
            with requests.get(GME_VCF_URL, stream=True) as r:
                r.raise_for_status()
                with open(local_file, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            print(f"✅ [Stage 1 Effect]: GME Variome downloaded to {local_file}")
            self.download_results.append(True)
        except Exception as e:
            print(f"❌ [Stage 1 Effect]: GME download failed. Error: {e}")
            self.download_results.append(False)
        return self

    def upload_to_gcs(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 2 - Cloud Persistence.
        Effect: Uploads the GME dataset to our research GCS bucket.
        """
        if not any(self.download_results):
            return self

        print(f"--- [Arab Stage 2]: Uploading to Research GCS: {BUCKET_NAME} ---")
        try:
            local_file = os.path.join(LOCAL_RAW_DIR, "gme_variome.vcf.gz")
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            blob = bucket.blob("raw/frequencies/gme_variome.vcf.gz")
            blob.upload_from_filename(local_file)
            print(f"✅ [Stage 2 Effect]: Successfully persisted GME data to gs://{BUCKET_NAME}/{blob.name}")
        except Exception as e:
            print(f"❌ [Stage 2 Effect]: GCS upload failed. Error: {e}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Result Reporting.
        Effect: Confirms the completion of the Arab frequency ingestion pipeline.
        """
        match (any(self.download_results)):
            case True:
                print("
🎉 [Final Effect]: Arab Frequency Ingestion Pipeline completed successfully!")
            case _:
                print("
⚠️  [Final Effect]: Arab Frequency Ingestion Pipeline failed.")
                sys.exit(1)

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Initiate the ingestion sequence.
    ArabFrequencyPipeline() 
        .download_gme() 
        .upload_to_gcs() 
        .finalize()
