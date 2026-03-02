# [AI-Agent: Gemini 2.0 Flash]: This script performs automated QC on ingested variant data.
# Each class method is a stage in the quality control and validation pipeline.

import sys
from typing import Final, Self
from google.cloud import storage

# [AI-Agent: Gemini 2.0 Flash]: QC Configuration.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
RAW_PREFIXES: Final[list[str]] = ["raw/clinvar/", "raw/frequencies/"]

class QCPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the automated QC and validation of raw data in GCS.
    Goal: Ensure all ingested datasets are present and pass basic integrity checks.
    """

    def __init__(self) -> None:
        self.qc_results: list[bool] = []

    def inventory_raw_data(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - Data Inventory.
        Effect: Lists all files in the raw/ directories and confirms their existence.
        """
        print(f"--- [QC Stage 1]: Inventorying GCS Bucket: {BUCKET_NAME} ---")
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            
            for prefix in RAW_PREFIXES:
                blobs = list(bucket.list_blobs(prefix=prefix))
                print(f"--- [Stage 1.1]: Prefix {prefix} ---")
                if not blobs:
                    print(f"⚠️  [Stage 1.1 Effect]: No files found under {prefix}")
                    self.qc_results.append(False)
                for blob in blobs:
                    print(f"✅ [Stage 1.1 Effect]: Found: {blob.name} (Size: {blob.size} bytes)")
                    self.qc_results.append(True)
        except Exception as e:
            print(f"❌ [Stage 1 Effect]: GCS inventory failed. Error: {e}")
            sys.exit(1)
        return self

    def verify_vcf_integrity(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 2 - Format Validation.
        Effect: Confirms that all .vcf.gz files have corresponding indices (.tbi).
        """
        print("--- [QC Stage 2]: Verifying VCF/Tabix Pairing ---")
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            blobs = [b.name for b in bucket.list_blobs(prefix="raw/")]
            
            for blob_name in blobs:
                if blob_name.endswith(".vcf.gz"):
                    index_name = blob_name + ".tbi"
                    if index_name in blobs:
                         print(f"✅ [Stage 2 Effect]: Correct pairing: {blob_name}")
                         self.qc_results.append(True)
                    else:
                         print(f"⚠️  [Stage 2 Effect]: Missing index for: {blob_name}")
                         self.qc_results.append(False)
        except Exception as e:
            print(f"❌ [Stage 2 Effect]: GCS verification failed. Error: {e}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Status Reporting.
        Effect: Summarizes the QC results and determines if the track can proceed.
        """
        match (all(self.qc_results) and len(self.qc_results) > 0):
            case True:
                print("
🎉 [Final Effect]: Automated QC Pipeline completed successfully!")
                sys.exit(0)
            case _:
                print("
⚠️  [Final Effect]: Automated QC Pipeline identified issues.")
                sys.exit(1)

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Initiate the automated QC sequence.
    QCPipeline() 
        .inventory_raw_data() 
        .verify_vcf_integrity() 
        .finalize()
