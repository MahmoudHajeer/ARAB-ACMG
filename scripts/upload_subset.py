# [AI-Agent: Gemini 2.0 Flash]: This script uploads the filtered BRCA subset to GCS.
# Each class method is a stage in the cloud persistence pipeline.

import os
import sys
from typing import Final, Self
from google.cloud import storage

# [AI-Agent: Gemini 2.0 Flash]: Configuration for cloud persistence.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
LOCAL_RAW_DIR: Final[str] = "data/raw/clinvar"
FILES_TO_UPLOAD: Final[list[str]] = [
    "clinvar_brca_subset.vcf.gz",
    "clinvar_brca_subset.vcf.gz.tbi"
]

class SubsetUploadPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the cloud storage upload pipeline.
    Goal: Persist the filtered and indexed BRCA data into GCS for downstream analysis.
    """

    def __init__(self) -> None:
        self.upload_results: list[bool] = []

    def check_local_files(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - Pre-flight Check.
        Effect: Confirms that the target filtered files and their indices exist.
        """
        print("--- [Upload Stage 1]: Verifying Local Subsets ---")
        for f_name in FILES_TO_UPLOAD:
            f_path = os.path.join(LOCAL_RAW_DIR, f_name)
            if not os.path.exists(f_path):
                print(f"❌ [Stage 1 Effect]: Missing file: {f_path}")
                sys.exit(1)
            print(f"✅ [Stage 1 Effect]: Verified file: {f_name}")
        return self

    def upload_to_gcs(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 2 - Cloud Persistence.
        Effect: Uploads the filtered BRCA files to the project's GCS bucket.
        """
        print(f"--- [Upload Stage 2]: Uploading to GCS: {BUCKET_NAME} ---")
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            
            for f_name in FILES_TO_UPLOAD:
                f_path = os.path.join(LOCAL_RAW_DIR, f_name)
                blob = bucket.blob(f"raw/clinvar/{f_name}")
                print(f"--- [Stage 2.1]: Uploading {f_name} ---")
                blob.upload_from_filename(f_path)
                print(f"✅ [Stage 2.1 Effect]: Successfully uploaded to gs://{BUCKET_NAME}/{blob.name}")
                self.upload_results.append(True)
        except Exception as e:
            print(f"❌ [Stage 2 Effect]: GCS upload failed. Error: {e}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Result Verification.
        Effect: Determines the final status of the upload pipeline.
        """
        match (len(self.upload_results) == len(FILES_TO_UPLOAD)):
            case True:
                print("
🎉 [Final Effect]: Subset Upload Pipeline completed successfully!")
            case _:
                print("
⚠️  [Final Effect]: Subset Upload Pipeline failed to complete all uploads.")
                sys.exit(1)

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Initiate the cloud persistence sequence.
    SubsetUploadPipeline() 
        .check_local_files() 
        .upload_to_gcs() 
        .finalize()
