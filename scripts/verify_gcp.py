# [AI-Agent]: This script follows the project's minimalist pipeline-style architecture.
# Each function represents a clear stage in the infrastructure verification pipeline.

import sys
from typing import Final, Self # [AI-Agent]: Self (introduced in 3.11) and typing for robust contracts.
from google.cloud import storage
from google.cloud import bigquery

# [AI-Agent]: Constants for pipeline configuration.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
DATASETS: Final[list[str]] = ["arab_acmg_raw", "arab_acmg_harmonized", "arab_acmg_results"]

class InfrastructurePipeline:
    """
    [AI-Agent]: Manages the verification pipeline for GCP resources.
    The goal is to provide clear feedback on connectivity to GCS and BigQuery.
    """

    def __init__(self) -> None:
        self.gcs_ok: bool = False
        self.bq_ok: bool = False

    def verify_gcs(self) -> Self:
        """
        [AI-Agent]: Stage 1 - GCS Verification.
        Effect: Confirms the presence and accessibility of the primary data bucket.
        """
        print(f"--- [Pipeline Stage 1]: Verifying GCS Bucket: {BUCKET_NAME} ---")
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            print(f"✅ [Effect]: Successfully connected to bucket: {bucket.name}")
            self.gcs_ok = True
        except Exception as e:
            print(f"❌ [Effect]: Failed to connect to GCS bucket. Error: {e}")
            self.gcs_ok = False
        return self

    def verify_bq(self) -> Self:
        """
        [AI-Agent]: Stage 2 - BigQuery Verification.
        Effect: Confirms that all three required datasets exist in the project.
        """
        print("--- [Pipeline Stage 2]: Verifying BigQuery Datasets ---")
        try:
            bq_client = bigquery.Client(project=PROJECT_ID)
            results = []
            for ds_id in DATASETS:
                ds_ref = f"{PROJECT_ID}.{ds_id}"
                try:
                    dataset = bq_client.get_dataset(ds_ref)
                    print(f"✅ [Effect]: Successfully connected to dataset: {dataset.dataset_id}")
                    results.append(True)
                except Exception as e:
                    print(f"❌ [Effect]: Failed to connect to dataset {ds_id}. Error: {e}")
                    results.append(False)
            self.bq_ok = all(results)
        except Exception as e:
            print(f"❌ [Effect]: Failed to initialize BigQuery client. Error: {e}")
            self.bq_ok = False
        return self

    def finalize(self) -> None:
        """
        [AI-Agent]: Final Stage - Result Aggregation.
        Effect: Determines the exit status of the pipeline based on previous stages.
        """
        # [AI-Agent]: Pattern matching (match/case) introduced in 3.10, utilized for clear flow control.
        match (self.gcs_ok, self.bq_ok):
            case (True, True):
                print("\n🎉 [Final Effect]: All GCP resources verified successfully!")
                sys.exit(0)
            case _:
                print("\n⚠️  [Final Effect]: Pipeline failed - Some GCP resources could not be verified.")
                sys.exit(1)

if __name__ == "__main__":
    # [AI-Agent]: Execute the pipeline stages in sequence.
    InfrastructurePipeline() \
        .verify_gcs() \
        .verify_bq() \
        .finalize()