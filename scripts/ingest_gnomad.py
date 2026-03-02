# [AI-Agent: Gemini 2.0 Flash]: This script automates gnomAD v4 BRCA1/2 subset ingestion.
# Each class method is a stage in the frequency data acquisition pipeline.

import os
import sys
import subprocess
from typing import Final, Self
from google.cloud import storage

# [AI-Agent: Gemini 2.0 Flash]: Configuration for the gnomAD v4 ingestion pipeline.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
GNOMAD_SOURCE: Final[str] = "gs://gcp-public-data--gnomad/release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr17.vcf.bgz" # [AI-Agent: Gemini 2.0 Flash]: Example for BRCA1 (chr17).
LOCAL_FREQ_DIR: Final[str] = "data/raw/frequencies"
BRCA1_REGION: Final[str] = "chr17:43044295-43125483"
BRCA2_REGION: Final[str] = "chr13:32315086-32400266"

class GnomadIngestionPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the gnomAD v4 BRCA1/2 ingestion pipeline.
    Goal: Fetch gene-specific frequency subsets from gnomAD's public GCS buckets.
    """

    def __init__(self) -> None:
        self.fetch_ok: bool = False
        os.makedirs(LOCAL_FREQ_DIR, exist_ok=True)

    def fetch_subset(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - Direct Cloud-to-Local Filter.
        Effect: Uses bcftools to stream and filter public gnomAD data to a local subset.
        """
        print(f"--- [gnomAD Stage 1]: Filtering and Downloading BRCA1 from gnomAD ---")
        output_file = os.path.join(LOCAL_FREQ_DIR, "gnomad_brca1_subset.vcf.gz")
        
        # [AI-Agent: Gemini 2.0 Flash]: Sequential bcftools command chain for cloud-aware filtering.
        # This streams the data directly from the public GCS bucket using bcftools view.
        command = [
            "bcftools", "view",
            "--regions", BRCA1_REGION,
            "--output-type", "z",
            "--output", output_file,
            GNOMAD_SOURCE
        ]
        
        try:
            subprocess.run(command, check=True)
            print(f"✅ [Stage 1 Effect]: gnomAD BRCA1 subset saved to {output_file}")
            self.fetch_ok = True
        except subprocess.CalledProcessError as e:
            print(f"❌ [Stage 1 Effect]: gnomAD fetch failed. Error: {e}")
            sys.exit(1)
        return self

    def upload_to_research_bucket(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 2 - Result Persistence.
        Effect: Persists the frequency subset to the project-specific GCS bucket.
        """
        if not self.fetch_ok:
            return self

        print(f"--- [gnomAD Stage 2]: Uploading to Research GCS: {BUCKET_NAME} ---")
        try:
            local_file = os.path.join(LOCAL_FREQ_DIR, "gnomad_brca1_subset.vcf.gz")
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            blob = bucket.blob("raw/frequencies/gnomad_brca1_subset.vcf.gz")
            blob.upload_from_filename(local_file)
            print(f"✅ [Stage 2 Effect]: Successfully persisted subset to gs://{BUCKET_NAME}/{blob.name}")
        except Exception as e:
            print(f"❌ [Stage 2 Effect]: GCS upload failed. Error: {e}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Result Reporting.
        Effect: Confirms the completion of the gnomAD frequency ingestion pipeline.
        """
        match (self.fetch_ok):
            case True:
                print("
🎉 [Final Effect]: gnomAD Ingestion Pipeline completed successfully!")
            case _:
                print("
⚠️  [Final Effect]: gnomAD Ingestion Pipeline failed.")
                sys.exit(1)

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Run the frequency ingestion sequence.
    GnomadIngestionPipeline() 
        .fetch_subset() 
        .upload_to_research_bucket() 
        .finalize()
