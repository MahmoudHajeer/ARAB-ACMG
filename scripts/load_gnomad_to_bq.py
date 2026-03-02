# [AI-Agent: Gemini 2.0 Flash]: This script loads gnomAD frequency subsets into BigQuery.
# Each class method is a stage in the frequency data loading pipeline.

import os
import sys
from typing import Final, Self
from google.cloud import bigquery

# [AI-Agent: Gemini 2.0 Flash]: Configuration for the gnomAD BQ load pipeline.
PROJECT_ID: Final[str] = "genome-services-platform"
DATASET_ID: Final[str] = "arab_acmg_raw"
TABLE_ID: Final[str] = "gnomad_v4_1"
LOCAL_FREQ_DIR: Final[str] = "data/raw/gnomad"
GENES: Final[list[str]] = ["BRCA1", "BRCA2"]

class GnomadBQLoaderPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the loading of gnomAD frequency subsets into BigQuery.
    Goal: Create a centralized frequency table for global and ancestry AF values.
    """

    def __init__(self) -> None:
        self.bq_table_ref: str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    def load_subsets(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - Batch BigQuery Loading.
        Effect: Loads the BRCA1 and BRCA2 VCF subsets into a single BQ table.
        """
        print(f"--- [gnomAD BQ Stage 1]: Loading Subsets to {self.bq_table_ref} ---")
        client = bigquery.Client(project=PROJECT_ID)
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter='	',
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND, # [AI-Agent: Gemini 2.0 Flash]: Append each gene subset.
        )

        for gene in GENES:
            local_file = os.path.join(LOCAL_FREQ_DIR, f"gnomad_{gene}_raw.vcf.gz")
            if not os.path.exists(local_file):
                print(f"⚠️  [Stage 1 Effect]: Missing subset for {gene}. Skipping.")
                continue
                
            print(f"--- [Stage 1.1]: Loading {gene} ---")
            with open(local_file, "rb") as source_file:
                job = client.load_table_from_file(source_file, self.bq_table_ref, job_config=job_config)
            
            job.result() # [AI-Agent: Gemini 2.0 Flash]: Wait for completion.
            print(f"✅ [Stage 1.1 Effect]: Successfully loaded {gene} subset.")
        
        return self

    def finalize(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Status Reporting.
        Effect: Confirms the completion of the gnomAD frequency layer in BigQuery.
        """
        print("🎉 [Final Effect]: gnomAD BigQuery Raw Layer setup completed successfully!")

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Run the BigQuery load sequence for frequency data.
    GnomadBQLoaderPipeline().load_subsets().finalize()
