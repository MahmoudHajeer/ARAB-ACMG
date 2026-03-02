# [AI-Agent: Gemini 2.0 Flash]: This script loads filtered ClinVar data into BigQuery raw layer.
# Each class method is a stage in the BigQuery load pipeline (filtered via bcftools).

import os
import sys
import subprocess
from typing import Final, Self
from google.cloud import bigquery
from google.cloud import storage

# [AI-Agent: Gemini 2.0 Flash]: Configuration for the BigQuery load pipeline.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
DATASET_ID: Final[str] = "arab_acmg_raw"
TABLE_ID: Final[str] = "clinvar"
# [AI-Agent: Gemini 2.0 Flash]: Genomic coordinates for BRCA1/2 (GRCh38).
BRCA_REGIONS: Final[str] = "chr17:43044295-43125483,chr13:32315086-32400266"

class ClinVarBQLoaderPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the loading of BRCA1/2 filtered ClinVar data into BigQuery.
    Goal: Create a clean raw table in BigQuery for the target genes.
    """

    def __init__(self) -> None:
        self.local_raw: str = "data/raw/clinvar/clinvar_raw.vcf.gz"
        self.local_filtered: str = "data/raw/clinvar/clinvar_brca_filtered.vcf"
        self.bq_table_ref: str = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    def filter_locally(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - Gene-Specific Filtering.
        Effect: Uses bcftools to extract BRCA1/2 variants to a local VCF for loading.
        """
        print(f"--- [BQ Load Stage 1]: Filtering {self.local_raw} for BRCA1/2 ---")
        if not os.path.exists(self.local_raw):
            print(f"❌ [Stage 1 Effect]: Local raw file missing. Run ingest script first.")
            sys.exit(1)
            
        # [AI-Agent: Gemini 2.0 Flash]: Sequential bcftools command for high-performance extraction.
        command = [
            "bcftools", "view",
            "--regions", BRCA_REGIONS,
            "--output-type", "v", # [AI-Agent: Gemini 2.0 Flash]: Output as VCF (uncompressed) for BQ load.
            "--output", self.local_filtered,
            self.local_raw
        ]
        
        try:
            subprocess.run(command, check=True)
            print(f"✅ [Stage 1 Effect]: Filtered VCF saved to {self.local_filtered}")
        except subprocess.CalledProcessError as e:
            print(f"❌ [Stage 1 Effect]: bcftools filter failed. Error: {e}")
            sys.exit(1)
        return self

    def load_to_bq(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 2 - BigQuery Load.
        Effect: Loads the filtered VCF into the arab_acmg_raw.clinvar table.
        """
        print(f"--- [BQ Load Stage 2]: Loading to BigQuery Table: {self.bq_table_ref} ---")
        client = bigquery.Client(project=PROJECT_ID)
        
        # [AI-Agent: Gemini 2.0 Flash]: Config for loading VCF. 
        # Note: BigQuery does not natively parse VCF columns into fields directly without extra steps.
        # We'll load the raw lines first or use a structured approach if required by contracts.
        # For simplicity in this stage, we load it as a single STRING field per row and will parse it via dbt.
        
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter='	', # [AI-Agent: Gemini 2.0 Flash]: VCFs are tab-delimited.
            skip_leading_rows=0,   # [AI-Agent: Gemini 2.0 Flash]: Keep headers if we want to parse them.
            autodetect=True,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        )

        try:
            with open(self.local_filtered, "rb") as source_file:
                job = client.load_table_from_file(source_file, self.bq_table_ref, job_config=job_config)
            
            job.result() # [AI-Agent: Gemini 2.0 Flash]: Wait for completion.
            print(f"✅ [Stage 2 Effect]: Successfully loaded {job.output_rows} rows to BigQuery.")
        except Exception as e:
            print(f"❌ [Stage 2 Effect]: BigQuery load failed. Error: {e}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Status Reporting.
        Effect: Confirms the completion of the BigQuery raw layer setup.
        """
        print("
🎉 [Final Effect]: ClinVar BigQuery Raw Layer setup completed successfully!")

if __name__ == "__main__":
    # [AI-Agent: Gemini 2.0 Flash]: Run the BigQuery load sequence.
    ClinVarBQLoaderPipeline() 
        .filter_locally() 
        .load_to_bq() 
        .finalize()
