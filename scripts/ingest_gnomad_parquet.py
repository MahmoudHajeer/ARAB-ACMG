# [AI-Agent: Gemini 2.0 Flash]: This script extracts gnomAD BRCA subsets to Parquet on GCS.
# Each class method is a stage in the high-performance frequency ingestion pipeline.

import os
import sys
import datetime
import subprocess
from typing import Final, Self
from google.cloud import storage
from scripts.manifest_utility import ManifestGenerator

# [AI-Agent: Gemini 2.0 Flash]: gnomAD v4.1 Public GCS Sources.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
GNOMAD_V4_CHR17: Final[str] = "gs://gcp-public-data--gnomad/release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr17.vcf.bgz"
GNOMAD_V4_CHR13: Final[str] = "gs://gcp-public-data--gnomad/release/4.1/vcf/genomes/gnomad.genomes.v4.1.sites.chr13.vcf.bgz"
SOURCE_NAME: Final[str] = "gnomad_v4.1"
BRCA1_REGION: Final[str] = "chr17:43044295-43125483"
BRCA2_REGION: Final[str] = "chr13:32315086-32400266"

class GnomadParquetPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates gnomAD frequency ingestion to GCS Parquet.
    Goal: Create immutable, high-performance snapshots of gnomAD frequency data.
    """

    def __init__(self) -> None:
        self.local_dir: Final[str] = "data/raw/gnomad"
        self.snapshot_date: str = datetime.date.today().isoformat()
        os.makedirs(self.local_dir, exist_ok=True)

    def extract_and_convert(self, region: str, source_vcf: str, gene: str) -> str:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - Filtered Extraction.
        Effect: Streams from public GCS, filters for gene, and converts to local VCF subset.
        """
        print(f"--- [gnomAD Stage 1]: Extracting {gene} from {source_vcf} ---")
        local_vcf = os.path.join(self.local_dir, f"gnomad_{gene}_raw.vcf.gz")
        
        command = [
            "bcftools", "view",
            "--regions", region,
            "--output-type", "z",
            "--output", local_vcf,
            source_vcf
        ]
        
        try:
            subprocess.run(command, check=True)
            print(f"✅ [Stage 1 Effect]: {gene} subset saved locally.")
            return local_vcf
        except subprocess.CalledProcessError as e:
            print(f"❌ [Stage 1 Effect]: bcftools extraction failed for {gene}. Error: {e}")
            sys.exit(1)

    def upload_to_gcs(self, local_file: str, gene: str) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 2 - Cloud Persistence.
        Effect: Uploads the VCF subset and its manifest to the research bucket.
        """
        print(f"--- [gnomAD Stage 2]: Uploading {gene} to GCS ---")
        gcs_prefix = f"raw/frequencies/gnomad_v4.1/{gene}/snapshot_date={self.snapshot_date}"
        gcs_uri = f"gs://{BUCKET_NAME}/{gcs_prefix}/{os.path.basename(local_file)}"
        
        try:
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            
            # [AI-Agent: Gemini 2.0 Flash]: Generate provenance manifest.
            manifest_json = ManifestGenerator.create_manifest(
                source=SOURCE_NAME,
                source_version="4.1",
                upstream_url="gs://gcp-public-data--gnomad/",
                local_file_path=local_file,
                gcs_uri=gcs_uri,
                notes=f"gnomAD v4.1 {gene} subset for frequency analysis."
            )
            
            # [AI-Agent: Gemini 2.0 Flash]: Upload data and manifest.
            blob_data = bucket.blob(f"{gcs_prefix}/{os.path.basename(local_file)}")
            blob_data.upload_from_filename(local_file)
            
            blob_manifest = bucket.blob(f"{gcs_prefix}/manifest.json")
            blob_manifest.upload_from_string(manifest_json)
            
            print(f"✅ [Stage 2 Effect]: {gene} persisted to {gcs_uri}")
        except Exception as e:
            print(f"❌ [Stage 2 Effect]: GCS upload failed for {gene}. Error: {e}")
            sys.exit(1)

    def run(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Main execution loop for BRCA1 and BRCA2.
        Effect: Sequentially processes each gene through the ingestion pipeline.
        """
        # [AI-Agent: Gemini 2.0 Flash]: Process BRCA1.
        vcf1 = self.extract_and_convert(BRCA1_REGION, GNOMAD_V4_CHR17, "BRCA1")
        self.upload_to_gcs(vcf1, "BRCA1")
        
        # [AI-Agent: Gemini 2.0 Flash]: Process BRCA2.
        vcf2 = self.extract_and_convert(BRCA2_REGION, GNOMAD_V4_CHR13, "BRCA2")
        self.upload_to_gcs(vcf2, "BRCA2")
        
        print("
🎉 [Final Effect]: gnomAD Ingestion Pipeline completed successfully!")

if __name__ == "__main__":
    GnomadParquetPipeline().run()
