# [AI-Agent: Gemini 2.0 Flash]: This script automates ingestion of Arab-enriched frequency data to GCS.
# Each class method is a stage in the Arab frequency data acquisition pipeline.

import os
import sys
import datetime
import requests
from typing import Final, Self
from google.cloud import storage
from scripts.manifest_utility import ManifestGenerator

# [AI-Agent: Gemini 2.0 Flash]: Arab-Enriched Frequency Sources.
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
GME_URL: Final[str] = "http://igm.ucsd.edu/gme/data/GME_Variome_v1.0.vcf.gz"
QGP_SUMMARY_URL: Final[str] = "https://example.com/qgp_brca_summary.csv" # [AI-Agent: Gemini 2.0 Flash]: Placeholder for QGP data access.

class ArabEnrichedIngestionPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the ingestion of Arab-specific datasets.
    Goal: Capture GME and QGP frequency data with full provenance in GCS.
    """

    def __init__(self) -> None:
        self.local_dir: Final[str] = "data/raw/arab_enriched"
        self.snapshot_date: str = datetime.date.today().isoformat()
        os.makedirs(self.local_dir, exist_ok=True)

    def ingest_source(self, name: str, url: str, file_name: str) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Unified Ingestion Stage for Arab sources.
        Effect: Downloads, manifests, and uploads a specific Arab frequency dataset.
        """
        print(f"--- [Arab Enriched]: Processing {name} from {url} ---")
        local_path = os.path.join(self.local_dir, file_name)
        gcs_prefix = f"raw/frequencies/{name}/v1.0/snapshot_date={self.snapshot_date}"
        gcs_uri = f"gs://{BUCKET_NAME}/{gcs_prefix}/{file_name}"
        
        try:
            # [AI-Agent: Gemini 2.0 Flash]: Stage 1 - Download.
            response = requests.get(url, stream=True)
            response.raise_for_status()
            with open(local_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            print(f"✅ [Effect]: {name} downloaded locally.")
            
            # [AI-Agent: Gemini 2.0 Flash]: Stage 2 - Manifest.
            manifest_json = ManifestGenerator.create_manifest(
                source=name,
                source_version="1.0",
                upstream_url=url,
                local_file_path=local_path,
                gcs_uri=gcs_uri,
                notes=f"Arab-enriched frequency source: {name}."
            )
            
            # [AI-Agent: Gemini 2.0 Flash]: Stage 3 - Upload.
            storage_client = storage.Client(project=PROJECT_ID)
            bucket = storage_client.get_bucket(BUCKET_NAME)
            
            bucket.blob(f"{gcs_prefix}/{file_name}").upload_from_filename(local_path)
            bucket.blob(f"{gcs_prefix}/manifest.json").upload_from_string(manifest_json)
            print(f"✅ [Effect]: {name} persisted to {gcs_uri}")
            
        except Exception as e:
            print(f"❌ [Effect]: Ingestion failed for {name}. Error: {e}")

    def run(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Main execution loop for Arab frequency sources.
        Effect: Sequentially ingests each identified source.
        """
        self.ingest_source("gme_variome", GME_URL, "gme_variome.vcf.gz")
        # [AI-Agent: Gemini 2.0 Flash]: QGP ingestion is ready once URL is confirmed.
        # self.ingest_source("qatar_genome", QGP_SUMMARY_URL, "qgp_brca_summary.csv")
        
        print("🎉 [Final Effect]: Arab Enriched Ingestion Pipeline completed!")

if __name__ == "__main__":
    ArabEnrichedIngestionPipeline().run()
