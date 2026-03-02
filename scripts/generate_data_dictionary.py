# [AI-Agent: Gemini 2.0 Flash]: This script generates a data dictionary for raw BigQuery tables.
# Each class method is a stage in the metadata documentation pipeline.

import sys
from typing import Final, Self
from google.cloud import bigquery

# [AI-Agent: Gemini 2.0 Flash]: Metadata Configuration.
PROJECT_ID: Final[str] = "genome-services-platform"
DATASET_ID: Final[str] = "arab_acmg_raw"

class DataDictionaryPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the creation of a research data dictionary.
    Goal: Document schemas and provenance for all raw ingested tables.
    """

    def __init__(self) -> None:
        self.tables_documented: int = 0

    def document_dataset(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - Schema Extraction.
        Effect: Iterates through BigQuery tables and extracts field descriptions and types.
        """
        print(f"--- [Dictionary Stage 1]: Documenting Dataset {DATASET_ID} ---")
        client = bigquery.Client(project=PROJECT_ID)
        
        try:
            tables = client.list_tables(DATASET_ID)
            for table in tables:
                print(f"--- [Stage 1.1]: Documenting Table {table.table_id} ---")
                # [AI-Agent: Gemini 2.0 Flash]: Fetch full table metadata.
                full_table = client.get_table(f"{PROJECT_ID}.{DATASET_ID}.{table.table_id}")
                for field in full_table.schema:
                    print(f"  - Field: {field.name} ({field.field_type}): {field.description or 'No description'}")
                self.tables_documented += 1
        except Exception as e:
            print(f"❌ [Stage 1 Effect]: Documentation failed. Error: {e}")
        return self

    def finalize(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Status Reporting.
        Effect: Confirms the completion of the data dictionary generation.
        """
        print(f"🎉 [Final Effect]: Data Dictionary for {self.tables_documented} tables completed!")

if __name__ == "__main__":
    DataDictionaryPipeline().document_dataset().finalize()
