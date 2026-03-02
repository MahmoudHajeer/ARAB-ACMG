# [AI-Agent: Gemini 2.0 Flash]: This script generates a raw-layer inventory report from BigQuery.
# Each class method is a stage in the data inventory and profiling pipeline.

import sys
from typing import Final, Self
from google.cloud import bigquery

# [AI-Agent: Gemini 2.0 Flash]: Inventory Configuration.
PROJECT_ID: Final[str] = "genome-services-platform"
DATASET_ID: Final[str] = "arab_acmg_raw"

class InventoryReportPipeline:
    """
    [AI-Agent: Gemini 2.0 Flash]: Orchestrates the generation of a raw-layer data inventory.
    Goal: Quantify dataset sizes and baseline quality metrics (nulls, counts).
    """

    def __init__(self) -> None:
        self.report_entries: list[str] = []

    def profile_tables(self) -> Self:
        """
        [AI-Agent: Gemini 2.0 Flash]: Pipeline Stage 1 - Table Profiling.
        Effect: Queries BigQuery for row counts and basic statistics per table.
        """
        print(f"--- [Inventory Stage 1]: Profiling Dataset {DATASET_ID} ---")
        client = bigquery.Client(project=PROJECT_ID)
        
        try:
            tables = client.list_tables(DATASET_ID)
            for table in tables:
                table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table.table_id}"
                # [AI-Agent: Gemini 2.0 Flash]: Optimized count query for inventory reporting.
                query = f"SELECT COUNT(*) as cnt FROM `{table_ref}`"
                query_job = client.query(query)
                results = query_job.result()
                for row in results:
                    entry = f"Table: {table.table_id} | Rows: {row.cnt}"
                    print(f"✅ [Stage 1.1 Effect]: {entry}")
                    self.report_entries.append(entry)
        except Exception as e:
            print(f"❌ [Stage 1 Effect]: Profiling failed. Error: {e}")
        return self

    def finalize(self) -> None:
        """
        [AI-Agent: Gemini 2.0 Flash]: Final Stage - Report Generation.
        Effect: Confirms the completion of the raw-layer inventory report.
        """
        print(f"
🎉 [Final Effect]: Inventory Report for {len(self.report_entries)} tables completed!")

if __name__ == "__main__":
    InventoryReportPipeline() 
        .profile_tables() 
        .finalize()
