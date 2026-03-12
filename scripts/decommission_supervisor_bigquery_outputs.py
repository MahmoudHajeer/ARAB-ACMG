"""Delete non-raw BigQuery outputs after freezing them to GCS."""

from __future__ import annotations

import sys
from typing import Final

from google.cloud import bigquery

PROJECT_ID: Final[str] = "genome-services-platform"
DATASETS_TO_EMPTY: Final[tuple[str, ...]] = (
    "arab_acmg_harmonized",
    "arab_acmg_results",
)


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)

    print("--- [Cleanup Stage]: Removing non-raw BigQuery outputs ---")
    for dataset_id in DATASETS_TO_EMPTY:
        dataset_ref = f"{PROJECT_ID}.{dataset_id}"
        for table_item in client.list_tables(dataset_ref):
            table_ref = f"{PROJECT_ID}.{table_item.dataset_id}.{table_item.table_id}"
            client.delete_table(table_ref, not_found_ok=True)
            print(f"🧹 [Cleanup Effect]: deleted {table_ref}")

    for dataset_id in DATASETS_TO_EMPTY:
        remaining = list(client.list_tables(f"{PROJECT_ID}.{dataset_id}"))
        print(f"dataset={dataset_id} remaining_tables={len(remaining)}")
        if remaining:
            print(f"❌ [Cleanup Effect]: dataset {dataset_id} is not empty")
            sys.exit(1)

    print("🎉 [Final Effect]: Only raw BigQuery tables remain in active use.")


if __name__ == "__main__":
    main()
