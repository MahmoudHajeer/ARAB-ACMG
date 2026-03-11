"""Verify the BRCA checkpoint tables and ensure obsolete harmonized outputs are gone."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

from google.cloud import bigquery

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.registry_queries import HARMONIZED_DATASET, PRE_GME_REGISTRY_TABLE_REF, REGISTRY_TABLE_REF

PROJECT_ID: Final[str] = "genome-services-platform"
DATASETS: Final[tuple[str, ...]] = (
    "arab_acmg_raw",
    "arab_acmg_harmonized",
    "arab_acmg_results",
)
EXPECTED_HARMONIZED_TABLES: Final[set[str]] = {
    PRE_GME_REGISTRY_TABLE_REF.split('.')[-1],
    REGISTRY_TABLE_REF.split('.')[-1],
}


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    ok = True

    print("--- BRCA Checkpoint Verification ---")
    for table_ref in (PRE_GME_REGISTRY_TABLE_REF, REGISTRY_TABLE_REF):
        try:
            table = client.get_table(table_ref)
            print(f"checkpoint_table={table_ref} rows={table.num_rows}")
            if int(table.num_rows) <= 0:
                ok = False
        except Exception as exc:
            print(f"checkpoint_table_error[{table_ref}]={exc}")
            ok = False

    harmonized_tables = {table.table_id for table in client.list_tables(f"{PROJECT_ID}.{HARMONIZED_DATASET}")}
    print(f"harmonized_tables={sorted(harmonized_tables)}")
    if harmonized_tables != EXPECTED_HARMONIZED_TABLES:
        ok = False

    for dataset_id in DATASETS:
        dataset = client.get_dataset(f"{PROJECT_ID}.{dataset_id}")
        is_public = any(
            entry.role == "READER" and entry.entity_id == "allAuthenticatedUsers"
            for entry in dataset.access_entries
        )
        print(f"dataset={dataset_id} public_reader={is_public}")
        if not is_public:
            ok = False

    if ok:
        print("✅ Checkpoint verification passed.")
        sys.exit(0)

    print("❌ Checkpoint verification failed.")
    sys.exit(1)


if __name__ == "__main__":
    main()
