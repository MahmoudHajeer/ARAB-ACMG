"""Verify the supervisor registry table and public dataset access."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

from google.cloud import bigquery

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.registry_queries import REGISTRY_TABLE_REF

PROJECT_ID: Final[str] = "genome-services-platform"
DATASETS: Final[tuple[str, ...]] = (
    "arab_acmg_raw",
    "arab_acmg_harmonized",
    "arab_acmg_results",
)


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    ok = True

    print("--- Supervisor Registry Verification ---")
    try:
        table = client.get_table(REGISTRY_TABLE_REF)
        print(f"registry_table={REGISTRY_TABLE_REF} rows={table.num_rows}")
        if int(table.num_rows) <= 0:
            ok = False
    except Exception as exc:
        print(f"registry_table_error={exc}")
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
        print("✅ Supervisor registry verification passed.")
        sys.exit(0)

    print("❌ Supervisor registry verification failed.")
    sys.exit(1)


if __name__ == "__main__":
    main()
