"""Materialize the BRCA checkpoint tables directly from raw sources and prune obsolete harmonized outputs."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

from google.cloud import bigquery

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.registry_queries import (
    HARMONIZED_DATASET,
    PRE_GME_REGISTRY_TABLE_REF,
    REGISTRY_TABLE_REF,
    build_pre_gme_registry_sql,
    build_registry_sql,
)

PROJECT_ID: Final[str] = "genome-services-platform"
KEEP_TABLES: Final[set[str]] = {
    PRE_GME_REGISTRY_TABLE_REF,
    REGISTRY_TABLE_REF,
}


def prune_harmonized_dataset(client: bigquery.Client) -> None:
    dataset_ref = f"{PROJECT_ID}.{HARMONIZED_DATASET}"
    for table_item in client.list_tables(dataset_ref):
        table_ref = f"{PROJECT_ID}.{table_item.dataset_id}.{table_item.table_id}"
        if table_ref in KEEP_TABLES:
            continue
        client.delete_table(table_ref, not_found_ok=True)
        print(f"🧹 [Prune Effect]: deleted obsolete harmonized object {table_ref}")


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    stages = (
        ("pre-GME review", PRE_GME_REGISTRY_TABLE_REF, build_pre_gme_registry_sql()),
        ("final registry", REGISTRY_TABLE_REF, build_registry_sql()),
    )

    for stage_name, table_ref, sql in stages:
        print(f"--- [Checkpoint Build]: {stage_name} -> {table_ref} ---")
        try:
            job = client.query(sql)
            job.result()
            table = client.get_table(table_ref)
        except Exception as exc:
            print(f"❌ [Checkpoint Effect]: {stage_name} build failed. Error: {exc}")
            sys.exit(1)
        print(f"✅ [Checkpoint Effect]: {stage_name} table ready with {table.num_rows} rows.")

    try:
        prune_harmonized_dataset(client)
    except Exception as exc:
        print(f"❌ [Prune Effect]: failed to prune obsolete harmonized outputs. Error: {exc}")
        sys.exit(1)

    print("🎉 [Final Effect]: only the checkpoint tables remain in arab_acmg_harmonized.")


if __name__ == "__main__":
    main()
