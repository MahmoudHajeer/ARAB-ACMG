"""Materialize the BRCA-focused supervisor review tables in BigQuery."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

from google.cloud import bigquery

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.registry_queries import (
    PRE_GME_REGISTRY_TABLE_REF,
    REGISTRY_TABLE_REF,
    build_pre_gme_registry_sql,
    build_registry_sql,
)

PROJECT_ID: Final[str] = "genome-services-platform"


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    stages = (
        ("pre-GME review", PRE_GME_REGISTRY_TABLE_REF, build_pre_gme_registry_sql()),
        ("final registry", REGISTRY_TABLE_REF, build_registry_sql()),
    )

    for stage_name, table_ref, sql in stages:
        print(f"--- [Supervisor Registry Stage]: Building {stage_name} table {table_ref} ---")
        try:
            # [AI-Agent: Codex]: Materialize each review checkpoint with one explicit
            # CREATE OR REPLACE TABLE query so the UI exposes a single authoritative SQL statement.
            job = client.query(sql)
            job.result()
            table = client.get_table(table_ref)
        except Exception as exc:
            print(f"❌ [Stage Effect]: {stage_name} build failed. Error: {exc}")
            sys.exit(1)

        print(f"✅ [Stage Effect]: {stage_name} table ready with {table.num_rows} rows.")

    print("🎉 [Final Effect]: BRCA pre-GME and final registry tables are ready for supervisor review.")


if __name__ == "__main__":
    main()
