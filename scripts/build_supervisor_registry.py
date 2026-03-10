"""Materialize the BRCA-focused supervisor registry table in BigQuery."""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Final

from google.cloud import bigquery

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.registry_queries import REGISTRY_TABLE_REF, build_registry_sql

PROJECT_ID: Final[str] = "genome-services-platform"


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    sql = build_registry_sql()

    print(f"--- [Supervisor Registry Stage 1]: Building BRCA registry {REGISTRY_TABLE_REF} ---")
    try:
        # [AI-Agent: Codex]: Run one explicit CREATE OR REPLACE TABLE query so the
        # UI and downstream users can inspect a single authoritative SQL statement.
        job = client.query(sql)
        job.result()
        table = client.get_table(REGISTRY_TABLE_REF)
    except Exception as exc:
        print(f"❌ [Stage 1 Effect]: Registry build failed. Error: {exc}")
        sys.exit(1)

    print(f"✅ [Stage 1 Effect]: BRCA registry table ready with {table.num_rows} rows.")
    print("🎉 [Final Effect]: supervisor_variant_registry_brca_v1 is ready for supervisor review.")


if __name__ == "__main__":
    main()
