"""BigQuery raw-layer health checks for the ARAB-ACMG pipeline.

This health check mirrors the style of `scripts/verify_gcp.py`, but validates
dataset/table readiness for runtime work (existence + non-zero row counts).
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from typing import Final

from google.api_core.exceptions import NotFound
from google.cloud import bigquery

PROJECT_ID: Final[str] = "genome-services-platform"
DATASET_ID: Final[str] = "arab_acmg_raw"
DEFAULT_REQUIRED_TABLES: Final[list[str]] = [
    "clinvar_raw_vcf",
    "gnomad_v4_1_genomes_chr13_raw",
    "gnomad_v4_1_genomes_chr17_raw",
    "gnomad_v4_1_exomes_chr13_raw",
    "gnomad_v4_1_exomes_chr17_raw",
]


@dataclass(frozen=True)
class TableHealth:
    """Health status for one required BigQuery table."""

    table: str
    exists: bool
    rows: int
    error: str | None = None


def _parse_required_tables() -> list[str]:
    """Read required table names from env or default to project policy."""

    raw_value = os.getenv("BQ_HEALTH_TABLES", "").strip()
    if not raw_value:
        return DEFAULT_REQUIRED_TABLES

    parsed = [item.strip() for item in raw_value.split(",") if item.strip()]
    return parsed or DEFAULT_REQUIRED_TABLES


def collect_bq_health(required_tables: list[str] | None = None) -> list[TableHealth]:
    """Collect existence + row count health for raw-layer tables."""

    tables = required_tables or _parse_required_tables()
    client = bigquery.Client(project=PROJECT_ID)
    results: list[TableHealth] = []

    for table in tables:
        table_ref = f"{PROJECT_ID}.{DATASET_ID}.{table}"
        try:
            client.get_table(table_ref)
            query = f"SELECT COUNT(*) AS cnt FROM `{table_ref}`"
            row_count = int(list(client.query(query).result())[0].cnt)
            results.append(TableHealth(table=table, exists=True, rows=row_count))
        except NotFound:
            results.append(TableHealth(table=table, exists=False, rows=0, error="missing"))
        except Exception as exc:  # pragma: no cover - defensive for runtime CLI
            results.append(TableHealth(table=table, exists=False, rows=0, error=str(exc)))

    return results


def verify_bq_health(required_tables: list[str] | None = None) -> bool:
    """Return True only when all required tables exist and have rows."""

    statuses = collect_bq_health(required_tables=required_tables)
    return all(item.exists and item.rows > 0 for item in statuses)


def main() -> None:
    statuses = collect_bq_health()

    print("--- BigQuery Health Check (raw layer) ---")
    for item in statuses:
        state = "ok" if item.exists and item.rows > 0 else "fail"
        print(
            f"{item.table}: status={state} exists={item.exists} rows={item.rows}"
            + (f" error={item.error}" if item.error else "")
        )

    if all(item.exists and item.rows > 0 for item in statuses):
        print("✅ BigQuery health check passed.")
        sys.exit(0)

    print("❌ BigQuery health check failed.")
    sys.exit(1)


if __name__ == "__main__":
    main()
