"""Export the pre-GME BRCA review table to an Excel workbook."""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from typing import Final

from google.cloud import bigquery

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
OUTPUT_DIR: Final[Path] = ROOT / "output" / "spreadsheet"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.export_workbook import PRE_GME_EXPORT_FILENAME, build_pre_gme_workbook_bytes
from ui.registry_queries import PRE_GME_REGISTRY_TABLE_REF, build_pre_gme_export_sql

PROJECT_ID: Final[str] = "genome-services-platform"


def main() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    output_path = OUTPUT_DIR / PRE_GME_EXPORT_FILENAME

    client = bigquery.Client(project=PROJECT_ID)
    sql = build_pre_gme_export_sql()

    print(f"--- [Pre-GME Export Stage]: Exporting {PRE_GME_REGISTRY_TABLE_REF} to {output_path} ---")
    try:
        result = client.query(sql).result()
        rows = [{field.name: row[field.name] for field in result.schema} for row in result]
        workbook_bytes = build_pre_gme_workbook_bytes(
            rows,
            created_at=dt.datetime.now(dt.UTC).strftime("%d/%m/%Y %H:%M"),
        )
        output_path.write_bytes(workbook_bytes)
    except Exception as exc:
        print(f"❌ [Stage Effect]: Pre-GME export failed. Error: {exc}")
        sys.exit(1)

    print(f"✅ [Stage Effect]: Pre-GME Excel review artifact written to {output_path}")


if __name__ == "__main__":
    main()
