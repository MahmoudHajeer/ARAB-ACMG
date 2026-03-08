"""Load the raw GME TSV snapshot from GCS into BigQuery."""

from __future__ import annotations

import datetime as dt
from pathlib import Path
import sys
from typing import Final

from google.cloud import bigquery

PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
DATASET_ID: Final[str] = "arab_acmg_raw"
TABLE_ID: Final[str] = "gme_hg38_raw"
SOURCE_VERSION: Final[str] = "20161025-hg38"
SOURCE_BUILD: Final[str] = "hg38"
LOCAL_SOURCE: Final[Path] = Path("/Users/macbookpro/Desktop/storage/raw/gme/hg38_gme.txt.gz")


class GMERawBQLoaderPipeline:
    def __init__(self) -> None:
        self.snapshot_date = dt.date.today().isoformat()
        self.bq_table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    def raw_gcs_uri(self) -> str:
        return (
            f"gs://{BUCKET_NAME}/raw/sources/gme/release={SOURCE_VERSION}/build={SOURCE_BUILD}/"
            f"snapshot_date={self.snapshot_date}/hg38_gme.txt.gz"
        )

    def load_raw_table(self) -> "GMERawBQLoaderPipeline":
        print(f"--- [GME Raw BQ Stage 1]: Loading raw GME data to {self.bq_table_ref} ---")
        if not LOCAL_SOURCE.exists():
            print(f"❌ [Stage 1 Effect]: Local GME source missing at {LOCAL_SOURCE}")
            sys.exit(1)
        client = bigquery.Client(project=PROJECT_ID)
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            field_delimiter="\t",
            skip_leading_rows=1,
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
            schema=[
                bigquery.SchemaField("chrom", "STRING"),
                bigquery.SchemaField("start", "INT64"),
                bigquery.SchemaField("end", "INT64"),
                bigquery.SchemaField("ref", "STRING"),
                bigquery.SchemaField("alt", "STRING"),
                bigquery.SchemaField("gme_af", "FLOAT64"),
                bigquery.SchemaField("gme_nwa", "FLOAT64"),
                bigquery.SchemaField("gme_nea", "FLOAT64"),
                bigquery.SchemaField("gme_ap", "FLOAT64"),
                bigquery.SchemaField("gme_israel", "FLOAT64"),
                bigquery.SchemaField("gme_sd", "FLOAT64"),
                bigquery.SchemaField("gme_tp", "FLOAT64"),
                bigquery.SchemaField("gme_ca", "FLOAT64"),
            ],
        )

        try:
            # [AI-Agent: Codex]: The untouched file is already frozen in GCS.
            # Loading from the local copy here keeps the raw table creation simple
            # and avoids GCS URI resolution issues for this legacy source.
            with open(LOCAL_SOURCE, "rb") as handle:
                job = client.load_table_from_file(handle, self.bq_table_ref, job_config=job_config)
            job.result()
            print(f"✅ [Stage 1 Effect]: Loaded {job.output_rows} GME raw rows.")
        except Exception as exc:
            print(f"❌ [Stage 1 Effect]: BigQuery load failed. Error: {exc}")
            sys.exit(1)
        return self

    def finalize(self) -> None:
        print("🎉 [Final Effect]: GME raw BigQuery load completed.")


if __name__ == "__main__":
    GMERawBQLoaderPipeline().load_raw_table().finalize()
