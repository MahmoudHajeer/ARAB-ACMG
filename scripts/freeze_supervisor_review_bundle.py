"""Freeze the supervisor review surface into static JSON and GCS artifacts.

This script performs the last intentional BigQuery reads for the current review cycle.
After it succeeds, the UI can operate without querying BigQuery at runtime.
"""

from __future__ import annotations

import datetime as dt
import json
import sys
from pathlib import Path
from textwrap import dedent
from typing import Any, Final

from google.cloud import bigquery, storage

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
UI_DIR: Final[Path] = ROOT / "ui"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ui.catalog import (  # noqa: E402
    FINAL_STEPS,
    HARMONIZATION_STEPS,
    WORKFLOW_PAGES,
    dataset_catalog_payload,
    pre_gme_catalog_payload,
    raw_dataset_catalog_payload,
    registry_catalog_payload,
)
from ui.export_workbook import PRE_GME_EXPORT_FILENAME, build_pre_gme_workbook_bytes  # noqa: E402
from ui.registry_queries import (  # noqa: E402
    PRE_GME_REGISTRY_TABLE_REF,
    REGISTRY_TABLE_REF,
    build_raw_sample_sql,
    gene_windows_payload,
)

PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
SNAPSHOT_DATE: Final[str] = dt.date.today().isoformat()
FREEZE_PREFIX: Final[str] = f"frozen/supervisor_review/snapshot_date={SNAPSHOT_DATE}"
PRE_GME_PARQUET_OBJECT: Final[str] = f"{FREEZE_PREFIX}/pre_gme/supervisor_variant_registry_brca_pre_gme_v1.parquet"
PRE_GME_XLSX_OBJECT: Final[str] = f"{FREEZE_PREFIX}/pre_gme/{PRE_GME_EXPORT_FILENAME}"
FINAL_PARQUET_OBJECT: Final[str] = f"{FREEZE_PREFIX}/final/supervisor_variant_registry_brca_v1.parquet"
FINAL_CSV_OBJECT: Final[str] = f"{FREEZE_PREFIX}/final/supervisor_variant_registry_brca_v1.csv"
MANIFEST_OBJECT: Final[str] = f"{FREEZE_PREFIX}/manifest.json"
BUNDLE_OBJECT: Final[str] = f"{FREEZE_PREFIX}/review_bundle.json"
BUNDLE_FILE: Final[Path] = UI_DIR / "review_bundle.json"
PUBLIC_FINAL_CSV_URL: Final[str] = f"https://storage.googleapis.com/{BUCKET_NAME}/{FINAL_CSV_OBJECT}"


# [AI-Agent: Codex]: Keep JSON serialization deterministic so the deployed UI can be audited against a fixed bundle.
def json_ready(value: Any) -> Any:
    if isinstance(value, (dt.datetime, dt.date)):
        return value.isoformat()
    if isinstance(value, list):
        return [json_ready(item) for item in value]
    if isinstance(value, tuple):
        return [json_ready(item) for item in value]
    if isinstance(value, dict):
        return {key: json_ready(item) for key, item in value.items()}
    return value


# [AI-Agent: Codex]: Use explicit small queries here once, then stop querying BigQuery from the UI entirely.
def run_query(client: bigquery.Client, sql: str) -> dict[str, Any]:
    result = client.query(sql).result()
    columns = [field.name for field in result.schema]
    rows = [{column: json_ready(row[column]) for column in columns} for row in result]
    return {"columns": columns, "rows": rows}


# [AI-Agent: Codex]: The frozen UI shows stable evidence, so samples use deterministic ordering instead of repeated random scans.
def build_static_sample_sql(table_ref: str, *, where: str | None = None, limit: int = 10) -> str:
    where_clause = f"WHERE {where}" if where else ""
    return dedent(
        f"""
        SELECT *
        FROM (
          SELECT
            ROW_NUMBER() OVER (ORDER BY `GENE`, `CHROM`, `POS`, `REF`, `ALT`) AS sample_row_number,
            *
          FROM `{table_ref}`
          {where_clause}
        )
        WHERE sample_row_number <= {limit}
        ORDER BY sample_row_number
        """
    ).strip()


# [AI-Agent: Codex]: Source counts now come from the already-materialized checkpoint tables, not from re-running raw extraction CTEs.
def build_presence_count_sql(table_ref: str, *, include_gme: bool) -> str:
    unions = [
        f"SELECT 'clinvar' AS source_name, COUNTIF(ALLELEID IS NOT NULL) AS row_count FROM `{table_ref}`",
        f"SELECT 'gnomad_genomes' AS source_name, COUNTIF(GNOMAD_GENOMES_AC IS NOT NULL) AS row_count FROM `{table_ref}`",
        f"SELECT 'gnomad_exomes' AS source_name, COUNTIF(GNOMAD_EXOMES_AC IS NOT NULL) AS row_count FROM `{table_ref}`",
    ]
    if include_gme:
        unions.append(f"SELECT 'gme' AS source_name, COUNTIF(GME_AF IS NOT NULL) AS row_count FROM `{table_ref}`")
    return "\nUNION ALL\n".join(unions) + "\nORDER BY source_name"


# [AI-Agent: Codex]: Each step sample is reduced to a simple filter over the frozen checkpoint tables so later review costs are flat.
def build_step_sample_sql(step_id: str, limit: int = 10) -> str:
    filters = {
        "clinvar_raw_brca": (PRE_GME_REGISTRY_TABLE_REF, "ALLELEID IS NOT NULL"),
        "gnomad_genomes_raw_brca": (PRE_GME_REGISTRY_TABLE_REF, "GNOMAD_GENOMES_AC IS NOT NULL"),
        "gnomad_exomes_raw_brca": (PRE_GME_REGISTRY_TABLE_REF, "GNOMAD_EXOMES_AC IS NOT NULL"),
        "pre_gme_checkpoint": (PRE_GME_REGISTRY_TABLE_REF, None),
        "gme_raw_brca": (REGISTRY_TABLE_REF, "GME_AF IS NOT NULL"),
        "final_checkpoint": (REGISTRY_TABLE_REF, None),
    }
    table_ref, where = filters[step_id]
    return build_static_sample_sql(table_ref, where=where, limit=limit)


# [AI-Agent: Codex]: Extract jobs avoid query-style UI scans and move the processed checkpoint outputs into GCS for low-cost reuse.
def export_table_to_gcs(
    client: bigquery.Client,
    table_ref: str,
    destination_uri: str,
    *,
    destination_format: str,
    compression: str | None = None,
) -> None:
    job_config = bigquery.job.ExtractJobConfig(destination_format=destination_format)
    if compression:
        job_config.compression = compression
    job = client.extract_table(table_ref, destination_uri, location="US", job_config=job_config)
    job.result()


def upload_bytes(storage_client: storage.Client, object_name: str, content: bytes, *, content_type: str) -> None:
    blob = storage_client.bucket(BUCKET_NAME).blob(object_name)
    blob.upload_from_string(content, content_type=content_type)


def upload_text(storage_client: storage.Client, object_name: str, content: str, *, content_type: str) -> None:
    upload_bytes(storage_client, object_name, content.encode("utf-8"), content_type=content_type)


def freeze_raw_payload(client: bigquery.Client) -> dict[str, Any]:
    payload = {"datasets": raw_dataset_catalog_payload()}
    for entry in payload["datasets"]:
        table = client.get_table(str(entry["table_ref"]))
        entry["row_count"] = int(table.num_rows)
        sql = build_raw_sample_sql(str(entry["table_ref"]), sample_percent=float(entry["sample_percent"]), limit=10)
        entry["sample"] = run_query(client, sql)
        entry["sample"]["query_sql"] = sql
        entry["sample"]["mode"] = "frozen"
        entry["sample"]["frozen_at"] = SNAPSHOT_DATE
        entry["download_url"] = None
    return json_ready(payload)


def freeze_checkpoint_dataset_payload(client: bigquery.Client) -> dict[str, Any]:
    payload = {"datasets": dataset_catalog_payload()}
    for entry in payload["datasets"]:
        table = client.get_table(str(entry["table_ref"]))
        entry["row_count"] = int(table.num_rows)
        entry["storage_ref"] = (
            f"gs://{BUCKET_NAME}/{PRE_GME_PARQUET_OBJECT}"
            if entry["key"] == "pre_gme_registry"
            else f"gs://{BUCKET_NAME}/{FINAL_PARQUET_OBJECT}"
        )
        sql = build_static_sample_sql(str(entry["table_ref"]), limit=10)
        entry["sample"] = run_query(client, sql)
        entry["sample"]["query_sql"] = sql
        entry["sample"]["mode"] = "frozen"
        entry["sample"]["frozen_at"] = SNAPSHOT_DATE
        entry["download_url"] = None
    return json_ready(payload)


def freeze_pre_gme_payload(client: bigquery.Client) -> dict[str, Any]:
    payload = pre_gme_catalog_payload()
    table = client.get_table(PRE_GME_REGISTRY_TABLE_REF)
    payload["row_count"] = int(table.num_rows)
    payload["table_ref"] = f"gs://{BUCKET_NAME}/{PRE_GME_PARQUET_OBJECT}"
    payload["download_url"] = None
    payload["csv_download_url"] = None
    payload["scientific_metrics"] = {
        "gene_windows": gene_windows_payload(),
        "source_row_counts": run_query(
            client,
            build_presence_count_sql(PRE_GME_REGISTRY_TABLE_REF, include_gme=False),
        )["rows"],
        "frozen_at": SNAPSHOT_DATE,
    }
    sample_sql = build_static_sample_sql(PRE_GME_REGISTRY_TABLE_REF, limit=10)
    payload["sample"] = run_query(client, sample_sql)
    payload["sample"]["query_sql"] = sample_sql
    payload["sample"]["mode"] = "frozen"
    payload["sample"]["frozen_at"] = SNAPSHOT_DATE
    return json_ready(payload)


def freeze_registry_payload(client: bigquery.Client) -> dict[str, Any]:
    payload = registry_catalog_payload()
    table = client.get_table(REGISTRY_TABLE_REF)
    payload["row_count"] = int(table.num_rows)
    payload["table_ref"] = f"gs://{BUCKET_NAME}/{FINAL_PARQUET_OBJECT}"
    payload["csv_download_url"] = PUBLIC_FINAL_CSV_URL
    payload["scientific_metrics"] = {
        "gene_windows": gene_windows_payload(),
        "source_row_counts": run_query(
            client,
            build_presence_count_sql(REGISTRY_TABLE_REF, include_gme=True),
        )["rows"],
        "frozen_at": SNAPSHOT_DATE,
    }
    sample_sql = build_static_sample_sql(REGISTRY_TABLE_REF, limit=10)
    payload["sample"] = run_query(client, sample_sql)
    payload["sample"]["query_sql"] = sample_sql
    payload["sample"]["mode"] = "frozen"
    payload["sample"]["frozen_at"] = SNAPSHOT_DATE
    return json_ready(payload)


def freeze_step_payloads(client: bigquery.Client) -> dict[str, Any]:
    payload: dict[str, Any] = {}
    for step in (*HARMONIZATION_STEPS, *FINAL_STEPS):
        sql = build_step_sample_sql(step["id"], limit=10)
        payload[step["id"]] = run_query(client, sql)
        payload[step["id"]]["query_sql"] = sql
        payload[step["id"]]["mode"] = "frozen"
        payload[step["id"]]["frozen_at"] = SNAPSHOT_DATE
    return json_ready(payload)


def build_bundle(client: bigquery.Client) -> dict[str, Any]:
    return {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "frozen_at": SNAPSHOT_DATE,
        "cost_mode": "static_review_bundle",
        "workflow": {
            "pages": list(WORKFLOW_PAGES),
            "harmonization_steps": list(HARMONIZATION_STEPS),
            "final_steps": list(FINAL_STEPS),
        },
        "raw_datasets": freeze_raw_payload(client),
        "datasets": freeze_checkpoint_dataset_payload(client),
        "pre_gme": freeze_pre_gme_payload(client),
        "registry": freeze_registry_payload(client),
        "step_samples": freeze_step_payloads(client),
        "artifacts": {
            "bucket": BUCKET_NAME,
            "freeze_prefix": FREEZE_PREFIX,
            "pre_gme_parquet_uri": f"gs://{BUCKET_NAME}/{PRE_GME_PARQUET_OBJECT}",
            "pre_gme_xlsx_uri": f"gs://{BUCKET_NAME}/{PRE_GME_XLSX_OBJECT}",
            "final_parquet_uri": f"gs://{BUCKET_NAME}/{FINAL_PARQUET_OBJECT}",
            "final_csv_uri": f"gs://{BUCKET_NAME}/{FINAL_CSV_OBJECT}",
            "final_csv_public_url": PUBLIC_FINAL_CSV_URL,
            "bundle_uri": f"gs://{BUCKET_NAME}/{BUNDLE_OBJECT}",
            "manifest_uri": f"gs://{BUCKET_NAME}/{MANIFEST_OBJECT}",
        },
    }


def main() -> None:
    client = bigquery.Client(project=PROJECT_ID)
    storage_client = storage.Client(project=PROJECT_ID)

    print("--- [Freeze Stage 1]: Exporting checkpoint artifacts to GCS ---")
    export_table_to_gcs(
        client,
        PRE_GME_REGISTRY_TABLE_REF,
        f"gs://{BUCKET_NAME}/{PRE_GME_PARQUET_OBJECT}",
        destination_format=bigquery.DestinationFormat.PARQUET,
        compression="SNAPPY",
    )
    export_table_to_gcs(
        client,
        REGISTRY_TABLE_REF,
        f"gs://{BUCKET_NAME}/{FINAL_PARQUET_OBJECT}",
        destination_format=bigquery.DestinationFormat.PARQUET,
        compression="SNAPPY",
    )
    export_table_to_gcs(
        client,
        REGISTRY_TABLE_REF,
        f"gs://{BUCKET_NAME}/{FINAL_CSV_OBJECT}",
        destination_format=bigquery.DestinationFormat.CSV,
    )
    print("✅ [Freeze Effect]: Checkpoint Parquet/CSV artifacts exported to GCS.")

    print("--- [Freeze Stage 2]: Creating archive workbook and static review bundle ---")
    pre_gme_rows = run_query(client, f"SELECT * FROM `{PRE_GME_REGISTRY_TABLE_REF}` ORDER BY `GENE`, `CHROM`, `POS`, `REF`, `ALT`")["rows"]
    workbook_bytes = build_pre_gme_workbook_bytes(
        pre_gme_rows,
        created_at=dt.datetime.now(dt.UTC).strftime("%d/%m/%Y %H:%M"),
    )
    upload_bytes(
        storage_client,
        PRE_GME_XLSX_OBJECT,
        workbook_bytes,
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    bundle = json_ready(build_bundle(client))
    BUNDLE_FILE.write_text(json.dumps(bundle, indent=2, ensure_ascii=True) + "\n", encoding="utf-8")
    upload_text(storage_client, BUNDLE_OBJECT, json.dumps(bundle, indent=2, ensure_ascii=True) + "\n", content_type="application/json")
    print(f"✅ [Freeze Effect]: Static review bundle written to {BUNDLE_FILE}")

    print("--- [Freeze Stage 3]: Publishing manifest and making final CSV public ---")
    manifest = {
        "snapshot_date": SNAPSHOT_DATE,
        "project_id": PROJECT_ID,
        "bucket": BUCKET_NAME,
        "artifacts": bundle["artifacts"],
        "row_counts": {
            "pre_gme": bundle["pre_gme"]["row_count"],
            "final_registry": bundle["registry"]["row_count"],
        },
        "ui_mode": "static_review_bundle",
    }
    upload_text(storage_client, MANIFEST_OBJECT, json.dumps(manifest, indent=2, ensure_ascii=True) + "\n", content_type="application/json")
    final_csv_blob = storage_client.bucket(BUCKET_NAME).blob(FINAL_CSV_OBJECT)
    final_csv_blob.make_public()
    print(f"✅ [Freeze Effect]: Final CSV is public at {PUBLIC_FINAL_CSV_URL}")

    print("🎉 [Final Effect]: Supervisor review bundle is frozen and the runtime can now stop querying BigQuery.")


if __name__ == "__main__":
    main()
