from __future__ import annotations

import csv
import datetime as dt
import io
from functools import lru_cache
from pathlib import Path
from typing import Final, Iterable

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response, StreamingResponse
from google.cloud import bigquery

try:  # pragma: no cover - import path differs between local package and Cloud Run container
    from ui.catalog import (
        FINAL_STEPS,
        HARMONIZED_DATASETS,
        HARMONIZATION_STEPS,
        RAW_DATASETS,
        WORKFLOW_PAGES,
        dataset_catalog_payload,
        pre_gme_catalog_payload,
        raw_dataset_catalog_payload,
        registry_catalog_payload,
    )
    from ui.overview_data import load_overview_payload as load_bundled_or_live_overview_payload
    from ui.export_workbook import PRE_GME_EXPORT_FILENAME, build_pre_gme_workbook_bytes
    from ui.registry_queries import (
        PRE_GME_REGISTRY_TABLE_REF,
        REGISTRY_TABLE_REF,
        build_export_sql,
        build_pre_gme_export_sql,
        build_pre_gme_source_count_sql,
        build_pre_gme_sample_sql,
        build_final_source_count_sql,
        build_raw_sample_sql,
        build_registry_export_sql,
        build_registry_sample_sql,
        build_registry_step_export_sql,
        build_registry_step_sql,
        build_sample_sql,
        gene_windows_payload,
    )
except ModuleNotFoundError:  # pragma: no cover - runtime fallback inside the ui/ build context
    from catalog import (  # type: ignore[no-redef]
        FINAL_STEPS,
        HARMONIZED_DATASETS,
        HARMONIZATION_STEPS,
        RAW_DATASETS,
        WORKFLOW_PAGES,
        dataset_catalog_payload,
        pre_gme_catalog_payload,
        raw_dataset_catalog_payload,
        registry_catalog_payload,
    )
    from overview_data import load_overview_payload as load_bundled_or_live_overview_payload
    from export_workbook import PRE_GME_EXPORT_FILENAME, build_pre_gme_workbook_bytes
    from registry_queries import (  # type: ignore[no-redef]
        PRE_GME_REGISTRY_TABLE_REF,
        REGISTRY_TABLE_REF,
        build_export_sql,
        build_pre_gme_export_sql,
        build_pre_gme_source_count_sql,
        build_pre_gme_sample_sql,
        build_final_source_count_sql,
        build_raw_sample_sql,
        build_registry_export_sql,
        build_registry_sample_sql,
        build_registry_step_export_sql,
        build_registry_step_sql,
        build_sample_sql,
        gene_windows_payload,
    )

UI_ROOT: Final[Path] = Path(__file__).resolve().parent
PROJECT_ID: Final[str] = "genome-services-platform"
PUBLIC_DATASETS: Final[tuple[str, ...]] = (
    "arab_acmg_raw",
    "arab_acmg_harmonized",
    "arab_acmg_results",
)
DEFAULT_LIMIT: Final[int] = 10

NO_STORE_HEADERS: Final[dict[str, str]] = {"Cache-Control": "no-store"}

app = FastAPI(title="ARAB-ACMG Supervisor UI", version="1.2.0")


@lru_cache(maxsize=1)
def bigquery_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


def run_query(sql: str) -> dict[str, object]:
    try:
        query_job = bigquery_client().query(sql)
        result = query_job.result()
    except Exception as exc:  # pragma: no cover - exercised via runtime calls
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    columns = [field.name for field in result.schema]
    rows = []
    for row in result:
        rows.append({column: row[column] for column in columns})
    return {"columns": columns, "rows": rows}


def iter_query_rows(sql: str) -> tuple[list[str], Iterable[dict[str, object]]]:
    try:
        query_job = bigquery_client().query(sql)
        result = query_job.result()
    except Exception as exc:  # pragma: no cover - exercised via runtime calls
        raise HTTPException(status_code=500, detail=str(exc)) from exc

    columns = [field.name for field in result.schema]

    def row_iter() -> Iterable[dict[str, object]]:
        for row in result:
            yield {column: row[column] for column in columns}

    return columns, row_iter()


def table_row_count(table_ref: str) -> int | None:
    try:
        return int(bigquery_client().get_table(table_ref).num_rows)
    except Exception:
        return None


def csv_response(sql: str, filename: str) -> StreamingResponse:
    columns, rows = iter_query_rows(sql)

    def iter_csv() -> Iterable[str]:
        buffer = io.StringIO()
        writer = csv.DictWriter(buffer, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        yield buffer.getvalue()
        buffer.seek(0)
        buffer.truncate(0)

        for row in rows:
            writer.writerow({column: "" if row.get(column) is None else row.get(column) for column in columns})
            yield buffer.getvalue()
            buffer.seek(0)
            buffer.truncate(0)

    return StreamingResponse(
        iter_csv(),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@lru_cache(maxsize=1)
def public_dataset_status() -> list[dict[str, object]]:
    client = bigquery_client()
    payload: list[dict[str, object]] = []
    for dataset_id in PUBLIC_DATASETS:
        dataset = client.get_dataset(f"{PROJECT_ID}.{dataset_id}")
        is_public = any(
            entry.role == "READER" and entry.entity_id == "allAuthenticatedUsers"
            for entry in dataset.access_entries
        )
        payload.append(
            {
                "dataset_id": dataset_id,
                "is_public": is_public,
                "access_entries": [
                    {
                        "role": entry.role,
                        "entity_type": entry.entity_type,
                        "entity_id": entry.entity_id,
                    }
                    for entry in dataset.access_entries
                ],
            }
        )
    return payload


def pre_gme_row_count() -> int | None:
    try:
        return int(bigquery_client().get_table(PRE_GME_REGISTRY_TABLE_REF).num_rows)
    except Exception:
        return None


def registry_row_count() -> int | None:
    try:
        return int(bigquery_client().get_table(REGISTRY_TABLE_REF).num_rows)
    except Exception:
        return None


def registry_scientific_metrics() -> dict[str, object]:
    return {
        "gene_windows": gene_windows_payload(),
        "source_row_counts": run_query(build_final_source_count_sql())["rows"],
    }


def pre_gme_metrics() -> dict[str, object]:
    return {
        "gene_windows": gene_windows_payload(),
        "source_row_counts": run_query(build_pre_gme_source_count_sql())["rows"],
    }


@app.get("/")
def index() -> FileResponse:
    return FileResponse(UI_ROOT / "index.html", headers=NO_STORE_HEADERS)


@app.get("/app.js")
def app_js() -> FileResponse:
    return FileResponse(UI_ROOT / "app.js", headers=NO_STORE_HEADERS)


@app.get("/styles.css")
def styles() -> FileResponse:
    return FileResponse(UI_ROOT / "styles.css", headers=NO_STORE_HEADERS)


@app.get("/status_snapshot.json")
def snapshot() -> FileResponse:
    return FileResponse(UI_ROOT / "status_snapshot.json", headers=NO_STORE_HEADERS)


@app.get("/favicon.ico")
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


def load_overview_payload() -> dict[str, object]:
    return load_bundled_or_live_overview_payload()


@app.get("/api/overview")
def overview() -> dict[str, object]:
    return load_overview_payload()


@app.get("/api/workflow")
def workflow_meta() -> dict[str, object]:
    return {
        "pages": list(WORKFLOW_PAGES),
        "harmonization_steps": list(HARMONIZATION_STEPS),
        "final_steps": list(FINAL_STEPS),
    }


@app.get("/api/public-datasets")
def public_datasets() -> dict[str, object]:
    return {"datasets": public_dataset_status()}


@app.get("/api/raw-datasets")
def raw_datasets() -> dict[str, object]:
    payload = raw_dataset_catalog_payload()
    for entry in payload:
        entry["row_count"] = table_row_count(str(entry["table_ref"]))
        entry["download_url"] = f"/api/raw-datasets/{entry['key']}/download.csv"
    return {"datasets": payload}


@app.get("/api/raw-datasets/{dataset_key}/sample")
def raw_dataset_sample(dataset_key: str, limit: int = DEFAULT_LIMIT) -> dict[str, object]:
    entry = RAW_DATASETS.get(dataset_key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown raw dataset: {dataset_key}")

    sql = build_raw_sample_sql(entry.table_ref, sample_percent=entry.sample_percent, limit=limit)
    result = run_query(sql)
    return {
        "dataset_key": dataset_key,
        "title": entry.title,
        "table_ref": entry.table_ref,
        "query_sql": sql,
        "columns": result["columns"],
        "rows": result["rows"],
    }


@app.get("/api/raw-datasets/{dataset_key}/download.csv")
def raw_dataset_download(dataset_key: str) -> StreamingResponse:
    entry = RAW_DATASETS.get(dataset_key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown raw dataset: {dataset_key}")
    return csv_response(build_export_sql(entry.table_ref), filename=f"{dataset_key}.csv")


@app.get("/api/datasets")
def datasets() -> dict[str, object]:
    payload = dataset_catalog_payload()
    for entry in payload:
        entry["row_count"] = table_row_count(str(entry["table_ref"]))
        entry["download_url"] = f"/api/datasets/{entry['key']}/download.csv"
    return {"datasets": payload}


@app.get("/api/datasets/{dataset_key}/sample")
def dataset_sample(dataset_key: str, limit: int = DEFAULT_LIMIT) -> dict[str, object]:
    entry = HARMONIZED_DATASETS.get(dataset_key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset_key}")

    sql = build_sample_sql(entry.table_ref, sample_percent=entry.sample_percent, limit=limit)
    result = run_query(sql)
    return {
        "dataset_key": dataset_key,
        "title": entry.title,
        "table_ref": entry.table_ref,
        "query_sql": sql,
        "columns": result["columns"],
        "rows": result["rows"],
    }


@app.get("/api/datasets/{dataset_key}/download.csv")
def dataset_download(dataset_key: str) -> StreamingResponse:
    entry = HARMONIZED_DATASETS.get(dataset_key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset_key}")
    return csv_response(build_export_sql(entry.table_ref), filename=f"{dataset_key}.csv")


@app.get("/api/pre-gme")
def pre_gme_metadata() -> dict[str, object]:
    payload = pre_gme_catalog_payload()
    payload["row_count"] = pre_gme_row_count()
    payload["scientific_metrics"] = pre_gme_metrics()
    payload["download_url"] = "/api/exports/pre-gme.xlsx"
    payload["csv_download_url"] = "/api/pre-gme/download.csv"
    return payload


@app.get("/api/pre-gme/sample")
def pre_gme_sample(limit: int = DEFAULT_LIMIT) -> dict[str, object]:
    sql = build_pre_gme_sample_sql(limit=limit)
    result = run_query(sql)
    return {
        "table_ref": PRE_GME_REGISTRY_TABLE_REF,
        "query_sql": sql,
        "columns": result["columns"],
        "rows": result["rows"],
    }


@app.get("/api/pre-gme/download.csv")
def pre_gme_download_csv() -> StreamingResponse:
    return csv_response(build_pre_gme_export_sql(), filename="supervisor_variant_registry_brca_pre_gme_v1.csv")


@app.get("/api/exports/pre-gme.xlsx")
def pre_gme_export() -> Response:
    sql = build_pre_gme_export_sql()
    _, rows = iter_query_rows(sql)
    timestamp = dt.datetime.now(dt.UTC).strftime("%d/%m/%Y %H:%M")
    workbook_bytes = build_pre_gme_workbook_bytes(rows, created_at=timestamp)
    headers = {
        "Content-Disposition": f'attachment; filename="{PRE_GME_EXPORT_FILENAME}"',
    }
    return Response(
        content=workbook_bytes,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers=headers,
    )


@app.get("/api/registry")
def registry_metadata() -> dict[str, object]:
    payload = registry_catalog_payload()
    payload["row_count"] = registry_row_count()
    payload["csv_download_url"] = "/api/registry/download.csv"
    try:
        payload["scientific_metrics"] = registry_scientific_metrics()
    except HTTPException as exc:
        payload["scientific_metrics"] = {}
        payload["scientific_metrics_error"] = str(exc.detail)
    return payload


@app.get("/api/registry/sample")
def registry_sample(limit: int = DEFAULT_LIMIT) -> dict[str, object]:
    sql = build_registry_sample_sql(limit=limit)
    result = run_query(sql)
    return {
        "table_ref": REGISTRY_TABLE_REF,
        "query_sql": sql,
        "columns": result["columns"],
        "rows": result["rows"],
    }


@app.get("/api/registry/download.csv")
def registry_download_csv() -> StreamingResponse:
    return csv_response(build_registry_export_sql(), filename="supervisor_variant_registry_brca_v1.csv")


@app.get("/api/registry/steps/{step_id}/sample")
def registry_step_sample(step_id: str, limit: int = DEFAULT_LIMIT) -> dict[str, object]:
    try:
        sql = build_registry_step_sql(step_id=step_id, limit=limit)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown registry step: {step_id}") from exc
    result = run_query(sql)
    return {
        "step_id": step_id,
        "query_sql": sql,
        "columns": result["columns"],
        "rows": result["rows"],
    }


@app.get("/api/registry/steps/{step_id}/download.csv")
def registry_step_download(step_id: str) -> StreamingResponse:
    try:
        sql = build_registry_step_export_sql(step_id=step_id)
    except KeyError as exc:
        raise HTTPException(status_code=404, detail=f"Unknown registry step: {step_id}") from exc
    return csv_response(sql, filename=f"{step_id}.csv")
