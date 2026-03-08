from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Final

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, Response
from google.cloud import bigquery

try:  # pragma: no cover - import path differs between local package and Cloud Run container
    from ui.catalog import RAW_DATASETS, dataset_catalog_payload, registry_catalog_payload
    from ui.registry_queries import (
        REGISTRY_TABLE_REF,
        build_registry_sample_sql,
        build_registry_step_sql,
    )
except ModuleNotFoundError:  # pragma: no cover - runtime fallback inside the ui/ build context
    from catalog import RAW_DATASETS, dataset_catalog_payload, registry_catalog_payload
    from registry_queries import (
        REGISTRY_TABLE_REF,
        build_registry_sample_sql,
        build_registry_step_sql,
    )

UI_ROOT: Final[Path] = Path(__file__).resolve().parent
PROJECT_ID: Final[str] = "genome-services-platform"
PUBLIC_DATASETS: Final[tuple[str, ...]] = (
    "arab_acmg_raw",
    "arab_acmg_harmonized",
    "arab_acmg_results",
)
DEFAULT_LIMIT: Final[int] = 50

app = FastAPI(title="ARAB-ACMG Supervisor UI", version="1.0.0")


@lru_cache(maxsize=1)
def bigquery_client() -> bigquery.Client:
    return bigquery.Client(project=PROJECT_ID)


# [AI-Agent: Codex]: Keep the sample SQL generation constrained to the
# pre-registered dataset catalog so button clicks can never inject SQL.
def build_raw_sample_sql(table_ref: str, sample_percent: float, limit: int = DEFAULT_LIMIT) -> str:
    return f"""
WITH sampled AS (
  SELECT *
  FROM `{table_ref}` TABLESAMPLE SYSTEM ({sample_percent} PERCENT)
),
numbered AS (
  SELECT ROW_NUMBER() OVER (ORDER BY RAND()) AS sample_row_number, *
  FROM sampled
)
SELECT *
FROM numbered
WHERE sample_row_number <= {limit}
ORDER BY sample_row_number
""".strip()


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


def registry_row_count() -> int | None:
    try:
        return int(bigquery_client().get_table(REGISTRY_TABLE_REF).num_rows)
    except Exception:
        return None


@app.get("/")
def index() -> FileResponse:
    return FileResponse(UI_ROOT / "index.html")


@app.get("/app.js")
def app_js() -> FileResponse:
    return FileResponse(UI_ROOT / "app.js")


@app.get("/styles.css")
def styles() -> FileResponse:
    return FileResponse(UI_ROOT / "styles.css")


@app.get("/status_snapshot.json")
def snapshot() -> FileResponse:
    return FileResponse(UI_ROOT / "status_snapshot.json")


@app.get("/favicon.ico")
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/public-datasets")
def public_datasets() -> dict[str, object]:
    return {"datasets": public_dataset_status()}


@app.get("/api/datasets")
def datasets() -> dict[str, object]:
    return {"datasets": dataset_catalog_payload()}


@app.get("/api/datasets/{dataset_key}/sample")
def dataset_sample(dataset_key: str, limit: int = DEFAULT_LIMIT) -> dict[str, object]:
    entry = RAW_DATASETS.get(dataset_key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset_key}")

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


@app.get("/api/registry")
def registry_metadata() -> dict[str, object]:
    payload = registry_catalog_payload()
    payload["row_count"] = registry_row_count()
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
