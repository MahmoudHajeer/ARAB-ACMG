from __future__ import annotations

from pathlib import Path
from typing import Final

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, JSONResponse, RedirectResponse, Response

try:  # pragma: no cover - import path differs between local package and Cloud Run container
    from ui.controlled_access import load_controlled_access_payload
    from ui.overview_data import load_overview_payload as load_bundled_or_live_overview_payload
    from ui.review_bundle import load_review_bundle
    from ui.source_review import load_source_review_payload
except ModuleNotFoundError:  # pragma: no cover - runtime fallback inside the ui/ build context
    from controlled_access import load_controlled_access_payload  # type: ignore[no-redef]
    from overview_data import load_overview_payload as load_bundled_or_live_overview_payload  # type: ignore[no-redef]
    from review_bundle import load_review_bundle  # type: ignore[no-redef]
    from source_review import load_source_review_payload  # type: ignore[no-redef]

UI_ROOT: Final[Path] = Path(__file__).resolve().parent
NO_STORE_HEADERS: Final[dict[str, str]] = {"Cache-Control": "no-store"}
app = FastAPI(title="ARAB-ACMG Supervisor UI", version="1.3.0")


# [AI-Agent: Codex]: The review runtime now serves only frozen artifacts so a page refresh cannot trigger new BigQuery cost.
def review_bundle() -> dict[str, object]:
    return load_review_bundle()


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


@app.get("/review_bundle.json")
def bundle_file() -> JSONResponse:
    return JSONResponse(review_bundle(), headers=NO_STORE_HEADERS)


@app.get("/controlled_access.json")
def controlled_access_file() -> FileResponse:
    return FileResponse(UI_ROOT / "controlled_access.json", headers=NO_STORE_HEADERS)


@app.get("/favicon.ico")
def favicon() -> Response:
    return Response(status_code=204)


@app.get("/api/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


# [AI-Agent: Codex]: API Group 1 / Overview shell - enough metadata to orient the supervisor without loading the heavier evidence panels yet.
@app.get("/api/overview")
def overview() -> dict[str, object]:
    return load_bundled_or_live_overview_payload()


@app.get("/api/workflow")
def workflow_meta() -> dict[str, object]:
    return review_bundle()["workflow"]


# [AI-Agent: Codex]: Serve the scientific source-review bundle as static evidence so build/liftover decisions stay auditable in the supervisor UI.
@app.get("/api/source-review")
def source_review() -> dict[str, object]:
    return load_source_review_payload()


# [AI-Agent: Codex]: API Group 1b / Controlled-access roadmap - static official guidance for datasets that still require provider approval.
@app.get("/api/controlled-access")
def controlled_access() -> dict[str, object]:
    return load_controlled_access_payload()


# [AI-Agent: Codex]: API Group 2 / Raw source evidence - frozen previews of the untouched source-of-truth tables only.
@app.get("/api/raw-datasets")
def raw_datasets() -> dict[str, object]:
    return review_bundle()["raw_datasets"]


@app.get("/api/raw-datasets/{dataset_key}/sample")
def raw_dataset_sample(dataset_key: str) -> dict[str, object]:
    datasets = {entry["key"]: entry for entry in review_bundle()["raw_datasets"]["datasets"]}
    entry = datasets.get(dataset_key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown raw dataset: {dataset_key}")
    sample = dict(entry["sample"])
    sample["dataset_key"] = dataset_key
    sample["title"] = entry["title"]
    sample["table_ref"] = entry["table_ref"]
    return sample


# [AI-Agent: Codex]: API Group 3 / Checkpoint evidence - static previews of the approved BRCA checkpoint artifacts.
@app.get("/api/datasets")
def datasets() -> dict[str, object]:
    return review_bundle()["datasets"]


@app.get("/api/datasets/{dataset_key}/sample")
def dataset_sample(dataset_key: str) -> dict[str, object]:
    datasets = {entry["key"]: entry for entry in review_bundle()["datasets"]["datasets"]}
    entry = datasets.get(dataset_key)
    if entry is None:
        raise HTTPException(status_code=404, detail=f"Unknown dataset: {dataset_key}")
    sample = dict(entry["sample"])
    sample["dataset_key"] = dataset_key
    sample["title"] = entry["title"]
    sample["table_ref"] = entry["table_ref"]
    return sample


@app.get("/api/pre-gme")
def pre_gme_metadata() -> dict[str, object]:
    return review_bundle()["pre_gme"]


@app.get("/api/pre-gme/sample")
def pre_gme_sample() -> dict[str, object]:
    payload = review_bundle()["pre_gme"]
    sample = dict(payload["sample"])
    sample["table_ref"] = payload["table_ref"]
    return sample


# [AI-Agent: Codex]: API Group 4 / Final registry evidence - the only downloadable artifact left in the supervisor runtime.
@app.get("/api/registry")
def registry_metadata() -> dict[str, object]:
    return review_bundle()["registry"]


@app.get("/api/registry/sample")
def registry_sample() -> dict[str, object]:
    payload = review_bundle()["registry"]
    sample = dict(payload["sample"])
    sample["table_ref"] = payload["table_ref"]
    return sample


@app.get("/api/registry/steps/{step_id}/sample")
def registry_step_sample(step_id: str) -> dict[str, object]:
    step_payload = review_bundle()["step_samples"].get(step_id)
    if step_payload is None:
        raise HTTPException(status_code=404, detail=f"Unknown registry step: {step_id}")
    return {"step_id": step_id, **step_payload}


@app.get("/api/registry/download.csv")
def registry_download_csv() -> RedirectResponse:
    download_url = review_bundle()["registry"].get("csv_download_url")
    if not download_url:
        raise HTTPException(status_code=404, detail="Final registry download is not configured.")
    return RedirectResponse(str(download_url), status_code=307)
