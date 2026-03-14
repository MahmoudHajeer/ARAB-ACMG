"""Compose the supervisor review bundle from frozen artifacts only.

This stage is intentionally cheaper than the main normalization build. It does
not recompute ClinVar/gnomAD/SHGP/GME artifacts. Instead it:
1. Loads the current T003 frozen bundle written by the normalization pipeline.
2. Loads the historical legacy bundle that predates the Arab extension.
3. Publishes CSV downloads for every derived artifact that is shown in the UI.
4. Rewrites the supervisor bundle so the baseline final table stays separate
   from the Arab-extension checkpoint tables.

The result is a supervisor-facing review surface that reads like a scientific
logbook rather than a developer console.
"""

from __future__ import annotations

import json
import logging
import tempfile
from copy import deepcopy
from pathlib import Path
from typing import Any, Final

import pandas as pd
from google.cloud import storage

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
UI_DIR: Final[Path] = ROOT / "ui"
PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
CURRENT_BUNDLE_PATH: Final[Path] = UI_DIR / "review_bundle.json"
LEGACY_BUNDLE_URI: Final[str] = (
    "gs://mahmoud-arab-acmg-research-data/"
    "frozen/supervisor_review/snapshot_date=2026-03-12/review_bundle.json"
)
REVIEW_BUNDLE_OBJECT: Final[str] = "frozen/review_bundle/snapshot_date=2026-03-14/review_bundle.json"

LOGGER = logging.getLogger("refresh_supervisor_review_bundle")

RAW_SOURCE_LINK_MAP: Final[dict[str, str]] = {
    "clinvar_raw_brca_window": "clinvar",
    "gnomad_genomes_raw_brca_window": "gnomad_genomes",
    "gnomad_exomes_raw_brca_window": "gnomad_exomes",
    "shgp_raw_brca_window": "shgp_saudi_af",
    "gme_raw_brca_window": "gme_hg38",
}


def parse_gs_uri(uri: str) -> tuple[str, str]:
    if not uri.startswith("gs://"):
        raise ValueError(f"Unsupported URI: {uri}")
    bucket_name, object_name = uri[5:].split("/", 1)
    return bucket_name, object_name


def json_dump(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")


def public_object_url(object_name: str) -> str:
    return f"https://storage.googleapis.com/{BUCKET_NAME}/{object_name}"


def download_json_from_gcs(storage_client: storage.Client, uri: str) -> dict[str, Any]:
    bucket_name, object_name = parse_gs_uri(uri)
    return json.loads(storage_client.bucket(bucket_name).blob(object_name).download_as_text())


def download_parquet_frame(storage_client: storage.Client, uri: str) -> pd.DataFrame:
    bucket_name, object_name = parse_gs_uri(uri)
    with tempfile.TemporaryDirectory(prefix="arab_acmg_bundle_") as tmpdir:
        parquet_path = Path(tmpdir) / Path(object_name).name
        storage_client.bucket(bucket_name).blob(object_name).download_to_filename(parquet_path)
        return pd.read_parquet(parquet_path)


def upload_public_csv(storage_client: storage.Client, frame: pd.DataFrame, object_name: str) -> dict[str, str]:
    with tempfile.TemporaryDirectory(prefix="arab_acmg_csv_") as tmpdir:
        csv_path = Path(tmpdir) / Path(object_name).name
        frame.to_csv(csv_path, index=False)
        blob = storage_client.bucket(BUCKET_NAME).blob(object_name)
        blob.upload_from_filename(str(csv_path), content_type="text/csv")
        blob.make_public()
    return {"gs_uri": f"gs://{BUCKET_NAME}/{object_name}", "public_url": public_object_url(object_name)}


def csv_object_from_parquet_uri(parquet_uri: str) -> str:
    bucket_name, object_name = parse_gs_uri(parquet_uri)
    if bucket_name != BUCKET_NAME:
        raise ValueError(f"Unexpected bucket for CSV export: {parquet_uri}")
    if not object_name.endswith(".parquet"):
        raise ValueError(f"Expected a parquet artifact URI, got: {parquet_uri}")
    return object_name[:-8] + ".csv"


def ensure_public_csv(storage_client: storage.Client, parquet_uri: str) -> dict[str, str]:
    frame = download_parquet_frame(storage_client, parquet_uri)
    return upload_public_csv(storage_client, frame, csv_object_from_parquet_uri(parquet_uri))


def normalize_review_entry(entry: dict[str, Any], *, table_label: str, scope_note: str) -> dict[str, Any]:
    normalized = deepcopy(entry)
    normalized["review_label"] = table_label
    normalized["scope_note"] = scope_note
    return normalized


def raw_source_catalog_entries(raw_payload: dict[str, Any], source_review: dict[str, Any]) -> list[dict[str, Any]]:
    source_lookup = {
        source["source_key"]: source
        for source in source_review.get("sources", [])
        if source.get("source_key")
    }

    entries = []
    for dataset in raw_payload["datasets"]:
        source_key = RAW_SOURCE_LINK_MAP.get(str(dataset["key"]))
        source = source_lookup.get(source_key or "", {})
        links = []
        if source.get("upstream_url"):
            links.append({"label": "Official source", "url": source["upstream_url"]})
        entries.append(
            {
                "key": dataset["key"],
                "title": dataset["title"],
                "group": "raw_public_sources",
                "stage": "Raw source-of-truth",
                "overview": dataset["simple_summary"],
                "row_count": dataset["row_count"],
                "downloads": [],
                "links": links,
                "references": [dataset["table_ref"], *dataset.get("notes", [])[:1]],
                "download_note": "No UI export is generated for raw sources. Review the frozen sample here, then use the official raw source link or the raw-vault reference.",
            }
        )
    return entries


def derived_catalog_entry(
    *,
    key: str,
    title: str,
    stage: str,
    overview: str,
    row_count: int,
    table_ref: str,
    csv_public_url: str,
    review_label: str,
) -> dict[str, Any]:
    return {
        "key": key,
        "title": title,
        "group": stage,
        "stage": review_label,
        "overview": overview,
        "row_count": row_count,
        "downloads": [{"label": "Download CSV", "url": csv_public_url}],
        "links": [],
        "references": [table_ref],
        "download_note": "This CSV is frozen from the approved checkpoint artifact. Opening the dashboard does not rebuild it.",
    }


def build_artifact_catalog(
    *,
    legacy_pre_gme: dict[str, Any],
    legacy_registry: dict[str, Any],
    arab_pre_gme: dict[str, Any],
    arab_registry: dict[str, Any],
    normalized_datasets: list[dict[str, Any]],
    raw_datasets: dict[str, Any],
    source_review: dict[str, Any],
) -> dict[str, Any]:
    groups = [
        {
            "id": "raw_public_sources",
            "title": "Raw source-of-truth references",
            "summary": "These sources stay raw. The dashboard shows only frozen 10-row samples plus the official source links or raw-vault references.",
            "entries": raw_source_catalog_entries(raw_datasets, source_review),
        },
        {
            "id": "normalized_artifacts",
            "title": "Normalized per-source artifacts",
            "summary": "These are the BRCA-normalized per-source outputs used to build the Arab extension checkpoint.",
            "entries": [
                {
                    "key": entry["key"],
                    "title": entry["title"],
                    "group": "normalized_artifacts",
                    "stage": "Normalized artifact",
                    "overview": entry["simple_summary"],
                    "row_count": entry["row_count"],
                    "downloads": [{"label": "Download CSV", "url": entry["download_url"]}],
                    "links": [],
                    "references": [entry["table_ref"]],
                    "download_note": "Frozen normalized CSV exported from the approved Parquet artifact.",
                }
                for entry in normalized_datasets
            ],
        },
        {
            "id": "legacy_checkpoint_artifacts",
            "title": "Legacy BRCA checkpoint surfaces",
            "summary": "These are the pre-Arab checkpoints that the supervisor already reviewed earlier. They stay visible as the stable baseline.",
            "entries": [
                derived_catalog_entry(
                    key="legacy_pre_gme",
                    title=legacy_pre_gme["title"],
                    stage="legacy_checkpoint_artifacts",
                    overview=legacy_pre_gme["scope_note"],
                    row_count=legacy_pre_gme["row_count"],
                    table_ref=legacy_pre_gme["table_ref"],
                    csv_public_url=legacy_pre_gme["csv_download_url"],
                    review_label="Legacy baseline pre-GME",
                ),
                derived_catalog_entry(
                    key="legacy_registry",
                    title=legacy_registry["title"],
                    stage="legacy_checkpoint_artifacts",
                    overview=legacy_registry["scope_note"],
                    row_count=legacy_registry["row_count"],
                    table_ref=legacy_registry["table_ref"],
                    csv_public_url=legacy_registry["csv_download_url"],
                    review_label="Legacy baseline final",
                ),
            ],
        },
        {
            "id": "arab_extension_artifacts",
            "title": "Arab extension artifacts",
            "summary": "These are the new T003 checkpoints that extend the legacy baseline with SHGP first, then GME as a supporting layer.",
            "entries": [
                derived_catalog_entry(
                    key="arab_pre_gme",
                    title=arab_pre_gme["title"],
                    stage="arab_extension_artifacts",
                    overview=arab_pre_gme["scope_note"],
                    row_count=arab_pre_gme["row_count"],
                    table_ref=arab_pre_gme["table_ref"],
                    csv_public_url=arab_pre_gme["csv_download_url"],
                    review_label="Arab extension before GME",
                ),
                derived_catalog_entry(
                    key="arab_registry",
                    title=arab_registry["title"],
                    stage="arab_extension_artifacts",
                    overview=arab_registry["scope_note"],
                    row_count=arab_registry["row_count"],
                    table_ref=arab_registry["table_ref"],
                    csv_public_url=arab_registry["csv_download_url"],
                    review_label="Arab extension final",
                ),
            ],
        },
    ]
    return {"groups": groups}


def refresh_bundle() -> dict[str, Any]:
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")
    storage_client = storage.Client(project=PROJECT_ID)

    # [AI-Agent: Codex]: Stage 1 / Load the current and historical frozen bundles so the UI can show baseline and Arab extension separately.
    LOGGER.info("Loading current review bundle from %s", CURRENT_BUNDLE_PATH)
    current_bundle = json.loads(CURRENT_BUNDLE_PATH.read_text(encoding="utf-8"))
    source_review = json.loads((UI_DIR / "source_review.json").read_text(encoding="utf-8"))
    LOGGER.info("Loading legacy review bundle from %s", LEGACY_BUNDLE_URI)
    legacy_bundle = download_json_from_gcs(storage_client, LEGACY_BUNDLE_URI)

    # [AI-Agent: Codex]: Stage 2 / Publish one CSV per derived artifact so the supervisor can download exactly what is displayed.
    LOGGER.info("Publishing CSV downloads for normalized artifacts and current Arab checkpoints")
    for dataset in current_bundle["datasets"]["datasets"]:
        dataset["download_url"] = ensure_public_csv(storage_client, dataset["table_ref"])["public_url"]

    arab_pre_gme = normalize_review_entry(
        current_bundle["pre_gme"],
        table_label="Arab extension before GME",
        scope_note="Arab-aware checkpoint from normalized ClinVar + gnomAD + SHGP before GME is added.",
    )
    arab_pre_gme["csv_download_url"] = ensure_public_csv(storage_client, arab_pre_gme["table_ref"])["public_url"]

    arab_registry = normalize_review_entry(
        current_bundle["registry"],
        table_label="Arab extension final",
        scope_note="Arab-aware checkpoint after GME is added as a supporting Arab/MENA layer.",
    )
    arab_registry["csv_download_url"] = current_bundle["registry"]["csv_download_url"]

    legacy_pre_gme = normalize_review_entry(
        legacy_bundle["pre_gme"],
        table_label="Legacy baseline pre-GME",
        scope_note="Historical checkpoint frozen before the Arab extension work began.",
    )
    legacy_pre_gme["csv_download_url"] = ensure_public_csv(storage_client, legacy_pre_gme["table_ref"])["public_url"]

    legacy_registry = normalize_review_entry(
        legacy_bundle["registry"],
        table_label="Legacy baseline final",
        scope_note="Historical final BRCA checkpoint frozen before adding the new Arab frequency datasets.",
    )
    legacy_registry["csv_download_url"] = legacy_bundle["registry"]["csv_download_url"]

    # [AI-Agent: Codex]: Stage 3 / Reframe the bundle so the legacy baseline stays under the original final-table routes and Arab work moves to its own page.
    LOGGER.info("Composing updated review bundle structure")
    bundle = deepcopy(current_bundle)
    bundle["workflow"]["pages"] = [
        {"id": "overview", "title": "Overview", "summary": "Track progress and the current scientific workflow."},
        {"id": "raw", "title": "Raw Sources", "summary": "Frozen untouched source packages and BRCA-window raw previews before any normalization."},
        {"id": "harmonization", "title": "Normalization", "summary": "Per-source BRCA normalization artifacts with explicit provenance and no live queries."},
        {"id": "pre-gme", "title": "Legacy Pre-GME", "summary": "Historical baseline checkpoint before the Arab extension work began."},
        {"id": "final", "title": "Legacy Final", "summary": "Historical final BRCA checkpoint preserved as the baseline review surface."},
        {"id": "arab-extension", "title": "Arab Extension", "summary": "New Arab-aware checkpoints built from normalized ClinVar, gnomAD, SHGP, and then GME."},
        {"id": "artifacts", "title": "Data Downloads", "summary": "Structured download center for all frozen derived artifacts shown in the dashboard."},
        {"id": "access", "title": "Controlled Access", "summary": "Official acquisition paths for restricted Arab datasets still outside the active workflow."},
    ]
    bundle["workflow"]["legacy_final_steps"] = legacy_bundle["workflow"]["final_steps"]
    bundle["workflow"]["arab_extension_steps"] = current_bundle["workflow"]["final_steps"]
    bundle["legacy_step_samples"] = legacy_bundle.get("step_samples", {})
    bundle["arab_step_samples"] = current_bundle.get("step_samples", {})
    bundle["pre_gme"] = legacy_pre_gme
    bundle["registry"] = legacy_registry
    bundle["arab_pre_gme"] = arab_pre_gme
    bundle["arab_registry"] = arab_registry
    bundle["artifact_catalog"] = build_artifact_catalog(
        legacy_pre_gme=legacy_pre_gme,
        legacy_registry=legacy_registry,
        arab_pre_gme=arab_pre_gme,
        arab_registry=arab_registry,
        normalized_datasets=bundle["datasets"]["datasets"],
        raw_datasets=bundle["raw_datasets"],
        source_review=source_review,
    )
    bundle["artifacts"]["legacy_bundle_uri"] = LEGACY_BUNDLE_URI
    bundle["artifacts"]["legacy_final_csv_public_url"] = legacy_registry["csv_download_url"]
    bundle["artifacts"]["arab_pre_gme_csv_public_url"] = arab_pre_gme["csv_download_url"]
    bundle["artifacts"]["arab_final_csv_public_url"] = arab_registry["csv_download_url"]

    # [AI-Agent: Codex]: Stage 4 / Persist the refreshed bundle locally and back to GCS so Cloud Run serves the exact same static state.
    LOGGER.info("Writing updated bundle to %s", CURRENT_BUNDLE_PATH)
    json_dump(CURRENT_BUNDLE_PATH, bundle)
    upload_blob = storage_client.bucket(BUCKET_NAME).blob(REVIEW_BUNDLE_OBJECT)
    upload_blob.upload_from_string(json.dumps(bundle, indent=2), content_type="application/json")

    return bundle


def main() -> None:
    bundle = refresh_bundle()
    print(
        json.dumps(
            {
                "status": "ok",
                "legacy_final_title": bundle["registry"]["title"],
                "arab_final_title": bundle["arab_registry"]["title"],
                "normalized_downloads": len(bundle["datasets"]["datasets"]),
                "artifact_groups": len(bundle["artifact_catalog"]["groups"]),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
