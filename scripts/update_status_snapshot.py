"""
Build a single JSON snapshot used by the supervisor UI.

The snapshot combines:
1) Conductor track statuses and plan progress
2) Data-collection roadmap highlights
3) Latest runtime outcomes recorded in T002 handoff log
4) Live cloud metrics (BigQuery row counts + GCS artifact presence)
"""

from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Final

from google.cloud import bigquery
from google.cloud import storage
from google.api_core.exceptions import NotFound

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
PROJECT_ID: Final[str] = "genome-services-platform"
TRACKS_FILE: Final[Path] = ROOT / "conductor" / "tracks.md"
DATA_COLLECTION_FILE: Final[Path] = ROOT / "Data collection.MD"
T002_INDEX_FILE: Final[Path] = ROOT / "conductor" / "tracks" / "T002-DataCollection" / "index.md"
SNAPSHOT_FILE: Final[Path] = ROOT / "ui" / "status_snapshot.json"

TRACK_PLAN_PATHS: Final[dict[str, Path]] = {
    "T001": ROOT / "conductor" / "tracks" / "T001-Infrastructure" / "plan.md",
    "T002": ROOT / "conductor" / "tracks" / "T002-DataCollection" / "plan.md",
    "T003": ROOT / "conductor" / "tracks" / "T003-DataHarmonization" / "plan.md",
    "T004": ROOT / "conductor" / "tracks" / "T004-AnalysisEngine" / "plan.md",
    "T005": ROOT / "conductor" / "tracks" / "T005-StatsResults" / "plan.md",
}

RAW_GCS_PREFIXES: Final[list[str]] = [
    "raw/sources/clinvar/",
    "raw/sources/gnomad_v4.1/",
    "raw/working/gnomad_v4.1/",
]

RAW_TABLES: Final[list[str]] = [
    "clinvar_raw_vcf",
    "gnomad_v4_1_genomes_chr13_raw",
    "gnomad_v4_1_genomes_chr17_raw",
    "gnomad_v4_1_exomes_chr13_raw",
    "gnomad_v4_1_exomes_chr17_raw",
]

SAMPLE_COLUMNS: Final[list[str]] = [
    "chrom",
    "pos",
    "id",
    "ref",
    "alt",
    "qual",
    "filter",
    "info",
]
SAMPLE_LIMIT: Final[int] = 10
MAX_SAMPLE_VALUE_LENGTH: Final[int] = 160


def status_label(symbol: str) -> str:
    if symbol == "x":
        return "done"
    if symbol == "~":
        return "in_progress"
    return "not_started"


def parse_tracks_registry() -> list[dict[str, str]]:
    tracks: list[dict[str, str]] = []
    for line in TRACKS_FILE.read_text(encoding="utf-8").splitlines():
        if not line.startswith("| `T"):
            continue
        columns = [column.strip() for column in line.strip().split("|")]
        # Expected markdown table columns:
        # 0: empty, 1: track id, 2: name, 3: description, 4: status, 5: folder, 6: empty
        if len(columns) < 6:
            continue
        track_id = columns[1].strip("`")
        name = columns[2].replace("**", "")
        description = columns[3]
        status_cell = columns[4].strip("`")
        folder = columns[5].strip("`")
        match = re.search(r"\[([x~ ])\]", status_cell)
        symbol = match.group(1) if match else " "
        tracks.append(
            {
                "track_id": track_id,
                "name": name,
                "description": description,
                "status_symbol": symbol,
                "status_label": status_label(symbol),
                "folder": folder,
            }
        )
    return tracks


def parse_plan_progress(plan_path: Path) -> dict[str, float | int]:
    content = plan_path.read_text(encoding="utf-8")
    checks = re.findall(r"^- \[([x~ ])\] ", content, flags=re.MULTILINE)

    total = len(checks)
    done = checks.count("x")
    in_progress = checks.count("~")
    todo = checks.count(" ")
    done_pct = round((done / total) * 100, 1) if total else 0.0
    progress_pct = round(((done + (in_progress * 0.5)) / total) * 100, 1) if total else 0.0

    return {
        "total_tasks": total,
        "done_tasks": done,
        "in_progress_tasks": in_progress,
        "todo_tasks": todo,
        "done_pct": done_pct,
        "progress_pct": progress_pct,
    }


def parse_data_collection_phases() -> list[dict[str, object]]:
    phases: list[dict[str, object]] = []
    current: dict[str, object] | None = None

    for line in DATA_COLLECTION_FILE.read_text(encoding="utf-8").splitlines():
        if line.startswith("## Phase "):
            if current:
                phases.append(current)
            current = {"title": line.replace("## ", "").strip(), "bullets": []}
            continue

        if current and line.startswith("- "):
            bullets = current["bullets"]
            assert isinstance(bullets, list)
            bullets.append(line[2:].strip())

    if current:
        phases.append(current)

    # Keep UI concise but informative.
    for phase in phases:
        bullets = phase["bullets"]
        assert isinstance(bullets, list)
        phase["bullets"] = bullets[:5]
    return phases


def parse_latest_t002_verification() -> list[dict[str, str]]:
    verification_line = ""
    for line in T002_INDEX_FILE.read_text(encoding="utf-8").splitlines():
        if line.strip().startswith("- verification:"):
            verification_line = line.strip()

    if not verification_line:
        return []

    commands = re.findall(r"`([^`]+)`", verification_line)
    results: list[dict[str, str]] = []
    for command in commands:
        lowered = command.lower()
        if "fail" in lowered:
            status = "fail"
        elif "pass" in lowered:
            status = "pass"
        else:
            status = "info"
        results.append({"command": command, "status": status})
    return results


def get_bigquery_metrics() -> dict[str, object]:
    table_metrics: list[dict[str, object]] = []
    error: str | None = None

    try:
        client = bigquery.Client(project=PROJECT_ID)
        for table_name in RAW_TABLES:
            table_ref = f"{PROJECT_ID}.arab_acmg_raw.{table_name}"
            try:
                table = client.get_table(table_ref)
                query = f"SELECT COUNT(*) AS cnt FROM `{table_ref}`"
                count_row = list(client.query(query).result())[0]
                table_metrics.append(
                    {
                        "table": table_name,
                        "rows": int(count_row.cnt),
                        "fields": [field.name for field in table.schema],
                        "status": "present",
                    }
                )
            except NotFound:
                table_metrics.append(
                    {
                        "table": table_name,
                        "rows": 0,
                        "fields": [],
                        "status": "missing",
                    }
                )
    except Exception as exc:
        error = str(exc)

    return {"tables": table_metrics, "error": error}


def format_sample_value(value: object) -> str:
    if value is None:
        return ""

    text = str(value)
    if len(text) <= MAX_SAMPLE_VALUE_LENGTH:
        return text
    return f"{text[: MAX_SAMPLE_VALUE_LENGTH - 3]}..."


def get_bigquery_samples() -> dict[str, object]:
    sample_tables: list[dict[str, object]] = []
    error: str | None = None

    try:
        client = bigquery.Client(project=PROJECT_ID)
        for table_name in RAW_TABLES:
            table_ref = f"{PROJECT_ID}.arab_acmg_raw.{table_name}"
            try:
                client.get_table(table_ref)
                query = (
                    f"SELECT {', '.join(SAMPLE_COLUMNS)} "
                    f"FROM `{table_ref}` "
                    "ORDER BY chrom, pos, ref, alt "
                    f"LIMIT {SAMPLE_LIMIT}"
                )
                rows = [
                    {
                        column: format_sample_value(getattr(row, column, ""))
                        for column in SAMPLE_COLUMNS
                    }
                    for row in client.query(query).result()
                ]
                sample_tables.append(
                    {
                        "table": table_name,
                        "columns": SAMPLE_COLUMNS,
                        "rows": rows,
                        "status": "present",
                    }
                )
            except NotFound:
                sample_tables.append(
                    {
                        "table": table_name,
                        "columns": SAMPLE_COLUMNS,
                        "rows": [],
                        "status": "missing",
                    }
                )
    except Exception as exc:
        error = str(exc)

    return {"tables": sample_tables, "error": error}


def get_gcs_metrics() -> dict[str, object]:
    bucket_name = "mahmoud-arab-acmg-research-data"
    prefix_metrics: list[dict[str, object]] = []
    error: str | None = None

    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(bucket_name)
        for prefix in RAW_GCS_PREFIXES:
            blobs = list(client.list_blobs(bucket, prefix=prefix, max_results=200))
            prefix_metrics.append(
                {
                    "prefix": prefix,
                    "count": len(blobs),
                    "sample_paths": [blob.name for blob in blobs[:5]],
                }
            )
    except Exception as exc:
        error = str(exc)

    return {"bucket": bucket_name, "prefixes": prefix_metrics, "error": error}


def build_snapshot() -> dict[str, object]:
    tracks = parse_tracks_registry()
    progress = {
        track["track_id"]: parse_plan_progress(TRACK_PLAN_PATHS[track["track_id"]])
        for track in tracks
        if track["track_id"] in TRACK_PLAN_PATHS
    }

    status_counts = {
        "done": sum(1 for track in tracks if track["status_label"] == "done"),
        "in_progress": sum(1 for track in tracks if track["status_label"] == "in_progress"),
        "not_started": sum(1 for track in tracks if track["status_label"] == "not_started"),
    }

    return {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "project_id": PROJECT_ID,
        "tracks": tracks,
        "track_status_counts": status_counts,
        "plan_progress": progress,
        "data_collection_roadmap": parse_data_collection_phases(),
        "latest_t002_verification": parse_latest_t002_verification(),
        "bigquery_metrics": get_bigquery_metrics(),
        "bigquery_samples": get_bigquery_samples(),
        "gcs_metrics": get_gcs_metrics(),
    }


def main() -> None:
    snapshot = build_snapshot()
    SNAPSHOT_FILE.parent.mkdir(parents=True, exist_ok=True)
    SNAPSHOT_FILE.write_text(json.dumps(snapshot, indent=2), encoding="utf-8")
    print(f"✅ Snapshot updated: {SNAPSHOT_FILE}")


if __name__ == "__main__":
    main()
