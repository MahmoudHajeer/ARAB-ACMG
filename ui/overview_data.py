from __future__ import annotations

import datetime as dt
import json
import re
from pathlib import Path
from typing import Final

PROJECT_ROOT: Final[Path] = Path(__file__).resolve().parents[1]
BUNDLED_OVERVIEW_FILE: Final[Path] = Path(__file__).resolve().parent / "overview_state.json"
TRACKS_FILE: Final[Path] = PROJECT_ROOT / "conductor" / "tracks.md"
SETUP_STATE_FILE: Final[Path] = PROJECT_ROOT / "conductor" / "setup_state.json"
T002_INDEX_FILE: Final[Path] = PROJECT_ROOT / "conductor" / "tracks" / "T002-DataCollection" / "index.md"
TRACK_PLAN_PATHS: Final[dict[str, Path]] = {
    "T001": PROJECT_ROOT / "conductor" / "tracks" / "T001-Infrastructure" / "plan.md",
    "T002": PROJECT_ROOT / "conductor" / "tracks" / "T002-DataCollection" / "plan.md",
    "T003": PROJECT_ROOT / "conductor" / "tracks" / "T003-DataHarmonization" / "plan.md",
    "T004": PROJECT_ROOT / "conductor" / "tracks" / "T004-AnalysisEngine" / "plan.md",
    "T005": PROJECT_ROOT / "conductor" / "tracks" / "T005-StatsResults" / "plan.md",
}


def status_label(symbol: str) -> str:
    if symbol == "x":
        return "done"
    if symbol == "~":
        return "in_progress"
    return "not_started"


def parse_tracks_registry(markdown: str) -> list[dict[str, str]]:
    tracks: list[dict[str, str]] = []
    for line in markdown.splitlines():
        if not line.startswith("| `T"):
            continue
        columns = [column.strip() for column in line.strip().split("|")]
        if len(columns) < 6:
            continue
        track_id = columns[1].strip("`")
        match = re.search(r"\[([x~ ])\]", columns[4].strip("`"))
        symbol = match.group(1) if match else " "
        tracks.append(
            {
                "track_id": track_id,
                "name": columns[2].replace("**", ""),
                "description": columns[3],
                "status_symbol": symbol,
                "status_label": status_label(symbol),
                "folder": columns[5].strip("`"),
            }
        )
    return tracks


def parse_plan_progress(markdown: str) -> dict[str, float | int]:
    checks = re.findall(r"^- \[([x~ ])\] ", markdown, flags=re.MULTILINE)
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


def parse_latest_t002_verification(markdown: str) -> list[dict[str, str]]:
    verification_line = ""
    for line in markdown.splitlines():
        stripped = line.strip()
        if stripped.startswith("- verification:") or stripped.startswith("- Verification run + result:"):
            verification_line = stripped

    if not verification_line:
        return []

    results: list[dict[str, str]] = []
    for command in re.findall(r"`([^`]+)`", verification_line):
        lowered = command.lower()
        if "fail" in lowered:
            status = "fail"
        elif "pass" in lowered:
            status = "pass"
        else:
            status = "info"
        results.append({"command": command, "status": status})
    return results


def load_json(path: Path) -> dict[str, object]:
    return json.loads(path.read_text(encoding="utf-8"))


def build_overview_payload(root: Path = PROJECT_ROOT) -> dict[str, object]:
    tracks_file = root / "conductor" / "tracks.md"
    setup_state_file = root / "conductor" / "setup_state.json"
    t002_index_file = root / "conductor" / "tracks" / "T002-DataCollection" / "index.md"
    track_plan_paths = {
        track_id: root / path.relative_to(PROJECT_ROOT)
        for track_id, path in TRACK_PLAN_PATHS.items()
    }

    tracks = parse_tracks_registry(tracks_file.read_text(encoding="utf-8"))
    plan_progress = {
        track_id: parse_plan_progress(plan_path.read_text(encoding="utf-8"))
        for track_id, plan_path in track_plan_paths.items()
        if plan_path.exists()
    }
    track_status_counts = {
        "done": sum(1 for track in tracks if track["status_label"] == "done"),
        "in_progress": sum(1 for track in tracks if track["status_label"] == "in_progress"),
        "not_started": sum(1 for track in tracks if track["status_label"] == "not_started"),
    }
    setup_state = load_json(setup_state_file)
    latest_t002_verification = parse_latest_t002_verification(t002_index_file.read_text(encoding="utf-8"))

    return {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "tracks": tracks,
        "plan_progress": plan_progress,
        "track_status_counts": track_status_counts,
        "latest_t002_verification": latest_t002_verification,
        "last_successful_step": str(setup_state.get("last_successful_step", "")),
    }


def load_overview_payload(root: Path = PROJECT_ROOT) -> dict[str, object]:
    tracks_file = root / "conductor" / "tracks.md"
    bundled_file = root / "ui" / "overview_state.json"
    if tracks_file.exists():
        return build_overview_payload(root=root)
    if bundled_file.exists():
        return load_json(bundled_file)
    if BUNDLED_OVERVIEW_FILE.exists():
        return load_json(BUNDLED_OVERVIEW_FILE)
    raise FileNotFoundError("Neither live Conductor files nor ui/overview_state.json are available.")


def write_bundled_overview_payload(destination: Path = BUNDLED_OVERVIEW_FILE) -> dict[str, object]:
    payload = build_overview_payload()
    destination.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    return payload
