from pathlib import Path

from ui.overview_data import build_overview_payload, load_overview_payload, parse_plan_progress, parse_tracks_registry


def test_parse_tracks_registry_reads_markdown_table():
    markdown = """
| Track ID | Name | Description | Status | Folder |
| :--- | :--- | :--- | :--- | :--- |
| `T001` | **Infrastructure** | Setup work. | `[x]` | `conductor/tracks/T001-Infrastructure/` |
| `T002` | **Data** | Raw tables. | `[~]` | `conductor/tracks/T002-DataCollection/` |
""".strip()

    payload = parse_tracks_registry(markdown)

    assert [track["track_id"] for track in payload] == ["T001", "T002"]
    assert payload[0]["status_label"] == "done"
    assert payload[1]["status_label"] == "in_progress"


def test_parse_plan_progress_counts_done_active_and_todo_tasks():
    markdown = """
- [x] 1.1 Done
- [~] 1.2 In progress
- [ ] 1.3 Todo
- [ ] 1.4 Todo
""".strip()

    payload = parse_plan_progress(markdown)

    assert payload["total_tasks"] == 4
    assert payload["done_tasks"] == 1
    assert payload["in_progress_tasks"] == 1
    assert payload["todo_tasks"] == 2
    assert payload["progress_pct"] == 37.5


def test_build_overview_payload_reads_live_conductor_state(tmp_path: Path):
    conductor = tmp_path / "conductor"
    tracks_dir = conductor / "tracks"
    (tracks_dir / "T001-Infrastructure").mkdir(parents=True)
    (tracks_dir / "T002-DataCollection").mkdir(parents=True)

    (conductor / "tracks.md").write_text(
        """
| Track ID | Name | Description | Status | Folder |
| :--- | :--- | :--- | :--- | :--- |
| `T001` | **Infrastructure** | Setup work. | `[x]` | `conductor/tracks/T001-Infrastructure/` |
| `T002` | **Data** | Raw tables. | `[~]` | `conductor/tracks/T002-DataCollection/` |
""".strip(),
        encoding="utf-8",
    )
    (conductor / "setup_state.json").write_text(
        '{"last_successful_step": "T002 step 5.5 finalized"}',
        encoding="utf-8",
    )
    (tracks_dir / "T001-Infrastructure" / "plan.md").write_text("- [x] 1.1 Done\n", encoding="utf-8")
    (tracks_dir / "T002-DataCollection" / "plan.md").write_text(
        "- [x] 2.1 Done\n- [~] 2.2 Active\n- [ ] 2.3 Todo\n",
        encoding="utf-8",
    )
    (tracks_dir / "T002-DataCollection" / "index.md").write_text(
        """
### Entry 1
- Verification run + result: `python3 -m pytest -q tests (49 passed)`, `curl /api/health (pass)`
""".strip(),
        encoding="utf-8",
    )

    payload = build_overview_payload(root=tmp_path)

    assert payload["track_status_counts"] == {"done": 1, "in_progress": 1, "not_started": 0}
    assert payload["last_successful_step"] == "T002 step 5.5 finalized"
    assert payload["plan_progress"]["T002"]["in_progress_tasks"] == 1
    assert payload["latest_t002_verification"][0]["status"] == "pass"


def test_load_overview_payload_falls_back_to_bundled_file_when_conductor_is_missing(tmp_path: Path):
    ui_dir = tmp_path / "ui"
    ui_dir.mkdir(parents=True)
    (ui_dir / "overview_state.json").write_text(
        '{"generated_at":"2026-03-12T12:00:00+00:00","tracks":[],"plan_progress":{},"track_status_counts":{"done":0,"in_progress":0,"not_started":0},"latest_t002_verification":[],"last_successful_step":"bundled"}',
        encoding="utf-8",
    )

    payload = load_overview_payload(root=tmp_path)

    assert payload["generated_at"] == "2026-03-12T12:00:00+00:00"
    assert payload["last_successful_step"] == "bundled"
