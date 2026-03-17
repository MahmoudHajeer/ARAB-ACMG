# T003 Active File Audit

Date: `2026-03-17T20:34:24+03:00`
Agent: `Codex`

## Goal
Audit the remaining dirty files after the cleanup pass and delete only files that are truly obsolete.

## Decision
- `WALKTHROUGH.md`: obsolete, no active references, safe to delete.
- `Data collection.MD`: keep. Still referenced by Conductor specs and index as the roadmap narrative.
- `scripts/ingest_clinvar_cloud.py`: keep. Active raw-freeze pipeline script documented in `scripts/README.md`.
- `scripts/ingest_gnomad_parquet.py`: keep. Active raw-freeze pipeline script documented in `scripts/README.md`.
- `scripts/verify_gcp.py`: keep. Still used by `tests/test_gcp_connectivity.py` and current infrastructure verification flow.
- `ui/visual_identity.md`: keep. Active design-reference document used for UI direction.

## Evidence
- `rg` over the repo found live references for all retained files except `WALKTHROUGH.md`.
- `tests/test_gcp_connectivity.py` imports `scripts.verify_gcp`.
- `scripts/README.md` lists the retained scripts as active supported entry points.

## Result
- Deleted only `WALKTHROUGH.md`.
- Left all active files untouched to avoid deleting current workflow dependencies.

## Next
- Continue `T003 / 5.1` with Great Expectations suites/checkpoints for frozen normalized artifacts.
