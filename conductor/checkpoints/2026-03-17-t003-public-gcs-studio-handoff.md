# T003 Step 5.1: Public GCS Download Hardening + GitHub/Google Studio Handoff

## Summary
- Added an explicit public-download policy so only safe GCS objects exposed in the supervisor download center are published anonymously.
- Kept private study workbooks private in GCS; the UI now exposes only de-identified public-safe extracts for those sources.
- Added a GitHub-first handoff document for Google AI Studio / cloud-workspace use.

## Key Outcomes
- `66` safe GCS objects in the supervisor download surface were verified as:
  - `HTTP 200`
  - non-empty
  - returned with `content-disposition: attachment`
- Browser download verification succeeded for:
  - `hg38_gme.txt.gz`
  - `supervisor_variant_registry_brca_arab_v2.csv`
- `source_review.json` now marks raw Saudi/UAE workbook copies as private audit-only rather than exposing public GCS links.

## Files
- `scripts/gcs_public_policy.py`
- `scripts/runtime_config.py`
- `scripts/sync_public_gcs_downloads.py`
- `scripts/refresh_supervisor_review_bundle.py`
- `scripts/update_source_review_state.py`
- `scripts/build_brca_normalized_artifacts.py`
- `scripts/verify_brca_normalized_artifacts.py`
- `ui/app.js`
- `ui/review_bundle.py`
- `ui/source_review.py`
- `ui/controlled_access.py`
- `GOOGLE_AI_STUDIO_HANDOFF.md`

## Verification
- `python3 -m pytest -q tests` -> `56 passed`
- `python3 scripts/update_source_review_state.py` -> pass
- `python3 scripts/refresh_supervisor_review_bundle.py` -> pass
- `python3 scripts/sync_public_gcs_downloads.py` -> pass (`count=66`)
- `python3 scripts/verify_brca_normalized_artifacts.py` -> pass
- `node --check ui/app.js` -> pass
- Playwright CLI local checks -> pass for download start events on:
  - `GME raw BRCA-window preview`
  - `supervisor_variant_registry_brca_arab_v2`

## Decision
- The bucket was **not** made globally public.
- Rationale: the bucket contains private raw workbooks from patient-level study supplements.
- Only the allowlisted public-safe objects surfaced in the supervisor UI are published anonymously.
