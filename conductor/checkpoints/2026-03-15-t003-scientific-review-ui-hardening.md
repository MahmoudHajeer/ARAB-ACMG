# T003 Scientific Review + UI Hardening

Date: `2026-03-15`
Track: `T003-DataHarmonization`
Task context: `5.1` remains open; this checkpoint records the scientific/UI hardening pass completed before GE suites.

## Findings fixed

1. `Arab final` display drift
- Cause: `scripts/refresh_supervisor_review_bundle.py` re-read an already split `ui/review_bundle.json` and treated `pre_gme/registry` as the active Arab tables again.
- Effect: the Arab extension page could show baseline identities instead of the real `arab_*` artifacts after later refreshes.
- Fix: refresh logic now prefers `arab_pre_gme` and `arab_registry` when they already exist.

2. Hidden `GRCh37 -> GRCh38` evidence
- Cause: `ui/source_review.json` was stale and still lacked the frozen AVDB liftover block/sample even though the frozen liftover artifacts existed.
- Effect: the build-conversion step was not visible enough in the UI.
- Fix: `scripts/update_source_review_state.py` was repaired and rerun so AVDB now exposes liftover method, counts, sample rows, and frozen artifact links.

3. SHGP inclusion test drift
- Cause: test still assumed SHGP was outside the current final table.
- Effect: one false test failure after the Arab extension became real.
- Fix: `tests/test_source_review_state.py` now matches the current scientific state.

4. Workflow naming and section clarity
- Cause: user-facing labels still carried internal wording such as `pre-gme`, and the baseline/Arab split was too easy to misread.
- Fix: the UI now presents the workflow as:
  - Raw Evidence
  - Genome Build Conversion
  - BRCA Normalization
  - Baseline Draft Table
  - Baseline Final Table
  - Arab Extension Tables
  - Data Downloads
  - Controlled Access

## Scientific state confirmed

- No baseline columns were dropped from the Arab final table.
- Current validation result:
  - baseline final columns: `65`
  - Arab final columns: `74`
  - preserved baseline columns: `65`
  - missing legacy columns: `0`
- Added Arab-final columns are explicit and traceable:
  - `VARIANT_KEY`
  - `SHGP_AC`
  - `SHGP_AN`
  - `SHGP_AF`
  - `PRESENT_IN_CLINVAR`
  - `PRESENT_IN_GNOMAD_GENOMES`
  - `PRESENT_IN_GNOMAD_EXOMES`
  - `PRESENT_IN_SHGP`
  - `PRESENT_IN_GME`

## UI decisions

- Workflow pages are review-only.
- Downloads appear only in `Data Downloads`.
- Raw cards remain reference-only.
- The `Genome Build Conversion` page now shows the AVDB conversion evidence as its own step.
- Table cards now surface:
  - stored artifact
  - frozen row count
  - evidence trail
  - short scientific purpose

## Verification

- `python3 scripts/build_brca_normalized_artifacts.py` -> pass
- `python3 scripts/update_source_review_state.py` -> pass
- `python3 scripts/refresh_supervisor_review_bundle.py` -> pass
- `python3 scripts/verify_brca_normalized_artifacts.py` -> pass
- `python3 -m pytest -q tests` -> `85 passed`
- `node --check ui/app.js` -> pass
- Local Playwright review on `#overview`, `#standardization`, `#final`, `#arab-extension`, `#artifacts` -> pass

## Next exact action

Build `T003 / 5.1` Great Expectations suites/checkpoints for the frozen normalized artifacts and checkpoint tables, then expose the validation outputs in the same review style.
