# Checkpoint: T003 Source Readiness Review

- Timestamp: `2026-03-13T09:20:00+03:00`
- Track: `T003-DataHarmonization`
- Task: `2.1`
- Agent: `Codex`

## What Was Reviewed
- ClinVar GRCh38 raw VCF freeze
- gnomAD v4.1 genomes chr13/chr17 raw freezes
- gnomAD v4.1 exomes chr13/chr17 raw freezes
- GME hg38 summary-frequency freeze
- Saudi `PMC10474689` de-identified extract freeze
- UAE `PMC12011969` de-identified extract freeze

## Scientific Outcome
- `ClinVar`, `gnomAD genomes`, and `gnomAD exomes` are `Ready` for BRCA normalization without liftover.
- `GME hg38` is `Partial`: it is already hg38, but it behaves like a summary table rather than a native VCF and therefore needs explicit canonical-key rules.
- `UAE PMC12011969` is `Partial`: retained rows include `Chr location (hg38)`, but row-level coordinate parsing still needs validation before normalization.
- `Saudi PMC10474689` is `Blocked`: the retained sheet is HGVS-oriented and does not yet expose genomic coordinates needed for canonical-key construction.

## Evidence Added
- Static supervisor payload: `ui/source_review.json`
- Static UI page section: Harmonization -> `Workflow Categories` + `Scientific Source Review`
- Code comments now classify the workflow into explicit review stages in:
  - `scripts/update_source_review_state.py`
  - `scripts/freeze_arab_study_sources.py`
  - `ui/app.js`
  - `ui/service.py`

## Supervisor-Facing Guarantees
- The new source-review page section is frozen/static and does not trigger BigQuery queries.
- Each source card now exposes:
  - build status
  - coordinate readiness
  - liftover decision
  - normalization decision
  - upstream URL
  - source version
  - raw-vault prefix
  - next exact action
- Checkpoint cards now show frozen GCS artifact paths first, with historical BigQuery build references shown only as provenance.

## Next Exact Action
- Start `T003 / 2.2` by implementing the first coordinate-aware normalization path for `gme_hg38` and the validated `uae_brca_pmc12011969` rows, while keeping `saudi_breast_cancer_pmc10474689` blocked until transcript-to-genome mapping is justified.
