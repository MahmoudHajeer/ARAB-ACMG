# Checkpoint: T003 Controlled-Access Roadmap

## Date
2026-03-13

## Scope
Add a frozen supervisor-UI section for high-value Arab datasets that still require provider approval, and clarify the scope of non-core GME subgroup columns in the final checkpoint glossary.

## Official process guides captured
- `EGA controlled-access workflow`
  - Source: https://ega-archive.org/access/request-data/how-to-request-data/
  - Verified on 2026-03-13
- `QPHI research-portal workflow`
  - Source: https://www.qphi.org.qa/research/how-to-apply
  - Verified on 2026-03-13

## Controlled-access sources now tracked in the UI
1. `Emirati Population Variome`
   - Official study: `EGAS50000001071`
   - Official dataset: `EGAD50000001558`
   - DAC: `EGAC00001001544`
   - Public metadata reviewed: published `2025-07-18`
   - Decision: `priority_1`
2. `QPHI-Qatari 25k release`
   - Official source: `https://www.qphi.org.qa/genomicdata`
   - Public metadata reviewed: release label `Version 1 (25k release)` visible on 2026-03-13
   - Decision: `priority_1`
3. `Moroccan Genome Project`
   - Official study: `EGAS50000000550`
   - DAC: `EGAC50000000353`
   - Public metadata reviewed: published `2025-04-25`
   - Decision: `secondary`
4. `Tunisian WES cohort`
   - Official study: `EGAS50000001064`
   - Public metadata reviewed: published `2025-07-07`
   - Decision: `secondary`
5. `EgyptRef healthy-volunteer cohort`
   - Official dataset: `EGAD00001001380`
   - DAC: `EGAC00001000205`
   - Public metadata reviewed: published `2020-08-12`
   - Decision: `secondary`

## Non-core GME extras clarified
The final checkpoint still carries `GME_ISRAEL`, `GME_TP`, and `GME_CA`, but they are now marked in the UI as `context_extra` rather than ordinary extras.

Interpretation rule:
- keep them only for upstream traceability, broader regional QC, or explicit sensitivity analysis
- do not treat them as core Arab-analysis outputs
- drop them from a publication-facing export if they do not answer a declared research question

## UI changes
- Added a new `Controlled Access` page.
- Added frozen official process guides.
- Added source-specific acquisition cards with exact links and project decisions.
- Kept runtime static: no new analytical queries are triggered by the page.

## Next exact action
Continue `T003 / 2.2` by freezing `SHGP` with provenance and deciding whether `AVDB` gets a dedicated GRCh37-to-GRCh38 path or remains secondary reference-only evidence.
