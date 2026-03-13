# Checkpoint: Arab Frequency Source Triage

- Timestamp: `2026-03-13T13:05:00+03:00`
- Track: `T003-DataHarmonization`
- Task: `2.2` (in progress)
- Agent: `Codex`

## Scientific Rule Before Intake
For Arab ACMG population evidence, `AF` alone is useful but not ideal.

Preferred field order:
1. `CHROM`, `POS`, `REF`, `ALT`
2. `AC`, `AN`
3. `AF`
4. `HOM` / `HET` or equivalent genotype-count context
5. explicit genome build and cohort definition

Practical decision:
- `AF-only` sources can be used as **secondary frequency support**.
- Sources with `AC/AN` and genomic coordinates are better suited for the main harmonized frequency layer.
- Small disease cohorts are **not** valid substitutes for population-frequency resources.

## Local Candidate Review

| Candidate | Local file | Type | Size / Rows | Build evidence | Source evidence | Decision | Why |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| `SHGP Saudi frequency table` | `/Users/macbookpro/Desktop/storage/raw/shgp/Saudi_Arabian_Allele_Frequencies.txt` | Population WGS allele-frequency table | `610.74 MB`; `25,488,982` rows; columns `#CHROM, POS, REF, ALT, AN, AC` | File structure is genomic and directly usable; legacy notes mark it `GRCh38` | Figshare article `A list of Saudi Arabian variants and their allele frequencies`, DOI `10.6084/m9.figshare.28059686.v1`, published `2024-12-19T05:46:10Z`; file MD5 `0d2e1c0e7a358229a23c437fdf2f7d6e` | `KEEP / PRIORITY 1` | Large, simple, frequency-focused, and substantially more useful than the small Arab study supplements. It should be onboarded next. |
| `AVDB UAE workbook` | `/Users/macbookpro/Desktop/storage/raw/uae/avdb_uae.xlsx` | Curated clinically relevant Emirati variant list | `0.06 MB`; `801` rows; `7` columns | Genomic column is `HGVS_Genomic_GRCh37`; workbook properties show created `2025-06-27` | Official download page exists at `https://avdb-arabgenome.ae/downloads`, but a clean release-date capture was not retrievable automatically today because the site is slow/unreliable from the CLI session | `KEEP / SECONDARY ONLY` | Useful as an Emirati curated allele-frequency supplement, but it is not a genome-wide frequency baseline and it will require provenance hardening plus `GRCh37 -> GRCh38` conversion if used downstream. |
| `UAE BRCA supplement` | `/Users/macbookpro/Desktop/storage/raw/arab_studies/uae_brca_pmc12011969_moesm1.xlsx` | Study supplement / case cohort | `0.12 MB`; `18` family-screening positives + `65` cancer-cohort positives | `Chr location (hg38)` present for mutation-positive rows | Paper DOI `10.1007/s00432-025-06188-9` | `EXCLUDE FROM FREQUENCY LAYER` | This is a clinical cohort supplement, not a population AF dataset. Keep only as targeted BRCA case evidence. |
| `Saudi breast-cancer supplement` | `/Users/macbookpro/Desktop/storage/raw/arab_studies/saudi_breast_cancer_pmc10474689_moesm1.xls` | Study supplement / carrier table | `0.06 MB`; `38` rows in retained `Table S5` | No genomic coordinates in retained sheet; HGVS transcript/protein only | Paper DOI `10.1186/s13073-023-01220-4` | `EXCLUDE FROM FREQUENCY LAYER` | Too small and not coordinate-ready. Keep only as disease-study evidence, not as population frequency input. |

## Already Accepted Arab / MENA Frequency Source

| Source | Current role | Decision |
| :--- | :--- | :--- |
| `gme_hg38` | Arab/Middle Eastern summary-frequency layer | `KEEP` as secondary MENA frequency support. It is older (`2016` generation) and summary-style, so it must not be treated as a raw VCF-equivalent source. |

## Web-Verified External Candidates

| Candidate | What was verified today | Decision |
| :--- | :--- | :--- |
| `Emirati Population Variome` | EGA dataset title indicates `43,608` individuals and accession `EGAS00001009352`, but access is controlled through EGA rather than open bulk download | `HIGH VALUE / NOT USABLE NOW` |
| `Almena` | Public site is online today and states `26 million variants` integrated from `2115 unrelated individuals`; the site behaves as a browser/search resource, and no bulk public download was confirmed today | `MANUAL CROSS-CHECK ONLY` |
| `Qatar Genome Program / QPHI` | Still treated as controlled-access in prior project notes; no direct public bulk frequency download was validated today | `RESTRICTED / NOT USABLE NOW` |

## Duplicate / Overlap Rule
Do **not** combine counts across `GME`, `SHGP`, `AVDB`, `Almena`, or any future Arab datasets as if they were independent cohorts.

Until a formal overlap policy exists:
- keep each source separate
- compute per-source frequencies independently
- use `max_af_across_sources` or source-tagged comparison logic rather than summed counts

## Intake Order Recommended
1. `SHGP Saudi frequency table` -> onboard next as the main new Arab population-frequency source.
2. `AVDB UAE workbook` -> onboard later as a curated secondary layer after provenance/date hardening and build conversion planning.
3. Keep `UAE PMC12011969` and `Saudi PMC10474689` outside the frequency layer.
4. Monitor `Emirati Population Variome` and `QGP` for public access changes, but do not build the pipeline around them now.

## Next Exact Action
- Freeze `SHGP` into the project raw-vault with provenance metadata.
- Decide whether `AVDB` is strong enough to justify a dedicated `GRCh37 -> GRCh38` conversion path, or whether it should remain a secondary manual-reference source only.
