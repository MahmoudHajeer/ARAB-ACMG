# GitHub-to-Google AI Studio Handoff

## Scope
This repo is now prepared as a GitHub-first research workspace:
- frozen source-of-truth artifacts live in GCS
- the supervisor UI reads frozen JSON and public GCS downloads only
- no BigQuery runtime dependency is required for review/download flows

## Public vs private storage policy
- Public anonymous download is allowed only for safe raw public-source packages and frozen derived artifacts shown in the supervisor download center.
- Private study workbooks remain private in GCS:
  - `raw/sources/saudi_breast_cancer_pmc10474689/...`
  - `raw/sources/uae_brca_pmc12011969/...`
- Public review users should consume:
  - `ui/review_bundle.json`
  - the frozen CSV/Parquet/report objects listed in the download center
  - de-identified study extracts only

## Why this is GitHub-first
According to the official Google AI Studio docs:
- AI Studio is the fastest way to start building with Gemini: `https://ai.google.dev/aistudio/`
- Build mode supports developing externally and pushing to GitHub: `https://ai.google.dev/gemini-api/docs/aistudio-build-mode`
- The same Build mode docs also state that locally developed apps cannot currently be imported back into AI Studio.

Practical implication:
- keep this repo as the source of truth in GitHub
- run the Python/GCS pipeline in a cloud workspace that can clone the repo
- use AI Studio for Gemini experimentation or companion app work, not as the only execution environment for this research pipeline

## Minimal cloud setup
From a fresh cloud workspace after cloning the GitHub repo:

```bash
conda env create -f environment.yml
conda activate arab-acmg

export GOOGLE_CLOUD_PROJECT=genome-services-platform
export ARAB_ACMG_BUCKET=mahmoud-arab-acmg-research-data
```

If you need to touch private GCS objects or republish artifacts:

```bash
gcloud auth application-default login
gcloud config set project genome-services-platform
```

If you only need review/download access:
- no GCS authentication is required for the public-safe objects in the supervisor bundle

## Active commands
Rebuild the current frozen BRCA artifacts:

```bash
python3 scripts/build_brca_normalized_artifacts.py
python3 scripts/verify_brca_normalized_artifacts.py
```

Refresh the scientific/static UI state:

```bash
python3 scripts/update_source_review_state.py
python3 scripts/refresh_supervisor_review_bundle.py
python3 scripts/sync_public_gcs_downloads.py
```

Run the local supervisor UI:

```bash
python3 -m pip install -r ui/requirements.txt
uvicorn ui.service:app --host 0.0.0.0 --port 8080
```

## What the next environment needs
- Python + Conda
- `bcftools`
- `curl`
- `gcloud` + `gsutil`
- access to the local raw source mirrors only if you intend to rebuild raw freezes

## Safe default workflow
1. Clone from GitHub.
2. Read frozen evidence from `ui/review_bundle.json`.
3. Use public GCS artifacts for review and downloads.
4. Authenticate only when maintaining private raw sources or republishing artifacts.
