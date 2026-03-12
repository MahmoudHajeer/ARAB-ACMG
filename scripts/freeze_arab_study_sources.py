"""Freeze public Arab study source packages into GCS and emit de-identified extracts.

This pipeline keeps the raw workbook untouched in the private raw vault, then
creates a small variant-centric extract for downstream harmonization. It avoids
BigQuery entirely and keeps future Arab-source onboarding config-driven.
"""

from __future__ import annotations

import datetime as dt
import json
import re
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Final

import pandas as pd
from google.cloud import storage

try:
    from scripts.manifest_utility import ManifestGenerator
except ModuleNotFoundError:
    from manifest_utility import ManifestGenerator

PROJECT_ID: Final[str] = "genome-services-platform"
BUCKET_NAME: Final[str] = "mahmoud-arab-acmg-research-data"
RAW_ROOT: Final[Path] = Path("/Users/macbookpro/Desktop/storage/raw/arab_studies")
LOCAL_WORK_DIR: Final[Path] = Path("data/raw/arab_studies")


@dataclass(frozen=True)
class ExtractSpec:
    sheet_name: str
    output_slug: str
    keep_columns: tuple[str, ...]
    filter_column: str | None = None
    exclude_values: tuple[str, ...] = ()
    notes: str = ""


@dataclass(frozen=True)
class StudySource:
    slug: str
    source_version: str
    citation_title: str
    article_url: str
    upstream_url: str
    local_source: Path
    license_notes: str
    notes: str
    extracts: tuple[ExtractSpec, ...]

    def raw_vault_prefix(self, snapshot_date: str) -> str:
        return f"raw/sources/{self.slug}/version={self.source_version}/snapshot_date={snapshot_date}"

    def frozen_extract_prefix(self, snapshot_date: str) -> str:
        return (
            f"frozen/arab_variant_evidence/source={self.slug}/"
            f"version={self.source_version}/snapshot_date={snapshot_date}"
        )


STUDY_SOURCES: Final[tuple[StudySource, ...]] = (
    StudySource(
        slug="saudi_breast_cancer_pmc10474689",
        source_version="moesm1",
        citation_title="Interplay of Mendelian and polygenic risk factors in Arab breast cancer patients",
        article_url="https://pmc.ncbi.nlm.nih.gov/articles/PMC10474689/",
        upstream_url=(
            "https://pmc.ncbi.nlm.nih.gov/articles/instance/10474689/"
            "bin/13073_2023_1220_MOESM1_ESM.xls"
        ),
        local_source=RAW_ROOT / "saudi_breast_cancer_pmc10474689_moesm1.xls",
        license_notes=(
            "Open-access supplementary workbook from a PMC article; verify publisher/article reuse "
            "terms before redistributing derived data outside the research workspace."
        ),
        notes=(
            "Saudi Arab breast-cancer study workbook. Only Table S5 is extracted for downstream "
            "variant-centric work because the other sheets are PRS/performance summaries."
        ),
        extracts=(
            ExtractSpec(
                sheet_name="Table S5",
                output_slug="variant_carriers",
                keep_columns=(
                    "Gene",
                    "Pathogenic Variant Type",
                    "Clinvar Significance",
                    "Clinvar Submissions",
                    "HGVS Codon Change",
                    "HGVS Protein Change",
                ),
                notes="Carrier identifier removed to keep the extract variant-centric and de-identified.",
            ),
        ),
    ),
    StudySource(
        slug="uae_brca_pmc12011969",
        source_version="moesm1",
        citation_title="Genetic characterization of BRCA1 and BRCA2 variants in cancer and high-risk family screening cohorts in the UAE population",
        article_url="https://pmc.ncbi.nlm.nih.gov/articles/PMC12011969/",
        upstream_url=(
            "https://pmc.ncbi.nlm.nih.gov/articles/instance/12572518/"
            "bin/432_2025_6188_MOESM1_ESM.xlsx"
        ),
        local_source=RAW_ROOT / "uae_brca_pmc12011969_moesm1.xlsx",
        license_notes=(
            "Open-access supplementary workbook from a PMC article; raw workbook remains private in "
            "GCS because it includes patient-level columns."
        ),
        notes=(
            "UAE cohort workbook with mixed nationalities. Derived extracts keep mutation-positive "
            "rows only and drop direct identifiers for downstream harmonization."
        ),
        extracts=(
            ExtractSpec(
                sheet_name="Family Screening",
                output_slug="family_screening_variant_rows",
                keep_columns=(
                    "Nationality",
                    "Age at PDX",
                    "Gender",
                    "PDX",
                    "Clinical Indication",
                    "Subtype",
                    "Patient Status",
                    "Mutations",
                    "HGVS",
                    "Chr location (hg38)",
                    "Pathogenicity",
                    "Zygosity",
                    "Reason for Referal",
                    "Dual Dx (Breast and Ovarian)",
                    "Family History",
                    "ER/PR Status",
                    "HER2 Status",
                ),
                filter_column="Mutations",
                exclude_values=("Negative",),
                notes="Rows without a reported mutation are excluded from the de-identified extract.",
            ),
            ExtractSpec(
                sheet_name="Cancer Cohort",
                output_slug="cancer_cohort_variant_rows",
                keep_columns=(
                    "Nationality",
                    "Age at PDX",
                    "Gender",
                    "PDX",
                    "Clinical Indication",
                    "Subtype",
                    "Patient Status",
                    "Mutations",
                    "HGVS",
                    "Chr location (hg38)",
                    "Pathogenicity",
                    "Zygosity",
                    "Reason for Referal",
                    "Dual Dx (Breast and Ovarian)",
                    "Family History",
                    "ER/PR Status",
                    "HER2 Status",
                ),
                filter_column="Mutations",
                exclude_values=("Negative",),
                notes="Rows without a reported mutation are excluded from the de-identified extract.",
            ),
        ),
    ),
)


def slugify(value: str) -> str:
    """Convert free text into a stable snake_case fragment for file names."""
    value = re.sub(r"[^0-9A-Za-z]+", "_", value.strip().lower())
    return re.sub(r"_+", "_", value).strip("_")


def normalize_headers(frame: pd.DataFrame) -> pd.DataFrame:
    """Trim sheet headers so workbook quirks do not break config-based extraction."""
    normalized = frame.copy()
    normalized.columns = [re.sub(r"\s+", " ", str(column)).strip() for column in normalized.columns]
    return normalized


def snake_case_columns(columns: list[str]) -> list[str]:
    """Convert display headers into stable machine-friendly extract columns."""
    return [slugify(column) for column in columns]


def apply_extract_spec(frame: pd.DataFrame, spec: ExtractSpec) -> pd.DataFrame:
    """Create one de-identified, variant-centric extract from a raw sheet."""
    trimmed = normalize_headers(frame).reset_index(drop=True)

    # [AI-Agent: Codex]: Preserve a stable locator back to the raw workbook while removing direct identifiers.
    trimmed["source_sheet_name"] = spec.sheet_name
    trimmed["source_row_number"] = trimmed.index + 2
    trimmed["source_record_locator"] = trimmed["source_row_number"].map(
        lambda row_number: f"sheet={spec.sheet_name};row={row_number}"
    )

    if spec.filter_column:
        filter_column = spec.filter_column.strip()
        values = trimmed[filter_column].fillna("").astype(str).str.strip()
        excluded = {value.strip().lower() for value in spec.exclude_values}
        keep_mask = values.ne("") & ~values.str.lower().isin(excluded)
        trimmed = trimmed.loc[keep_mask].copy()

    keep_columns = list(spec.keep_columns) + [
        "source_sheet_name",
        "source_row_number",
        "source_record_locator",
    ]
    missing = [column for column in spec.keep_columns if column not in trimmed.columns]
    if missing:
        raise KeyError(f"missing expected columns for {spec.sheet_name}: {missing}")

    extracted = trimmed.loc[:, keep_columns].copy()
    extracted.columns = snake_case_columns(list(extracted.columns))
    return extracted


def upload_file(storage_client: storage.Client, local_path: Path, gcs_path: str, *, content_type: str) -> str:
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(str(local_path), content_type=content_type)
    return f"gs://{BUCKET_NAME}/{gcs_path}"


def upload_text(storage_client: storage.Client, content: str, gcs_path: str, *, content_type: str) -> str:
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(gcs_path)
    blob.upload_from_string(content, content_type=content_type)
    return f"gs://{BUCKET_NAME}/{gcs_path}"


def build_raw_manifest(source: StudySource, snapshot_date: str) -> str:
    gcs_uri = f"gs://{BUCKET_NAME}/{source.raw_vault_prefix(snapshot_date)}/{source.local_source.name}"
    return ManifestGenerator.create_manifest(
        source=source.slug,
        source_version=source.source_version,
        upstream_url=source.upstream_url,
        local_file_path=str(source.local_source),
        gcs_uri=gcs_uri,
        row_count=-1,
        license_notes=source.license_notes,
        notes=f"title={source.citation_title}; article_url={source.article_url}; {source.notes}",
    )


def build_extract_manifest(
    source: StudySource,
    spec: ExtractSpec,
    snapshot_date: str,
    row_count: int,
    columns: list[str],
    csv_uri: str,
    parquet_uri: str,
) -> str:
    payload = {
        "source": source.slug,
        "source_version": source.source_version,
        "snapshot_date": snapshot_date,
        "citation_title": source.citation_title,
        "article_url": source.article_url,
        "upstream_url": source.upstream_url,
        "sheet_name": spec.sheet_name,
        "extract_slug": spec.output_slug,
        "row_count": row_count,
        "columns": columns,
        "csv_uri": csv_uri,
        "parquet_uri": parquet_uri,
        "notes": spec.notes,
    }
    return json.dumps(payload, indent=2, ensure_ascii=True) + "\n"


def run_source_pipeline(storage_client: storage.Client, source: StudySource, snapshot_date: str) -> dict[str, object]:
    if not source.local_source.exists():
        raise FileNotFoundError(f"missing local source package: {source.local_source}")

    LOCAL_WORK_DIR.mkdir(parents=True, exist_ok=True)
    summary: dict[str, object] = {
        "source": source.slug,
        "source_version": source.source_version,
        "snapshot_date": snapshot_date,
        "article_url": source.article_url,
        "citation_title": source.citation_title,
        "raw_gcs_prefix": f"gs://{BUCKET_NAME}/{source.raw_vault_prefix(snapshot_date)}/",
        "extracts": [],
    }

    with tempfile.TemporaryDirectory(prefix=f"{source.slug}-") as temp_dir:
        temp_path = Path(temp_dir)

        raw_manifest_path = temp_path / "manifest.json"
        raw_manifest_path.write_text(build_raw_manifest(source, snapshot_date), encoding="utf-8")

        raw_gcs_uri = upload_file(
            storage_client,
            source.local_source,
            f"{source.raw_vault_prefix(snapshot_date)}/{source.local_source.name}",
            content_type=(
                "application/vnd.ms-excel"
                if source.local_source.suffix.lower() == ".xls"
                else "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            ),
        )
        upload_file(
            storage_client,
            raw_manifest_path,
            f"{source.raw_vault_prefix(snapshot_date)}/manifest.json",
            content_type="application/json",
        )

        for spec in source.extracts:
            # [AI-Agent: Codex]: Keep extraction logic linear and explicit so every retained column is reviewable.
            raw_frame = pd.read_excel(source.local_source, sheet_name=spec.sheet_name)
            extracted = apply_extract_spec(raw_frame, spec)

            csv_name = f"{spec.output_slug}.csv"
            parquet_name = f"{spec.output_slug}.parquet"
            csv_path = temp_path / csv_name
            parquet_path = temp_path / parquet_name
            manifest_name = f"{spec.output_slug}.manifest.json"
            manifest_path = temp_path / manifest_name

            extracted.to_csv(csv_path, index=False)
            extracted.to_parquet(parquet_path, index=False)

            extract_prefix = source.frozen_extract_prefix(snapshot_date)
            csv_uri = upload_file(
                storage_client,
                csv_path,
                f"{extract_prefix}/{csv_name}",
                content_type="text/csv",
            )
            parquet_uri = upload_file(
                storage_client,
                parquet_path,
                f"{extract_prefix}/{parquet_name}",
                content_type="application/octet-stream",
            )
            manifest_path.write_text(
                build_extract_manifest(
                    source,
                    spec,
                    snapshot_date,
                    row_count=int(len(extracted)),
                    columns=list(extracted.columns),
                    csv_uri=csv_uri,
                    parquet_uri=parquet_uri,
                ),
                encoding="utf-8",
            )
            manifest_uri = upload_file(
                storage_client,
                manifest_path,
                f"{extract_prefix}/{manifest_name}",
                content_type="application/json",
            )

            summary["extracts"].append(
                {
                    "sheet_name": spec.sheet_name,
                    "extract_slug": spec.output_slug,
                    "row_count": int(len(extracted)),
                    "csv_uri": csv_uri,
                    "parquet_uri": parquet_uri,
                    "manifest_uri": manifest_uri,
                    "columns": list(extracted.columns),
                }
            )

        summary["raw_workbook_uri"] = raw_gcs_uri
    return summary


def main() -> None:
    snapshot_date = dt.date.today().isoformat()
    storage_client = storage.Client(project=PROJECT_ID)

    # [AI-Agent: Codex]: Process sources one by one so failures are easy to attribute and resume.
    summaries = [run_source_pipeline(storage_client, source, snapshot_date) for source in STUDY_SOURCES]

    report = {
        "generated_at": dt.datetime.now(dt.UTC).isoformat(),
        "snapshot_date": snapshot_date,
        "sources": summaries,
    }
    report_text = json.dumps(report, indent=2, ensure_ascii=True) + "\n"
    report_uri = upload_text(
        storage_client,
        report_text,
        f"frozen/arab_variant_evidence/snapshot_date={snapshot_date}/intake_report.json",
        content_type="application/json",
    )
    print(report_text, end="")
    print(f"report_uri={report_uri}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"freeze_arab_study_sources_failed={exc}", file=sys.stderr)
        sys.exit(1)
