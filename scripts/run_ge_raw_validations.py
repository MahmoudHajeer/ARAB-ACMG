"""Run Great Expectations raw-layer checkpoints for T002.

This script builds (or updates) the ClinVar and gnomAD expectation suites,
creates validation definitions and checkpoints against live BigQuery query
assets, then runs the checkpoints and writes JSON summaries to `logs/`.
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from typing import Final

ROOT: Final[Path] = Path(__file__).resolve().parents[1]
SCRIPT_DIR: Final[Path] = Path(__file__).resolve().parent
GE_PROJECT_DIR: Final[Path] = ROOT / "great_expectations"
LOG_DIR: Final[Path] = ROOT / "logs"
CHECKPOINT_DIR: Final[Path] = GE_PROJECT_DIR / "checkpoints"

if str(ROOT) in sys.path:
    sys.path.remove(str(ROOT))
if str(SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(SCRIPT_DIR))

import great_expectations as gx  # type: ignore[import-not-found]
from great_expectations.checkpoint.checkpoint import Checkpoint
from great_expectations.core.expectation_suite import ExpectationSuite
from great_expectations.core.validation_definition import ValidationDefinition

from ge_expectation_specs import (
    CLINVAR_CHECKPOINT_NAME,
    CLINVAR_EXPECTATION_SUITE_NAME,
    CLINVAR_TABLE,
    GNOMAD_CHECKPOINT_NAME,
    GNOMAD_EXPECTATION_SUITE_NAME,
    GNOMAD_GE_TABLES,
    HARMONIZED_DATASOURCE_NAME,
    HARMONIZED_DATASET,
    RAW_DATASOURCE_NAME,
    build_clinvar_raw_query,
    clinvar_raw_expectations,
    gnomad_raw_expectations,
)

PROJECT_ID: Final[str] = "genome-services-platform"
RAW_DATASET: Final[str] = "arab_acmg_raw"


def get_context() -> gx.data_context.AbstractDataContext:
    return gx.get_context(project_root_dir=str(GE_PROJECT_DIR), mode="file")


def reset_raw_datasource(context: gx.data_context.AbstractDataContext):
    try:
        context.data_sources.delete(RAW_DATASOURCE_NAME)
    except Exception:
        pass

    return context.data_sources.add_or_update_bigquery(
        name=RAW_DATASOURCE_NAME,
        connection_string=f"bigquery://{PROJECT_ID}/{RAW_DATASET}",
    )


def reset_harmonized_datasource(context: gx.data_context.AbstractDataContext):
    try:
        context.data_sources.delete(HARMONIZED_DATASOURCE_NAME)
    except Exception:
        pass

    return context.data_sources.add_or_update_bigquery(
        name=HARMONIZED_DATASOURCE_NAME,
        connection_string=f"bigquery://{PROJECT_ID}/{HARMONIZED_DATASET}",
    )


def create_suite(context: gx.data_context.AbstractDataContext, name: str, expectations: list[dict], notes: str):
    suite = ExpectationSuite(
        name=name,
        expectations=expectations,
        notes=notes,
        meta={"great_expectations_version": gx.__version__},
    )
    return context.suites.add_or_update(suite)


def create_validation_definition(context, name: str, batch_definition, suite):
    validation = ValidationDefinition(name=name, data=batch_definition, suite=suite)
    return context.validation_definitions.add_or_update(validation)


def create_checkpoint(context, name: str, validation_definitions: list[ValidationDefinition]):
    checkpoint = Checkpoint(name=name, validation_definitions=validation_definitions)
    return context.checkpoints.add_or_update(checkpoint)


def persist_result(name: str, result) -> None:
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    target = LOG_DIR / f"{name}.json"
    target.write_text(result.describe(), encoding="utf-8")


def persist_checkpoint(name: str, checkpoint: Checkpoint) -> None:
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    target = CHECKPOINT_DIR / f"{name}.json"
    target.write_text(json.dumps(checkpoint.dict(), indent=2), encoding="utf-8")


def main() -> None:
    context = get_context()
    raw_datasource = reset_raw_datasource(context)
    harmonized_datasource = reset_harmonized_datasource(context)

    clinvar_asset = raw_datasource.add_query_asset(
        name="clinvar_raw_vcf_query",
        query=build_clinvar_raw_query(CLINVAR_TABLE),
    )
    clinvar_batch = clinvar_asset.add_batch_definition_whole_table("whole_table")

    gnomad_batches = {}
    for table_name in GNOMAD_GE_TABLES:
        asset = harmonized_datasource.add_table_asset(
            name=table_name,
            table_name=table_name,
        )
        gnomad_batches[table_name] = asset.add_batch_definition_whole_table("whole_table")

    clinvar_suite = create_suite(
        context=context,
        name=CLINVAR_EXPECTATION_SUITE_NAME,
        expectations=clinvar_raw_expectations(),
        notes="Baseline raw ClinVar QC over the frozen 2026-03-03 ClinVar snapshot.",
    )
    gnomad_suite = create_suite(
        context=context,
        name=GNOMAD_EXPECTATION_SUITE_NAME,
        expectations=gnomad_raw_expectations(),
        notes=(
            "Baseline gnomAD QC over raw-derived staging views backed by the frozen v4.1 chr13/chr17 raw tables. "
            "The checkpoint uses dbt materialized views because direct GX query-asset reflection on the largest raw tables exceeds BigQuery response-size limits."
        ),
    )

    clinvar_validation = create_validation_definition(
        context=context,
        name="clinvar_raw_validation",
        batch_definition=clinvar_batch,
        suite=clinvar_suite,
    )
    gnomad_validations = [
        create_validation_definition(
            context=context,
            name=f"{table_name}_validation",
            batch_definition=batch_definition,
            suite=gnomad_suite,
        )
        for table_name, batch_definition in gnomad_batches.items()
    ]

    clinvar_checkpoint = create_checkpoint(
        context=context,
        name=CLINVAR_CHECKPOINT_NAME,
        validation_definitions=[clinvar_validation],
    )
    gnomad_checkpoint = create_checkpoint(
        context=context,
        name=GNOMAD_CHECKPOINT_NAME,
        validation_definitions=gnomad_validations,
    )

    persist_checkpoint(CLINVAR_CHECKPOINT_NAME, clinvar_checkpoint)
    persist_checkpoint(GNOMAD_CHECKPOINT_NAME, gnomad_checkpoint)

    print(f"[ge] running {CLINVAR_CHECKPOINT_NAME}")
    clinvar_result = clinvar_checkpoint.run()
    print(f"[ge] {CLINVAR_CHECKPOINT_NAME} success={clinvar_result.success}")

    print(f"[ge] running {GNOMAD_CHECKPOINT_NAME}")
    gnomad_result = gnomad_checkpoint.run()
    print(f"[ge] {GNOMAD_CHECKPOINT_NAME} success={gnomad_result.success}")

    persist_result(CLINVAR_CHECKPOINT_NAME, clinvar_result)
    persist_result(GNOMAD_CHECKPOINT_NAME, gnomad_result)

    if os.getenv("GE_BUILD_DATA_DOCS", "").strip() == "1":
        try:
            context.build_data_docs(site_names=["local_site"])
        except Exception as exc:
            print(f"[ge] warning: build_data_docs failed: {exc}")

    if clinvar_result.success and gnomad_result.success:
        print("[ge] raw-layer checkpoints passed")
        return

    print("[ge] raw-layer checkpoints failed")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
