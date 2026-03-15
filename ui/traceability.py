from __future__ import annotations

from copy import deepcopy
from typing import Any


def _trace_card(*, input_surface: str, operation: str, count_basis: str, display_basis: str) -> dict[str, str]:
    return {
        "input_surface": input_surface,
        "operation": operation,
        "count_basis": count_basis,
        "display_basis": display_basis,
    }


def _raw_dataset_trace(entry: dict[str, Any], frozen_at: str, bundle_uri: str) -> dict[str, str]:
    return _trace_card(
        input_surface=str(entry.get("table_ref", "raw table ref not recorded")),
        operation="No transformation. This card shows the frozen raw source package or BRCA-window raw extract before any normalization.",
        count_basis=f"Frozen raw-source row count or BRCA-window raw count captured in the review bundle on {frozen_at}.",
        display_basis=f"Displayed from {bundle_uri}; the 10-row sample is the frozen sample stored in the bundle.",
    )


def _checkpoint_dataset_trace(entry: dict[str, Any], frozen_at: str, bundle_uri: str) -> dict[str, str]:
    storage_ref = str(entry.get("storage_ref") or entry.get("table_ref") or "checkpoint artifact not recorded")
    historical_ref = entry.get("table_ref") if entry.get("storage_ref") else None
    historical_note = (
        f" Historical build reference: {historical_ref}."
        if historical_ref and historical_ref != storage_ref
        else ""
    )
    return _trace_card(
        input_surface=storage_ref,
        operation=f"Frozen artifact only. The card shows a persisted normalized source artifact or checkpoint instead of rerunning the build.{historical_note}",
        count_basis=f"Frozen artifact row count captured in the review bundle on {frozen_at}.",
        display_basis=f"Displayed from {bundle_uri}; the 10-row sample comes from the frozen artifact sample stored in the bundle.",
    )


def _legacy_pre_gme_trace(payload: dict[str, Any], frozen_at: str, bundle_uri: str) -> dict[str, str]:
    return _trace_card(
        input_surface=str(payload.get("table_ref", "legacy pre-GME artifact not recorded")),
        operation="Historical baseline checkpoint preserved from the legacy frozen review bundle. No Arab-extension source is added at this stage.",
        count_basis=f"Frozen legacy pre-GME row count captured on {frozen_at}.",
        display_basis=f"Displayed from {bundle_uri}; preview rows come from the frozen legacy pre-GME sample.",
    )


def _legacy_registry_trace(payload: dict[str, Any], frozen_at: str, bundle_uri: str) -> dict[str, str]:
    return _trace_card(
        input_surface=str(payload.get("table_ref", "legacy final artifact not recorded")),
        operation="Historical final BRCA checkpoint preserved as the baseline review surface before the Arab extension work.",
        count_basis=f"Frozen legacy final-checkpoint row count captured on {frozen_at}.",
        display_basis=f"Displayed from {bundle_uri}; preview rows come from the frozen legacy final sample and the downloadable CSV is served from GCS.",
    )


def _arab_pre_gme_trace(payload: dict[str, Any], frozen_at: str, bundle_uri: str) -> dict[str, str]:
    return _trace_card(
        input_surface="Normalized ClinVar + normalized gnomAD genomes + normalized gnomAD exomes + normalized SHGP",
        operation="Join canonical variant keys from the normalized source artifacts and materialize the mandated publication-facing header before any GME layer is added.",
        count_basis=f"Frozen Arab pre-GME checkpoint row count captured on {frozen_at}.",
        display_basis=f"Displayed from {bundle_uri}; preview rows come from the frozen Arab pre-GME checkpoint sample.",
    )


def _arab_registry_trace(payload: dict[str, Any], frozen_at: str, bundle_uri: str) -> dict[str, str]:
    return _trace_card(
        input_surface="Pre-GME Arab checkpoint + normalized GME",
        operation="Preserve the baseline-compatible checkpoint surface, then add SHGP/GME-derived Arab extras without dropping earlier legacy review columns.",
        count_basis=f"Frozen Arab final-checkpoint row count captured on {frozen_at}.",
        display_basis=f"Displayed from {bundle_uri}; preview rows come from the frozen Arab final checkpoint sample and the downloadable CSV is served from GCS.",
    )


def _step_trace(step: dict[str, Any], frozen_at: str, bundle_uri: str) -> dict[str, str]:
    source_hint = {
        "clinvar_normalized_brca": "ClinVar BRCA normalized artifact",
        "gnomad_genomes_normalized_brca": "gnomAD genomes BRCA normalized artifact",
        "gnomad_exomes_normalized_brca": "gnomAD exomes BRCA normalized artifact",
        "shgp_normalized_brca": "SHGP BRCA normalized artifact",
        "pre_gme_checkpoint": "Pre-GME checkpoint artifact",
        "gme_normalized_brca": "GME BRCA normalized artifact",
        "final_checkpoint": "Final checkpoint artifact",
    }.get(str(step.get("id")), "Frozen checkpoint artifact")
    return _trace_card(
        input_surface=source_hint,
        operation=str(step.get("technical") or step.get("simple") or "Frozen workflow step"),
        count_basis=f"This step card has no independent metric; it shows a frozen 10-row evidence sample tied to the approved bundle from {frozen_at}.",
        display_basis=f"Displayed from {bundle_uri}; sample SQL is shown only as provenance for the frozen preview.",
    )


def enrich_review_bundle_trace(bundle: dict[str, Any]) -> dict[str, Any]:
    enriched = deepcopy(bundle)
    frozen_at = str(enriched.get("frozen_at", "not recorded"))
    bundle_uri = str(enriched.get("artifacts", {}).get("bundle_uri", "ui/review_bundle.json"))

    for entry in enriched.get("raw_datasets", {}).get("datasets", []):
        entry["trace"] = _raw_dataset_trace(entry, frozen_at, bundle_uri)

    for entry in enriched.get("datasets", {}).get("datasets", []):
        entry["trace"] = _checkpoint_dataset_trace(entry, frozen_at, bundle_uri)

    if "pre_gme" in enriched:
        enriched["pre_gme"]["trace"] = _legacy_pre_gme_trace(enriched["pre_gme"], frozen_at, bundle_uri)

    if "registry" in enriched:
        enriched["registry"]["trace"] = _legacy_registry_trace(enriched["registry"], frozen_at, bundle_uri)

    if "arab_pre_gme" in enriched:
        enriched["arab_pre_gme"]["trace"] = _arab_pre_gme_trace(enriched["arab_pre_gme"], frozen_at, bundle_uri)

    if "arab_registry" in enriched:
        enriched["arab_registry"]["trace"] = _arab_registry_trace(enriched["arab_registry"], frozen_at, bundle_uri)

    for group_name in ("harmonization_steps", "final_steps", "arab_extension_steps", "legacy_final_steps"):
        for step in enriched.get("workflow", {}).get(group_name, []):
            step["trace"] = _step_trace(step, frozen_at, bundle_uri)

    return enriched


def _source_count_basis(source: dict[str, Any]) -> str:
    source_key = str(source.get("source_key"))
    snapshot_date = str(source.get("snapshot_date", "not recorded"))
    if source_key in {"clinvar", "gnomad_genomes", "gnomad_exomes", "gme_hg38"}:
        return f"Row count comes from the frozen raw/checkpoint snapshot captured on {snapshot_date}."
    if source_key == "avdb_uae":
        return "Row count comes from the frozen AVDB workbook and liftover report."
    return "Row count comes from the frozen source manifest or de-identified extract manifest."


def enrich_source_review_trace(payload: dict[str, Any]) -> dict[str, Any]:
    enriched = deepcopy(payload)
    generated_at = str(enriched.get("generated_at", "not recorded"))

    for source in enriched.get("sources", []):
        raw_path = str(source.get("raw_vault_prefix") or source.get("upstream_url") or "source path not recorded")
        workflow = source.get("workflow_position", {})
        source["trace"] = _trace_card(
            input_surface=raw_path,
            operation=f"{workflow.get('raw_stage', 'No raw-stage note')}"
            f" Then: {workflow.get('brca_stage', 'No BRCA-stage note')}",
            count_basis=_source_count_basis(source),
            display_basis=f"Displayed from ui/source_review.json generated at {generated_at}; the evidence sample below is frozen and not queried live.",
        )

    return enriched
