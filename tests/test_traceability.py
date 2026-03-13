from ui.traceability import enrich_review_bundle_trace, enrich_source_review_trace


def test_enrich_review_bundle_trace_adds_trace_cards():
    bundle = {
        "frozen_at": "2026-03-12",
        "artifacts": {"bundle_uri": "gs://bucket/review_bundle.json"},
        "raw_datasets": {
            "datasets": [
                {
                    "key": "clinvar_raw_vcf",
                    "table_ref": "project.dataset.clinvar_raw_vcf",
                    "row_count": 10,
                }
            ]
        },
        "datasets": {
            "datasets": [
                {
                    "key": "pre_gme_registry",
                    "table_ref": "project.dataset.pre_gme",
                    "storage_ref": "gs://bucket/pre_gme.parquet",
                }
            ]
        },
        "pre_gme": {"title": "pre_gme"},
        "registry": {"title": "registry"},
        "workflow": {
            "harmonization_steps": [{"id": "clinvar_normalized_brca", "simple": "simple", "technical": "technical"}],
            "final_steps": [{"id": "final_checkpoint", "simple": "simple", "technical": "technical"}],
        },
    }

    enriched = enrich_review_bundle_trace(bundle)

    assert enriched["raw_datasets"]["datasets"][0]["trace"]["input_surface"] == "project.dataset.clinvar_raw_vcf"
    assert "Frozen raw-source row count" in enriched["raw_datasets"]["datasets"][0]["trace"]["count_basis"]
    assert enriched["datasets"]["datasets"][0]["trace"]["input_surface"] == "gs://bucket/pre_gme.parquet"
    assert "Pre-GME Arab checkpoint" in enriched["registry"]["trace"]["input_surface"]
    assert "sample SQL" in enriched["workflow"]["harmonization_steps"][0]["trace"]["display_basis"]


def test_enrich_source_review_trace_adds_display_and_count_basis():
    payload = {
        "generated_at": "2026-03-13T10:00:00+00:00",
        "sources": [
            {
                "source_key": "shgp_saudi_af",
                "raw_vault_prefix": "gs://bucket/raw/sources/shgp/",
                "snapshot_date": "2026-03-13",
                "workflow_position": {
                    "raw_stage": "Frozen raw package",
                    "brca_stage": "Ready for BRCA normalization",
                    "final_stage": "Not yet in final",
                },
            }
        ],
    }

    enriched = enrich_source_review_trace(payload)

    trace = enriched["sources"][0]["trace"]
    assert trace["input_surface"] == "gs://bucket/raw/sources/shgp/"
    assert "Ready for BRCA normalization" in trace["operation"]
    assert "manifest" in trace["count_basis"].lower() or "snapshot" in trace["count_basis"].lower()
    assert "ui/source_review.json" in trace["display_basis"]
