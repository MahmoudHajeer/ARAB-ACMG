from fastapi.testclient import TestClient

from ui import service
from ui.service import app


client = TestClient(app)


def sample_bundle():
    return {
        "workflow": {
            "pages": [{"id": "overview", "title": "Overview", "summary": "Summary"}],
            "harmonization_steps": [{"id": "clinvar_raw_brca", "title": "Step 1", "simple": "Simple", "technical": "Tech"}],
            "final_steps": [{"id": "final_checkpoint", "title": "Step 6", "simple": "Simple", "technical": "Tech"}],
        },
        "raw_datasets": {
            "datasets": [
                {
                    "key": "clinvar_raw_vcf",
                    "title": "ClinVar raw VCF table",
                    "table_ref": "genome-services-platform.arab_acmg_raw.clinvar_raw_vcf",
                    "row_count": 100,
                    "sample": {
                        "columns": ["sample_row_number", "chrom"],
                        "rows": [{"sample_row_number": 1, "chrom": "17"}],
                        "query_sql": "SELECT * FROM raw_sample",
                    },
                }
            ]
        },
        "datasets": {
            "datasets": [
                {
                    "key": "pre_gme_registry",
                    "title": "Pre-GME unified checkpoint",
                    "table_ref": "gs://bucket/pre_gme.parquet",
                    "row_count": 77,
                    "sample": {
                        "columns": ["sample_row_number", "GENE"],
                        "rows": [{"sample_row_number": 1, "GENE": "BRCA1"}],
                        "query_sql": "SELECT * FROM pre_gme_sample",
                    },
                }
            ]
        },
        "pre_gme": {
            "title": "supervisor_variant_registry_brca_pre_gme_v1",
            "table_ref": "gs://bucket/pre_gme.parquet",
            "row_count": 77,
            "columns": [{"name": "CHROM", "kind": "required", "description": "chromosome"}],
            "sample": {
                "columns": ["sample_row_number", "GENE"],
                "rows": [{"sample_row_number": 1, "GENE": "BRCA1"}],
                "query_sql": "SELECT * FROM pre_gme_sample",
            },
        },
        "registry": {
            "title": "supervisor_variant_registry_brca_v1",
            "table_ref": "gs://bucket/final.parquet",
            "row_count": 123,
            "columns": [{"name": "GME_AF", "kind": "extra", "description": "gme"}],
            "csv_download_url": "https://storage.googleapis.com/example/final.csv",
            "sample": {
                "columns": ["sample_row_number", "GENE"],
                "rows": [{"sample_row_number": 1, "GENE": "BRCA2"}],
                "query_sql": "SELECT * FROM final_sample",
            },
        },
        "step_samples": {
            "clinvar_raw_brca": {
                "columns": ["sample_row_number", "GENE"],
                "rows": [{"sample_row_number": 1, "GENE": "BRCA1"}],
                "query_sql": "SELECT * FROM step_sample",
            }
        },
    }


def sample_source_review():
    return {
        "generated_at": "2026-03-13T08:00:00+00:00",
        "decision_summary": [
            {
                "tier": "adopted_100",
                "label": "Adopted 100%",
                "summary": "Core input",
                "count": 1,
                "members": ["ClinVar GRCh38 VCF"],
            }
        ],
        "workflow_categories": [
            {
                "id": "raw_freeze",
                "title": "Stage 1",
                "purpose": "Freeze raw source package",
                "evidence_types": ["manifest checksum"],
                "output": "raw/sources/...",
            }
        ],
        "sources": [
            {
                "source_key": "clinvar",
                "display_name": "ClinVar GRCh38 VCF",
                "category": "Global clinical classification anchor",
                "source_kind": "VCF",
                "source_build": "GRCh38",
                "coordinate_readiness": "Genomic coordinates ready",
                "liftover_decision": "not_needed",
                "normalization_decision": "normalize VCF",
                "brca_relevance": "Direct",
                "review_status": "ready",
                "project_fit": "adopted_100",
                "project_fit_note": "Primary clinical truth source.",
                "use_tier": "adopted_100",
                "use_tier_label": "Adopted 100%",
                "use_tier_summary": "Core input",
                "snapshot_date": "2026-03-03",
                "source_version": "lastmod-20260302",
                "upstream_url": "https://example.org/clinvar.vcf.gz",
                "raw_vault_prefix": "gs://bucket/raw/sources/clinvar/",
                "raw_manifest_uri": "gs://bucket/raw/sources/clinvar/manifest.json",
                "row_count": 123,
                "notes": ["Evidence note"],
                "artifact_links": [{"label": "Raw manifest", "url": "gs://bucket/raw/sources/clinvar/manifest.json"}],
                "workflow_position": {
                    "raw_stage": "Raw page sample",
                    "brca_stage": "Direct BRCA extraction",
                    "final_stage": "Included in final checkpoint",
                    "included_in_current_final": True,
                },
                "next_action": "Normalize alleles",
                "sample": {
                    "columns": ["chrom", "pos"],
                    "rows": [{"chrom": "17", "pos": 43044295}],
                },
            }
        ],
    }


def sample_controlled_access():
    return {
        "generated_at": "2026-03-13T10:00:00+00:00",
        "scope_note": "Controlled datasets still need approval.",
        "decision_note": "Priority favors scale and coordinate readiness.",
        "process_guides": [
            {
                "key": "ega",
                "title": "EGA controlled-access workflow",
                "official_links": [{"label": "EGA register", "url": "https://ega-archive.org/register/"}],
                "steps": ["Create account", "Request access"],
                "source_note": "Official EGA guide",
            }
        ],
        "sources": [
            {
                "key": "emirati_population_variome",
                "display_name": "Emirati Population Variome",
                "country_or_region": "UAE",
                "priority": "priority_1",
                "access_model": "controlled_access",
                "process_guide": "ega",
                "data_scope": "GRCh38 WGS",
                "why_we_need_it": "Large Arab AF source",
                "official_release_evidence": "Published 2025-07-18",
                "build_or_coordinate_note": "GRCh38 public metadata",
                "official_links": [{"label": "Study", "url": "https://ega-archive.org/studies/EGAS50000001071/"}],
                "access_steps": ["Register", "Request access"],
                "practical_decision": "Priority dataset",
            }
        ],
        "browse_only_sources": [
            {
                "display_name": "Almena",
                "status": "browse_only",
                "summary": "Manual cross-check only",
                "url": "https://clingen.igib.res.in/almena/",
            }
        ],
    }


def test_index_route_disables_cache():
    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"


def test_health_route_returns_ok():
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_workflow_route_returns_page_navigation(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/workflow")

    assert response.status_code == 200
    payload = response.json()
    assert payload["pages"][0]["id"] == "overview"


def test_source_review_route_returns_frozen_payload(monkeypatch):
    monkeypatch.setattr(service, "load_source_review_payload", sample_source_review)

    response = client.get("/api/source-review")

    assert response.status_code == 200
    payload = response.json()
    assert payload["workflow_categories"][0]["id"] == "raw_freeze"
    assert payload["sources"][0]["display_name"] == "ClinVar GRCh38 VCF"


def test_controlled_access_route_returns_frozen_payload(monkeypatch):
    monkeypatch.setattr(service, "load_controlled_access_payload", sample_controlled_access)

    response = client.get("/api/controlled-access")

    assert response.status_code == 200
    payload = response.json()
    assert payload["process_guides"][0]["key"] == "ega"
    assert payload["sources"][0]["display_name"] == "Emirati Population Variome"


def test_overview_route_returns_payload(monkeypatch):
    monkeypatch.setattr(
        service,
        "load_bundled_or_live_overview_payload",
        lambda: {
            "generated_at": "2026-03-12T10:00:00+00:00",
            "tracks": [{"track_id": "T002", "name": "Data", "status_label": "in_progress"}],
            "plan_progress": {"T002": {"progress_pct": 50.0}},
            "track_status_counts": {"done": 1, "in_progress": 1, "not_started": 0},
            "latest_t002_verification": [{"command": "pytest", "status": "pass"}],
            "last_successful_step": "T002 step 5.7 frozen",
        },
    )

    response = client.get("/api/overview")

    assert response.status_code == 200
    payload = response.json()
    assert payload["track_status_counts"]["in_progress"] == 1
    assert payload["last_successful_step"] == "T002 step 5.7 frozen"


def test_raw_datasets_route_returns_frozen_catalog(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/raw-datasets")

    assert response.status_code == 200
    payload = response.json()
    assert payload["datasets"][0]["key"] == "clinvar_raw_vcf"


def test_raw_dataset_sample_route_returns_frozen_payload(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/raw-datasets/clinvar_raw_vcf/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["title"] == "ClinVar raw VCF table"
    assert payload["rows"][0]["chrom"] == "17"


def test_dataset_sample_route_returns_frozen_payload(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/datasets/pre_gme_registry/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["title"] == "Pre-GME unified checkpoint"
    assert payload["rows"][0]["GENE"] == "BRCA1"


def test_pre_gme_metadata_route_returns_frozen_payload(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/pre-gme")

    assert response.status_code == 200
    payload = response.json()
    assert payload["row_count"] == 77
    assert payload["table_ref"] == "gs://bucket/pre_gme.parquet"


def test_pre_gme_sample_route_returns_frozen_payload(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/pre-gme/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"][0]["GENE"] == "BRCA1"


def test_registry_metadata_route_returns_frozen_payload(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/registry")

    assert response.status_code == 200
    payload = response.json()
    assert payload["row_count"] == 123
    assert payload["csv_download_url"].endswith("final.csv")


def test_registry_sample_route_returns_frozen_payload(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/registry/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"][0]["GENE"] == "BRCA2"


def test_registry_step_sample_route_returns_frozen_payload(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/registry/steps/clinvar_raw_brca/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["step_id"] == "clinvar_raw_brca"
    assert payload["rows"][0]["GENE"] == "BRCA1"


def test_registry_download_route_redirects_to_static_csv(monkeypatch):
    monkeypatch.setattr(service, "review_bundle", sample_bundle)

    response = client.get("/api/registry/download.csv", follow_redirects=False)

    assert response.status_code == 307
    assert response.headers["location"] == "https://storage.googleapis.com/example/final.csv"


def test_removed_raw_download_route_is_not_exposed():
    response = client.get("/api/raw-datasets/clinvar_raw_vcf/download.csv")

    assert response.status_code == 404
