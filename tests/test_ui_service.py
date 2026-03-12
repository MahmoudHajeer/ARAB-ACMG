from fastapi.testclient import TestClient

from ui.service import app


client = TestClient(app)


def test_index_route_disables_cache():
    response = client.get("/")

    assert response.status_code == 200
    assert response.headers["cache-control"] == "no-store"


def test_health_route_returns_ok():
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_workflow_route_returns_page_navigation():
    response = client.get("/api/workflow")

    assert response.status_code == 200
    payload = response.json()
    assert any(page["id"] == "pre-gme" for page in payload["pages"])


def test_overview_route_returns_live_payload(monkeypatch):
    from ui import service

    monkeypatch.setattr(
        service,
        "load_overview_payload",
        lambda: {
            "generated_at": "2026-03-12T10:00:00+00:00",
            "tracks": [{"track_id": "T002", "name": "Data", "status_label": "in_progress"}],
            "plan_progress": {"T002": {"progress_pct": 50.0}},
            "track_status_counts": {"done": 1, "in_progress": 1, "not_started": 0},
            "latest_t002_verification": [{"command": "pytest", "status": "pass"}],
            "last_successful_step": "T002 step 5.5 finalized",
        },
    )

    response = client.get("/api/overview")

    assert response.status_code == 200
    payload = response.json()
    assert payload["track_status_counts"]["in_progress"] == 1
    assert payload["last_successful_step"] == "T002 step 5.5 finalized"
    assert payload["latest_t002_verification"][0]["status"] == "pass"


def test_raw_datasets_route_returns_catalog():
    response = client.get("/api/raw-datasets")

    assert response.status_code == 200
    payload = response.json()
    assert any(entry["key"] == "clinvar_raw_vcf" for entry in payload["datasets"])


def test_datasets_route_returns_checkpoint_catalog():
    response = client.get("/api/datasets")

    assert response.status_code == 200
    payload = response.json()
    assert any(entry["key"] == "pre_gme_registry" for entry in payload["datasets"])


def test_raw_dataset_sample_route_uses_mocked_query(monkeypatch):
    from ui import service

    monkeypatch.setattr(
        service,
        "run_query",
        lambda sql: {"columns": ["sample_row_number", "chrom"], "rows": [{"sample_row_number": 1, "chrom": "17"}]},
    )

    response = client.get("/api/raw-datasets/clinvar_raw_vcf/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["title"] == "ClinVar raw VCF table"
    assert payload["rows"][0]["chrom"] == "17"
    assert "TABLESAMPLE SYSTEM" in payload["query_sql"]


def test_dataset_sample_route_uses_mocked_query(monkeypatch):
    from ui import service

    monkeypatch.setattr(
        service,
        "run_query",
        lambda sql: {"columns": ["sample_row_number", "CHROM"], "rows": [{"sample_row_number": 1, "CHROM": "chr13"}]},
    )

    response = client.get("/api/datasets/pre_gme_registry/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["title"] == "Pre-GME unified checkpoint"
    assert payload["rows"][0]["CHROM"] == "chr13"
    assert "supervisor_variant_registry_brca_pre_gme_v1" in payload["query_sql"]


def test_pre_gme_metadata_uses_mocked_row_count(monkeypatch):
    from ui import service

    monkeypatch.setattr(service, "pre_gme_row_count", lambda: 77)
    monkeypatch.setattr(service, "pre_gme_metrics", lambda: {"gene_windows": [], "source_row_counts": []})

    response = client.get("/api/pre-gme")

    assert response.status_code == 200
    payload = response.json()
    assert payload["row_count"] == 77
    assert payload["title"] == "supervisor_variant_registry_brca_pre_gme_v1"
    assert payload["download_url"] == "/api/exports/pre-gme.xlsx"
    assert payload["csv_download_url"] == "/api/pre-gme/download.csv"
    assert payload["columns"][0]["name"] == "CHROM"


def test_pre_gme_sample_route_uses_mocked_query(monkeypatch):
    from ui import service

    monkeypatch.setattr(
        service,
        "run_query",
        lambda sql: {"columns": ["sample_row_number", "GENE"], "rows": [{"sample_row_number": 1, "GENE": "BRCA1"}]},
    )

    response = client.get("/api/pre-gme/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"][0]["GENE"] == "BRCA1"
    assert "supervisor_variant_registry_brca_pre_gme_v1" in payload["query_sql"]


def test_pre_gme_export_route_returns_xlsx(monkeypatch):
    from ui import service

    monkeypatch.setattr(service, "iter_query_rows", lambda sql: (["CHROM"], [{"CHROM": "chr13"}]))
    monkeypatch.setattr(service, "build_pre_gme_workbook_bytes", lambda rows, created_at=None: b"xlsx-bytes")

    response = client.get("/api/exports/pre-gme.xlsx")

    assert response.status_code == 200
    assert response.content == b"xlsx-bytes"
    assert response.headers["content-type"].startswith(
        "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    )


def test_registry_metadata_uses_mocked_public_state(monkeypatch):
    from ui import service

    monkeypatch.setattr(service, "registry_row_count", lambda: 123)
    monkeypatch.setattr(service, "registry_scientific_metrics", lambda: {"gene_windows": [], "source_row_counts": []})

    response = client.get("/api/registry")

    assert response.status_code == 200
    payload = response.json()
    assert payload["row_count"] == 123
    assert payload["title"] == "supervisor_variant_registry_brca_v1"
    assert payload["csv_download_url"] == "/api/registry/download.csv"
    assert any(column["name"] == "GME_AF" for column in payload["columns"])


def test_raw_dataset_download_route_returns_csv(monkeypatch):
    from ui import service

    monkeypatch.setattr(service, "iter_query_rows", lambda sql: (["chrom", "pos"], [{"chrom": "17", "pos": 43000000}]))

    response = client.get("/api/raw-datasets/clinvar_raw_vcf/download.csv")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert "attachment; filename=\"clinvar_raw_vcf.csv\"" == response.headers["content-disposition"]
    assert "chrom,pos" in response.text
    assert "17,43000000" in response.text


def test_registry_download_route_returns_csv(monkeypatch):
    from ui import service

    monkeypatch.setattr(service, "iter_query_rows", lambda sql: (["CHROM", "GENE"], [{"CHROM": "chr13", "GENE": "BRCA2"}]))

    response = client.get("/api/registry/download.csv")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert response.headers["content-disposition"] == 'attachment; filename="supervisor_variant_registry_brca_v1.csv"'
    assert "CHROM,GENE" in response.text
    assert "chr13,BRCA2" in response.text


def test_registry_step_download_route_returns_csv(monkeypatch):
    from ui import service

    monkeypatch.setattr(service, "iter_query_rows", lambda sql: (["variant_key"], [{"variant_key": "chr17:1:A:T"}]))

    response = client.get("/api/registry/steps/clinvar_raw_brca/download.csv")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert response.headers["content-disposition"] == 'attachment; filename="clinvar_raw_brca.csv"'
    assert "variant_key" in response.text
    assert "chr17:1:A:T" in response.text
