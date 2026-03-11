from fastapi.testclient import TestClient

from ui.service import app


client = TestClient(app)


def test_health_route_returns_ok():
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_workflow_route_returns_page_navigation():
    response = client.get("/api/workflow")

    assert response.status_code == 200
    payload = response.json()
    assert any(page["id"] == "pre-gme" for page in payload["pages"])


def test_raw_datasets_route_returns_catalog():
    response = client.get("/api/raw-datasets")

    assert response.status_code == 200
    payload = response.json()
    assert any(entry["key"] == "clinvar_raw_vcf" for entry in payload["datasets"])


def test_datasets_route_returns_catalog():
    response = client.get("/api/datasets")

    assert response.status_code == 200
    payload = response.json()
    assert any(entry["key"] == "h_brca_clinvar_variants" for entry in payload["datasets"])


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
        lambda sql: {"columns": ["sample_row_number", "chrom"], "rows": [{"sample_row_number": 1, "chrom": "13"}]},
    )

    response = client.get("/api/datasets/h_brca_clinvar_variants/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["title"] == "ClinVar BRCA harmonized"
    assert payload["rows"][0]["chrom"] == "13"
    assert "TABLESAMPLE SYSTEM" in payload["query_sql"]


def test_pre_gme_metadata_uses_mocked_row_count(monkeypatch):
    from ui import service

    monkeypatch.setattr(service, "pre_gme_row_count", lambda: 77)
    monkeypatch.setattr(service, "pre_gme_metrics", lambda: {"source_row_counts": []})

    response = client.get("/api/pre-gme")

    assert response.status_code == 200
    payload = response.json()
    assert payload["row_count"] == 77
    assert payload["title"] == "supervisor_variant_registry_brca_pre_gme_v1"
    assert payload["download_url"] == "/api/exports/pre-gme.xlsx"


def test_pre_gme_sample_route_uses_mocked_query(monkeypatch):
    from ui import service

    monkeypatch.setattr(
        service,
        "run_query",
        lambda sql: {"columns": ["sample_row_number", "gene_symbol"], "rows": [{"sample_row_number": 1, "gene_symbol": "BRCA1"}]},
    )

    response = client.get("/api/pre-gme/sample")

    assert response.status_code == 200
    payload = response.json()
    assert payload["rows"][0]["gene_symbol"] == "BRCA1"
    assert "supervisor_variant_registry_brca_pre_gme_v1" in payload["query_sql"]


def test_pre_gme_export_route_returns_xlsx(monkeypatch):
    from ui import service

    monkeypatch.setattr(service, "iter_query_rows", lambda sql: (["chrom"], [{"chrom": "chr13"}]))
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
    monkeypatch.setattr(service, "registry_scientific_metrics", lambda: {"gene_windows": [], "clinvar_gene_audit": [], "source_row_counts": []})

    response = client.get("/api/registry")

    assert response.status_code == 200
    payload = response.json()
    assert payload["row_count"] == 123
    assert payload["title"] == "supervisor_variant_registry_brca_v1"
