from fastapi.testclient import TestClient

from ui.service import app


client = TestClient(app)


def test_health_route_returns_ok():
    response = client.get("/api/health")

    assert response.status_code == 200
    assert response.json() == {"status": "ok"}


def test_datasets_route_returns_catalog():
    response = client.get("/api/datasets")

    assert response.status_code == 200
    payload = response.json()
    assert any(entry["key"] == "h_brca_clinvar_variants" for entry in payload["datasets"])


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


def test_registry_metadata_uses_mocked_public_state(monkeypatch):
    from ui import service

    monkeypatch.setattr(service, "registry_row_count", lambda: 123)
    monkeypatch.setattr(service, "registry_scientific_metrics", lambda: {"gene_windows": [], "clinvar_gene_audit": [], "source_row_counts": []})

    response = client.get("/api/registry")

    assert response.status_code == 200
    payload = response.json()
    assert payload["row_count"] == 123
    assert payload["title"] == "supervisor_variant_registry_brca_v1"
