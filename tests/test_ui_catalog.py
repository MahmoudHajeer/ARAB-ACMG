from ui.catalog import dataset_catalog_payload, registry_catalog_payload


def test_dataset_catalog_payload_includes_brca_harmonized_entries():
    payload = dataset_catalog_payload()
    entries = {entry["key"]: entry for entry in payload}

    assert "h_brca_gme_variants" in entries
    assert any("coordinate-based" in note for note in entries["h_brca_gme_variants"]["notes"])
    assert entries["h_brca_clinvar_variants"]["table_ref"].endswith("h_brca_clinvar_variants")


def test_registry_catalog_payload_exposes_accuracy_notes_and_sql():
    payload = registry_catalog_payload()

    assert payload["table_ref"].endswith("supervisor_variant_registry_brca_v1")
    assert any("GRCh38" in note for note in payload["accuracy_notes"])
    assert any("BRCA1 window" in note for note in payload["scientific_notes"])
    assert "CREATE OR REPLACE TABLE" in payload["build_sql"]
    assert any(column["name"] == "gene_symbol" for column in payload["columns"])
