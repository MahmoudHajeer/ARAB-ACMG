from ui.catalog import dataset_catalog_payload, registry_catalog_payload


def test_dataset_catalog_payload_includes_gme_notes_and_rows():
    payload = dataset_catalog_payload()
    entries = {entry["key"]: entry for entry in payload}

    assert "gme_hg38_raw" in entries
    assert any("1-22 and X only" in note for note in entries["gme_hg38_raw"]["notes"])
    assert entries["clinvar_raw_vcf"]["table_ref"].endswith("clinvar_raw_vcf")


def test_registry_catalog_payload_exposes_accuracy_notes_and_sql():
    payload = registry_catalog_payload()

    assert payload["table_ref"].endswith("supervisor_variant_registry_v1")
    assert any("AC_eur/AF_eur" in note for note in payload["accuracy_notes"])
    assert "CREATE OR REPLACE TABLE" in payload["build_sql"]
    assert any(column["name"] == "variant_key" for column in payload["columns"])
