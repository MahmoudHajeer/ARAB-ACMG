from ui.catalog import dataset_catalog_payload, pre_gme_catalog_payload, raw_dataset_catalog_payload, registry_catalog_payload


def test_dataset_catalog_payload_includes_only_checkpoint_entries():
    payload = dataset_catalog_payload()
    entries = {entry["key"]: entry for entry in payload}

    assert set(entries) == {"pre_gme_registry", "final_registry"}
    assert entries["pre_gme_registry"]["table_ref"].endswith("supervisor_variant_registry_brca_pre_gme_v1")


def test_raw_dataset_catalog_payload_includes_raw_source_entries():
    payload = raw_dataset_catalog_payload()
    entries = {entry["key"]: entry for entry in payload}

    assert "clinvar_raw_vcf" in entries
    assert entries["gme_hg38_raw"]["table_ref"].endswith("gme_hg38_raw")
    assert any("raw" in note.lower() for note in entries["clinvar_raw_vcf"]["notes"])


def test_pre_gme_catalog_payload_exposes_required_header_and_kind_metadata():
    payload = pre_gme_catalog_payload()

    assert payload["table_ref"].endswith("supervisor_variant_registry_brca_pre_gme_v1")
    assert payload["export_filename"].endswith(".xlsx")
    assert payload["export_metadata_preview"][0].startswith("ARAB_ACMG_PRE_GME_PIPELINE")
    assert payload["export_header_columns"][0] == "CHROM"
    assert payload["columns"][0]["kind"] == "required"
    assert any(column["kind"] == "extra" for column in payload["columns"])


def test_registry_catalog_payload_exposes_required_and_extra_columns():
    payload = registry_catalog_payload()

    assert payload["table_ref"].endswith("supervisor_variant_registry_brca_v1")
    assert any("NULL" in note for note in payload["scientific_notes"])
    assert "CREATE OR REPLACE TABLE" in payload["build_sql"]
    assert any(column["name"] == "CHROM" for column in payload["columns"])
    assert any(column["name"] == "GME_AF" and column["kind"] == "extra" for column in payload["columns"])
