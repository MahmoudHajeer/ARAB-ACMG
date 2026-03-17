from scripts.refresh_supervisor_review_bundle import build_artifact_catalog, csv_object_from_parquet_uri, schema_lineage_summary


def test_csv_object_from_parquet_uri_swaps_extension():
    object_name = csv_object_from_parquet_uri("gs://mahmoud-arab-acmg-research-data/frozen/path/artifact.parquet")

    assert object_name == "frozen/path/artifact.csv"


def test_build_artifact_catalog_groups_raw_and_derived_entries():
    catalog = build_artifact_catalog(
        storage_client=None,
        legacy_pre_gme={
            "title": "legacy_pre",
            "scope_note": "legacy pre scope",
            "row_count": 10,
            "table_ref": "gs://bucket/legacy_pre.parquet",
            "csv_download_url": "https://storage.googleapis.com/example/legacy_pre.csv",
        },
        legacy_registry={
            "title": "legacy_final",
            "scope_note": "legacy final scope",
            "row_count": 11,
            "table_ref": "gs://bucket/legacy_final.parquet",
            "csv_download_url": "https://storage.googleapis.com/example/legacy_final.csv",
        },
        arab_pre_gme={
            "title": "arab_pre",
            "scope_note": "arab pre scope",
            "row_count": 12,
            "table_ref": "gs://bucket/arab_pre.parquet",
            "csv_download_url": "https://storage.googleapis.com/example/arab_pre.csv",
        },
        arab_registry={
            "title": "arab_final",
            "scope_note": "arab final scope",
            "row_count": 13,
            "table_ref": "gs://bucket/arab_final.parquet",
            "csv_download_url": "https://storage.googleapis.com/example/arab_final.csv",
        },
        normalized_datasets=[
            {
                "key": "clinvar_normalized_brca",
                "title": "ClinVar normalized",
                "simple_summary": "normalized artifact",
                "row_count": 100,
                "download_url": "https://storage.googleapis.com/mahmoud-arab-acmg-research-data/frozen/harmonized/checkpoint=clinvar_normalized_brca_v1/snapshot_date=2026-03-15/clinvar_normalized_brca_v1.csv",
                "table_ref": "gs://mahmoud-arab-acmg-research-data/frozen/harmonized/checkpoint=clinvar_normalized_brca_v1/snapshot_date=2026-03-15/clinvar_normalized_brca_v1.parquet",
            }
        ],
        raw_datasets={
            "datasets": [
                {
                    "key": "clinvar_raw_brca_window",
                    "title": "ClinVar raw",
                    "simple_summary": "raw preview",
                    "row_count": 5,
                    "table_ref": "gs://mahmoud-arab-acmg-research-data/raw/sources/clinvar/lastmod-20260302/snapshot_date=2026-03-03/clinvar.vcf.gz",
                    "notes": ["raw note"],
                }
            ]
        },
        source_review={
            "sources": [
                {
                    "source_key": "clinvar",
                    "upstream_url": "https://example.org/clinvar.vcf.gz",
                }
            ]
        },
    )

    groups = {group["id"]: group for group in catalog["groups"]}
    assert {
        "raw_public_sources",
        "standardization_artifacts",
        "normalized_artifacts",
        "legacy_checkpoint_artifacts",
        "arab_extension_artifacts",
        "reference_study_artifacts",
        "review_documents",
    }.issubset(set(groups))
    assert groups["raw_public_sources"]["entries"][0]["files"][0]["gs_uri"].endswith("clinvar.vcf.gz")
    assert groups["normalized_artifacts"]["entries"][0]["files"][0]["public_url"].endswith("clinvar_normalized_brca_v1.csv")
    assert groups["legacy_checkpoint_artifacts"]["entries"][0]["title"] == "legacy_pre"
    assert groups["arab_extension_artifacts"]["entries"][1]["title"] == "arab_final"
    reference_files = groups["reference_study_artifacts"]["entries"][0]["files"]
    assert all("raw/sources/uae_brca_pmc12011969" not in file_item["gs_uri"] for file_item in reference_files)
    assert all(file_item["access"] == "public" for file_item in reference_files)


def test_schema_lineage_summary_reports_added_and_missing_columns():
    lineage = schema_lineage_summary(
        baseline_entry={"columns": [{"name": "CHROM"}, {"name": "POS"}, {"name": "OLD_COL"}]},
        current_entry={"columns": [{"name": "CHROM"}, {"name": "POS"}, {"name": "NEW_COL"}]},
        added_label="Added fields",
    )

    assert lineage["baseline_column_count"] == 3
    assert lineage["current_column_count"] == 3
    assert lineage["preserved_column_count"] == 2
    assert lineage["missing_columns"] == ["OLD_COL"]
    assert lineage["added_columns"] == ["NEW_COL"]
