from scripts.gcs_public_policy import (
    attachment_header_value,
    gcs_access_profile,
    is_public_safe_gcs_uri,
)


def test_public_policy_allows_safe_raw_package():
    uri = "gs://mahmoud-arab-acmg-research-data/raw/sources/gme/release=20161025-hg38/build=hg38/snapshot_date=2026-03-08/hg38_gme.txt.gz"

    assert is_public_safe_gcs_uri(uri) is True
    profile = gcs_access_profile(uri)
    assert profile["access"] == "public"
    assert profile["public_url"].endswith("hg38_gme.txt.gz")


def test_public_policy_blocks_private_study_workbook():
    uri = "gs://mahmoud-arab-acmg-research-data/raw/sources/uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/uae_brca_pmc12011969_moesm1.xlsx"

    assert is_public_safe_gcs_uri(uri) is False
    profile = gcs_access_profile(uri)
    assert profile["access"] == "restricted"
    assert profile["public_url"] == ""


def test_attachment_header_uses_object_filename():
    uri = "gs://mahmoud-arab-acmg-research-data/frozen/results/checkpoint=supervisor_variant_registry_brca_arab_v2/snapshot_date=2026-03-15/supervisor_variant_registry_brca_arab_v2.csv"

    assert attachment_header_value(uri) == 'attachment; filename="supervisor_variant_registry_brca_arab_v2.csv"'
