import pandas as pd

from scripts.verify_brca_normalized_artifacts import (
    canonical_variant_key,
    duplicate_groups_are_exact,
    parse_gs_uri,
)


def test_parse_gs_uri_splits_bucket_and_object():
    bucket, object_name = parse_gs_uri("gs://example-bucket/path/to/file.parquet")

    assert bucket == "example-bucket"
    assert object_name == "path/to/file.parquet"


def test_canonical_variant_key_uses_display_columns():
    frame = pd.DataFrame([{"CHROM": "chr13", "POS": 10, "REF": "A", "ALT": "G"}])

    series = canonical_variant_key(
        frame,
        chrom_column="CHROM",
        pos_column="POS",
        ref_column="REF",
        alt_column="ALT",
    )

    assert series.tolist() == ["chr13:10:A:G"]


def test_duplicate_groups_are_exact_ignores_allowed_columns():
    frame = pd.DataFrame(
        [
            {"variant_key": "chr13:10:A:G", "value": 1, "source_record_locator": "row=1"},
            {"variant_key": "chr13:10:A:G", "value": 1, "source_record_locator": "row=2"},
        ]
    )

    exact_ok, failures = duplicate_groups_are_exact(
        frame,
        key_column="variant_key",
        ignored_columns=("source_record_locator",),
    )

    assert exact_ok is True
    assert failures == []
