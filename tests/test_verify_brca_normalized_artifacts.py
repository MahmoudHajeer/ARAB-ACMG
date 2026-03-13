from scripts.verify_brca_normalized_artifacts import parse_gs_uri


def test_parse_gs_uri_splits_bucket_and_object():
    bucket, object_name = parse_gs_uri("gs://example-bucket/path/to/file.parquet")

    assert bucket == "example-bucket"
    assert object_name == "path/to/file.parquet"
