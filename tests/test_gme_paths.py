from scripts.ingest_gme_cloud import GMEIngestionPipeline
from scripts.load_gme_to_bq import GMERawBQLoaderPipeline


def test_gme_raw_vault_prefix_uses_release_build_and_snapshot():
    pipeline = GMEIngestionPipeline()
    pipeline.snapshot_date = "2026-03-08"

    assert (
        pipeline.raw_vault_prefix()
        == "raw/sources/gme/release=20161025-hg38/build=hg38/snapshot_date=2026-03-08"
    )


def test_gme_raw_gcs_uri_points_to_vault_snapshot():
    pipeline = GMERawBQLoaderPipeline()
    pipeline.snapshot_date = "2026-03-08"

    assert (
        pipeline.raw_gcs_uri()
        == "gs://mahmoud-arab-acmg-research-data/raw/sources/gme/release=20161025-hg38/build=hg38/snapshot_date=2026-03-08/hg38_gme.txt.gz"
    )
