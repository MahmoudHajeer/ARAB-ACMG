from scripts.sync_public_gcs_downloads import collect_publicable_gcs_uris


def test_collect_publicable_gcs_uris_filters_private_objects():
    bundle = {
        "artifact_catalog": {
            "groups": [
                {
                    "entries": [
                        {
                            "files": [
                                {"gs_uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/gme/release=20161025-hg38/build=hg38/snapshot_date=2026-03-08/hg38_gme.txt.gz"},
                                {"gs_uri": "gs://mahmoud-arab-acmg-research-data/raw/sources/uae_brca_pmc12011969/version=moesm1/snapshot_date=2026-03-12/uae_brca_pmc12011969_moesm1.xlsx"},
                                {"gs_uri": ""},
                            ]
                        }
                    ]
                }
            ]
        }
    }

    assert collect_publicable_gcs_uris(bundle) == [
        "gs://mahmoud-arab-acmg-research-data/raw/sources/gme/release=20161025-hg38/build=hg38/snapshot_date=2026-03-08/hg38_gme.txt.gz"
    ]
