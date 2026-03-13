import pandas as pd

from scripts.update_source_review_state import clean_value, compact_rows, parse_source_freeze_register


def test_parse_source_freeze_register_reads_markdown_table():
    markdown = """
| Source | Source Version | Snapshot Date | Upstream URL | Raw Vault Prefix | Notes |
| :--- | :--- | :--- | :--- | :--- | :--- |
| clinvar | v1 | 2026-03-03 | https://example.org/clinvar.vcf.gz | `gs://bucket/raw/clinvar/` | explicit grch38 |
| gme_hg38 | hg38 | 2026-03-08 | file:///tmp/gme.txt.gz | `gs://bucket/raw/gme/` | hg38 summary |
""".strip()

    payload = parse_source_freeze_register(markdown)

    assert payload["clinvar"]["snapshot_date"] == "2026-03-03"
    assert payload["gme_hg38"]["raw_vault_prefix"] == "gs://bucket/raw/gme/"


def test_clean_value_converts_nan_to_none():
    assert clean_value(float("nan")) is None
    assert clean_value("value") == "value"


def test_compact_rows_serializes_small_preview():
    frame = pd.DataFrame(
        [
            {"gene": "BRCA1", "coord": "chr17:43044295", "score": 1.0},
            {"gene": "BRCA2", "coord": None, "score": float("nan")},
        ]
    )

    preview = compact_rows(frame, limit=2)

    assert preview["columns"] == ["gene", "coord", "score"]
    assert preview["rows"][1]["coord"] is None
    assert preview["rows"][1]["score"] is None
