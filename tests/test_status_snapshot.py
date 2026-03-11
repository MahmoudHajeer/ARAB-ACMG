from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import NotFound

from scripts.update_status_snapshot import (
    RAW_TABLES,
    format_sample_value,
    get_bigquery_samples,
    parse_latest_t002_verification,
    quote_identifier,
)


def test_format_sample_value_truncates_long_values():
    long_text = "A" * 300
    formatted = format_sample_value(long_text)

    assert formatted.endswith("...")
    assert len(formatted) < len(long_text)


def test_quote_identifier_wraps_reserved_names():
    assert quote_identifier("end") == "`end`"
    assert quote_identifier("normal_name") == "`normal_name`"


@patch("scripts.update_status_snapshot.bigquery.Client")
def test_get_bigquery_samples_collects_rows(mock_bq_client):
    mock_client = MagicMock()
    mock_bq_client.return_value = mock_client
    mock_table = MagicMock()
    mock_table.schema = [
        SimpleNamespace(name="chrom"),
        SimpleNamespace(name="pos"),
        SimpleNamespace(name="id"),
        SimpleNamespace(name="ref"),
        SimpleNamespace(name="alt"),
        SimpleNamespace(name="qual"),
        SimpleNamespace(name="filter"),
        SimpleNamespace(name="info"),
    ]
    mock_client.get_table.return_value = mock_table

    long_info = "INFO=" + ("X" * 300)

    def query_side_effect(_query):
        query_job = MagicMock()
        query_job.result.return_value = [
            SimpleNamespace(
                chrom="chr13",
                pos=123,
                id=".",
                ref="A",
                alt="G",
                qual="100",
                filter="PASS",
                info=long_info,
            )
        ]
        return query_job

    mock_client.query.side_effect = query_side_effect

    result = get_bigquery_samples()

    assert result["error"] is None
    assert len(result["tables"]) == len(RAW_TABLES)
    assert result["tables"][0]["columns"] == [
        "chrom",
        "pos",
        "id",
        "ref",
        "alt",
        "qual",
        "filter",
        "info",
    ]
    assert result["tables"][0]["rows"][0]["chrom"] == "chr13"
    assert result["tables"][0]["rows"][0]["info"].endswith("...")


@patch("scripts.update_status_snapshot.bigquery.Client")
def test_get_bigquery_samples_quotes_reserved_columns(mock_bq_client):
    mock_client = MagicMock()
    mock_bq_client.return_value = mock_client
    mock_table = MagicMock()
    mock_table.schema = [
        SimpleNamespace(name="chrom"),
        SimpleNamespace(name="start"),
        SimpleNamespace(name="end"),
        SimpleNamespace(name="ref"),
        SimpleNamespace(name="alt"),
    ]

    def get_table_side_effect(table_ref):
        if table_ref.endswith(".gme_hg38_raw"):
            return mock_table
        raise NotFound("missing")

    mock_client.get_table.side_effect = get_table_side_effect

    def query_side_effect(query):
        if "gme_hg38_raw" in query:
            assert "SELECT `chrom`, `start`, `end`, `ref`, `alt`" in query
            assert "ORDER BY `chrom`, `start`, `ref`, `alt`" in query
            query_job = MagicMock()
            query_job.result.return_value = [
                SimpleNamespace(chrom="chr13", start=1, end=2, ref="A", alt="G")
            ]
            return query_job
        query_job = MagicMock()
        query_job.result.return_value = []
        return query_job

    mock_client.query.side_effect = query_side_effect

    result = get_bigquery_samples()

    gme_entry = next(table for table in result["tables"] if table["table"] == "gme_hg38_raw")
    assert gme_entry["status"] == "present"
    assert gme_entry["rows"][0]["end"] == "2"


@patch("scripts.update_status_snapshot.bigquery.Client")
def test_get_bigquery_samples_handles_missing_table(mock_bq_client):
    mock_client = MagicMock()
    mock_bq_client.return_value = mock_client
    mock_table = MagicMock()
    mock_table.schema = [SimpleNamespace(name="chrom")]
    mock_client.get_table.side_effect = [NotFound("missing")] + [mock_table] * (len(RAW_TABLES) - 1)

    def query_side_effect(_query):
        query_job = MagicMock()
        query_job.result.return_value = []
        return query_job

    mock_client.query.side_effect = query_side_effect

    result = get_bigquery_samples()

    assert result["tables"][0]["status"] == "missing"
    assert result["tables"][0]["rows"] == []


@patch("scripts.update_status_snapshot.T002_INDEX_FILE")
def test_parse_latest_t002_verification_supports_new_handoff_field(mock_index_file):
    mock_index_file.read_text.return_value = (
        "### Entry 8\n"
        "- verification: `old check (fail)`\n"
        "### Entry 9\n"
        "- Verification run + result: `pytest -q tests (8 passed)`, `python3 scripts/verify_bq_health.py (pass)`\n"
    )

    result = parse_latest_t002_verification()

    assert result[0]["command"] == "pytest -q tests (8 passed)"
    assert result[0]["status"] == "pass"
    assert result[1]["command"] == "python3 scripts/verify_bq_health.py (pass)"
    assert result[1]["status"] == "pass"
