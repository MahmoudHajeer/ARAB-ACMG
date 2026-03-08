from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import NotFound

from scripts.update_status_snapshot import (
    SAMPLE_COLUMNS,
    format_sample_value,
    get_bigquery_samples,
)


def test_format_sample_value_truncates_long_values():
    long_text = "A" * 300
    formatted = format_sample_value(long_text)

    assert formatted.endswith("...")
    assert len(formatted) < len(long_text)


@patch("scripts.update_status_snapshot.bigquery.Client")
def test_get_bigquery_samples_collects_rows(mock_bq_client):
    mock_client = MagicMock()
    mock_bq_client.return_value = mock_client
    mock_client.get_table.return_value = object()

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
    assert len(result["tables"]) == 5
    assert result["tables"][0]["columns"] == SAMPLE_COLUMNS
    assert result["tables"][0]["rows"][0]["chrom"] == "chr13"
    assert result["tables"][0]["rows"][0]["info"].endswith("...")


@patch("scripts.update_status_snapshot.bigquery.Client")
def test_get_bigquery_samples_handles_missing_table(mock_bq_client):
    mock_client = MagicMock()
    mock_bq_client.return_value = mock_client
    mock_client.get_table.side_effect = [NotFound("missing")] + [object()] * 4

    def query_side_effect(_query):
        query_job = MagicMock()
        query_job.result.return_value = []
        return query_job

    mock_client.query.side_effect = query_side_effect

    result = get_bigquery_samples()

    assert result["tables"][0]["status"] == "missing"
    assert result["tables"][0]["rows"] == []
