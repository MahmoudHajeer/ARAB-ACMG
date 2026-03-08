from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from google.api_core.exceptions import NotFound

from scripts.verify_bq_health import verify_bq_health


@patch("scripts.verify_bq_health.bigquery.Client")
def test_verify_bq_health_success(mock_bq_client):
    mock_client = MagicMock()
    mock_bq_client.return_value = mock_client

    # All required tables exist.
    mock_client.get_table.return_value = object()

    # All table counts are positive.
    def query_side_effect(_query):
        query_job = MagicMock()
        query_job.result.return_value = [SimpleNamespace(cnt=10)]
        return query_job

    mock_client.query.side_effect = query_side_effect

    assert verify_bq_health() is True


@patch("scripts.verify_bq_health.bigquery.Client")
def test_verify_bq_health_missing_table(mock_bq_client):
    mock_client = MagicMock()
    mock_bq_client.return_value = mock_client

    # First table exists, second is missing.
    mock_client.get_table.side_effect = [object(), NotFound("missing")]

    assert (
        verify_bq_health(
            required_tables=[
                "clinvar_raw_vcf",
                "gnomad_v4_1_genomes_chr13_raw",
            ]
        )
        is False
    )


@patch("scripts.verify_bq_health.bigquery.Client")
def test_verify_bq_health_zero_rows(mock_bq_client):
    mock_client = MagicMock()
    mock_bq_client.return_value = mock_client
    mock_client.get_table.return_value = object()

    query_job = MagicMock()
    query_job.result.return_value = [SimpleNamespace(cnt=0)]
    mock_client.query.return_value = query_job

    assert verify_bq_health(required_tables=["clinvar_raw_vcf"]) is False
