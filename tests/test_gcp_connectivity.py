from unittest.mock import patch, MagicMock
from scripts.verify_gcp import verify_gcs_connectivity, verify_bq_connectivity

@patch('scripts.verify_gcp.storage.Client')
def test_verify_gcs_connectivity_success(mock_storage_client):
    # Setup mock
    mock_client_instance = MagicMock()
    mock_storage_client.return_value = mock_client_instance
    mock_bucket = MagicMock()
    mock_bucket.name = "test-bucket"
    mock_client_instance.get_bucket.return_value = mock_bucket

    # Execute
    result = verify_gcs_connectivity()

    # Assert
    assert result is True
    mock_storage_client.assert_called_once()
    mock_client_instance.get_bucket.assert_called_once_with("mahmoud-arab-acmg-research-data")

@patch('scripts.verify_gcp.bigquery.Client')
def test_verify_bq_connectivity_success(mock_bq_client):
    # Setup mock
    mock_client_instance = MagicMock()
    mock_bq_client.return_value = mock_client_instance
    mock_dataset = MagicMock()
    mock_dataset.dataset_id = "test_dataset"
    mock_client_instance.get_dataset.return_value = mock_dataset

    # Execute
    result = verify_bq_connectivity()

    # Assert
    assert result is True
    mock_bq_client.assert_called_once()
    assert mock_client_instance.get_dataset.call_count == 3  # For the 3 datasets
