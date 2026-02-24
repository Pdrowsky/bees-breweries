import pytest
from unittest.mock import patch, Mock, MagicMock
import requests
import time

from breweries_pipeline.apis.client import get_all_breweries

class TestGetAllBreweries:
    """Test suite for API client"""

    # ==================== SUCCESSFUL REQUESTS ====================
    
    @patch('breweries_pipeline.apis.client.requests.get')
    def test_single_page_request(self, mock_get):
        """Test fetching all breweries when they fit on single page"""
        mock_response = Mock()
        mock_response.json.side_effect = [
            [{"id": "1", "name": "Brewery 1"}],
            []  # Empty page signals end
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_all_breweries()
        
        assert len(result) == 1
        assert result[0]["id"] == "1"

    @patch('breweries_pipeline.apis.client.requests.get')
    def test_multiple_pages_request(self, mock_get):
        """Test fetching breweries across multiple pages"""
        mock_response = Mock()
        mock_response.json.side_effect = [
            [{"id": "1", "name": "Brewery 1"}],
            [{"id": "2", "name": "Brewery 2"}],
            [{"id": "3", "name": "Brewery 3"}],
            []  # Empty page signals end
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_all_breweries()
        
        assert len(result) == 3
        assert result[0]["id"] == "1"
        assert result[1]["id"] == "2"
        assert result[2]["id"] == "3"

    @patch('breweries_pipeline.apis.client.requests.get')
    def test_large_batch_of_breweries(self, mock_get):
        """Test fetching large number of breweries"""
        # Create mock responses with 100 breweries per page
        mock_response = Mock()
        page_responses = [
            [{"id": f"{i}", "name": f"Brewery {i}"} for i in range(1, 101)],
            [{"id": f"{i}", "name": f"Brewery {i}"} for i in range(101, 201)],
            []  # End
        ]
        mock_response.json.side_effect = page_responses
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        result = get_all_breweries()
        
        assert len(result) == 200

    # ==================== PAGINATION ====================
    
    @patch('breweries_pipeline.apis.client.requests.get')
    def test_pagination_parameters(self, mock_get):
        """Test that correct pagination parameters are sent"""
        mock_response = Mock()
        mock_response.json.side_effect = [[], []]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        get_all_breweries()
        
        # Verify API was called with pagination params
        first_call = mock_get.call_args_list[0]
        params = first_call[1]["params"]
        
        assert "page" in params
        assert "per_page" in params
        assert params["page"] == 1

    @patch('breweries_pipeline.apis.client.requests.get')
    def test_pagination_increments(self, mock_get):
        """Test that page number increments correctly"""
        mock_response = Mock()
        mock_response.json.side_effect = [
            [{"id": "1"}],
            [{"id": "2"}],
            []
        ]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        get_all_breweries()
        
        # Check that page number incremented
        calls = mock_get.call_args_list
        assert calls[0][1]["params"]["page"] == 1
        assert calls[1][1]["params"]["page"] == 2

    # ==================== RETRY LOGIC ====================
    
    @patch('breweries_pipeline.apis.client.requests.get')
    @patch('breweries_pipeline.apis.client.time.sleep')
    def test_retry_on_connection_error(self, mock_sleep, mock_get):
        """Test that function retries on connection error"""
        mock_response = Mock()
        mock_get.side_effect = [
            requests.exceptions.ConnectionError("Connection failed"),
            requests.exceptions.ConnectionError("Connection failed"),
            mock_response  # Success on 3rd attempt
        ]
        mock_response.json.side_effect = [[{"id": "1"}], []]
        mock_response.raise_for_status.return_value = None
        
        result = get_all_breweries()
        
        assert len(result) == 1
        # Sleep should be called twice (after 2 failures)
        assert mock_sleep.call_count >= 2

    @patch('breweries_pipeline.apis.client.requests.get')
    @patch('breweries_pipeline.apis.client.time.sleep')
    def test_retry_on_timeout(self, mock_sleep, mock_get):
        """Test that function retries on timeout"""
        mock_response = Mock()
        mock_get.side_effect = [
            requests.exceptions.Timeout("Request timed out"),
            requests.exceptions.Timeout("Request timed out"),
            mock_response  # Success on 3rd attempt
        ]
        mock_response.json.side_effect = [[{"id": "1"}], []]
        mock_response.raise_for_status.return_value = None
        
        result = get_all_breweries()
        
        assert len(result) == 1

    @patch('breweries_pipeline.apis.client.requests.get')
    @patch('breweries_pipeline.apis.client.time.sleep')
    def test_max_retries_exhausted(self, mock_sleep, mock_get):
        """Test behavior when max retries are exhausted"""
        # Make all attempts fail
        mock_get.side_effect = requests.exceptions.ConnectionError("Always fails")
        
        # This should eventually raise or return empty after retries
        with pytest.raises(Exception):
            get_all_breweries()

    # ==================== ERROR HANDLING ====================
    
    @patch('breweries_pipeline.apis.client.requests.get')
    def test_http_error_handling(self, mock_get):
        """Test handling of HTTP errors"""
        mock_response = Mock()
        mock_response.raise_for_status.side_effect = requests.exceptions.HTTPError("404 Not Found")
        mock_get.return_value = mock_response
        
        with pytest.raises(Exception):
            get_all_breweries()

    @patch('breweries_pipeline.apis.client.requests.get')
    def test_json_decode_error(self, mock_get):
        """Test handling of invalid JSON response"""
        mock_response = Mock()
        mock_response.raise_for_status.return_value = None
        mock_response.json.side_effect = ValueError("Invalid JSON")
        mock_get.return_value = mock_response
        
        with pytest.raises(Exception):
            get_all_breweries()

    # ==================== HEADERS & CONFIGURATION ====================
    
    @patch('breweries_pipeline.apis.client.requests.get')
    def test_headers_sent_with_request(self, mock_get):
        """Test that proper headers are sent"""
        mock_response = Mock()
        mock_response.json.side_effect = [[], []]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        get_all_breweries()
        
        # Verify headers were included
        first_call = mock_get.call_args_list[0]
        assert "headers" in first_call[1]

    @patch('breweries_pipeline.apis.client.requests.get')
    def test_timeout_set(self, mock_get):
        """Test that request timeout is configured"""
        mock_response = Mock()
        mock_response.json.side_effect = [[], []]
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        get_all_breweries()
        
        # Verify timeout was set
        first_call = mock_get.call_args_list[0]
        assert "timeout" in first_call[1]
        assert first_call[1]["timeout"] > 0