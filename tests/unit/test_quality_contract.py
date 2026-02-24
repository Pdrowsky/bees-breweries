import pytest
from breweries_pipeline.quality.contract import validate_breweries_data


class TestDataContract:
    """Tests for hard-fail data structure validation"""

    def test_valid_data_structure(self):
        """Valid list of data passes contract validation"""
        data = [{"id": "1", "name": "Brewery"}]
        is_valid, errors = validate_breweries_data(data)
        assert is_valid is True
        assert errors["data_type"] is False
        assert errors["data_empty"] is False

    def test_invalid_data_type_not_list(self):
        """Hard fail: data is not a list"""
        data = {"id": "1"}  # Dict, not list
        is_valid, errors = validate_breweries_data(data)
        assert is_valid is False
        assert errors["data_type"] is True
        assert errors["data_empty"] is False

    def test_invalid_data_type_string(self):
        """Hard fail: data is string"""
        data = "not a list"
        is_valid, errors = validate_breweries_data(data)
        assert is_valid is False
        assert errors["data_type"] is True
        assert errors["data_empty"] is False

    def test_invalid_data_empty_list(self):
        """Hard fail: data is empty list"""
        data = []
        is_valid, errors = validate_breweries_data(data)
        assert is_valid is False
        assert errors["data_empty"] is True
        assert errors["data_type"] is False

    def test_invalid_data_none(self):
        """Hard fail: data is None"""
        data = None
        is_valid, errors = validate_breweries_data(data)
        assert is_valid is False
        assert errors["data_type"] is True
        assert errors["data_empty"] is True

