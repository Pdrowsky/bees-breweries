import pytest
from breweries_pipeline.quality.contract import validate_breweries_data, expected_brewery_fields

class TestValidadeBreweriesDataContract:
    """ Tests for breweries data contract validation. """

    # ==================== VALID DATA ====================

    def test_valid_breweries_data(self):
        """Test validation passes for complete, valid brewery records"""

        valid_data = [
            {
                "id": "1",
                "name": "Test Brewery",
                "brewery_type": "micro",
                "city": "San Francisco",
                "country": "United States",
                "state": "California"
            },
            {
                "id": "2",
                "name": "Another Brewery",
                "brewery_type": "nano",
                "city": "Portland",
                "country": "United States",
                "state": "Oregon"
            }
        ]

        is_valid, errors = validate_breweries_data(valid_data)
        
        assert is_valid is True
        assert errors["data_type"] is False
        assert errors["data_empty"] is False
        assert errors["missing_fields_indexes"] == []

    # ==================== INVALID DATA TYPE ====================
    
    def test_invalid_data_type_dict(self):
        """Test validation fails when data is a dict instead of list"""
        invalid_data = {"id": "1", "name": "Test"}
        
        is_valid, errors = validate_breweries_data(invalid_data)
        
        assert is_valid is False
        assert errors["data_type"] is True
        assert errors["data_empty"] is False

    def test_invalid_data_type_none(self):
        """Test validation fails when data is None"""
        invalid_data = None
        
        is_valid, errors = validate_breweries_data(invalid_data)
        
        assert is_valid is False
        assert errors["data_type"] is True

    # ==================== EMPTY DATA ====================
    
    def test_empty_list(self):
        """Test validation fails for empty list"""
        invalid_data = []
        
        is_valid, errors = validate_breweries_data(invalid_data)
        
        assert is_valid is False
        assert errors["data_empty"] is True

    # ==================== MISSING FIELDS ====================
    
    def test_missing_single_field(self):
        """Test validation identifies record with missing required field"""
        data_with_missing = [
            {
                "id": "1",
                "name": "Test Brewery",
                "brewery_type": "micro",
                "city": "San Francisco",
                "country": "United States"
                # Missing "state"
            }
        ]
        
        is_valid, errors = validate_breweries_data(data_with_missing)
        
        assert is_valid is True
        assert 0 in errors["missing_fields_indexes"]

    def test_missing_multiple_fields(self):
        """Test validation identifies records with multiple missing fields"""
        data_with_missing = [
            {
                "id": "1",
                "name": "Test Brewery"
                # Missing: brewery_type, city, country, state
            },
            {
                "id": "2",
                "name": "Another Brewery",
                "brewery_type": "micro",
                "city": "Portland",
                "country": "United States",
                "state": "Oregon"
                # This one is complete
            }
        ]
        
        is_valid, errors = validate_breweries_data(data_with_missing)
        
        assert is_valid is True
        assert 0 in errors["missing_fields_indexes"]
        assert 1 not in errors["missing_fields_indexes"]

    def test_all_records_missing_required_fields(self):
        """Test when all records are missing required fields"""
        data_with_missing = [
            {"id": "1", "name": "Test"},
            {"id": "2", "name": "Another"}
        ]
        
        is_valid, errors = validate_breweries_data(data_with_missing)
        
        assert is_valid is True
        assert len(errors["missing_fields_indexes"]) == 2

    # ==================== EDGE CASES ====================
    
    def test_null_values_in_fields(self):
        """Test that null/None values in fields don't cause validation to fail
        (nulls in data are handled downstream, not at contract level)"""
        data_with_nulls = [{
            "id": None,
            "name": "Test Brewery",
            "brewery_type": None,
            "city": "San Francisco",
            "country": "United States",
            "state": "California"
        }]
        
        is_valid, errors = validate_breweries_data(data_with_nulls)
        
        # Contract validation only checks field presence, not value validity
        assert is_valid is True
        assert 0 not in errors["missing_fields_indexes"]

    def test_empty_string_values(self):
        """Test that empty strings don't fail contract validation"""
        data_with_empty_strings = [{
            "id": "",
            "name": "",
            "brewery_type": "micro",
            "city": "San Francisco",
            "country": "United States",
            "state": "California"
        }]
        
        is_valid, errors = validate_breweries_data(data_with_empty_strings)
        
        assert is_valid is True
        assert 0 not in errors["missing_fields_indexes"]
