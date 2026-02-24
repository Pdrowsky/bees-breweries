import pytest
from breweries_pipeline.quality.integrity import validate_breweries_fields


class TestDataIntegrity:
    """Tests for soft-fail record field completeness"""

    def test_all_records_have_required_fields(self):
        """All records have id and name - no soft fails"""
        data = [
            {"id": "1", "name": "Brewery One"},
            {"id": "2", "name": "Brewery Two"},
        ]
        errors = validate_breweries_fields(data)
        assert errors["missing_fields_indexes"] == []

    def test_record_missing_id(self):
        """Record missing id field - soft fail"""
        data = [
            {"id": "1", "name": "Brewery One"},
            {"name": "Brewery Two"},  # Missing id
        ]
        errors = validate_breweries_fields(data)
        assert 1 in errors["missing_fields_indexes"]

    def test_record_missing_name(self):
        """Record missing name field - soft fail"""
        data = [
            {"id": "1", "name": "Brewery One"},
            {"id": "2"},  # Missing name
        ]
        errors = validate_breweries_fields(data)
        assert 1 in errors["missing_fields_indexes"]

    def test_multiple_records_missing_fields(self):
        """Multiple records with missing fields"""
        data = [
            {"id": "1", "name": "Brewery One"},
            {"name": "Brewery Two"},  # Missing id
            {"id": "3"},  # Missing name
            {"id": "4", "name": "Brewery Four"},
        ]
        errors = validate_breweries_fields(data)
        assert errors["missing_fields_indexes"] == [1, 2]

    def test_empty_record(self):
        """Record is empty dict - both fields missing"""
        data = [
            {"id": "1", "name": "Brewery One"},
            {},  # Empty
        ]
        errors = validate_breweries_fields(data)
        assert 1 in errors["missing_fields_indexes"]

    def test_extra_fields_are_ignored(self):
        """Extra fields don't cause soft fails - only checks required fields"""
        data = [
            {"id": "1", "name": "Brewery One", "brewery_type": "micro", "country": "US"}
        ]
        errors = validate_breweries_fields(data)
        assert errors["missing_fields_indexes"] == []

