import pytest
import pandas as pd
from breweries_pipeline.transform.data_manipulation import json_to_dataframe, split_df_by_column, clean_string

class TestJsonToDataframe:
    """Test suite for JSON to DataFrame conversion"""

    def test_convert_single_record(self):
        """Test converting single JSON object to DataFrame"""
        data = [{
            "id": "1",
            "name": "Test Brewery",
            "brewery_type": "micro",
            "city": "San Francisco",
            "country": "United States",
            "state": "California"
        }]
        
        df = json_to_dataframe(data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 1
        assert df["id"].iloc[0] == "1"
        assert df["name"].iloc[0] == "Test Brewery"

    def test_convert_multiple_records(self):
        """Test converting multiple JSON objects to DataFrame"""
        data = [
            {"id": "1", "name": "Brewery 1", "city": "SF"},
            {"id": "2", "name": "Brewery 2", "city": "LA"},
            {"id": "3", "name": "Brewery 3", "city": "NYC"}
        ]
        
        df = json_to_dataframe(data)
        
        assert len(df) == 3
        assert list(df["id"]) == ["1", "2", "3"]
        assert list(df["name"]) == ["Brewery 1", "Brewery 2", "Brewery 3"]

    def test_convert_preserves_data_types(self):
        """Test that conversion preserves appropriate data types"""
        data = [
            {
                "id": 1,           # int
                "name": "Test",    # string
                "active": True,    # bool
                "count": 42        # int
            }
        ]
        
        df = json_to_dataframe(data)
        
        assert df["id"].dtype in [int, "int64", "int32"]
        assert df["name"].dtype == object  # string
        assert df["active"].dtype == bool
        assert df["count"].dtype in [int, "int64", "int32"]

    def test_convert_empty_list(self):
        """Test converting empty list creates empty DataFrame"""
        data = []
        
        df = json_to_dataframe(data)
        
        assert isinstance(df, pd.DataFrame)
        assert len(df) == 0

    def test_convert_with_missing_fields(self):
        """Test conversion handles records with inconsistent fields"""
        data = [
            {"id": "1", "name": "Brewery 1", "city": "SF", "state": "CA"},
            {"id": "2", "name": "Brewery 2", "city": "LA"},  # missing state
            {"id": "3", "name": "Brewery 3"}  # missing city and state
        ]
        
        df = json_to_dataframe(data)
        
        assert len(df) == 3
        assert df["city"].isna().sum() == 1  # Brewery 3 missing city
        assert df["state"].isna().sum() == 2  # Breweries 2,3 missing state

    def test_convert_with_null_values(self):
        """Test conversion handles null values in data"""
        data = [
            {"id": "1", "name": "Brewery 1", "city": None},
            {"id": "2", "name": None, "city": "Portland"}
        ]
        
        df = json_to_dataframe(data)
        
        assert df["city"].isna().sum() == 1
        assert df["name"].isna().sum() == 1

    
class TestSplitDataframeByColumn:
    """Test suite for DataFrame column-based splitting"""

    def test_split_single_group(self):
        """Test splitting DataFrame with single unique value"""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "country": ["USA", "USA", "USA"],
            "name": ["A", "B", "C"]
        })
        
        result = split_df_by_column(df, "country")
        
        assert len(result) == 1
        assert "USA" in result
        assert len(result["USA"]) == 3

    def test_split_multiple_groups(self):
        """Test splitting DataFrame with multiple groups"""
        df = pd.DataFrame({
            "id": [1, 2, 3, 4],
            "state": ["CA", "TX", "CA", "NY"],
            "name": ["A", "B", "C", "D"]
        })
        
        result = split_df_by_column(df, "state")
        
        assert len(result) == 3
        assert len(result["CA"]) == 2
        assert len(result["TX"]) == 1
        assert len(result["NY"]) == 1

    def test_split_preserves_data(self):
        """Test that splitting preserves all data"""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "category": ["A", "B", "A"],
            "value": [10, 20, 30]
        })
        
        result = split_df_by_column(df, "category")
        
        assert len(result["A"]) + len(result["B"]) == len(df)
        assert result["A"]["value"].sum() == 40  # 10 + 30
        assert result["B"]["value"].sum() == 20

    def test_split_with_null_values(self):
        """Test splitting handles null/NaN values as 'unknown' group"""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "category": ["A", None, "B"],
            "name": ["X", "Y", "Z"]
        })
        
        result = split_df_by_column(df, "category")
        
        assert "unknown" in result
        assert len(result["unknown"]) == 1
        assert len(result["A"]) == 1
        assert len(result["B"]) == 1

    def test_split_empty_dataframe(self):
        """Test splitting empty DataFrame"""
        df = pd.DataFrame({"id": [], "category": []})
        
        result = split_df_by_column(df, "category")
        
        assert len(result) == 0


class TestCleanString:
    """Test suite for string cleaning function"""

    def test_lowercase_conversion(self):
        """Test conversion to lowercase"""
        assert clean_string("CALIFORNIA") == "california"
        assert clean_string("Texas") == "texas"
        assert clean_string("NEW YORK") == "new_york"

    def test_space_replacement(self):
        """Test spaces are replaced with underscores"""
        assert clean_string("New York") == "new_york"
        assert clean_string("Los Angeles") == "los_angeles"
        assert clean_string("Saint Louis") == "saint_louis"

    def test_special_character_removal(self):
        """Test special characters are replaced"""
        assert clean_string("São Paulo") == "sao_paulo"
        assert clean_string("Côte d'Ivoire") == "cote_d_ivoire"
        assert clean_string("Zürich") == "zurich"

    def test_accent_removal(self):
        """Test accents are normalized and removed"""
        assert clean_string("Ångström") == "angstrom"
        assert clean_string("Île-de-France") == "ile-de-france"
        assert clean_string("Åland") == "aland"

    def test_special_symbols(self):
        """Test various special symbols are handled"""
        assert clean_string("New-York") == "new-york"
        assert clean_string("Saint.Louis") == "saint.louis"
        assert clean_string("Test@City") == "test_city"
        assert clean_string("City#1") == "city_1"

    def test_multiple_consecutive_underscores(self):
        """Test consecutive underscores are collapsed"""
        assert clean_string("Multiple   Spaces") == "multiple_spaces"
        assert clean_string("Test  __  City") == "test_city"

    def test_leading_trailing_cleanup(self):
        """Test leading/trailing special chars are removed"""
        assert clean_string("_test_") == "test"
        assert clean_string(".city.") == "city"
        assert clean_string("_._test_._") == "test"

    def test_empty_string(self):
        """Test empty string returns underscore"""
        assert clean_string("") == "_"
        assert clean_string("_") == "_"
        assert clean_string("_._") == "_"