import pytest
import pandas as pd
import tempfile
import os
import json
from unittest.mock import patch, Mock
from pathlib import Path

from breweries_pipeline.quality.contract import validate_breweries_data
from breweries_pipeline.transform.data_manipulation import (
    json_to_dataframe,
    split_df_by_column,
    clean_string
)
from breweries_pipeline.datalake.filesystem import (
    save_to_bronze,
    save_to_silver,
    save_to_gold,
    read_from_bronze,
    read_from_silver
)


@pytest.fixture
def mock_brewery_data():
    """Fixture providing sample brewery data for testing"""
    return [
        {
            "id": "1",
            "name": "Brewery One",
            "brewery_type": "micro",
            "city": "San Francisco",
            "country": "United States",
            "state": "California"
        },
        {
            "id": "2",
            "name": "Brewery Two",
            "brewery_type": "nano",
            "city": "Los Angeles",
            "country": "United States",
            "state": "California"
        },
        {
            "id": "3",
            "name": "Brewery Three",
            "brewery_type": "brewpub",
            "city": "Portland",
            "country": "United States",
            "state": "Oregon"
        },
        {
            "id": "4",
            "name": "Brewery Four",
            "brewery_type": "micro",
            "city": "Austin",
            "country": "United States",
            "state": "Texas"
        }
    ]


@pytest.fixture
def temp_lake_root(tmp_path):
    """Fixture providing temporary data lake"""
    lake_root = tmp_path / "test_lake"
    lake_root.mkdir()
    
    with patch("breweries_pipeline.datalake.filesystem.LAKE_ROOT", str(lake_root)):
        yield lake_root


class TestBronzeIngestionsFlow:
    """Test suite for bronze ingestion flow"""

    def test_save_and_validate_bronze_data(self, mock_brewery_data, temp_lake_root):
        """Test complete bronze layer: ingest, validate, persist"""
        # Validate data
        is_valid, errors = validate_breweries_data(mock_brewery_data)
        assert is_valid is True
        
        # Save to bronze
        filename = "breweries_api/breweries/exec_time=2024-01-01/run_id=test_run/data.json"
        filepath = save_to_bronze(mock_brewery_data, filename)
        
        # Verify file exists and can be read
        loaded = read_from_bronze(filepath)
        assert len(loaded) == 4
        assert loaded[0]["id"] == "1"

    def test_bronze_ingestion_with_invalid_records(self, mock_brewery_data, temp_lake_root):
        """Test bronze layer handles data with missing fields gracefully"""
        # Remove required field from one record
        invalid_data = mock_brewery_data.copy()
        del invalid_data[1]["state"]  # Remove state from record 2
        
        is_valid, errors = validate_breweries_data(invalid_data)
        assert is_valid is True
        assert 1 in errors["missing_fields_indexes"]
        
        # Filter out invalid records
        valid_data = [d for idx, d in enumerate(invalid_data) if idx not in errors["missing_fields_indexes"]]
        
        filepath = save_to_bronze(valid_data, "test.json")
        loaded = read_from_bronze(filepath)
        
        assert len(loaded) == 3  # One record filtered out


class TestSilverTransformationFlow:
    """Test suite for silver layer transformation"""

    def test_bronze_to_silver_transformation(self, mock_brewery_data, temp_lake_root):
        """Test complete bronze → silver transformation"""
        # Save to bronze first
        bronze_filename = "breweries_api/breweries/exec_time=2024-01-01/run_id=test/data.json"
        bronze_path = save_to_bronze(mock_brewery_data, bronze_filename)
        
        # Read and transform
        breweries = read_from_bronze(bronze_path)
        breweries_df = json_to_dataframe(breweries)
        
        # Apply transformations
        keep = ["id", "name", "brewery_type", "country", "state", "city"]
        breweries_df = breweries_df[keep]
        breweries_df = breweries_df.drop_duplicates(subset="id")
        breweries_df = breweries_df.dropna(subset=["country", "state"])
        
        # Split by country and state
        breweries_by_country = split_df_by_column(breweries_df, "country")
        for country, df in breweries_by_country.items():
            breweries_by_country[country] = split_df_by_column(df, "state")
        
        # Save to silver
        saved_paths = []
        for country, states in breweries_by_country.items():
            for state, df in states.items():
                df_clean = df.drop(columns=["country", "state"])
                filename = f"breweries/country={clean_string(country)}/state={clean_string(state)}/data.parquet"
                filepath = save_to_silver(df_clean, filename)
                saved_paths.append(filepath)
        
        # Verify silver data
        assert len(saved_paths) > 0
        for filepath in saved_paths:
            df = read_from_silver(filepath)
            assert len(df) > 0
            assert "country" not in df.columns
            assert "state" not in df.columns

    def test_silver_partitioning_by_location(self, mock_brewery_data, temp_lake_root):
        """Test that silver layer correctly partitions by country and state"""
        breweries_df = json_to_dataframe(mock_brewery_data)
        keep = ["id", "name", "brewery_type", "country", "state", "city"]
        breweries_df = breweries_df[keep]
        
        # Count expected partitions
        unique_countries = breweries_df["country"].nunique()
        unique_states = breweries_df.groupby("country")["state"].apply(lambda x: x.nunique()).sum()
        
        assert unique_countries == 1  # All US
        assert unique_states == 3  # CA, OR, TX


class TestGoldAggregationFlow:
    """Test suite for gold layer aggregation"""

    def test_silver_to_gold_aggregation(self, mock_brewery_data, temp_lake_root):
        """Test complete silver → gold aggregation"""
        # Prepare data (simulate silver layer)
        breweries_df = json_to_dataframe(mock_brewery_data)
        keep = ["id", "name", "brewery_type", "country", "state", "city"]
        breweries_df = breweries_df[keep]
        
        # Aggregate
        agg_df = breweries_df.groupby(
            ["brewery_type", "country", "state"], observed=True
        ).size().reset_index(name="brewery_count")
        
        # Save to gold
        filename = "breweries/aggregated/breweries_by_type_country_state.parquet"
        filepath = save_to_gold(agg_df, filename)
        
        # Verify gold data
        gold_df = read_from_silver(filepath)
        
        assert "brewery_count" in gold_df.columns
        assert len(gold_df) > 0
        assert gold_df["brewery_count"].sum() == 4  # Total breweries

    def test_gold_aggregation_correctness(self, mock_brewery_data, temp_lake_root):
        """Test that gold aggregation produces correct counts"""
        breweries_df = json_to_dataframe(mock_brewery_data)
        
        agg_df = breweries_df.groupby(
            ["brewery_type", "country"], observed=True
        ).size().reset_index(name="brewery_count")
        
        # Verify expected aggregations
        # Expected: micro=2, nano=1, brewpub=1 (all in US)
        assert len(agg_df) == 3
        
        micro_count = agg_df[agg_df["brewery_type"] == "micro"]["brewery_count"].values[0]
        assert micro_count == 2


class TestFullPipelineFlow:
    """Test suite for complete end-to-end pipeline"""

    def test_full_pipeline_execution(self, mock_brewery_data, temp_lake_root):
        """Test complete pipeline from ingestion to aggregation"""
        # Step 1: Validate and save to bronze
        is_valid, errors = validate_breweries_data(mock_brewery_data)
        assert is_valid is True
        
        bronze_filename = "breweries_api/breweries/exec_time=2024-01-01/run_id=test/data.json"
        bronze_path = save_to_bronze(mock_brewery_data, bronze_filename)
        
        # Step 2: Transform and save to silver
        breweries = read_from_bronze(bronze_path)
        breweries_df = json_to_dataframe(breweries)
        
        keep = ["id", "name", "brewery_type", "country", "state", "city"]
        breweries_df = breweries_df[keep]
        breweries_df = breweries_df.drop_duplicates(subset="id")
        breweries_df = breweries_df.dropna(subset=["country", "state"])

        # Store the original values before dropping
        country_state_pairs = breweries_df[["id", "country", "state"]].copy()
        
        breweries_by_country = split_df_by_column(breweries_df, "country")
        silver_paths = []
        partition_metadata = []
        
        for country, df in breweries_by_country.items():
            breweries_by_country[country] = split_df_by_column(df, "state")
            for state, state_df in breweries_by_country[country].items():
                state_df = state_df.drop(columns=["country", "state"])
                filename = f"breweries/country={clean_string(country)}/state={clean_string(state)}/data.parquet"
                filepath = save_to_silver(state_df, filename)
                silver_paths.append(filepath)
                partition_metadata.append({
                    "filepath": filepath,
                    "country": country,
                    "state": state
                })
        
        # Step 3: Aggregate and save to gold
        # Read back all silver partitions
        all_silver_dfs = []
        for metadata in partition_metadata:
            df = read_from_silver(metadata["filepath"])
            # Add back the partition columns
            df["country"] = metadata["country"]
            df["state"] = metadata["state"]
            all_silver_dfs.append(df)
        
        combined_silver_df = pd.concat(all_silver_dfs, ignore_index=True)
        
        gold_agg = combined_silver_df.groupby(
            ["brewery_type", "country", "state"], observed=True
        ).size().reset_index(name="brewery_count")
        
        gold_filename = "breweries/aggregated/breweries_by_type_country_state.parquet"
        save_to_gold(gold_agg, gold_filename)
        
        # Verify final output
        assert len(gold_agg) > 0
        assert gold_agg["brewery_count"].sum() == 4


class TestPipelineErrorRecovery:
    """Test suite for pipeline error handling and recovery"""
    
    def test_pipeline_recovery_from_bronze_failure(self, mock_brewery_data, temp_lake_root):
        """Test that pipeline can recover if bronze layer partially fails"""
        # Simulate some records failing validation
        invalid_data = mock_brewery_data.copy()
        del invalid_data[1]["state"]  # One bad record
        
        is_valid, errors = validate_breweries_data(invalid_data)
        # Filter bad records
        valid_data = [d for idx, d in enumerate(invalid_data) 
                      if idx not in errors["missing_fields_indexes"]]
        
        # Continue with valid records
        bronze_path = save_to_bronze(valid_data, "test.json")
        
        # Verify we saved what we could
        loaded = read_from_bronze(bronze_path)
        assert len(loaded) == len(mock_brewery_data) - 1
    
    def test_partial_silver_transformation(self, mock_brewery_data, temp_lake_root):
        """Test silver transformation handles partial data"""
        # Simulate scenario where some partitions fail
        df = json_to_dataframe(mock_brewery_data)
        
        # Only transform California breweries
        ca_df = df[df["state"] == "California"]
        assert len(ca_df) > 0
        
        filepath = save_to_silver(ca_df, "breweries/country=US/state=california/data.parquet")
        assert os.path.exists(filepath)