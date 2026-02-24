import pytest
import os
import json
import tempfile
import pandas as pd
from pathlib import Path
from unittest.mock import patch, MagicMock
import shutil

from breweries_pipeline.datalake.filesystem import (
    _ensure_dir_exists,
    _atomic_json_write,
    save_to_parquet,
    read_json,
    read_parquet,
    get_bronze_path,
    get_silver_path,
    get_gold_path,
    save_to_bronze,
    save_to_silver,
    save_to_gold,
    read_from_bronze,
    read_from_silver,
    read_from_gold
)

@pytest.fixture
def temp_lake_root(tmp_path):
    """Fixture providing a temporary data lake root directory"""
    lake_root = tmp_path / "test_lake"
    lake_root.mkdir()
    
    # Mock the LAKE_ROOT to point to temp directory
    with patch("breweries_pipeline.datalake.filesystem.LAKE_ROOT", str(lake_root)):
        yield lake_root
    
    # Cleanup
    if lake_root.exists():
        shutil.rmtree(lake_root)


class TestEnsureDirExists:
    """Test suite for directory creation helper"""

    def test_create_single_directory(self):
        """Test creating a single directory"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            test_path = os.path.join(tmp_dir, "test_dir", "test_file.txt")
            _ensure_dir_exists(os.path.dirname(test_path))
            
            assert os.path.exists(os.path.dirname(test_path))

    def test_create_nested_directories(self):
        """Test creating nested directory structure"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            nested_path = os.path.join(tmp_dir, "a", "b", "c", "d", "file.txt")
            _ensure_dir_exists(os.path.dirname(nested_path))
            
            assert os.path.exists(os.path.dirname(nested_path))

    def test_directory_already_exists(self):
        """Test that function handles existing directory gracefully"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            # First call
            _ensure_dir_exists(tmp_dir)
            
            # Second call on same directory
            _ensure_dir_exists(tmp_dir)
            
            assert os.path.exists(tmp_dir)


class TestAtomicJsonWrite:
    """Test suite for atomic JSON write operation"""

    def test_write_valid_json(self):
        """Test writing valid JSON data atomically"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "data", "test.json")
            test_data = [{"id": "1", "name": "Test"}]
            
            _atomic_json_write(test_data, filepath)
            
            assert os.path.exists(filepath)
            with open(filepath, 'r') as f:
                loaded = json.load(f)
            assert loaded == test_data

    def test_write_creates_nested_dirs(self):
        """Test that nested directories are created"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "a", "b", "c", "data.json")
            test_data = [{"test": "data"}]
            
            _atomic_json_write(test_data, filepath)
            
            assert os.path.exists(filepath)

    def test_write_special_characters(self):
        """Test writing data with special characters"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "test.json")
            test_data = [{"name": "Café", "location": "São Paulo"}]
            
            _atomic_json_write(test_data, filepath)
            
            with open(filepath, 'r', encoding='utf-8') as f:
                loaded = json.load(f)
            assert loaded[0]["name"] == "Café"

    def test_write_empty_list(self):
        """Test writing empty list"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "empty.json")
            test_data = []
            
            _atomic_json_write(test_data, filepath)
            
            with open(filepath, 'r') as f:
                loaded = json.load(f)
            assert loaded == []

    def test_atomic_write_cleanup(self):
        """Test that temporary files are cleaned up after successful write"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "test.json")
            test_data = [{"test": "data"}]
            
            _atomic_json_write(test_data, filepath)
            
            # Temporary file should not exist
            assert not os.path.exists(filepath + ".tmp")
            # Actual file should exist
            assert os.path.exists(filepath)


class TestSaveAndReadJsonJson:
    """Test suite for JSON file operations"""

    def test_read_valid_json_file(self):
        """Test reading a valid JSON file"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "test.json")
            test_data = [{"id": "1", "name": "Test"}]
            
            with open(filepath, 'w') as f:
                json.dump(test_data, f)
            
            loaded = read_json(filepath)
            
            assert loaded == test_data

    def test_read_json_encoding(self):
        """Test reading JSON with special character encoding"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "test.json")
            test_data = [{"location": "São Paulo"}]
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(test_data, f, ensure_ascii=False)
            
            loaded = read_json(filepath)
            
            assert loaded[0]["location"] == "São Paulo"

    def test_read_nonexistent_file(self):
        """Test reading non-existent file raises error"""
        with pytest.raises(FileNotFoundError):
            read_json("/nonexistent/path/file.json")


class TestSaveAndReadParquet:
    """Test suite for Parquet file operations"""

    def test_save_dataframe_to_parquet(self):
        """Test saving DataFrame to Parquet"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "test.parquet")
            df = pd.DataFrame({
                "id": [1, 2, 3],
                "name": ["A", "B", "C"]
            })
            
            save_to_parquet(df, filepath)
            
            assert os.path.exists(filepath)

    def test_read_parquet_file(self):
        """Test reading Parquet file back"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "test.parquet")
            original_df = pd.DataFrame({
                "id": [1, 2, 3],
                "name": ["A", "B", "C"]
            })
            
            save_to_parquet(original_df, filepath)
            loaded_df = read_parquet(filepath)
            
            pd.testing.assert_frame_equal(original_df, loaded_df)

    def test_parquet_preserves_types(self):
        """Test that Parquet preserves data types"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "test.parquet")
            df = pd.DataFrame({
                "int_col": [1, 2, 3],
                "float_col": [1.1, 2.2, 3.3],
                "str_col": ["a", "b", "c"],
                "bool_col": [True, False, True]
            })
            
            save_to_parquet(df, filepath)
            loaded_df = read_parquet(filepath)
            
            assert loaded_df["int_col"].dtype in [int, "int64", "int32"]
            assert loaded_df["float_col"].dtype == float
            assert loaded_df["str_col"].dtype == object
            assert loaded_df["bool_col"].dtype == bool

    def test_parquet_creates_nested_dirs(self):
        """Test that nested directories are created"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            filepath = os.path.join(tmp_dir, "a", "b", "c", "test.parquet")
            df = pd.DataFrame({"col": [1, 2, 3]})
            
            save_to_parquet(df, filepath)
            
            assert os.path.exists(filepath)


class TestLayerPaths:
    """Test suite for layer path getters"""

    def test_bronze_path(self, temp_lake_root):
        """Test bronze layer path"""
        path = get_bronze_path()
        assert "bronze" in path

    def test_silver_path(self, temp_lake_root):
        """Test silver layer path"""
        path = get_silver_path()
        assert "silver" in path

    def test_gold_path(self, temp_lake_root):
        """Test gold layer path"""
        path = get_gold_path()
        assert "gold" in path


class TestSaveAndReadLayers:
    """Test suite for layer-specific save/read operations"""

    def test_save_and_read_bronze(self, temp_lake_root):
        """Test saving and reading from bronze layer"""
        test_data = [{"id": "1", "name": "Test"}]
        filename = "source/entity/ingest_date=2024-01-01/run_id=run1/data.json"
        
        filepath = save_to_bronze(test_data, filename)
        loaded = read_from_bronze(filepath)
        
        assert loaded == test_data
        assert "bronze" in filepath

    def test_save_and_read_silver(self, temp_lake_root):
        """Test saving and reading from silver layer"""
        df = pd.DataFrame({
            "id": [1, 2, 3],
            "name": ["A", "B", "C"]
        })
        filename = "breweries/country=US/state=CA/data.parquet"
        
        filepath = save_to_silver(df, filename)
        loaded = read_from_silver(filepath)
        
        pd.testing.assert_frame_equal(df, loaded)
        assert "silver" in filepath

    def test_save_and_read_gold(self, temp_lake_root):
        """Test saving and reading from gold layer"""
        df = pd.DataFrame({
            "brewery_type": ["micro", "nano"],
            "count": [100, 50]
        })
        filename = "breweries/aggregated/summary.parquet"
        
        filepath = save_to_gold(df, filename)
        loaded = read_from_gold(filepath)
        
        pd.testing.assert_frame_equal(df, loaded)
        assert "gold" in filepath

    def test_read_nonexistent_bronze_file(self, temp_lake_root):
        """Test reading non-existent bronze file raises error"""
        with pytest.raises(FileNotFoundError):
            read_from_bronze("/nonexistent/path/file.json")

    def test_read_nonexistent_silver_file(self, temp_lake_root):
        """Test reading non-existent silver file raises error"""
        with pytest.raises(Exception):  # FileNotFoundError or pyarrow error
            read_from_silver("/nonexistent/path/file.parquet")