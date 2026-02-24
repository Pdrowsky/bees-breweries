import os
import json
import pandas as pd

from breweries_pipeline.config import LAKE_ROOT

# helper functions

def _ensure_dir_exists(path: str) -> None:
    """Ensures that the directory for the given path exists."""
    os.makedirs(path, exist_ok=True)

# approach works for local fs, which is the case, but not on cloud.
def _atomic_json_write(data: list, path: str) -> None:
    """writes data to a json file atomically by writing to a temporary file and then renaming it."""
    _ensure_dir_exists(os.path.dirname(path))
    tmp_path = path + ".tmp"
    with open(tmp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)

def save_to_parquet(df: pd.DataFrame, path: str) -> None:
    """saves a dataframe to a parquet file"""
    _ensure_dir_exists(os.path.dirname(path))
    tmp_path = path + ".tmp"
    df.to_parquet(tmp_path, index=False)
    os.replace(tmp_path, path)

def read_json(path: str) -> list:
    """reads a json file and returns its content as a list"""
    with open(path, "r", encoding="utf-8") as file:
        return json.load(file)
    
def read_parquet(path: str) -> pd.DataFrame:
    """reads a parquet file and returns its content as a dataframe"""
    return pd.read_parquet(path)

# path getters for each layer

def get_bronze_path() -> str:
    """returns the path to the bronze layer"""
    return f"{LAKE_ROOT}/bronze/"

def get_silver_path() -> str:
    """returns the path to the silver layer"""
    return f"{LAKE_ROOT}/silver/"

def get_gold_path() -> str:
    """returns the path to the gold layer"""
    return f"{LAKE_ROOT}/gold/"

# writing functions for each layer

def save_to_bronze(data: list, filename: str) -> str:
    """saves data to the bronze layer as json"""
    save_path = get_bronze_path() + filename
    _atomic_json_write(data, save_path)
    return save_path

def save_to_silver(data: pd.DataFrame, filename: str) -> str:
    """saves data to the silver layer as parquet"""
    save_path = get_silver_path() + filename
    save_to_parquet(data, save_path)
    return save_path

def save_to_gold(data: pd.DataFrame, filename: str) -> str:
    """saves data to the gold layer as parquet"""
    save_path = get_gold_path() + filename
    save_to_parquet(data, save_path)
    return save_path

# reading functions for each layer

def read_from_bronze(path: str) -> list:
    """reads data from the bronze layer json files"""
    return read_json(path)

def read_from_silver(path: str) -> pd.DataFrame:
    """reads data from the silver layer parquet files"""
    return read_parquet(path)

def read_from_gold(path: str) -> pd.DataFrame:
    """reads data from the gold layer parquet files"""
    return read_parquet(path)
