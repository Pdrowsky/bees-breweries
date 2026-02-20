import os
import json

from breweries_pipeline.config import LAKE_ROOT


def _ensure_dir_exists(path: str):
    os.makedirs(path, exist_ok=True)

def _atomic_json_write(data, path):
    _ensure_dir_exists(os.path.dirname(path))
    tmp_path = path + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(data, f)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp_path, path)

def get_bronze_path():
    return f"{LAKE_ROOT}/bronze/"

def get_silver_path():
    return f"{LAKE_ROOT}/silver/"

def get_gold_path():
    return f"{LAKE_ROOT}/gold/"

# TODO: implement save functions
def save_to_bronze(data, filename):
    save_path = get_bronze_path() + filename
    print(f"Data saved to {save_path}")
    return

# TODO: implement save functions
def save_to_silver(data, filename):
    save_path = get_silver_path() + filename
    print(f"Data saved to {save_path}")
    return 

# TODO: implement save functions
def save_to_gold(data, filename):
    save_path = get_gold_path() + filename
    print(f"Data saved to {save_path}")
    return 
