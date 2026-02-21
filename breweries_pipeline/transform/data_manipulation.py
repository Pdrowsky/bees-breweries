import pandas as pd
import unicodedata
import re

def json_to_dataframe(data: list) -> pd.DataFrame:
    """converts a list of json objects into a pandas dataframe"""
    return pd.DataFrame(data)

def split_df_by_column(df: pd.DataFrame, column: str) -> dict:
    """splits a dataframe into multiple dataframes based on the unique values of a given column,
    returns a dictionary with the unique values as keys and the corresponding dataframes as values"""

    splited_df = {}

    for key, group in df.groupby(column):
        if pd.isna(key):
            # if there is no value, bucket it under 'unknown' key
            key = "unknown"
        splited_df[key] = group.reset_index(drop=True)
    
    return splited_df

def clean_string(string: str) -> str:
    """cleans a string by removing special characters, extra spaces and converting to lowercase"""

    # remove accents
    string = unicodedata.normalize('NFKD', string).encode('ascii', 'ignore').decode('ascii')
    # replace anything not alphanumeric or dot/dash with underscore
    string = re.sub(r'[^a-zA-Z0-9.-]', '_', string)
    # rollapse multiple underscores
    string = re.sub(r'_+', '_', string)
    # remove leading/trailing dots/underscores
    return string.strip('._').lower() or '_'

