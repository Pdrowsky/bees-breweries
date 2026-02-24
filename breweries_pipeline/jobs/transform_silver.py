from breweries_pipeline.datalake.filesystem import read_from_bronze, save_to_silver
from breweries_pipeline.transform.data_manipulation import json_to_dataframe, split_df_by_column, clean_string

# load data from bronze layer

def transform_silver(bronze_path: str):
    """transforms the data from the bronze layer into columnar format and partitions it by country and state,
    then saves it to the silver layer as parquet files"""
    
    # load data from bronze layer
    breweries = read_from_bronze(bronze_path)

    # transform data into dataframe
    breweries_df = json_to_dataframe(breweries)

    # drop unnecessary columns to optimize storage and performance
    keep = ["id", "name", "brewery_type", "country", "state", "city"]
    breweries_df = breweries_df[keep]

    # remove duplicates based on id
    breweries_df = breweries_df.drop_duplicates(subset="id")

    # drop rows with missing country or state values (ensure partitioning columns are not null)
    breweries_df = breweries_df.dropna(subset=["country", "state"])

    # split dataframe based on country column
    breweries_by_country = split_df_by_column(breweries_df, "country")

    # split each country dataframe by state
    for country, df in breweries_by_country.items():
        breweries_by_country[country] = split_df_by_column(df, "state")

    # -> could also be done by city to increase granularity,
    # however it would lead to a very high number of small files which could impact performance,
    # specially on a local filesystem (such as the one mocking the lake in this case) where the
    # folders are not virtual as in cloud

    # save each dataframe to the silver layer, partitioned by country and state
    for country, states in breweries_by_country.items():
        for state, df in states.items():

            df = df.drop(columns=["country", "state"])

            # clean country and state strings to ensure valid file paths
            filename = f"breweries/country={clean_string(country)}/state={clean_string(state)}/data.parquet"
            filepath = save_to_silver(df, filename)

    # ============= MONITORING AND ALERTING =============
    # Post transformation, monitoring hooks to Prometheus (or other monitoring system) would be triggered here
    # Content: unique record count, records dropped, "unknown" values count, etc.
    # Alerts: send if unknown or dropped values are too high
    # ===================================================

    return filepath.split('/country')[0]
