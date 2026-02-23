from breweries_pipeline.datalake.filesystem import read_from_silver, save_to_gold


def build_gold(silver_path: str):
    """builds an aggregated view of the quantity of breweries by country and state,
    then saves it to the gold layer as a parquet file"""

    # reads data from silver layer
    breweries_df = read_from_silver(silver_path)

    # aggregate the quantity of breweries by country and state
    agg_breweries = breweries_df.groupby(
        ["brewery_type","country", "state"], observed=True
        ).size().reset_index(name="brewery_count")

    # save the aggregated data to gold layer
    filename = "breweries/aggregated/breweries_by_type_country_state.parquet"
    save_to_gold(agg_breweries, filename)

    # create aggregated view with 

    return filename
