from breweries_pipeline.datalake.filesystem import read_from_silver, save_to_gold
from breweries_pipeline.analytics.gold_aggregations import BreweriesByCountryState


def build_gold(silver_path: str):
    """builds an aggregated view of the quantity of breweries by country and state,
    then saves it to the gold layer as a parquet file"""

    # reads data from silver layer
    breweries_df = read_from_silver(silver_path)

    agg_breweries = BreweriesByCountryState(breweries_df)

    # save the aggregated data to gold layer
    filename = "breweries/aggregated/breweries_by_type_country_state.parquet"
    save_to_gold(agg_breweries, filename)

    # create aggregated view with 

    return filename
