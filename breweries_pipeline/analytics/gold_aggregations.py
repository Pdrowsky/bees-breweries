import pandas as pd


def BreweriesByCountryState(breweries_df):
    # aggregate the quantity of breweries by country and state
    agg_breweries = breweries_df.groupby(
        ["brewery_type","country", "state"], observed=True
        ).size().reset_index(name="brewery_count")
    return agg_breweries
