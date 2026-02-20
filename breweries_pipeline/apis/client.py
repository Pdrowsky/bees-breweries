# - API client module -
#
# This module contains the requests to the APIs

import time
import requests

from breweries_pipeline.apis.config import (
    BASE_URL,
    ENDPOINTS,
    REQUEST_TIMEOUT,
    MAX_RETRIES,
    RETRY_BACKOFF_SEC,
    DEFAULT_PAGE_SIZE,
    DEFAULT_HEADER
)

# function to return a list of all breweries from the API
def get_all_breweries() -> list:
    # sets up URL
    url = f"{BASE_URL}{ENDPOINTS['all_breweries']}"

    # sets up pagination params
    page = 1
    params = {
        "per_page": DEFAULT_PAGE_SIZE,
        "page": page
    }

    # initializes list to store breweries
    all_breweries = []

    # while API doensn't return empty list
    while True:

        for retry in range(MAX_RETRIES):

            try:
                response = requests.get(url, 
                                        params=params, 
                                        headers=DEFAULT_HEADER, 
                                        timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
                breweries = response.json()
                break

            except requests.exceptions.RequestException as e:
                print(f"Request failed. Cause: {e}. Retrying in {RETRY_BACKOFF_SEC} seconds.")
                time.sleep(RETRY_BACKOFF_SEC)

        if not breweries:
            return all_breweries
        
        all_breweries.extend(breweries)
        page += 1
        params['page'] = page




