# - API config module -
#
# This module contains static info about the APIs

# base url for breweries API
BASE_URL = "https://api.openbrewerydb.org/v1/breweries"

# endpoints
ENDPOINTS = {
    "all_breweries": "",
    "random_brewery": "/random"
}

# behaviour
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
RETRY_BACKOFF_SEC = 3

# page size
DEFAULT_PAGE_SIZE = 100

# headers ?
DEFAULT_HEADER = {

}

# if this wasn't a public API:

# API_KEY = os.getenv("API_KEY")
# AUTH_HEADER = {"Authorization": f"Bearer {API_KEY}"}