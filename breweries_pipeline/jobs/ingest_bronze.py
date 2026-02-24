from airflow.exceptions import AirflowFailException
import logging

from breweries_pipeline.apis.client import get_all_breweries
from breweries_pipeline.datalake.filesystem import save_to_bronze
from breweries_pipeline.quality.contract import validate_breweries_data
from breweries_pipeline.quality.integrity import validate_breweries_fields


logger = logging.getLogger(__name__)
source = "breweries_api"
entity = "breweries"

def ingest_to_bronze(run_id: str, execution_time: str):
    # fetch the breweries data form the API
    breweries = get_all_breweries()

    # get data contract validation results
    is_valid, errors = validate_breweries_data(breweries)

    # logs if validation failed
    if not is_valid:
        logger.warning("Data contract validation failed.")

    # checks for critical errors that stop the pipeline execution. Hard fail, can't proceed with invalid data type
    if errors["data_type"]:
        raise AirflowFailException("Invalid data type returned by API, expected list of breweries.")

    # hard fail in this case since we're fetching all time data
    elif errors["data_empty"]:
        raise AirflowFailException("No data returned by API, expected a list of breweries.")

    # checks for non critical errors that allow the pipeline to proceed
    fields_errors = validate_breweries_fields(breweries)
    missing = fields_errors.get("missing_fields_indexes", [])

    if missing:
        logger.warning("Some records are missing mandatory fields at indexes: %s", missing) # logs errors found
        breweries = [b for idx, b in enumerate(breweries) if idx not in set(missing)] # remove invalid records

        if not breweries: # if no records left, hard fails
            raise AirflowFailException("All records are missing mandatory fields, no valid data to save to bronze layer.")

    # saves data to bronze layer
    filename = f"{source}/{entity}/exec_time={execution_time}/run_id={run_id}/data.json"
    file_path = save_to_bronze(breweries, filename)

    logger.info("Data successfully saved to bronze layer at: %s", file_path)

    # ============= MONITORING AND ALERTING =============
    # Post ingestion, monitoring hooks to Prometheus (or other monitoring system) would be triggered here
    # Content: metrics about the ingestion process (e.g. number of records ingested, validation errors, etc.)
    # Alerts: if number of records ingested is too high or too low, if integrity errors are above a certain threshold, etc.
    # ===================================================

    return file_path
