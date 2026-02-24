from airflow.decorators import dag, task
from datetime import timedelta

from breweries_pipeline.jobs.ingest_bronze import ingest_to_bronze
from breweries_pipeline.jobs.transform_silver import transform_silver
from breweries_pipeline.jobs.build_gold import build_gold

@dag(
    dag_id= "breweries",
    schedule_interval= "@daily", # Run daily
    retries= 2, # Retry run 2 times max
    retry_delay= timedelta(minutes=5), # Delay between retries
    catchup=False, # Don't backfill missed runs
    owner="data-engineering", # Team responsible for the DAG
    email=["pedro.virgilio@bees.com"], # Who to alert
    email_on_failure=True, # Alert on failure
    email_on_retry=False, # Don't spam on retry
    max_active_runs=1, # Only one run at a time
)
def breweries():

    @task
    def bronze_task(**context) -> str:
        run_id = context["run_id"]
        ds = context["ds"] # YYYY-MM-DD
        return ingest_to_bronze(run_id=run_id, execution_time=ds)

    @task
    def silver_task(bronze_path: str) -> str:
        return transform_silver(bronze_path=bronze_path)

    @task
    def gold_task(silver_path: str) -> str:
        return build_gold(silver_path=silver_path)

    bronze_path = bronze_task()
    silver_path = silver_task(bronze_path)
    gold_task(silver_path)

breweries()