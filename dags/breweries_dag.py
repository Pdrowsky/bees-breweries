from airflow.decorators import dag, task
from datetime import timedelta
import pendulum

from breweries_pipeline.jobs.ingest_bronze import ingest_to_bronze
from breweries_pipeline.jobs.transform_silver import transform_silver
from breweries_pipeline.jobs.build_gold import build_gold


default_args = {
    "owner": "data-engineering", # Team responsible for the DAG
    "retries": 2, # Retry run 2 times max
    "retry_delay": timedelta(minutes=5), # Delay between retries
    "email": ["pedro.virgilio@bees.com"], # Who to alert
    "email_on_failure": True, # Alert on failure
    "email_on_retry": False, # Don't spam on retry
}

@dag(
    dag_id="breweries",
    schedule="@daily",
    start_date=pendulum.datetime(2027, 2, 24, tz="UTC"), # future start date to avoid dag running as soon as the code is deployed
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
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