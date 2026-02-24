from airflow.decorators import dag, task
from datetime import timedelta

from breweries_pipeline.jobs.ingest_bronze import ingest_to_bronze
from breweries_pipeline.jobs.transform_silver import transform_silver
from breweries_pipeline.jobs.build_gold import build_gold

@dag(
    dag_id= "breweries",
    schedule_interval= "@daily",
    retries= 2,
    retry_delay= timedelta(minutes=5)
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