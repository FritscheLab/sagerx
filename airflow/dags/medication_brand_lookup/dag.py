import pendulum

from airflow.exceptions import AirflowSkipException
from airflow.decorators import task
from airflow.providers.postgres.operators.postgres import PostgresOperator

from airflow_operator import create_dag
from common_dag_tasks import get_most_recent_dag_run, get_ds_folder
from sagerx import read_sql_file

dag_id = "medication_brand_lookup"

dag = create_dag(
    dag_id=dag_id,
    schedule="0 6 * * 2",
    start_date=pendulum.yesterday(),
    catchup=False,
    concurrency=1,
    max_active_runs=1,
)

with dag:
    ds_folder = get_ds_folder(dag_id)

    @task
    def require_recent_build_marts():
        last_run = get_most_recent_dag_run("build_marts")
        if last_run is None:
            raise AirflowSkipException("build_marts has never run")

    refresh_mv = PostgresOperator(
        task_id="refresh_medication_brand_lookup",
        postgres_conn_id="postgres_default",
        sql=read_sql_file(ds_folder / "load_medication_brand_lookup.sql"),
        dag=dag,
    )

    require_recent_build_marts() >> refresh_mv
