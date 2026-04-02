from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "tinglu",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="airbnb_snowflake_dbt_pipeline",
    default_args=default_args,
    description="End-to-end Airbnb analytics pipeline with Snowflake + dbt",
    start_date=datetime(2026, 4, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["airbnb", "dbt", "snowflake", "analytics-engineering"],
) as dag:

    dbt_run_bronze = BashOperator(
        task_id="dbt_run_bronze",
        bash_command='echo "running bronze layer"'
    )

    dbt_run_silver = BashOperator(
        task_id="dbt_run_silver",
        bash_command='echo "running silver layer"'
    )

    dbt_run_gold = BashOperator(
        task_id="dbt_run_gold",
        bash_command='echo "running gold layer"'
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command='echo "running snapshot"'
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command='echo "running tests"'
    )

    dbt_run_bronze >> dbt_run_silver >> dbt_run_gold >> dbt_snapshot >> dbt_test