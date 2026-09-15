from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'peter',
    'start_date': datetime(2026, 9, 10),
}

with DAG(
    dag_id='weather_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False
) as dag:
    task_1_ingestion = BashOperator(
        task_id='run_main_py',
        bash_command='cd /opt/airflow/project && python src/main.py'
    )

    task_2_dbt_run = BashOperator(
        task_id='run_dbt_run',
        bash_command='cd /opt/airflow/project/weather_dbt && dbt run'
    )

    task_3_dbt_test = BashOperator(
        task_id='run_dbt_test',
        bash_command='cd /opt/airflow/project/weather_dbt && dbt test'
    )
    task_1_ingestion >> task_2_dbt_run >> task_3_dbt_test