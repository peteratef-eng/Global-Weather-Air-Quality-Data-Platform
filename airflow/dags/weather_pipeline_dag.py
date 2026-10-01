from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator    
from airflow.providers.smtp.notifications.smtp import SmtpNotifier    

ALERT_EMAIL = "petterattef763@gmail.com"

failure_email = SmtpNotifier(                                          
    to=ALERT_EMAIL,
    subject="❌ Airflow failed: {{ dag.dag_id }}.{{ ti.task_id }}",
    html_content=(
        "<h3>Task failed</h3>"
        "<p><b>DAG:</b> {{ dag.dag_id }}<br>"
        "<b>Task:</b> {{ ti.task_id }}<br>"
        "<b>Run:</b> {{ run_id }}<br>"
        "<b>Try:</b> {{ ti.try_number }}</p>"
        "<p>Check the task logs in the Airflow UI.</p>"
    ),
)

default_args = {
    "owner": "peter",
    "start_date": datetime(2026, 9, 10),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": failure_email,                      
}

with DAG(
    dag_id="weather_pipeline",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
) as dag:
    task_1_ingestion = BashOperator(
        task_id="run_main_py",
        bash_command="cd /opt/airflow/project && python src/main.py",
    )

    task_2_dbt_run = BashOperator(
        task_id="run_dbt_run",
        bash_command="cd /opt/airflow/project/weather_dbt && dbt run",
    )

    task_3_dbt_test = BashOperator(
        task_id="run_dbt_test",
        bash_command="cd /opt/airflow/project/weather_dbt && dbt test",
    )

    task_1_ingestion >> task_2_dbt_run >> task_3_dbt_test