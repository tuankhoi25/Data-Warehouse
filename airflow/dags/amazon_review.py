from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
import pendulum

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
}

with DAG(
    dag_id="amazon_review",
    default_args=default_args,
    schedule=None,
    start_date=pendulum.datetime(2025, 1, 1, tz="Asia/Ho_Chi_Minh"),
    catchup=False,
) as dag:

    raw_task = BashOperator(
        task_id="raw_task",
        bash_command="""
        cd /opt/airflow/dags/dbt_project && \
        dbt run --target prod --select raw --vars '{"logical_date": "{{ (logical_date + macros.timedelta(hours=7)).isoformat() }}"}'
        """
    )

    staging_task = BashOperator(
        task_id="staging_task",
        bash_command="""
        cd /opt/airflow/dags/dbt_project && \
        dbt run --target prod --select staging --vars '{"logical_date": "{{ (logical_date + macros.timedelta(hours=7)).isoformat() }}"}'
        """
    )

    intermediate_task = BashOperator(
        task_id="intermediate_task",
        bash_command="echo 'Skip'"
    )

    marts_task = BashOperator(
        task_id="marts_task",
        bash_command="""
        cd /opt/airflow/dags/dbt_project && \
        dbt run --target prod --select marts --exclude dim_dates --vars '{"logical_date": "{{ (logical_date + macros.timedelta(hours=7)).isoformat() }}"}'
        """
    )

    raw_task >> staging_task >> intermediate_task >> marts_task