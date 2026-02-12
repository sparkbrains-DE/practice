from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2026, 2, 1),
    'retries': 1,
}

dag = DAG(
    'snowflake_copy_and_tasks_example',
    default_args=default_args,
    description='DAG with Snowflake COPY and SQL tasks',
    catchup=False,
    tags=['snowflake', 'data-engineering'],
)


def run_snowflake_sql(**context):
    """	
    Lazy import Snowflake operator INSIDE task function.
    This avoids heavy imports during DAG parsing.
    """
    from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator

    op = SQLExecuteQueryOperator(
        task_id='run_sql_after_copy_inner',
        sql="""
        CREATE TABLE LEARNING.PUBLIC.AIRFLOW_CUST_SF1 AS
        SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER
        LIMIT 100;
        """,
        conn_id="snowflake",
    )
    return op.execute(context=context)


sql_task = PythonOperator(
    task_id="run_sql_after_copy",
    python_callable=run_snowflake_sql,
    dag=dag,
)

