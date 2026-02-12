from datetime import datetime
import pendulum
from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator  # Use SnowflakeOperator or SQLExecuteQueryOperator
from airflow.sdk import task  
from airflow.sdk import dag, task 

@dag(
    dag_id="sf_xcom",
    schedule=None,  # Or your desired schedule
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    tags=["snowflake", "xcom"],
)
def sf_xcom_dag():
    
    @task
    def vals():
        return 20
    
    first_val = vals()
    
    snowflake_task = SQLExecuteQueryOperator(
        task_id='run_sql_after_copy_inner',
        sql="""
        INSERT INTO LEARNING.PUBLIC.AIRFLOW_CUST_SF1 
        SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER	
        LIMIT {{ ti.xcom_pull(task_ids='vals') }};
        """,
        conn_id="snowflake",
    )
    
    first_val >> snowflake_task	

sf_xcom_dag()

