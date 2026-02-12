from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
import pendulum


@dag(
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False
)
def conditional_data_insertion():

    @task.branch
    def check_for_table():
        hook = SnowflakeHook(snowflake_conn_id="snowflake")

        query = """
        SELECT COUNT(*)
        FROM LEARNING.INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = 'PUBLIC'
        AND TABLE_NAME = 'AIRFLOW_CUST_SF1';
        """

        result = hook.get_first(query)
        count = result[0]

        if count > 0:
            return "insert_task"
        else:
            return "create_insert_task"

    @task
    def create_insert_task():
        hook = SnowflakeHook(snowflake_conn_id="snowflake")

        sql = """
        CREATE TABLE LEARNING.PUBLIC.AIRFLOW_CUST_SF1 AS
        SELECT *
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER
        LIMIT 100;
        """

        hook.run(sql)

    @task
    def insert_task():
        hook = SnowflakeHook(snowflake_conn_id="snowflake")

        sql = """
        INSERT INTO LEARNING.PUBLIC.AIRFLOW_CUST_SF1
        SELECT *
        FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF100.CUSTOMER
        LIMIT 20;
        """

        hook.run(sql)

    branch = check_for_table()
    branch >> [insert_task(), create_insert_task()]


conditional_data_insertion()
