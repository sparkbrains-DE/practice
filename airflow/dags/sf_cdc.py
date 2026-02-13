# from airflow.sdk import dag
# from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
# import pendulum


# @dag(
#     dag_id="stored_procedure_pipeline_operator_style",
#     start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
#     schedule=None,
#     catchup=False,
# )
# def stored_procedure_pipeline():

#     insert_task = SQLExecuteQueryOperator(
#         task_id="insert_records",
#         sql="CALL INSERT_RECORDS();",
#         conn_id="snowflake",
#         autocommit=True,
#     )

#     merge_task = SQLExecuteQueryOperator(
#         task_id="merge_records",
#         sql="CALL MERGE_RECORDS();",
#         conn_id="snowflake",
#         autocommit=True,
#     )

#     audit_task = SQLExecuteQueryOperator(
#         task_id="audit_insert",
#         sql="CALL AUDIT_INSERT();",
#         conn_id="snowflake",
#         autocommit=True,
#     )

#     insert_task >> merge_task >> audit_task


# stored_procedure_pipeline()










from airflow.sdk import dag
from airflow.providers.snowflake.operators.snowflake import SQLExecuteQueryOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.empty import EmptyOperator
import pendulum


@dag(
    dag_id="stored_procedure_pipeline_operator_style",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
)
def stored_procedure_pipeline():

    insert_task = SQLExecuteQueryOperator(
        task_id="insert_records",
        sql="CALL INSERT_RECORDS();",
        conn_id="snowflake",
        autocommit=True,
    )

    merge_task = SQLExecuteQueryOperator(
        task_id="merge_records",
        sql="CALL MERGE_RECORDS();",
        conn_id="snowflake",
        autocommit=True,
        do_xcom_push=True,  # Enable XCom push to capture output
    )

    def check_merge_status(**context):
        """Check merge task output and decide next task"""
        ti = context['ti']
        merge_result = ti.xcom_pull(task_ids='merge_records')
        
        # Parse the result - adjust based on your stored procedure's actual output format
        if merge_result and len(merge_result) > 0:
            # Extract the status from the result
            status = str(merge_result[0][0]).strip().upper()
            
            if status == "SUCCESS":
                return "audit_insert" 
            else:
                return "handle_merge_failure"  
        else:
            return "handle_merge_failure"

    branch_task = BranchPythonOperator(
        task_id="check_merge_result",
        python_callable=check_merge_status,
    )

    handle_failure = SQLExecuteQueryOperator(
        task_id="handle_merge_failure",
        sql="CALL INSERT_INTO_AUDIT();",  
        conn_id="snowflake",
        autocommit=True,
    )
    
    insert_into_duplicates = SQLExecuteQueryOperator(
        task_id="insert_into_duplicates",
        sql="CALL INSERT_INTO_DUPLICATES_TABLE();",  
        conn_id="snowflake",
        autocommit=True,
    )
    
    insert_into_unique = SQLExecuteQueryOperator(
        task_id="insert_into_unique",
        sql="CALL INSERT_UNIQUE_RECS_FROM_AUDIT();",  
        conn_id="snowflake",
        autocommit=True,
    )
    
    merge_correct_records = SQLExecuteQueryOperator(
        task_id="merge_correct_records",
        sql="CALL MERGE_UNIQUE_RECORDS();",  
        conn_id="snowflake",
        autocommit=True,
    )


    audit_task = SQLExecuteQueryOperator(
        task_id="audit_insert",
        sql="CALL AUDIT_INSERT();",
        conn_id="snowflake",
        autocommit=True,
    )

    join_task = EmptyOperator(
        task_id="join",
        trigger_rule="none_failed_min_one_success",
    )

    # DAG dependencies
    insert_task >> merge_task >> branch_task
    branch_task >> audit_task >> join_task
    branch_task >> handle_failure >> insert_into_duplicates >> insert_into_unique >> merge_correct_records >> audit_task >> join_task


stored_procedure_pipeline()