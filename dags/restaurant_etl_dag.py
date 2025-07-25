from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.email import EmailOperator
from airflow.utils.task_group import TaskGroup
import logging
import sys
sys.path.append('/opt/airflow/project')

#importing scripts
from pyspark_scripts.clean_restaurant_details import main


default_args = {
    'owner': 'karthik',
    'start_date': datetime(2025, 7, 25),
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': False
}

with DAG(
    dag_id = 'restaurant_etl',
    description = 'a dag to trigger to etl pieline form s3 to snowflake db',
    schedule_interval = '@daily',
    default_args = default_args,
    catchup = False
) as my_dag:
    
    start = DummyOperator(
        task_id = 'start'
    )

    clean_data = PythonOperator(
        task_id = 'pyspark_clean_data',
        python_callable = main
    )

    with TaskGroup(
        group_id = 'snowflake_task_group'
    ) as tsk_group:
        
        try:
            with open('/opt/airflow/project/snowflake/silver/create_table.sql', 'r') as fh:
                sql_content = fh.read()
        except Exception as e:
            logging.error("create_table.sql not found.")
        
        create_table = SnowflakeOperator(
            task_id = 'create_table',
            snowflake_conn_id = 'snowflake_conn',
            sql = sql_content
        )
        
        load_silver_data = SnowflakeOperator(
            task_id = 'load_silver',
            snowflake_conn_id = 'snowflake_conn',
            sql = '''
            CALL restaurant_dwh.silver.load_data_to_silver();
            '''
        )

        load_gold_data = SnowflakeOperator(
            task_id = 'load_gold_star',
            snowflake_conn_id = 'snowflake_conn',
            sql = '''
            CALL restaurant_dwh.gold.load_data_to_gold();
            '''
        )

        create_table >> load_silver_data >> load_gold_data

    send_email = EmailOperator(
        task_id='send_alert',
        to='karthikesan.in@gmail.com',
        subject='DAG run status',
        html_content="DAG ran successfully",
        conn_id='email_conn',
        trigger_rule='all_done'
    )

    end = DummyOperator(
        task_id = 'end'
    )
    

    start >> clean_data >> tsk_group >> send_email >> end
    # start >> tsk_group >> end