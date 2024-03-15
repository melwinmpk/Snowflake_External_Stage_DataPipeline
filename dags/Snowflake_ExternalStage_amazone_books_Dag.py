 

import datetime
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from utility.database_helper import database_helper,snowflake_helper
from utility.utility import get_lastextract_mysql, update_mysql_config, get_currentdate_extract_mysql, upload_data_to_s3
import json

main_data = {
        'destination_database_name' : 'amazonebooks_external',
        'source_database_name' : 'amazonebooks',
        'schema' : 'PUBLIC',
        'stage' : 'amazone_books_stage',
        'table_name' : 'amazone_books',
        'destination_bucket' : 'lz-snowflake',
        'destination_s3_dir_path' : 'Snowflake/',
        'warehouse' : 'COMPUTE_WH',
        'load_type' : 'Incremental'
       }

def Upload_data_to_s3(ti):

    # Get Last extract Date from Snowflake Config Schema
    snow_df = get_lastextract_mysql(main_data)

    # Get Current Extract Dates (It couls be moer than 1 Date) get it from on-prem Database
    current_extract_date_objs = get_currentdate_extract_mysql(snow_df,main_data)

    data = {
        "destination_database_name":main_data["destination_database_name"],
        "table_name":main_data["table_name"],
        "source_database_name": main_data["source_database_name"],
        "current_extract_date_objs":current_extract_date_objs,
        "destination_bucket":main_data["destination_bucket"],
        "destination_s3_dir_path":main_data["destination_s3_dir_path"]
           }

    upload_data_to_s3(data)

def update_config():
    data = {
    "database_name" : main_data["source_database_name"],
    "table_name" : main_data["table_name"]
        }
    update_mysql_config(data)


with DAG(
    dag_id="Snowflake_ExternalStage_amazone_books_Dag",
    start_date=datetime.datetime(2024,1,1),
    schedule=None
    ) as dag:

    Upload_data_to_S3_task = PythonOperator(
            task_id='Upload_data_to_S3',
            python_callable=Upload_data_to_s3
        )

    Update_configs_task = PythonOperator(
            task_id='Update_Configs',
            python_callable=update_config
        )


    Start = EmptyOperator(task_id="Start")
    End   = EmptyOperator(task_id="End")
    Start >> Upload_data_to_S3_task >> Update_configs_task >> End
