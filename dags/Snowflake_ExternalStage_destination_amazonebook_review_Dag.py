 
import datetime
from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from utility.utility import send_email,get_lastextract_snowflake,load_data_to_snowsql_ext_table,load_ext_table_to_snowsql_table,update_snowsql_config
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule

dag_name = 'Snowflake_ExternalStage_destination_amazonebook_review_Dag'

def get_meta_data():
    return Variable.get(f"{dag_name}_config", deserialize_json=True)

def get_the_file_path():
    main_data = get_meta_data()

    date_df = get_lastextract_snowflake(main_data)

    # Snowflake/amazonebooks_external/amazonebook_reviews/20240227/20240227-amazonebook-reviews.csv

    partition_date = (date_df['LAST_EXTRACT_DATE'][0] + datetime.timedelta(days=1)).strftime("%Y%m%d")
    file_abs_path = f'''{main_data["output_dir_path"]}/{main_data["database_name"]}/{main_data["table_name"]}/{partition_date}/{partition_date}-{(main_data["table_name"]).replace("_","-")}.{main_data["file_type"]}'''

    main_data["file_abs_path"] = file_abs_path

    # Updating Airflow Variable
    Variable.set(key=f"{dag_name}_config", value=main_data, serialize_json=True)


def send_dag_failed_email(context):
    main_data = get_meta_data()

    Subject  = f' Dag => {(str(context["dag"]).split(":")[1]).replace(">","")} Failed'

    msg = f'''
        Airflow Dag {(str(context["dag"]).split(":")[1]).replace(">","")} Failed,
        Task Id => {(str(context["task"]).split(":")[1]).replace(">","")},
        Exception => {context["exception"]}
    '''

    arn = main_data.get("arn")
    send_email({"Subject":Subject,"message":msg,"arn":arn})

def load_data_to_ext_table():
    main_data = get_meta_data()
    date_partition = []
    date_partition.append(main_data["file_abs_path"].replace(f'''{main_data["output_dir_path"]}/{main_data["database_name"]}/{main_data["table_name"]}/''',""))
    load_data_to_snowsql_ext_table(main_data,date_partition)

def load_data_from_ext_table_to_table():
    load_ext_table_to_snowsql_table(get_meta_data())

def update_config():
    update_snowsql_config(get_meta_data())

with DAG(
    dag_id=dag_name,
    start_date=datetime.datetime(2024,1,1),
    schedule=None
    ) as dag:

    Get_current_data_path_task = PythonOperator(
            task_id='Get_current_data_path',
            python_callable=get_the_file_path,
            on_failure_callback=send_dag_failed_email
        )
    s3_sensor = S3KeySensor(
            task_id = 's3_file_check',
            poke_interval=5,
            timeout=60,
            soft_fail=False,
            mode='reschedule',
            on_failure_callback=send_dag_failed_email,
            bucket_key = (get_meta_data())["file_abs_path"],
            bucket_name = 'lz-snowflake',
            aws_conn_id = 'aws_default'
            #,wildcard_match = True
        )
    load_data_to_ext_table_task = PythonOperator(
            task_id='Load_data_to_ext_table',
            python_callable=load_data_to_ext_table,
            on_failure_callback=send_dag_failed_email
        )
    load_data_from_ext_table_to_table_task = PythonOperator(
            task_id='Load_data_to_table',
            python_callable=load_data_from_ext_table_to_table,
            on_failure_callback=send_dag_failed_email
        )
    update_config_task = PythonOperator(
            task_id='Update_Config',
            python_callable=update_config,
            on_failure_callback=send_dag_failed_email,
            trigger_rule=TriggerRule.ALL_DONE
        )

    Start = EmptyOperator(task_id="Start")
    End   = EmptyOperator(task_id="End")

    Start >> Get_current_data_path_task >> s3_sensor >> load_data_to_ext_table_task >> load_data_from_ext_table_to_table_task >> update_config_task >> End
