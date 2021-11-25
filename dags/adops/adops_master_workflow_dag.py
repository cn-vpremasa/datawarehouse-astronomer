from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Connection
from airflow import settings
from datetime import datetime, timedelta, date
import os
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.email import send_email_smtp
from dags.utils.common_fun_utils import *
from dags.utils.common_utils import read_config_file
from dags.utils.common_sensor_funct_utils import ClientRes
from dags.utils.common_sensor_funct_utils import Criteria
from dags.utils.common_sensor_funct_utils import *
from dags.utils.common_utils import read_config_file
import pandas as pd
import pandasql as ps
import time

CONFIG = read_config_file()
S3_PATH = 's3://' + CONFIG['BUCKET_NAME'] + '/'

def print_task_type(**kwargs):
    print(f"The {kwargs['task_type']} task has completed.")

def create_dag(dag_id, schedule_time):
    default_args = {
        'owner': CONFIG['dags']['owner'],
        'depends_on_past': CONFIG['dags']['depends_on_past'],
        'retries': CONFIG['dags']['retries'],
        'retry_delay': CONFIG['dags']['retry_delay'],
        'on_failure_callback': failure_msg,
        'provide_context': True
    }

    new_dag = DAG(dag_id,
                  catchup=False,
                  max_active_runs=1,
                  schedule_interval=schedule_time,
                  default_args=default_args,
                  start_date=datetime(2021, 11, 17, 20, 30)
                  )

    with new_dag:
        start_task = PythonOperator(
            task_id='starting_task',
            python_callable=print_task_type,
            op_kwargs={'task_type': 'starting'}
        )

        gam_validation_dag = TriggerDagRunOperator(
            task_id="gam_validation"+ "_"+ CONFIG['ENV'] + "_"+ "dag",
            trigger_dag_id="gam_validation"+ "_"+ CONFIG['ENV'] + "_"+ "dag",
            wait_for_completion=True
        )

        gam_standardization_dag = TriggerDagRunOperator(
            task_id=CONFIG['dags']['gam_stnd_dag_id'] + '_'  +CONFIG['ENV'] + '_incremental_dag',
            trigger_dag_id=CONFIG['dags']['gam_stnd_dag_id'] + '_'  +CONFIG['ENV'] + '_incremental_dag',
            wait_for_completion=True
        )

        gam_dimension_dag = TriggerDagRunOperator(
            task_id=CONFIG['dags']['gam_dim_dag_id'] + '_' +CONFIG['ENV'] + '_incremental_dag',
            trigger_dag_id=CONFIG['dags']['gam_dim_dag_id'] + '_' +CONFIG['ENV'] + '_incremental_dag',
            wait_for_completion=True
        )

        end_task = PythonOperator(
            task_id='end_task',
            python_callable=print_task_type,
            op_kwargs={'task_type': 'ending'}
        )

        start_task >> [gam_validation_dag, gam_standardization_dag] >> gam_dimension_dag >>  end_task

    return new_dag

globals()[CONFIG['dags']['gam_master_dag_id']] = create_dag(
    CONFIG['dags']['gam_master_dag_id'] + '_' +CONFIG['ENV'] + '_incremental_dag' , CONFIG['dags']['gam_schedule'])