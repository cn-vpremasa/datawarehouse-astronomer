from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
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

ENV = CONFIG['ENV']
CONFIG = read_config_file()
S3_PATH = 's3://' + CONFIG['BUCKET_NAME'] + '/'

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
                  start_date=datetime(2021, 11, 23, 20, 30)
                  )

    with new_dag:
        busmap_download_notebook_params = {
            "existing_cluster_id": CONFIG['cluster']['cluster_id'],
            "notebook_task": {
                'notebook_path': CONFIG['NOTEBOOK_PATHS']['BUSMAP_NOTEBOOK'],
                'base_parameters': {
                    "ENV": CONFIG['ENV'],
                    "BUCKET": S3_PATH,
                    "LOAD_TYPE": os.environ['LOAD_TYPE'],
                    "RUN_DATE": (datetime.now()).strftime('%Y-%m-%d') }
            }
        }

        stnd_location_notebook_params = {
            'existing_cluster_id': CONFIG['cluster']['cluster_id'],
            'notebook_task': {
                'notebook_path': CONFIG['NOTEBOOK_PATHS']['STND_DRIVER_NOTEBOOK'],
                'base_parameters': {
                    "ENV": CONFIG['ENV'],
                    "BUCKET": S3_PATH,
                    'PROCESS_NAME': CONFIG['PROCESS_NAME']['LOCATION'],
                    'LOAD_TYPE': os.environ['LOAD_TYPE'],
                    'SOURCE': os.environ['SOURCE_GAM'],
                    "LOGDATE": (datetime.now()).strftime('%Y-%m-%d')},
                    'MARKETS':os.environ['MARKETS']
            }
        }
        stnd_brand_notebook_params = {
            'existing_cluster_id': CONFIG['cluster']['cluster_id'],
            'notebook_task': {
                'notebook_path': CONFIG['NOTEBOOK_PATHS']['STND_DRIVER_NOTEBOOK'],
                'base_parameters': {
                    'ENV': CONFIG['ENV'],
                    'BUCKET': S3_PATH,
                    'PROCESS_NAME': CONFIG['PROCESS_NAME']['BRAND'],
                    'LOAD_TYPE': os.environ['LOAD_TYPE'],
                    'SOURCE': os.environ['SOURCE_GAM'],
                    'LOGDATE': (datetime.now()).strftime('%Y-%m-%d'),
                    'MARKETS': os.environ['MARKETS']
                }
            }
        }

        stnd_advertiser_notebook_params = {
            'existing_cluster_id': CONFIG['cluster']['cluster_id'],
            'notebook_task': {
                'notebook_path': CONFIG['NOTEBOOK_PATHS']['STND_DRIVER_NOTEBOOK'],
                'base_parameters': {
                    'ENV': CONFIG['ENV'],
                    'BUCKET': S3_PATH,
                    'PROCESS_NAME': CONFIG['PROCESS_NAME']['ADVERTISER'],
                    'LOAD_TYPE': os.environ['LOAD_TYPE'],
                    'SOURCE': os.environ['SOURCE_GAM'],
                    'LOGDATE': (datetime.now()).strftime('%Y-%m-%d'),
                    'MARKETS': os.environ['MARKETS']
                }
            }
        }


        stnd_device_notebook_params = {
            'existing_cluster_id': CONFIG['cluster']['cluster_id'],
            'notebook_task': {
                'notebook_path': CONFIG['NOTEBOOK_PATHS']['STND_DRIVER_NOTEBOOK'],
                'base_parameters': {
                    'ENV': CONFIG['ENV'],
                    'BUCKET': S3_PATH,
                    'PROCESS_NAME': CONFIG['PROCESS_NAME']['DEVICE'],
                    'LOAD_TYPE': os.environ['LOAD_TYPE'],
                    'SOURCE': os.environ['SOURCE_GAM'],
                    'LOGDATE': (datetime.now()).strftime('%Y-%m-%d'),
                    'MARKETS': os.environ['MARKETS']
                }
            }
        }

        start = DummyOperator(task_id='start')

        gam_stnd_check_sensor = PythonOperator(task_id='gam_standardization_check_sensor_task',
                               python_callable=Criteria,
                               op_args=(CONFIG['gam_stand_src_list_dict'], CONFIG['gam_stand_sql_query']))

        busmap_download_task = DatabricksSubmitRunOperator(
            task_id='busmap_download_task',
            databricks_conn_id=new_databricks_conn.conn_id,
            json=busmap_download_notebook_params)

        stnd_location_task = DatabricksSubmitRunOperator(
            task_id='stnd_location_task',
            databricks_conn_id=new_databricks_conn.conn_id,
            json=stnd_location_notebook_params)

        stnd_brand_task = DatabricksSubmitRunOperator(
            task_id='stnd_brand_task',
            databricks_conn_id=new_databricks_conn.conn_id,
            json=stnd_brand_notebook_params)

        stnd_advertiser_task = DatabricksSubmitRunOperator(
            task_id='stnd_advertiser_task',
            databricks_conn_id=new_databricks_conn.conn_id,
            json=stnd_advertiser_notebook_params)

        stnd_device_task = DatabricksSubmitRunOperator(
            task_id='stnd_device_task',
            databricks_conn_id=new_databricks_conn.conn_id,
            json=stnd_device_notebook_params)

        email_notification = DummyOperator(task_id='email_notification',
                                           trigger_rule=TriggerRule.ALL_SUCCESS,
                                           on_success_callback=success_msg )

        end = DummyOperator(task_id='end')

        start >> gam_stnd_check_sensor >> busmap_download_task >> \
        [stnd_location_task,stnd_brand_task,stnd_advertiser_task,stnd_device_task] >> email_notification >> end

    return new_dag

new_databricks_conn = setup_databricks_connection()

globals()[CONFIG['dags']['gam_stnd_dag_id']] = create_dag(
    CONFIG['dags']['gam_stnd_dag_id'] + '_'  +CONFIG['ENV'] + '_incremental_dag' , CONFIG['dags']['gam_stnd_schedule'])