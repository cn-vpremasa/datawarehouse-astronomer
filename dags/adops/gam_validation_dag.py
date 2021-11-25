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


env = os.environ['ENV']
if env=='dev':
  os.environ['ENVIRONMENT'] = 'nonprod'
from tardis import client

CONFIG = read_config_file()
new_databricks_conn = setup_databricks_connection()
AUTH_FILE = CONFIG['AUTH_FILE']

default_args = {
    'owner': CONFIG['dags']['owner'],
    'depends_on_past': CONFIG['dags']['depends_on_past'],
    'retries': CONFIG['dags']['retries'],
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': failure_msg,
    'provide_context': True
}
with DAG("gam_validation"+ "_"+ CONFIG['ENV'] + "_"+ "dag" ,start_date=datetime(2021, 11, 23),schedule_interval='00 12 * * *',default_args=default_args,catchup=False) as dag:

    start = DummyOperator(task_id='start')

    gam_report_download_task = DatabricksSubmitRunOperator(task_id='gam_report_download_task',
                                databricks_conn_id=new_databricks_conn.conn_id,
                                json={'existing_cluster_id': CONFIG['cluster']['cluster_id'],
                                        'notebook_task': {'notebook_path':CONFIG['NOTEBOOK_PATHS']['DOWNLOAD_REPORT'],
                                        'base_parameters':
                                            { "ENV": os.environ['ENV'],
                                              "BUCKET": os.environ['BUCKET'],
                                              "SOURCE_GAM": os.environ['SOURCE_GAM'],
                                              "API_BASE_PATH": os.environ['API_BASE_PATH'],
                                               "VERSION": os.environ['VERSION'],
                                               "AUTH_FILE": CONFIG['AUTH_FILE']}}})

    gam_report_check_sensor_task = PythonOperator(task_id='gam_report_check_sensor_task',
                                    python_callable=Criteria,
                                    op_args=(CONFIG['gam_report_src_list_dict'],CONFIG['gam_report_sql_query']))

    gam_source_check_sensor_task = PythonOperator(task_id='gam_source_check_sensor_task',
                                    python_callable=Criteria,
                                    op_args=(CONFIG['gam_val_src_list_dict'],CONFIG['gam_val_sql_query']))

    gam_etl_validation_task = DatabricksSubmitRunOperator(task_id='gam_etl_validation_task',
                               databricks_conn_id=new_databricks_conn.conn_id,
                                json={'existing_cluster_id': CONFIG['cluster']['cluster_id'],
                                'notebook_task': {'notebook_path':CONFIG['NOTEBOOK_PATHS']['GAM_ETL_VALDATION'],
                                                  'base_parameters': {
                                                      "ENV": os.environ['ENV'],
                                                      "BUCKET": os.environ['BUCKET'],
                                                      "SOURCE_GAM": os.environ['SOURCE_GAM'],
                                                      "MARKETS": os.environ['MARKETS']}}})

    email_notification = DummyOperator(task_id='email_notification',
                                    trigger_rule=TriggerRule.ALL_SUCCESS,
                                    on_success_callback=success_msg)

    end = DummyOperator(task_id='end')

    start >> gam_report_download_task >> [gam_report_check_sensor_task,gam_source_check_sensor_task] >> gam_etl_validation_task >> email_notification >> end