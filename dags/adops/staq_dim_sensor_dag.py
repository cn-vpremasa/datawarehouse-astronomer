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
from dags.utils.common_utils import read_config_file
from dags.utils.common_sensor_funct_utils import ClientRes
from dags.utils.common_sensor_funct_utils import Criteria
from dags.utils.common_sensor_funct_utils import *
import pandas as pd
import pandasql as ps
import time

os.environ['ENVIRONMENT'] = 'production'
from tardis import client

CONFIG = read_config_file()

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
with DAG("staq_dim_sensor_dag" ,start_date=datetime(2021, 8, 19),schedule_interval='00 15 * * *',catchup=False,) as dag:


    t1 = PythonOperator(task_id='staq_dim_sensor_task',
                        python_callable=Criteria,
                        op_args=(CONFIG['staq_dim_src_list_dict'],CONFIG['staq_dim_sql_query']))

    t2 = DummyOperator(task_id='start')

    t3 = DummyOperator(task_id='end')

    t1 >> t2 >> t3