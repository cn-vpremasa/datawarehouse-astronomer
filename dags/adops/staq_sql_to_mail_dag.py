from airflow import settings
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

CONFIG = read_config_file()


def ClientRest():
  n=0
  print(env)
  print(f"Connected to Tardis at URL '{client.API_URL}'")
  while n < 10:
    response = client.Get(get_type='DataAvailability')
    if response.status_code == 200:
        res = response.read()
        dataAvail_json_pd_DF = res['data']['dataAvailability']['results']
        dataAvail_normalize_pd_DF = pd.json_normalize(dataAvail_json_pd_DF)
        dataAvail_normalize_pd_DF.rename(columns={'source.source': 'source', 'status.status': 'status'}, inplace=True)
        Final = pd.DataFrame(dataAvail_normalize_pd_DF)
        result = ps.sqldf("select * from Final where source in (select distinct source from Final where source like 'staq%') limit 20")
        data = pd.DataFrame(result)
        html = data.to_html()
        final=html
        n = 11
    else:
        if n == 10:
            raise RuntimeError("Tardis Data Log Failed. Reason: No reply from Tardis Server")
        n = n + 1
        time.sleep(60)
        continue
    return final

def success_msg(context):
    dag_name = context['dag']
    subject = "[Airflow] DAG {0} : CNHW-NONPROD - sql_test".format(
        dag_name
    )
    html_content = ClientRest()
    send_email_smtp(CONFIG['dags']['email'], subject, html_content)



email_notification = DummyOperator(task_id='email_notification', trigger_rule=TriggerRule.ALL_SUCCESS, on_success_callback=success_msg)





default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}



with DAG("sta_sql_to_mail_dag" ,start_date=datetime(2021, 8, 19),schedule_interval="@daily",catchup=False,) as dag:

    t1 = DummyOperator(task_id='email_notification',trigger_rule = TriggerRule.ALL_SUCCESS, on_success_callback = success_msg)

    t2 = DummyOperator(task_id='end')


    t1 >> t2