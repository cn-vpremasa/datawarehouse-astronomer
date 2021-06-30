from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Connection
from airflow import settings
from datetime import datetime, timedelta, date
import os
from airflow.sensors.sql_sensor import SqlSensor
from dags.utils.common_utils import read_config_file
from airflow.utils.email import send_email_smtp
from tardis import client
import pandas as pd

def sqlsqnsorquery():
  response = client.Get(get_type='DataAvailability',sourceName=['cnhw_ads.standardization','cnhw_ads.val_Gam','dfp.line_items','dfp.ad_units','dfp.orders','cnhw_ads.dimension'],status='Complete')
  res=response.read()
  dataAvail_json_pd_DF=res['data']['dataAvailability']['results']
  dataAvail_normalize_pd_DF=pd.json_normalize(dataAvail_json_pd_DF)
  dataAvail_normalize_pd_DF.rename(columns={'source.source':'source','status.status':'status'},inplace=True)
  Final=pd.DataFrame(dataAvail_normalize_pd_DF)
  result=ps.sqldf("select * from (select count(*) as final_count from  (select count(source) from Final where source in   ('cnhw_ads.standardization','cnhw_ads.val_Gam','dfp.line_items','dfp.ad_units', 'dfp.orders')  and date(logdate) > (select max(date(logdate)) from Final where source = 'cnhw_ads.dimension' and date(logdate) < current_date) and status = 'Complete' group by source )a  )a where final_count=5")
  return result

def create_dag(dag_id, schedule_time):
    default_args = {
        'owner': CONFIG['dags']['owner'],
        'depends_on_past': CONFIG['dags']['depends_on_past'],
        'retries': CONFIG['dags']['retries'],
        'retry_delay': CONFIG['dags']['retry_delay'],
        'provide_context': True
    }
    new_dag = DAG(dag_id,
                  catchup=False,
                  max_active_runs=1,
                  schedule_interval=schedule_time,
                  default_args=default_args,
                  start_date=datetime(2020, 2, 27, 13, 30)
                  )

    # Example of using the JSON parameter to initialize the operator.
    with new_dag:

        GAN_SENSOR_TESTING_dag = SqlSensor(task_id='GAN_SENSOR_TESTING_task',
                                     conn_id='',
                                     sql=GAM_val_stmt,
                                     poke_interval=15 * 60,
                                     timeout=60 * 60 * 3
                                     )
        # download_notebook >> [advertiser_notebook, location_notebook, brand_notebook, device_notebook] >> email_notification
        GAN_SENSOR_TESTING_dag

    return new_dag

# creating SQL statement for primary sources(GAM and STAQ except 'staq.amazon_pmp_data').

GAM_val_stmt=sqlsqnsorquery()

create_dag('GAN_SENSOR_TESTING_dag', '30 16 * * *')
