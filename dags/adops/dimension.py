from functools import partial
from airflow import DAG
import os,logging
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from plugins.adops.airflow_connections import create_airflow_connection, list_connections
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.models import Connection
from plugins.adops import clusters, config
# from plugins.adops.vault import vault_instance
import logging, json

ASTRONOMER_ENV = os.environ.get("ENV", "dev")

notebook_params = {
    'argument': '{{ds}}',  # processing data for day-1
    'env': ASTRONOMER_ENV
}

gam_common_dim_deals = config.dimension_notebooks['gam_common_dim_deals'][config.env]

default_args = {
    'owner': 'Bala Murugan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
}

# evergreen_dev_workspace_token = vault_instance.get_secret("automation-sp-data-warehouse-group-development")
evergreen_dev_workspace_token = "dapi9a833498f2d1c91d08da7c762c4f5a4a"

if ASTRONOMER_ENV.lower() == "dev":
    WORKSPACE_TOKEN = evergreen_dev_workspace_token
    WORKSPACE_HOST = os.environ['DATABRICKS_HOST']
    WORKSPACE_CONN_ID = os.environ['DATABRICKS_WORKSPACE']



def my_callable():
    logger = logging.getLogger("airflow.task")
    TOKEN = WORKSPACE_TOKEN
    HOST = WORKSPACE_HOST
    CONN_ID = WORKSPACE_CONN_ID
    logger.info("Attempting to Create Airflow Connection")
    create_airflow_connection(conn_id=CONN_ID,
                              conn_type="databricks",
                              host=HOST,
                              login=None,
                              password=None,
                              port=None,
                              extra=json.dumps({"token": TOKEN, "host": HOST}),
                              uri=None
                              )
logging.info(WORKSPACE_CONN_ID)




notebook_task_params_transactions = {
    'libraries': clusters.adops_libs,
    'notebook_task': {
        'notebook_path': gam_common_dim_deals,
        'base_parameters': notebook_params
    }, 'existing_cluster_id': clusters.adops['cluster_id']}
print(clusters.adops['cluster_id'])
with DAG('adops_dimensions',
         start_date=datetime(2021, 1, 1),
         max_active_runs=3,
         schedule_interval='30 14 * * *',  # https://airflow.apache.org/docs/stable/scheduler.html#dag-runs
         default_args=default_args,
         catchup=False
         ) as dag:
    dummy_task_start = DummyOperator(
        task_id='Start')
    dummy_task_start

    task_gam_common_dim_deal = DatabricksSubmitRunOperator(
        task_id='gam_common_dim_deal',
        databricks_conn_id=WORKSPACE_CONN_ID,
        json=notebook_task_params_transactions)
    task_gam_common_dim_deal

    dummy_task_end = DummyOperator(
        task_id='End')
    dummy_task_end

    conn_ops = PythonOperator(
        task_id='SetConnections',
        python_callable=my_callable,
    )

    dummy_task_start >> conn_ops >> task_gam_common_dim_deal >> dummy_task_end
