from functools import partial
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from plugins.adops import clusters, config
from tardis import client

# VAULT_HASH = encrypt_vault_data(client_data)

notebook_params = {
    'argument': '{{ds}}',  # processing data for day-1
    'env': config.env,
#    'tokens': VAULT_HASH
}

gam_common_dim_deals = config.dimension_notebooks['gam_common_dim_deals'][config.env]

notebook_task_params_transactions = {
    'libraries': clusters.adops_trx_libs,
    'notebook_task': {
        'notebook_path': gam_common_dim_deals,
        'base_parameters': notebook_params
    }, 'existing_cluster': clusters.adops['cluster_id']}

default_args = {
    'owner': 'Bala Murugan',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=15)
}

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
        json=notebook_task_params_transactions)
    task_gam_common_dim_deal

    dummy_task_end = DummyOperator(
        task_id='End')
    dummy_task_end

    dummy_task_start >> task_gam_common_dim_deal >> dummy_task_end
