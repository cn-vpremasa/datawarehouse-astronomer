from airflow import DAG
from airflow.contrib.operators.databricks_operator import DatabricksSubmitRunOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models import Connection
from airflow import settings
from datetime import datetime, timedelta, date
import os
from dags.utils.common_utils import read_config_file
from airflow.utils.email import send_email_smtp

CONFIG = read_config_file()
S3_PATH = 's3://' + CONFIG['BUCKET_NAME'] + '/'
LOAD_TYPE = os.environ['LOAD_TYPE']
SOURCE_GAM = os.environ['SOURCE_GAM']


def setup_databrick_connection():
    new_databrick_conn = Connection(
        conn_id=os.environ['DATABRICKS_CONN_ID'],
        conn_type=os.environ['DATABRICKS_CONN_TYPE'],
        host=os.environ['DATABRICKS_HOST'],
        login=os.environ['DATABRICKS_LOGIN'],
        password=os.environ['DATABRICKS_PASSWORD']
    )
    session = settings.Session()  # get the session
    if not (session.query(Connection).filter(Connection.conn_id == new_databrick_conn.conn_id).first()):
        msg = '\n\tA connection with `conn_id`={conn_id} does not exist\n'
        msg = msg.format(conn_id=new_databrick_conn.conn_id)
        print(msg)
        session.add(new_databrick_conn)
        session.commit()
    else:
        msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
        msg = msg.format(conn_id=new_databrick_conn.conn_id)
        print(msg)
    return new_databrick_conn


new_databricks_cluster = {
    "autoscale": {
        "min_workers": CONFIG['INITIAL']['MIN_WORKERS'],
        "max_workers": CONFIG['INITIAL']['MAX_WORKERS']
    },
    "spark_version": CONFIG['INITIAL']['SPARK_VERSION'],
    "aws_attributes": {
        "first_on_demand": 1,
        "availability": "SPOT_WITH_FALLBACK",
        "zone_id": "us-east-1b",
        "instance_profile_arn": "arn:aws:iam::802965631854:instance-profile/Databricks-EC2Role",
        "spot_bid_price_percent": 100,
        "ebs_volume_type": "GENERAL_PURPOSE_SSD",
        "ebs_volume_count": 3,
        "ebs_volume_size": 100
    },
    "node_type_id": CONFIG['INITIAL']['NODE_TYPE_ID'],
    "driver_node_type_id": CONFIG['INITIAL']['DRIVER_NODE_TYPE_ID'],
    "spark_env_vars": {
        "PYSPARK_PYTHON": "/databricks/python3/bin/python3"
    },
    "custom_tags": {"Group": "Data Intelligence",
                    "Project": "CNHW-NONPRODD"},
    "enable_elastic_disk": False
}


def failure_msg(context):
    dag_name = context['dag']
    task_name = context['task'].task_id

    subject = "[Airflow] DAG {0} - Task {1}: Failed".format(
        dag_name,
        task_name
    )
    html_content = """
      DAG: {0}<br>
      Task: {1}<br>
      Failed on: {2}
      """.format(dag_name, task_name, datetime.now())

    send_email_smtp(CONFIG['dags']['email'], subject, html_content)


def success_msg(context):
    dag_name = context['dag']
    subject = "[Airflow] DAG {0} : CNHW-NONPROD - Dev Refactoring Standardisation GAM process completed ".format(
        dag_name
    )
    html_content = """
    CNHW-NONPROD - Dev Refactoring Standardisation GAM process completed: {0}
    """.format(
        datetime.now()
    )
    send_email_smtp(CONFIG['dags']['email'], subject, html_content)


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
                  start_date=datetime(2021, 8, 11, 20, 30)
                  )

    with new_dag:
        location_notebook_params = {
            'new_cluster': new_databricks_cluster,
            'notebook_task': {
                'notebook_path': CONFIG['NOTEBOOK_PATHS']['MAIN_NOTEBOOK'],
                'base_parameters': {"ENV": CONFIG['ENV'],
                                    "BUCKET": S3_PATH,
                                    'LOGDATE': (datetime.now()).strftime('%Y-%m-%d'),
                                    'PROCESS_NAME': CONFIG['PROCESS_NAME']['LOCATION'],
                                    'LOAD_TYPE': LOAD_TYPE,
                                    'SOURCE': os.environ['SOURCE_GAM']}
            }
        }
        brand_notebook_params = {
            'new_cluster': new_databricks_cluster,
            'notebook_task': {
                'notebook_path': CONFIG['NOTEBOOK_PATHS']['MAIN_NOTEBOOK'],
                'base_parameters': {
                    "ENV": CONFIG['ENV'],
                    "BUCKET": S3_PATH,
                    'LOGDATE': (datetime.now()).strftime('%Y-%m-%d'),
                    'PROCESS_NAME': CONFIG['PROCESS_NAME']['BRAND'],
                    'LOAD_TYPE': LOAD_TYPE,
                    'SOURCE': os.environ['SOURCE_GAM']
                }
            }
        }

        location_notebook = DatabricksSubmitRunOperator(
            task_id='location_notebook',
            databricks_conn_id=new_databrick_conn.conn_id,
            json=location_notebook_params)

        brand_notebook = DatabricksSubmitRunOperator(
            task_id='brand_notebook',
            databricks_conn_id=new_databrick_conn.conn_id,
            json=brand_notebook_params)

        email_notification = DummyOperator(task_id='email_notification',
                                           trigger_rule=TriggerRule.ALL_SUCCESS,
                                           on_success_callback=success_msg
                                           )


        location_notebook >> brand_notebook >> email_notification

    return new_dag


new_databrick_conn = setup_databrick_connection()

globals()[CONFIG['dags']['dag_id']] = create_dag(
    CONFIG['dags']['dag_id'] + '_' + os.environ['SOURCE_GAM'] + '_INCREMNENTAL', CONFIG['dags']['gam_schedule'])