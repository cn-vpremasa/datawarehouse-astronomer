from airflow.models import Connection
from airflow.utils.email import send_email_smtp
from airflow.utils.trigger_rule import TriggerRule
from airflow import settings
from airflow.utils.email import send_email_smtp
from datetime import datetime, timedelta, date
import os
from dags.utils.common_utils import read_config_file
CONFIG = read_config_file()

def setup_databricks_connection():
    new_databricks_conn = Connection(
        conn_id=os.environ['DATABRICKS_CONN_ID'],
        conn_type=os.environ['DATABRICKS_CONN_TYPE'],
        host=os.environ['DATABRICKS_HOST'],
        login=os.environ['DATABRICKS_LOGIN'],
        password=os.environ['DATABRICKS_PASSWORD'])
    session = settings.Session()
    if not (session.query(Connection).filter(Connection.conn_id == new_databricks_conn.conn_id).first()):
        msg = '\n\tA connection with `conn_id`={conn_id} does not exist\n'
        msg = msg.format(conn_id=new_databricks_conn.conn_id)
        print(msg)
        session.add(new_databricks_conn)
        session.commit()
    else:
        msg = '\n\tA connection with `conn_id`={conn_id} already exists\n'
        msg = msg.format(conn_id=new_databricks_conn.conn_id)
        print(msg)
    return new_databricks_conn

def failure_msg(context):
    dag_name = context['dag']
    task_name = context['task'].task_id
    subject = "[Airflow] DAG {0} - Task {1}: Failed".format(dag_name,task_name)
    html_content = """DAG: {0}<br>Task: {1}<br>Failed on: {2}""".format(dag_name, task_name, datetime.now())
    send_email_smtp(CONFIG['dags']['email'], subject, html_content)

def success_msg(context):
    dag_name = context['dag']
    subject = "[Airflow] DAG {0} :cnhw-nonprod Refactor gam dim incremental load is completed".format(dag_name)
    html_content = """cnhw-nonprod Refactor gam dim incremental load is completed: {0}""".format(datetime.now())
    send_email_smtp(CONFIG['dags']['email'], subject, html_content)