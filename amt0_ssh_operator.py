from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.contrib.operators.ssh_operator import SSHOperator
from airflow.contrib.hooks.ssh_hook import SSHHook

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'start_date': datetime.now() - timedelta(minutes=20),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
hook = SSHHook(ssh_conn_id="sas1pcrpkthomson")
#hook = SSHHook(remote_host = '172.29.6.4', username='pcrp_kthomson', password='*****', port = 22)
print(hook)
dag = DAG(dag_id='amt0_ssh_operator',
          default_args=default_args,
          schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=120))

t1_bash = """
echo 'Hello World'
"""
t1 = SSHOperator(
    ssh_hook= hook,
    task_id='amt0_ssh_operator0',
    command=t1_bash,
    dag=dag)
t1