from datetime import timedelta, datetime
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator, PythonVirtualenvOperator
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


dag = DAG(dag_id='amt1_ssh_operator',
          default_args=default_args,
        #   schedule_interval='0,10,20,30,40,50 * * * *',
          dagrun_timeout=timedelta(seconds=120))


def create_the_file():

    # ssh = SSHHook(remote_host = '172.29.6.4', username='kthomson', password='***', port = 22)
    ssh = SSHHook(ssh_conn_id="sas1pcrpkthomson")
    print(ssh)
    
    ssh_client = None
    try:
        ssh_client = ssh.get_conn()
        ssh_client.load_system_host_keys()
        # ssh_client.exec_command('cd ~/airflow; touch test.txt; ls -la')
        # stdin, stdout, stderr = ssh_client.exec_command('cd /Users/kthomson/; touch test.txt; ls -la')
        # stdin, stdout, stderr = ssh_client.exec_command('dir')
        # stdin, stdout, stderr = ssh_client.exec_command('sas test5.sas')
        stdin, stdout, stderr = ssh_client.exec_command('cd Documents/My SAS Files/9.4/  && sas ccd_nonfiscal_state_test.sas')
        out = stdout.read().decode().strip()
        error = stderr.read().decode().strip()
        print(out)
        print(error)
    finally:
        if ssh_client:
            ssh_client.close()

call_ssh_task = PythonOperator(
    task_id='call_ssh_task',
    python_callable=create_the_file,
    dag=dag
)
