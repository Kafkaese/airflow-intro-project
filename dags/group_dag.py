from operator import sub
from airflow import DAG
from airflow.operators.bash import BashOperator
from groups.group_download import downlaod_tasks
from groups.group_transform import trandform_tasks
from datetime import datetime
    
with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
    
    args = {'start_date': dag.start_date, 'schedule_interval': dag.schedule_interval, 'catchup': dag.catchup}
    
    downloads = downlaod_tasks()
    
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
    
    transforms = trandform_tasks()
    
    downloads >> check_files >> transforms

