from operator import sub
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.subdag import SubDagOperator
from subdags.subdag_download import subdag_downlaod
from subdags.subdag_transform import subdag_transform
from datetime import datetime
    
with DAG('group_dag', start_date=datetime(2022, 1, 1), 
    schedule_interval='@daily', catchup=False) as dag:
    
    args = {'start_date': dag.start_date, 'schedule_interval': dag.schedule_interval, 'catchup': dag.catchup}
    
    downloads = SubDagOperator(
        task_id = 'downloads',
        subdag=subdag_downlaod(dag.dag_id, 'downloads', args))
    
    check_files = BashOperator(
        task_id='check_files',
        bash_command='sleep 10'
    )
    
    transforms = SubDagOperator(
        task_id = 'tranforms',
        subdag=subdag_transform(dag.dag_id, 'tranforms', args))
    
    
    downloads >> check_files >> transforms

