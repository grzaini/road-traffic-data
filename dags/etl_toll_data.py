#import
from datetime import timedelta
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
#from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

#DAG argument
default_args = {
    'owner': 'zaini',
    'start_date': days_ago(0),
    'email': 'grzaini@gmail.com',
    'email_on_failure': True,
    'email_on_retry': True,
    'retries':1,
    'retry_delay': timedelta(minutes=5),
}

#DAG
dag = DAG(
    dag_id = 'etl_toll_data',
    default_args = default_args,
    description = 'apache airflow etl process',
    schedule_interval = timedelta(days=1),
)

#extraction tasks
extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = "cut -d, -f1,2,3,4 /opt/airflow/data/tolldata/vehicle-data.csv > /opt/airflow/data/csv_data.csv",
    dag = dag
)

#consolidate data
csv_file = '/opt/airflow/data/csv_data.csv'
extracted_data = '/opt/airflow/data/extracted_data.csv'
consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = f'paste -d"," {csv_file} > {extracted_data}',
    dag = dag
)

#transformation tasks
transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = "awk 'BEGIN {FS=OFS\", \"} {$4 = toupper($4)} 1' /opt/airflow/data/extracted_data.csv > /opt/airflow/data/transformed_data.csv",
    dag = dag
)

""" def task1():
 print ("Executing Task 1")

task_1 = PythonOperator(
 task_id='task_1',
 python_callable=task1,
 dag=dag,
) """
   
#tasks pipeline
extract_data_from_csv >> consolidate_data >> transform_data

