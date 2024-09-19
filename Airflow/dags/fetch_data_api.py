from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime
import os
import sys

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
MAIN_DIR = os.path.dirname(BASE_DIR)
sys.path.append(f"{MAIN_DIR}/utils/fetch_api")

from get_data import get_all_data


def print_current_path():
    print("Current path:", os.getcwd())
    # print folder names
    print("Folder names:", os.listdir())
    # now print folders inside the utils folder
    print("Folders inside utils:", os.listdir(f"{MAIN_DIR}/utils/fetch_api"))


with DAG('fetch_TLC_Data', start_date=datetime(2022, 1, 1), 
        schedule='5 5 * * *', catchup=False) as dag:
    
    
    t1 = PythonOperator(task_id='task1',
                        python_callable=print_current_path,
                        dag=dag)
    
    t1
    
