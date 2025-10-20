import os
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import requests
import subprocess



current_dir=os.path.dirname(__file__) 
talend_folder=os.path.join(current_dir,"J_JiraToGCS_Principal_0.1/J_JiraToGCS_Principal/J_JiraToGCS_Principal_run.sh ")


default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 7, 11),
    'schedule_interval': '0 8 * * 6' #Run once a week at 8 AM on saturday
}

dag = DAG('bash_script_execution', default_args=default_args, catchup=False)


run_talend_job = BashOperator(
    task_id='run_talend_job',
    bash_command=talend_folder,
    dag=dag
)

def run_flask_app():
    command = "cd ../../mnt/c/dags && python3 my_flask_app.py"
    process = subprocess.Popen(command, shell=True)
    process.wait(timeout=60)

def send_post_request():
    url = "http://localhost:5000/primary_keys" 
    data={
        "dataset": "JiraConfluence",
        "bucket_name": "os-gc-dpf-prj-ing-02-dev",
        "folder_name": "Talend",
        "tables": [
            {
                "table_name": "raw_contents",
                "keys": ["id"]
            },
            {
                "table_name": "raw_projects",
                "keys": ["id", "key"]
            },
            {
                "table_name": "raw_spaces",
                "keys": ["id", "key"]
            },
            {
                "table_name": "raw_sprints",
                "keys": ["id", "name"]
            },
            {
                "table_name": "raw_tickets",
                "keys": ["id"]
            },
            {
                "table_name": "raw_users",
                "keys": ["accountId"]
            }
        ]
    }

    headers = {"Content-Type": "application/json"}

    response = requests.post(url, json=data, headers=headers)

    if response.status_code == 200:
        print("Flask app responded:", response.json())
    else:
        print("Error calling Flask app. Status code:", response.status_code)


run_flask_task = PythonOperator(
    task_id='run_flask_app_task',
    python_callable=run_flask_app,
    dag=dag,
)


# Task to send the POST request to Flask app
send_post_request_task = PythonOperator(
    task_id='send_post_request_task',
    python_callable=send_post_request,
    dag=dag,
)




run_talend_job >> run_flask_task >> send_post_request_task





















