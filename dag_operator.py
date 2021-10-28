import unittest
import airflow
from datetime import timedelta, datetime
from airflow.models import dagbag
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow import DAG
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator


def check_object_exists_in_bucket():
    bucket_name = "clarityschoologydata"
    object_name = "schoologyprod/dimensiondata/course/2021-04-18-course000.gz"
    hook = GoogleCloudStorageHook()
    check_object = bool(hook.exists(bucket_name, object_name))
    assert check_object == True, f"Object {object_name} does not exist in bucket {bucket_name}."


default_args = {
    'owner': 'Devisahay Mishra',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(1),
    'email': ['devisahay.mishra@learningmate.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=15)
}

with DAG(
    'test_etl_script',
    start_date=datetime(2021, 1, 1),
    max_active_runs=1,
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
    ) as dag:

    run_pipeline = BashOperator(
        task_id='trigger_test',
        bash_command="./script/trigger.sh"
    )

    check_object_exists = PythonOperator(
            task_id='check_object_exists',
            python_callable=check_object_exists_in_bucket
    )