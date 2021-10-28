import gzip
import sys
import io
import json
import shutil
import logging
from requests_oauthlib import OAuth1
from datetime import timedelta

import airflow
from airflow import DAG
from airflow.utils.decorators import apply_defaults
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook
from airflow.hooks.mssql_hook import MsSqlHook
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.providers.google.cloud.hooks.pubsub import PubSubHook


connector_name = "gcp_unzip_file_source"
source_name = "schoology_activity"

"""
SELECT s.source_id, s.source_name, s.task_id, s.project_id, s.topic_id, s.subscriber_id, s.export_format, s.out_filename, 
s.db_connection_id, s.is_db, s.source_type, s.is_active, strg.google_cloud_storage_conn_id,strg.bucket, strg.folder, s.file_from, 
strg.destination_bucket, strg.destination_folder, strg.archival_bucket,strg.composer_bucket_name FROM clarity-lm-idla-dev.connector_etl_setting.source s  
inner join clarity-lm-idla-dev.connector_etl_setting.storage 
strg on s.source_id = strg.source_id where s.source_name='Schoology' and s.is_active=true
"""

class GCPHook:
    def __init__(self, bucket, target_folder=None):
        self.hook = GoogleCloudStorageHook()
        self.bucket = bucket
        self.client = self.hook.get_conn()
        self.bucket_obj = self.client.bucket(self.bucket)
        if target_folder:
            self.target_folder = target_folder.strip('/')

    def check_folder(self, file_name):
        exists = self.hook.exists(self.bucket, file_name)
        return exists

    def file_list(self, file_path):
        file_list = self.hook.list(self.bucket, prefix=file_path)
        return file_list

    def extract_module(self, input_file, output_file_path):
        try:
            gz_byte = io.BytesIO(input_file.download_as_bytes())

            with gzip.open(gz_byte, 'rb') as input_file:
                with open(output_file_path.split("/")[-1], 'wb') as output_file:
                    shutil.copyfileobj(input_file, output_file)
                    filename = output_file_path
            return filename
        except Exception as e:
            print("Error in extract module : ",e)
            print("Error at : {}".format(sys.exc_info()[-1].tb_lineno))
        return None

    def extract_files(self, file_path, dest_hook, dest_file_path=None):
        try:
            print("file path :",file_path)
            print("file path >",file_path.name)
            if not file_path.name.endswith('.gz'):
                raise ValueError("The archive type should be .gz")
                
            if not dest_file_path:
                dest_file_path = file_path.name.split('.gz')[0]

            extracted_file_obj = self.extract_module(file_path, dest_file_path)

            # upload extracted path to GCP
            up_file_name = dest_file_path.split('/')[-1]
            print("UPFILE : ",up_file_name)
            upload_file = dest_hook.target_folder + "/" + up_file_name
            # if up_file_name in ['2021-04-18-course000', '2021-05-16-course000']:
            self.hook.upload(dest_hook.bucket, extracted_file_obj, up_file_name)
            return dest_file_path
        except Exception as e:
            print("error in extracted files : ",e)
            print("error at : {}".format(sys.exc_info()[-1].tb_lineno))


class BigQueryOperations:
    """docstring for BigQueryOperations."""

    config_instance = None

    def __init__(self):
        if BigQueryOperations.config_instance:
            raise Exception("Incorrection call to Singleton class.")
        else:
            BigQueryOperations.config_instance = self
            self.big_query_connection_id = "bigquery_default"

            self.logger = logging.getLogger(f"{__name__}.BigQueryOperations")

    @staticmethod
    def get_instance():
        if not BigQueryOperations.config_instance:
            BigQueryOperations()

        return BigQueryOperations.config_instance

    def execute_query(self, big_query_connection_id=None, query_string=None, is_update=False, source_id=None):
        if not big_query_connection_id:
            self.logger.info("Default big_query_connection_id not in use")
            hook = BigQueryHook(bigquery_conn_id=self.big_query_connection_id, delegate_to=None, use_legacy_sql=False)
        else:
            self.logger.info("Default big_query_connection_id in use")
            hook = BigQueryHook(bigquery_conn_id=big_query_connection_id, delegate_to=None, use_legacy_sql=False)

        self.logger.info("Connection established")

        connection = hook.get_conn()
        cursor = connection.cursor()

        if not is_update:
            if not query_string:
                message = "No query string provided"
                self.logger.error(message)
                raise Exception(message)
        else:
            if not source_id:
                message = "No source id provided"
                self.logger.error(message)
                raise Exception(message)

            # TODO: Unclear usage of connection variable in .format()
            # select max(__$start_lsn) from [cdc].[dbo_County_CT]

        self.logger.info(f"Running query: {query_string}")
        cursor.execute(query_string)

        result = cursor.fetchall()
        cursor.close()
        connection.close()
        self.logger.info("Connection closed")

        return result

    def log_in_db(self, table, log_type, message, line_number, big_query_connection_id=None):
        message = str(message).replace("'", "")
        sql = f"INSERT INTO {table} (source, log_type, message, line_number, logged_on) VALUES ('{source_name}', '{log_type}', '{message}', {line_number}, CURRENT_DATETIME())"
        self.insert_row(sql=sql, big_query_connection_id=big_query_connection_id)
        return "Executed"

    def file_location_in_db(self, file_to_process_table, table_id, table_name, bucket, file_path):
        sql = f"INSERT INTO `{file_to_process_table}` (message_id, table_id, table_name, " \
              f"bucket, file_path, date_time) VALUES " \
              f"((select case when max(message_id) is null then 1 else (max(message_id)) +1  end " \
              f"from `{file_to_process_table}`), " \
              f"{table_id}, '{table_name}', '{bucket}', '{file_path}', CURRENT_DATETIME())"
        print('manoj ' + sql)

        self.insert_row(sql=sql)
        return "Executed"

    def insert_row(self, sql, big_query_connection_id=None):
        if not big_query_connection_id:
            self.logger.info("Default big_query_connection_id not in use")
            hook = BigQueryHook(bigquery_conn_id=self.big_query_connection_id, delegate_to=None, use_legacy_sql=False)
        else:
            self.logger.info("Default big_query_connection_id in use")
            hook = BigQueryHook(bigquery_conn_id=big_query_connection_id, delegate_to=None, use_legacy_sql=False)

        conn = hook.get_conn()
        hook.run(sql=sql)
        conn.close()
        return "Executed"

def process_extraction(source):
    
    # source_bucket, source_folder = source[0][13], source[0][14]
    # destination_bucket, destination_folder = source[0][16], source[0][17]

    # # Temp defined
    source_bucket, source_folder = "clarityschoologydata", "schoologyprod"
    destination_bucket, destination_folder = "clarity-dev-bucket", "clarity-dev-bucket/Activity_data"


    hook = GCPHook(bucket=source_bucket, target_folder=source_folder)
    dest_hook = GCPHook(bucket=destination_bucket, target_folder=destination_folder)

    print(f"Source Bucket : {source_bucket}  -->  Folder :  {source_folder}")
    print(f"Destination Bucket : {destination_bucket}  -->  Folder : {destination_folder}")

    files = hook.file_list(source_folder)
    print(f"Files in {source_folder} >> {files}")

    for gz_file in files:
        # print("File Name :: ",gz_file)
        if 'dimensiondata/course/' in gz_file:
            print("File : ",gz_file)
            input_file = hook.bucket_obj.blob(gz_file)
            

            if input_file.name.endswith('.gz'):
                print("this in input file >> ",input_file)
                op_file = hook.extract_files(input_file, dest_hook)
                print("Extracted File : ",op_file)
    

def initiate_data_migration(**kwargs):
    
    # logger = logging.getLogger(f"{__name__}.initiate_data_migration")
    # logger.info("Initiating process for MS SQL to GCS migration")

    # query = Variable.get('configuration_query')
    # result = BigQueryOperations.get_instance().execute_query(query_string=query)
    # configuration = json.loads(result[0][0], strict=False)

    # source_last_run_field = configuration['source_last_run_field']
    # log_table = configuration["log_table"]
    # data_set = configuration["configuration_dataset_name"]
    # message_table = configuration["message_table"]
    # source_query = configuration["source_query"]

    """ 8  16 20
SELECT s.source_id, s.source_name, s.task_id, s.project_id, s.topic_id, s.subscriber_id, s.export_format, s.out_filename, 
s.db_connection_id, s.is_db, s.source_type, s.is_active, strg.google_cloud_storage_conn_id,strg.bucket, strg.folder, s.file_from, 
strg.destination_bucket, strg.destination_folder, strg.archival_bucket,strg.composer_bucket_name FROM clarity-lm-idla-dev.connector_etl_setting.source s  
inner join clarity-lm-idla-dev.connector_etl_setting.storage 
strg on s.source_id = strg.source_id where s.source_name='Schoology' and s.is_active=true
"""

    # source = BigQueryOperations.get_instance().execute_query(query_string=source_query.replace("[SOURCE_NAME]",
    #                                                                                            source_name))

    # file_from = source[0][-1]
    # source_id = source[0][0]
    
    process_extraction([])
    

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

dag = DAG(connector_name,
          description="The DAG is to extract files from GCP.",
          default_args=default_args,
          schedule_interval="@daily")

database_connector_process = PythonOperator(task_id=connector_name, python_callable=initiate_data_migration, dag=dag)