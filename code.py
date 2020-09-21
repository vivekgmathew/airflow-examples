from datetime import datetime, timedelta
from airflow.models import DAG
from airflow.sensors.s3_key_sensor import S3KeySensor
from airflow.utils.dates import days_ago
from airflow.contrib.operators.s3_delete_objects_operator import S3DeleteObjectsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.operators.emr_terminate_job_flow_operator import EmrTerminateJobFlowOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable
import boto3

schedule = timedelta(minutes=45)

args = {
    'owner': 'Vivek Mathew',
    'start_date': days_ago(1),
    'depends_on_past': False,
}


def set_job_flow_var(job_var):
    Variable.set("__pfd_stlmt_cluster_id", job_var)


def create_job_flow_file(job_var):
    client = boto3.client('s3')
    client.put_object(Body=job_var, Bucket='vivek-mathew', Key='job-flow.txt')


dag = DAG(
    dag_id='Emr',
    schedule_interval=schedule,
    default_args=args,
    catchup=False
)

# @TODO : Add a task for getting the latest AMI


create_cluster = EmrCreateJobFlowOperator(
    task_id="create_cluster",
    aws_conn_id='aws_default',
    emr_conn_id='test_emr',
    dag=dag
)

create_job_flow_variable = PythonOperator(
    task_id="set_jobflow_var",
    python_callable=set_job_flow_var,
    op_args=["{{ task_instance.xcom_pull('create_cluster', key='return_value') }}"],
    dag=dag
)

create_job_flow_file = PythonOperator(
    task_id="create_job_flow_file",
    python_callable=create_job_flow_file,
    op_args=["{{ task_instance.xcom_pull('create_cluster', key='return_value') }}"],
    dag=dag
)

file_sensor = S3KeySensor(
    task_id='recap_cntrl_file_sensor',
    poke_interval=60,  # (seconds); checking file every 60 seconds
    timeout=60 * 60 * 18,  # timeout in 18 hours
    bucket_key="s3://vivek-mathew/recap-cntrl.txt",
    bucket_name=None,
    wildcard_match=False,
    dag=dag)

terminate_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_cluster",
    job_flow_id="{{ task_instance.xcom_pull('create_cluster', key='return_value') }}",
    aws_conn_id='aws_default',
    dag=dag
)

recap_file_delete = S3DeleteObjectsOperator(
    task_id="delete_recap_cntrl_file",
    bucket="vivek-mathew",
    keys="recap-cntrl.txt",
    dag=dag
)

job_flow_file_delete = S3DeleteObjectsOperator(
    task_id="delete_job_flow_file_delete",
    bucket="vivek-mathew",
    keys="job-flow.txt",
    dag=dag
)

create_cluster >> create_job_flow_variable >> create_job_flow_file >> file_sensor >> terminate_cluster \
>> recap_file_delete >> job_flow_file_delete
