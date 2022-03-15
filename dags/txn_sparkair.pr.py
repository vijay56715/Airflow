from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from datetime import timedelta, datetime

# These args will get passed on to the python operator
default_args = {
    'owner': 'vijay',
    'depends_on_past': False,
    'start_date': datetime(2022, 3, 12),
    'email': ['vijay.viju251196@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# define the DAG
dag = DAG(
    'txn_sparkair',
    default_args=default_args,
    description='How to use Bash Operator?',
    schedule_interval=timedelta(days=1)
)

# define the first task
spark_submit_operator = SparkSubmitOperator(
    application='/home/saif/airflow/dags/txn_airflow.py',
    jars='/home/saif/LFS/cohort_c9/jars/mysql-connector-java-8.0.28.jar',
    task_id='spark_submit_operator',
    # conf={'master':'yarn','deploy-mode':'client'},
    # driver_memory='1g',executor_memory='2g',num_executors=1,executor_cores=2,
    dag=dag)

spark_submit_operator

