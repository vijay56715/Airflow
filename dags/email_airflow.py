from airflow import DAG
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy_operator import DummyOperator
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

# define the python function:



# define the DAG
dag = DAG(
    'emailairflow',
    default_args=default_args,
    description='How to use Python Operator?',
    schedule_interval=timedelta(days=1),
    catchup = False
)
start=DummyOperator(
    task_id='start',
    dag=dag
)
# define the first task
sendemail= EmailOperator(
    task_id ='sendemail',
    to = ['akashsjce8050@gmail.com','airflowft@gmail.com','vijay.viju251196@gmail.com'],
    subject = 'Airflow Email Test',
    html_content = '''<h3> Test from Airflow </h3>''',
    dag=dag
)

end=DummyOperator(
    task_id='end',
    dag=dag
)

start >> sendemail >> end
#callf1 >> callf2
