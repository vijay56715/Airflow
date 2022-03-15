from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import timedelta, datetime

# These args will get passed on to the python operator
default_args = {
    'owner': 'vijay',
    'depends_on_past': True,
    'start_date': datetime(2022, 3, 7),
    'email': ['vijay.viju251196@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


# define the DAG
dag = DAG(
    'falsetest',
    default_args=default_args,
    description='How to use Bash Operator?',
    schedule_interval=timedelta(days=1)
)

# define the first task


test1 = BashOperator(
    task_id ='test1',
    bash_command="`echo date`;`echo datetime`",
    dag=dag
)
test2 = BashOperator(
    task_id ='test2',
    bash_command="echo date;echo datetime",
    dag=dag
)

test1 >> test2

