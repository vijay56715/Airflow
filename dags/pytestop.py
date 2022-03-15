#Importing Libraries:
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import timedelta, datetime

# These args will get passed on to the python operator


# define the python function:
def my_function1(x):
    return x + " is a must have tool for Data Engineers."

def my_function2(x):
    return x + " is learning Airflow"

# define the DAG
dag = DAG(
    'pytestop.py',
    default_args=default_args,
    description='How to use Python Operator?',
    schedule_interval=timedelta(days=1)
)

# define the first task
callf1 = PythonOperator(
    task_id ='callf1',
    python_callable = my_function1,
    op_kwargs ={"x" : "Apache Airflow"},
    python_callable = my_function2,
    op_kwargs ={"x" : "vijay"},
    dag=dag
)

callf1 

