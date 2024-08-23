from airflow import DAG
from airflow.operators.python import PythonOperator

from inkling.dag_stat_utils import DAGStatusDigester


def start_op_callable(service_config, **context):
    dag_stat_proc = DAGStatusDigester(service_config)
    dag_stat_proc.load(context, body={})

class Start(PythonOperator):
    def __init__(self, op_kwargs, dag: DAG):
        super(Start, self).__init__(task_id='start', python_callable=start_op_callable, op_kwargs=op_kwargs, dag=dag, default_args=None)
