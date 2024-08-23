from airflow import DAG
from airflow.operators.python import PythonOperator

from inkling.dag_stat_utils import DAGStatusDigester


def exit_op_callable(service_config, **context):
    dag_stat_proc = DAGStatusDigester(service_config)
    dag_stat = dag_stat_proc.fetch(context)
    dag_stat_proc.load(context, body=dag_stat.body)

class Exit(PythonOperator):

    def __init__(self, op_kwargs, dag: DAG):
        super(Exit, self).__init__(task_id='exit', python_callable=exit_op_callable, op_kwargs=op_kwargs, dag=dag, default_args=None)
