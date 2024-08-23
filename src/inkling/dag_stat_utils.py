import json

from pydantic import BaseModel

from tunip.datetime_utils import dag_runid2snapshot
from tunip.file_utils import services as file_services
from tunip.service_config import (
    ServiceLevelConfig,
    ServiceModule
)
from tunip.snapshot_utils import SnapshotPathProvider

from inkling import LOGGER
from inkling.utils.airflow_utils import get_run_snapshot_dt


class DAGStatusHeader(BaseModel):
    dag_id: str

    """
    dag_runid = '2022-10-01T00:00:00.000000'
    run_snapshot_dt = '20221001_000000_000000'
    refer to tunip.datetime_utils
    """
    run_id: str
    run_snapshot_dt: str
    task_id: str


class DAGStatusPayload(BaseModel):
    header: DAGStatusHeader
    body: dict = dict()

    class Config:
        arbitrary_types_allowed = True
    
    def __setitem__(self, key, value):
        self.body.update({key: value})

    def __getitem__(self, key):
        return self.body.get(key)

    @classmethod
    def apply(cls, context, additionals: dict=None):
        """
        context: airflow task context variable
        """
        dag_header = DAGStatusHeader(
            dag_id=context['dag'].dag_id,
            run_id=context['dag_run'].run_id,
            run_snapshot_dt=get_run_snapshot_dt(context),
            task_id=context['task'].task_id
        )
        dag_body = {
            "prev_logical_date": context.get('prev_logical_date') or None,
            "logical_date": context['logical_date'],
            "data_interval_end": context.get('data_interval_end') or None,
            # "prev_end_date": end_date,
            "additionals": additionals
        }
        dag_stat = DAGStatusPayload.parse_obj(DAGStatusPayload(header=dag_header, body=dag_body))
        return dag_stat


class LatestDAGRunStatPicker:

    DEFAULT_START_DATE = '20190101_000000_000000'

    def __call__(self, service_config, dag_id, task_id):
        try:
            dag_stat_path = f'/user/{service_config.username}/dag_stat/{dag_id}'
            snapshot_provider = SnapshotPathProvider(service_config)
            latest_dag_stat_snapshot = snapshot_provider.latest(dag_stat_path)
            dag_stat_fetch = DAGStatusFetcher(service_config)
            dag_stat: DAGStatusPayload = dag_stat_fetch(dag_id, latest_dag_stat_snapshot, task_id)
            return dag_stat
        except:
            return None

    def prev_end_date(self, service_config, dag_id, task_id, default_time=DEFAULT_START_DATE):
        try:
            dag_stat_path = f'/user/{service_config.username}/dag_stat/{dag_id}'
            snapshot_provider = SnapshotPathProvider(service_config)
            latest_dag_stat_snapshot = snapshot_provider.latest(dag_stat_path)
            dag_stat_fetch = DAGStatusFetcher(service_config)
            dag_stat: DAGStatusPayload = dag_stat_fetch(dag_id, latest_dag_stat_snapshot, task_id)
            end_date = dag_stat.body.prev_end_date
        except:
            end_date = default_time
        finally:
            return end_date


class DAGStatusManager:
    def __init__(self, service_config: ServiceLevelConfig):
        self.fs_service = file_services.get(service_config.filesystem.upper(), config=service_config.config)
        self.service_config = service_config

    def dag_stat_snapshot_path(self, dag_id, run_snapshot_dt, task_id):
        # snapshot_dt = dag_runid2snapshot(run_id)
        dag_path = f'/user/{self.service_config.username}/dag_stat/{dag_id}/{run_snapshot_dt}/{task_id}'
        return dag_path
    
    def dag_stat_path(self, dag_id):
        return f'/user/{self.service_config.username}/dag_stat/{dag_id}'


class DAGStatusLoader(DAGStatusManager):
    def __init__(self, service_config: ServiceLevelConfig):
        super(DAGStatusLoader, self).__init__(service_config)

    def __call__(self, dag_stat: DAGStatusPayload):
        dag_stat_path = self.dag_stat_snapshot_path(
            dag_stat.header.dag_id,
            dag_stat.header.run_snapshot_dt,
            dag_stat.header.task_id
        )
        self.fs_service.mkdirs(dag_stat_path)
        LOGGER.info(f"Load dag_stat to \"{dag_stat_path}/result.json\" ...")
        self.fs_service.write(path=f"{dag_stat_path}/result.json", contents=dag_stat.json())
        return 0


class DAGStatusFetcher(DAGStatusManager):
    def __init__(self, service_config: ServiceLevelConfig):
        super(DAGStatusFetcher, self).__init__(service_config)

    def __call__(self, dag_id, run_snapshot_dt, task_id) -> DAGStatusPayload:
        dag_stat_path = self.dag_stat_snapshot_path(dag_id, run_snapshot_dt, task_id)
        LOGGER.info(f"Fetch dag_stat from \"{dag_stat_path}/result.json\" ...")
        try:
            stat = self.fs_service.load(f"{dag_stat_path}/result.json")
        except FileNotFoundError as fne:
            LOGGER.error(str(fne))
            LOGGER.error(f"THERE IS NO SUCH FILE {dag_stat_path}/result.json")

        dag_stat_json = json.loads(stat)
        
        return DAGStatusPayload.parse_obj(dag_stat_json)

    def latest_stat(self, dag_id, task_id):
        latest_snapshot = LatestDAGRunStatPicker()(self.service_config, dag_id, task_id)
        return self.__call__(dag_id, latest_snapshot, task_id)


class DAGStatusDigester(ServiceModule):
    def __init__(self, service_config: ServiceLevelConfig, is_containerized: bool=True):
        super(DAGStatusDigester, self).__init__(service_config)
        self.is_containerized = is_containerized

    def fetch(self, context):
        if not context:
            return None

        prev_dag_id, run_snapshot_dt, prev_task_id = None, None, None
        if self.is_containerized is True:

            run_id = context['dag_run'].run_id

            prev_dag_id = context['task_instance'].xcom_pull(key='prev_dag_id')
            prev_task_id = context['task_instance'].xcom_pull(key='prev_task_id')
            run_snapshot_dt = context['task_instance'].xcom_pull(key='run_snapshot_dt') or get_run_snapshot_dt(context)

            LOGGER.info(f"xcom pull prev_dag_id: {prev_dag_id}")
            LOGGER.info(f"xcom pull prev_task_id: {prev_task_id}")
            LOGGER.info(f"xcom pull run_snapshot_dt: {run_snapshot_dt}")
            LOGGER.info(f"context.logical_date: {context['logical_date']}")

        else:
            run_id = context["header"]["run_id"]

            prev_dag_id = context["body"].get("prev_dag_id", None)
            prev_task_id = context["body"].get("prev_task_id", None)
            run_snapshot_dt = context["body"].get("run_snapshot_dt", get_run_snapshot_dt(context))

            print(run_id)
            print(prev_dag_id)
            print(prev_task_id)
            print(run_snapshot_dt)


        prev_dag_stat = DAGStatusFetcher(self.service_config)(prev_dag_id, run_snapshot_dt, prev_task_id)
        return prev_dag_stat

    def load(self, context, body):
        if not context:
            return

        dag_load = DAGStatusLoader(self.service_config)
        
        if self.is_containerized:

            dag_id = context['dag'].dag_id
            task_id = context['task'].task_id
            run_id = context['dag_run'].run_id

            prev_dag_id = context['task_instance'].xcom_pull(key='prev_dag_id') or -1
            prev_task_id = context['task_instance'].xcom_pull(key='prev_task_id') or -1
            run_snapshot_dt = context['task_instance'].xcom_pull(key='run_snapshot_dt') or get_run_snapshot_dt(context)


            dag_stat_dict = {
                "header": {
                    "dag_id": dag_id,
                    "run_id": run_id,
                    "run_snapshot_dt": run_snapshot_dt,
                    "task_id": task_id
                },
                "body": {
                    "prev_logical_date": context.get('prev_logical_date') or None,
                    "logical_date": context['logical_date'],
                    "data_interval_end": context.get('data_interval_end') or None,

                    "prev_dag_id": prev_dag_id,
                    "prev_task_id": prev_task_id
                    # "save_snapshot_dt": response.snapshot_date,
                }
            }
        else:
            dag_id = context['header']["dag_id"]
            task_id = context['header']["task_id"]
            run_id = context['header']["run_id"]
            run_snapshot_dt = context['header'].get('run_snapshot_dt', get_run_snapshot_dt(context))

            prev_dag_id = context['body'].get('prev_dag_id', None)
            prev_task_id = context['body'].get('prev_task_id', None)

            dag_stat_dict = {
                "header": {
                    "dag_id": dag_id,
                    "task_id": task_id,
                    "run_id": run_id,
                    "run_snapshot_dt": run_snapshot_dt,
                },
                "body": {
                    "prev_dag_id": prev_dag_id,
                    "prev_task_id": prev_task_id
                }
            }

        dag_stat_dict['body'].update(body)

        dag_stat = DAGStatusPayload.parse_obj(dag_stat_dict)
        dag_load(dag_stat)

        if self.is_containerized is True:
            # record dag_id at last operator
            context['task_instance'].xcom_push(key='prev_dag_id', value=dag_id)
            context['task_instance'].xcom_push(key='prev_task_id', value=task_id)
            context['task_instance'].xcom_push(key='run_snapshot_dt', value=run_snapshot_dt)
            LOGGER.info(f"xcom push current dag_id: {dag_id} to prev_dag_id")
            LOGGER.info(f"xcom push current task_id: {task_id} to prev_task_id")
            LOGGER.info(f"xcom push current run_snapshot_dt: {run_snapshot_dt} to run_snapshot_dt")
