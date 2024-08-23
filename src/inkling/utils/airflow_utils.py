import os

from inkling import LOGGER

from tunip.constants import DATA_POOL_PERIOD_INTERVAL
from tunip.datetime_utils import dag_runid2snapshot, to_snapshot_strftime


def get_my_task(dag_config, context, dag_id=None, task_id=None):
    if context and not dag_id:
        task_configs = dag_config[context['dag'].dag_id]
    else:
        assert dag_id is not None
        task_configs = dag_config[dag_id]
    
    task_id = task_id or context['task'].task_id

    deploy_env = dag_config.get('deploy_env') or None
    only_task_id = '.'.join(task_id.split('.')[-len(task_id.split('.'))+1:])
    LOGGER.info(f'find task:{only_task_id} from task configs')
    LOGGER.info(task_configs[0]['task_id'])
    try:
        tasks = list(filter(lambda t: t['task_id'] == only_task_id , task_configs))
        if tasks:
            tasks[0]['task_id'] = task_id
            LOGGER.info(f"get task_config: {tasks[0]}")
            return tasks[0]
        else:
            LOGGER.error(f'CANNOT FIND task: {task_id}')
    except:
        LOGGER.error(f'CANNOT FIND task: {task_id}')
        return None
 
def get_run_snapshot_dt(context):
    run_type = context['dag_run'].run_type
    if run_type == "scheduled":
        data_interval_end = context['dag_run'].data_interval_end
        run_snapshot_dt = to_snapshot_strftime(data_interval_end)
    elif run_type == "manual" or run_type == "dataset_triggered":  # manual or trigger
        run_id = context['dag_run'].run_id
        run_snapshot_dt = dag_runid2snapshot(run_id)
    else:
        raise NotImplementedError(f"RunType - {run_type} Not supported yet.")

    LOGGER.info(f"DAG RUN TYPE: {run_type} (run_snapshot_dt: {run_snapshot_dt})")
    return run_snapshot_dt

def get_data_pool_info(task_config: dict):
    date_interval_start = os.environ.get(
        "DATA_POOL_START_DATE",
        task_config["ingress"].get("date_interval_start") or None
    )
    date_interval_end = os.environ.get(
        "DATA_POOL_END_DATE",
        task_config["ingress"].get("date_interval_end") or None
    )
    pool_period_interval = DATA_POOL_PERIOD_INTERVAL
    if ("DATA_POOL_START_DATE" in os.environ) and ("DATA_POOL_END_DATE" in os.environ):
        pool_period_interval = os.environ.get("DATA_POOL_PERIOD_MONTHS", DATA_POOL_PERIOD_INTERVAL)
    
    return date_interval_start, date_interval_end, pool_period_interval


def get_dates_by_phase_type(task_config: dict, phase_type: str):
    date_interval_start, date_interval_end = None, None

    if phase_type.lower() == "training":
        date_interval_start = os.environ.get(
            "TRAINING_DATA_START_DATE",
            task_config["ingress"].get("date_interval_start") or None
        )
        date_interval_end = os.environ.get(
            "TRAINING_DATA_END_DATE",
            task_config["ingress"].get("date_interval_end") or None
        )
    elif phase_type.lower() == "test":
        date_interval_start = os.environ.get(
            "TEST_DATA_START_DATE",
            task_config["ingress"].get("date_interval_start") or None
        )
        date_interval_end = os.environ.get(
            "TEST_DATA_END_DATE",
            task_config["ingress"].get("date_interval_end") or None
        )
    else:
        raise Exception(f"NOT ALLOWED phase_type: {phase_type}")

    return date_interval_start, date_interval_end


def get_mart_dates(build_opt: dict):
    if build_opt["phase_type"] == "training":
        start_date = os.environ.get("TRAINING_DATA_START_DATE", build_opt["start_date_interval"])
        end_date = os.environ.get("TRAINING_DATA_END_DATE", build_opt["end_date_interval"])
    elif build_opt["phase_type"] == "test":
        start_date = os.environ.get("TEST_DATA_START_DATE", build_opt["start_date_interval"])
        end_date = os.environ.get("TEST_DATA_END_DATE", build_opt["end_date_interval"])
    else:
        raise Exception(f"NOT ALLOWED phase_type: {build_opt.get('phase_type')}")
    
    return start_date, end_date
