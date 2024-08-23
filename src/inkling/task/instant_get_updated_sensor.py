import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine

from tunip.constants import TIME_ZONE
from tunip.datetime_utils import SNAPSHOT_DT_FORMAT, to_sql_strftime
from tunip.service_config import DbConfig, ServiceLevelConfig
from tunip.snapshot_utils import snapshot_now

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester


class InstantGetUpdatedSensor(DAGStatusDigester):
    
    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.task_id = self.task_config["task_id"]
        self.delta_seconds = self.task_config["ingress"]["delta_seconds"]

    def __call__(self, context=None, snapshot_dt=None):
        triggered = False

        LOGGER.info(f"==== START {__class__.__name__} - {self.task_id}")

        run_snapshot_dt = snapshot_dt if snapshot_dt else snapshot_now()
 
        run_datetime = to_sql_strftime(datetime.strptime(run_snapshot_dt, SNAPSHOT_DT_FORMAT), timezone="UTC")
        query = self.task_config["ingress"]["query"].format(
            run_datetime=run_datetime,
            delta_seconds=self.delta_seconds
        )

        db_config: DbConfig = self.service_config.db_config
        engine = create_engine(f'mysql+mysqldb://{db_config.user}:{db_config.password}@127.0.0.1:{db_config.port}/{db_config.database}?charset=utf8')

        LOGGER.info(f"==== Query to trigger update: {query} ====")

        rows: pd.DataFrame = pd.read_sql(con=engine, sql=query)

        LOGGER.info(f"==== Result to trigger update: {rows}")

        if len(rows) > 0 and rows.shape == (1, 1) and rows.values[0][0] > 0:
            LOGGER.info(f"==== Trigger to get {rows.values[0][0]} sids updated ====")
            triggered = True

        self.load(
            context,
            body={
                "query": query,
                "run_snapshot_dt": run_snapshot_dt,
                "run_datetime": run_datetime,
                "delta_seconds": self.delta_seconds
            }
        )
        LOGGER.info(f"==== END {__class__.__name__} ====")

        return triggered

