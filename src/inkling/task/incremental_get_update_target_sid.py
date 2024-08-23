import pandas as pd
from sqlalchemy import create_engine

from tunip import file_utils
from tunip.snapshot_utils import snapshot_now, SnapshotCleaner
from tunip.datetime_utils import to_sql_strftime
from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.utils.airflow_utils import get_my_task
from inkling.dag_stat_utils import DAGStatusDigester


class IncrementalGetUpdateTargetSid(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.task_id = self.task_config["task_id"]

        self.query_format = self.task_config["ingress"]["query"]
        self.sid_type = self.task_config["ingress"]["sid_type"]

        self.egress_path = f"/user/{self.service_config.username}/{self.task_config['egress']['repository_type']}/{self.task_config['egress']['domain_name']}/{self.task_config['egress']['schema_type']}/{self.task_config['egress']['source_type']}"

        self.num_paths_to_left = self.task_config["egress"].get("num_paths_to_left")

    def __call__(self, context=None, snapshot_dt=None):
        LOGGER.info(f"===== START {__class__.__name__} - {self.task_id} =====")

        run_snapshot_dt = snapshot_dt if snapshot_dt else snapshot_now()
        save_snapshot_dt = run_snapshot_dt

        save_path = f"{self.egress_path}/{save_snapshot_dt}"

        file_service = file_utils.services(self.service_config)
        file_service.mkdirs(save_path)

        db_user = self.service_config.config.get('db.user')
        db_password = self.service_config.config.get('db.password')
        db_port = self.service_config.config.get('db.port')
        db_database = self.service_config.config.get('db.database')

        engine = create_engine(f'mysql+mysqldb://{db_user}:{db_password}@127.0.0.1:{db_port}/{db_database}?charset=utf8')

        run_datetime = to_sql_strftime(context["data_interval_end"])
        query = self.query_format.format(run_datetime=run_datetime)

        pd_docs = pd.read_sql(con=engine, sql=query)
        if len(pd_docs) > 0:
            sids = list(pd_docs[self.sid_type])
            file_service.write(save_path + "/" + "sids.txt", ", ".join(sids))
            
            LOGGER.info(f"===== Success to write {len(sids)} sids to {save_path} =====")
        else:
            file_service.write(save_path + "/" + "sids.txt", "")
            LOGGER.info(f"===== No sids to write to {save_path}. (Create empty file) =====")

        if self.num_paths_to_left:
            snapshot_cleaner = SnapshotCleaner(service_config=self.service_config, paths_left=self.num_paths_to_left)
            snapshot_cleaner.clean(root_path=self.egress_path)
            LOGGER.info(f"===== Cleaning old data... ({self.num_paths_to_left} snapshots will remain.) =====")
        else:
            LOGGER.info(f"===== Skip cleaning data... (All snapshots will remain.) =====")

        self.load(context, body={"query": query, "run_snapshot_dt": run_snapshot_dt})
        LOGGER.info(f"===== END {__class__.__name__} =====")

