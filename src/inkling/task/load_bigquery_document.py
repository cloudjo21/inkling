from tunip.service_config import get_service_config, ServiceLevelConfig
from tunip.snapshot_utils import snapshot_now, SnapshotCleaner
from tunip.yaml_loader import YamlLoader

from inkling.utils.airflow_utils import get_my_task
from inkling.dag_stat_utils import DAGStatusDigester
from inkling.utils.bigquery_utils import BigQuery2PandasDataFrame
from inkling import LOGGER


class LoadBigQueryDocument(DAGStatusDigester):
    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.query = self.task_config["ingress"]["big_query"]

        self.egress_path = f"/user/{self.service_config.username}/{self.task_config['egress']['repository_type']}/{self.task_config['egress']['domain_name']}/{self.task_config['egress']['schema_type']}/{self.task_config['egress']['source_type']}"

        self.num_paths_to_left = self.task_config["egress"].get("num_paths_to_left")

    def __call__(self, context=None, snapshot_dt=None):
        LOGGER.info(f"===== START {__class__.__name__} =====")
        
        save_snapshot_dt = snapshot_dt or snapshot_now()
        save_path = f"{self.egress_path}/{save_snapshot_dt}"

        LOGGER.info(f"The following query will be run : {self.query}")

        bq2pdf = BigQuery2PandasDataFrame(
            service_config=self.service_config,
            bigquery=self.query,
            save_path=save_path
        )
        bq2pdf.save()

        if self.num_paths_to_left:
            snapshot_cleaner = SnapshotCleaner(service_config=self.service_config, paths_left=self.num_paths_to_left)
            snapshot_cleaner.clean(root_path=self.egress_path)
            LOGGER.info(f"===== Cleaning old data... ({self.num_paths_to_left} snapshots will remain.) =====")
        else:
            LOGGER.info(f"===== Skip cleaning data... (All snapshots will remain.) =====")

        self.load(context, body={"bigquery": self.query, "save_snapshot_dt": save_snapshot_dt})

        LOGGER.info(f"===== END {__class__.__name__}: {self.task_config['task_id']} =====")

