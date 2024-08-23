import json

from pyspark.sql.types import StructType
from pyspark.sql.functions import concat_ws, col

from tunip.service_config import get_service_config
from tunip.snapshot_utils import snapshot_now, SnapshotPathProvider
from tunip.spark_utils import SparkConnector
from tunip.yaml_loader import YamlLoader
from tunip.file_utils import services as file_services

from inkling.utils.airflow_utils import get_my_task
from inkling.dag_stat_utils import DAGStatusDigester
from inkling import LOGGER


class MergeDocuments(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        snapshot_path_provider = SnapshotPathProvider(self.service_config)

        file_service = file_services(self.service_config)

        self.ingress_path_list = []
        self.ingress_schema_list = []

        # Get list of schema and ingress path
        for path in self.task_config["ingress"]["path_list"]:
            ingress_path = f"/user/{self.service_config.username}/{path['repository_type']}/{path['domain_name']}/{path['schema_type']}/{path['source_type']}"
            self.ingress_path_list.append(ingress_path)

            schema_type = path['schema_type'].split(".")[0]

            schema_root_path = f"/user/{self.service_config.username}/lake/{path['domain_name']}/{schema_type}.schema"

            latest_schema_path = snapshot_path_provider.latest(schema_root_path, return_only_nauts_path=True)
            schema_dict = file_service.load(f"{latest_schema_path}/data.json").decode("utf-8")

            schema = StructType.fromJson(json.loads(schema_dict))
            
            self.ingress_schema_list.append(schema)

        self.id_set_info_list = self.task_config["ingress"]["id_set_info_list"]

        self.egress_path = f"/user/{self.service_config.username}/{self.task_config['egress']['repository_type']}/{self.task_config['egress']['domain_name']}/{self.task_config['egress']['schema_type']}/{self.task_config['egress']['source_type']}"

    def __call__(self, context=None):
        LOGGER.info(f"===== START {__class__.__name__} =====")

        prev_dag_stat = self.fetch(context)

        run_snapshot_dt = prev_dag_stat.header.run_snapshot_dt or None
        save_snapshot_dt = run_snapshot_dt if run_snapshot_dt else snapshot_now()

        save_path = f"{self.egress_path}/{save_snapshot_dt}"

        spark = SparkConnector.getOrCreate(local=True)

        merged_df = None
        id_columns = [path_info["id_column"] for path_info in self.id_set_info_list]

        id_set_list = []
        for id_set_info in self.id_set_info_list:
            id_set_path = f"/user/{self.service_config.username}/{id_set_info['repository_type']}/{id_set_info['domain_name']}/{id_set_info['schema_type']}/{id_set_info['source_type']}"
            id_column = id_set_info["id_column"]
            id_set_list.append((id_column, id_set_path))

        # Merge Saved Data
        for ingress_path, ingress_schema in zip(self.ingress_path_list, self.ingress_schema_list):
            docs_df = spark.read.schema(ingress_schema).json(f"{ingress_path}/{run_snapshot_dt}")
            if not merged_df:  # 첫 feature에 대해 LEFT JOIN하는 경우
                if docs_df.isEmpty():
                    continue
                # 모든 id set에 대해 LEFT JOIN 실행
                for id_column, id_set_path in id_set_list:
                    id_set_df = spark.read.json(f"{id_set_path}/{run_snapshot_dt}")
                    docs_df = id_set_df.join(docs_df, [id_column], "LEFT")
                merged_df = docs_df
            else:
                merged_df = merged_df.join(docs_df, id_columns, how="LEFT")

        save_df = merged_df.dropna(how="any", subset=id_columns)
        save_df.write.option('compression', 'gzip').format('json').mode('overwrite').save(save_path)

        LOGGER.info(f"{__class__.__name__} have written {save_df.count()} documents to {save_path}.")

        self.load(context, body={"save_snapshot_dt": save_snapshot_dt})

        LOGGER.info(f"===== END {__class__.__name__} =====")


