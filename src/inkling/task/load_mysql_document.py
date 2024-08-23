import json

from pyspark.sql.types import StructType, DoubleType

from tunip.service_config import get_service_config
from tunip.snapshot_utils import snapshot_now, SnapshotPathProvider, SnapshotCleaner
from tunip.datetime_utils import to_sql_strftime
from tunip.spark_utils import SparkConnector
from tunip.yaml_loader import YamlLoader
from tunip.file_utils import services as file_services

from inkling.utils.airflow_utils import get_my_task
from inkling.dag_stat_utils import DAGStatusDigester
from inkling import LOGGER


class LoadMySqlDocument(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.task_id = self.task_config["task_id"]

        self.schema_root_path = f"/user/{self.service_config.username}/lake/{self.task_config['ingress']['domain_name']}/{self.task_config['ingress']['schema_type']}.schema"

        self.query_format = self.task_config["ingress"]["query"]

        self.egress_path = f"/user/{self.service_config.username}/{self.task_config['egress']['repository_type']}/{self.task_config['egress']['domain_name']}/{self.task_config['egress']['schema_type']}/{self.task_config['egress']['source_type']}"

        self.num_paths_to_left = self.task_config["egress"].get("num_paths_to_left")

    def __call__(self, context=None, snapshot_dt=None):
        LOGGER.info(f"===== START {__class__.__name__} - {self.task_id} =====")

        spark = SparkConnector.getOrCreate(local=True)

        run_snapshot_dt = snapshot_dt if snapshot_dt else snapshot_now()
        save_snapshot_dt = run_snapshot_dt

        save_path = f"{self.egress_path}/{save_snapshot_dt}"

        snapshot_path_provider = SnapshotPathProvider(self.service_config)
        latest_schema_path = snapshot_path_provider.latest(self.schema_root_path, return_only_nauts_path=True)

        file_service = file_services(self.service_config)
        schema_dict = file_service.load(f"{latest_schema_path}/data.json").decode("utf-8")

        schema = StructType.fromJson(json.loads(schema_dict))

        run_datetime = to_sql_strftime(context["data_interval_end"])
        query = self.query_format.format(run_datetime=run_datetime)

        LOGGER.info(f"The following query will be run : {query}")

        docs_df = spark.read.option(
            "driver",
            "com.mysql.jdbc.Driver"
        ).option(
            "url",
            f"jdbc:mysql://localhost:{self.service_config.db_config.port}/{self.service_config.db_config.database}?charset=utf8"
        ).option(
            "query",
            query
        ).option(
            "user",
            self.service_config.db_config.user
        ).option(
            "password",
            self.service_config.db_config.password
        ).schema(
            schema
        ).format(
            "jdbc"
        ).load()

        save_df = self._cast_column_type_decimal2double(docs_df)  # PGMT-795

        save_df.write.option('compression', 'gzip').format('json').mode('overwrite').save(save_path)
    
        LOGGER.info(f"===== Success to write data to {save_path} =====")

        if self.num_paths_to_left:
            snapshot_cleaner = SnapshotCleaner(service_config=self.service_config, paths_left=self.num_paths_to_left)
            snapshot_cleaner.clean(root_path=self.egress_path)
            LOGGER.info(f"===== Cleaning old data... ({self.num_paths_to_left} snapshots will remain.) =====")
        else:
            LOGGER.info(f"===== Skip cleaning data... (All snapshots will remain.) =====")

        self.load(context, body={"query": query, "run_snapshot_dt": run_snapshot_dt})
        LOGGER.info(f"===== END {__class__.__name__} - {self.task_id} =====")

    # PGMT-795 MySQL5.7 버전에서 CAST(~ AS FLOAT) 지원하지않는 문제로 인해 코드에서 decimal -> double 변경
    def _cast_column_type_decimal2double(self, docs_df):
        cols_to_cast = {}
        for schema in docs_df.schema:
            if schema.dataType.typeName() == "decimal":
                cols_to_cast[schema.name] = docs_df[schema.name].cast(DoubleType())
        casted_df = docs_df.withColumns(cols_to_cast)

        return casted_df

