import json
import pyspark.sql.functions as F

from elasticsearch import BadRequestError
from pyspark.sql.types import StructType

from tunip.es_utils import (
    create_index,
    init_elastic_client,
    list_indexes_of
)
from tunip.file_utils import services as file_services
from tunip.service_config import get_service_config
from tunip.snapshot_utils import SnapshotPathProvider
from tunip.spark_utils import SparkConnector
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester
from inkling.utils.airflow_utils import get_my_task
from inkling.utils.spark_schema_infer import SparkDataFrameSchemaInferer


class UpdateIndexDocumentWithSchema(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.ingress_path = f"/user/{service_config.username}/mart/{self.task_config['ingress']['domain_name']}/{self.task_config['ingress']['schema_type']}/{self.task_config['ingress']['source_type']}"

        schema_type = self.task_config["ingress"]["schema_type"].split(".")[0]

        self.schema_root_path = f"/user/{self.service_config.username}/lake/{self.task_config['ingress']['domain_name']}/{schema_type}.schema"
        
        self.spark_config = self.task_config["egress"].get("spark_config", None)

        self.indexing_type = self.task_config["egress"]["indexing_type"]

        self.index_name = self.task_config["egress"]["index_name"]
        self.mapping_id = self.task_config["egress"].get("mapping_id")
        self.index_fields = self.task_config["egress"]["index_field"]
        self.default_valued_fields = self.task_config["egress"].get("default_valued_fields", None)

    def __call__(self, context=None):
        LOGGER.info(f"===== START {__class__.__name__} =====")
        prev_dag_stat = self.fetch(context)

        run_snapshot_dt = prev_dag_stat.header.run_snapshot_dt or None
        save_snapshot_dt = run_snapshot_dt

        spark = SparkConnector.getOrCreate(local=True, spark_config=self.spark_config)

        snapshot_path_provider = SnapshotPathProvider(self.service_config)
        latest_schema_path = snapshot_path_provider.latest(self.schema_root_path, return_only_nauts_path=True)

        file_service = file_services(self.service_config)
        schema_dict = file_service.load(f"{latest_schema_path}/data.json").decode("utf-8")

        schema = StructType.fromJson(json.loads(schema_dict))

        data = spark.read.schema(schema).json(f"{self.ingress_path}/{run_snapshot_dt}")

        LOGGER.info(f"===== Start Indexing {self.index_name} =====")

        self.es_client = init_elastic_client(self.service_config)
        type_inferer = SparkDataFrameSchemaInferer()
        
        select_columns = [col for col in self.index_fields[self.index_name] if col in data.columns]
        docs_df = data.select(select_columns)
        if self.default_valued_fields is not None:
            docs_df = docs_df.withColumns(
                {
                    vf["field_name"]: F.lit(vf["value"]).cast(
                        type_inferer.get_spark_primitive_type(vf["field_name"], vf["value"])
                    ) for vf in self.default_valued_fields
                }
            )

        if self.indexing_type == "full":
            write_mode = "overwrite"

            index_setting_path = self.task_config["ingress"]["index_setting_path"]

            current_index_name = f"{self.index_name}-{save_snapshot_dt}"
            index_mapping_path = f"{self.service_config.resource_path}/{index_setting_path}/index_setting.json"
            with open(index_mapping_path, mode="r") as f:
                index_setting = json.load(f)
                try:
                    create_index(es=self.es_client, index_name=current_index_name, mappings=index_setting["mappings"])
                except BadRequestError as e:
                    if e.message == "resource_already_exists_exception":
                        LOGGER.info("===== Index Already Exists... =====")
                    else:
                        raise
        elif self.indexing_type == "incremental":
            write_mode = "append"

            index_names = list_indexes_of(self.es_client, alias=self.index_name, elastic_host=self.service_config.elastic_host, desc=True)
            current_index_name = index_names[0]
        else:
            raise ValueError(f"===== update_type < {self.indexing_type} > Not Implemented Yet. =====")

        if self.mapping_id:
            docs_df.write.option(
                "es.nodes",
                self.service_config.elastic_host
            ).option(
                "es.mapping.id",
                self.mapping_id
            ).option(
                "es.batch.write.refresh",
                "true"
            ).option(
                "es.write.operation",
                "index"
            ).format(
                "org.elasticsearch.spark.sql"
            ).mode(
                write_mode
            ).save(
                current_index_name
            )
            LOGGER.info(f"{__class__.__name__} have written documents dataframe to {current_index_name} with es.mapping.id:{self.mapping_id}")
        else:
            docs_df.write.option(
                "es.nodes",
                self.service_config.elastic_host
            ).option(
                "es.batch.write.refresh",
                "true"
            ).option(
                "es.write.operation",
                "index"
            ).format(
                "org.elasticsearch.spark.sql"
            ).mode(
                write_mode
            ).save(
                current_index_name
            )
            LOGGER.info(f"{__class__.__name__} have written documents dataframe to {current_index_name}")

        if self.indexing_type == "full":
            file_service = file_services(self.service_config)
            index_snapshot_path = f"/user/{self.service_config.username}/warehouse/index2alias/{self.index_name}/{save_snapshot_dt}"
            file_service.mkdirs(index_snapshot_path)
            file_service.write(f"{index_snapshot_path}/data.txt", f"{current_index_name}")
            file_service.mkdirs(f"{index_snapshot_path}/_SUCCESS")
            
        LOGGER.info(f"===== Ingest {current_index_name} to Elasticsearch Done =====")

        self.load(context, body={"run_snapshot_dt": run_snapshot_dt})

