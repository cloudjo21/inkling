from pyspark.sql.functions import col, isnan, length, when

from tunip.file_utils import services as file_services
from tunip.feature_path_utils import (
    DomainBasedFeaturePath,
    SchemaTypedFeaturePath,
    SchemaTypedSnapshotFeaturePath,
)
from tunip.path.lake import LakePath
from tunip.service_config import ServiceLevelConfig
from tunip.snapshot_utils import SnapshotPathProvider, snapshot_now
from tunip.spark_utils import SparkConnector

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester


class BuildFeatureItemIntroLength(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super(BuildFeatureItemIntroLength, self).__init__(service_config, task_config.get("is_containerized", True))

        self.service_config = service_config
        self.task_config = task_config

    def __call__(self, context):
        LOGGER.info(f"==== START {__class__.__name__} ====")

        ingress_domain_name = self.task_config["ingress"]["domain_name"]
        ingress_schema_type = self.task_config["ingress"]["schema_type"]
        ingress_source_type = self.task_config["ingress"]["source_type"]
        
        domain_name = self.task_config["egress"]["domain_name"]
        schema_type = self.task_config["egress"]["schema_type"]
        phase_type = self.task_config["egress"]["phase_type"]

        
        file_service = file_services(self.service_config)
        spark = SparkConnector.getOrCreate(local=True)
        snapshot_provider = SnapshotPathProvider(self.service_config)

        ingress_path = f"/user/{self.service_config.username}/lake/pool/{ingress_domain_name}/{ingress_schema_type}/{ingress_source_type}"
        latest_path = snapshot_provider.latest(ingress_path, return_only_nauts_path=True)
        item_char_df = spark.read.parquet(latest_path)

        t_intro_and_len_df = item_char_df.withColumn(
            "intro_len",
            when(
                col("item_introduction").isNull() | isnan(col("item_introduction")), 0
            ).otherwise(
                length(col("item_introduction"))
            )
        ).select(
            "account_sid",
            "intro_len",
            'item_introduction',
        ).filter("t_intro_care_len > 0")


        domain_path = DomainBasedFeaturePath(
            lake_path=LakePath(self.service_config.username),
            domain_name=domain_name
        )
        schema_feature_path = SchemaTypedFeaturePath(
            domain_path=domain_path,
            schema_type=schema_type,
            phase_type=phase_type
        )
        schema_snapshot_path = SchemaTypedSnapshotFeaturePath(
            schema_path=schema_feature_path,
            snapshot_dt=snapshot_now()
        )
        t_intro_len_df = t_intro_and_len_df.select(
            "account_sid",
            "t_intro_care_len",
            "t_intro_stem_len",
            "t_intro_sports_len",
            "t_intro_art_len",
            "t_intro_foreign_language_len",
            "t_intro_korean_len",
        )

        file_service.mkdirs(repr(schema_snapshot_path))
        t_intro_len_df.write.format("parquet").mode("overwrite").save(repr(schema_snapshot_path))

        LOGGER.info(f"==== END {__class__.__name__} ====")
 
