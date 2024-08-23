import orjson

from pyspark.sql.functions import col, explode, lit, when, udf
from pyspark.sql.types import ArrayType, BooleanType, StringType

from tunip.feature_path_utils import (
    DomainBasedFeaturePath,
    SchemaTypedFeaturePath,
    SchemaTypedSnapshotFeaturePath,
)
from tunip.file_utils import services as file_services
from tunip.path.lake import LakePath
from tunip.service_config import ServiceLevelConfig
from tunip.snapshot_utils import SnapshotPathProvider, snapshot_now
from tunip.spark_utils import SparkConnector

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester


DEFAULT_HAS_BADGE_TYPE = 0

def get_badges(badge_type: str):
    badges = []
    if badge_type:
        badges = badge_type.split("|")
    else:
        badges = []
    return badges

def has_badge(badge, badge_type):
    if not badge_type:
        return False
    if badge in badge_type:
        return True
    else:
        return False


class BuildFeatureItemBadgeTypes(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super(BuildFeatureItemBadgeTypes, self).__init__(service_config, task_config.get("is_containerized", True))

        self.service_config = service_config
        self.task_config = task_config

    def __call__(self, context):
        @udf(returnType=ArrayType(StringType()))
        def get_badges_udf(badge_type: str):
            return get_badges(badge_type)

        @udf(returnType=BooleanType())
        def has_badge_udf(badge, badge_type):
            return has_badge(badge, badge_type)

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

        path = f"/user/{self.service_config.username}/lake/pool/{ingress_domain_name}/{ingress_schema_type}/{ingress_source_type}"
        latest_path = snapshot_provider.latest(path, return_only_nauts_path=True)
        item_char_df = spark.read.parquet(latest_path)

        snapshot_dt = snapshot_now()

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
            snapshot_dt=snapshot_dt
        )

        badge_df = item_char_df.select(
            "badge_type"
        ).withColumn(
            "badge",
            explode(get_badges_udf(col("badge_type")))
        )

        badge_rows = badge_df.select("badge").distinct().orderBy("badge").filter("badge != ''").collect()
        unique_badges = [r.badge for r in badge_rows]

        feature_sdf = item_char_df.select("account_sid", "badge_type")

        for badge_cat in unique_badges:
            feature_sdf = feature_sdf.withColumn(
                badge_cat,
                when(has_badge_udf(lit(badge_cat), col("badge_type")), lit(1)).otherwise(lit(0))
            )

        file_service.mkdirs(repr(schema_snapshot_path))
        feature_sdf.write.format("parquet").mode("overwrite").save(repr(schema_snapshot_path))

        schema_info_feature_path = SchemaTypedFeaturePath(
            domain_path=domain_path,
            schema_type=f"{schema_type}.info",
            phase_type=phase_type
        )
        schema_info_snapshot_path = SchemaTypedSnapshotFeaturePath(
            schema_path=schema_info_feature_path,
            snapshot_dt=snapshot_dt
        )

        file_service.mkdirs(repr(schema_info_snapshot_path))

        data = orjson.dumps([{"badge": b} for b in unique_badges]).decode()
        file_service.write(f"{repr(schema_info_snapshot_path)}/data.json", data)

        LOGGER.info(f"==== END {__class__.__name__} ====")
