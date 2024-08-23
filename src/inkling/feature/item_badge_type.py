import orjson

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from tunip.feature_path_utils import (
    DomainBasedFeaturePath,
    SchemaTypedFeaturePath,
    SchemaTypedSnapshotFeaturePath,
)
from tunip.file_utils import services as file_services
from tunip.service_config import ServiceLevelConfig
from tunip.snapshot_utils import SnapshotPathProvider
from tunip.spark_utils import SparkConnector

from inkling.feature import MultiFieldMerger


class ItemBadgeTypes(MultiFieldMerger):

    schema_type = "item_badge_types"
    schema_info_type = "item_badge_types.info"

    def __init__(self, service_config: ServiceLevelConfig, domain_feature_path: DomainBasedFeaturePath, phase_type: str):
        self.service_config = service_config
        file_service = file_services(self.service_config)

        self.schema_feature_path = SchemaTypedFeaturePath(
            domain_path=domain_feature_path,
            schema_type=self.schema_type,
            phase_type="training"
        )
        self.latest_snapshot_dt = SnapshotPathProvider(service_config).latest_snapshot_dt(repr(self.schema_feature_path), force_fs=service_config.filesystem.upper())

        schema_info_path = SchemaTypedFeaturePath(
            domain_path=domain_feature_path,
            schema_type=self.schema_info_type,
            phase_type="training"
        )
        schema_info_snapshot_path = SchemaTypedSnapshotFeaturePath(
            snapshot_dt=self.latest_snapshot_dt,
            schema_path=schema_info_path
        )
        data_json = file_service.load(f"{repr(schema_info_snapshot_path)}/data.json")
        self.field_names = [r["badge"] for r in orjson.loads(data_json)]

    def apply(self, reco_sdf: DataFrame) -> DataFrame:
        spark: SparkSession = SparkConnector.getOrCreate(local=True)

        schema_snapshot_path = SchemaTypedSnapshotFeaturePath(
            snapshot_dt=self.latest_snapshot_dt,
            schema_path=self.schema_feature_path
        )

        feature_df = spark.read.parquet(
            repr(schema_snapshot_path)
        ).withColumnRenamed(
            "account_sid",
            "item_account_sid"
        )

        featured_sdf = reco_sdf.join(feature_df, on="item_account_sid", how="left")
        return featured_sdf

    def get_field_names(self):
        return self.field_names
        
