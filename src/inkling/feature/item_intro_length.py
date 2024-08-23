import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from tunip.feature_path_utils import (
    DomainBasedFeaturePath,
    SchemaTypedFeaturePath,
    SchemaTypedSnapshotFeaturePath,
)
from tunip.service_config import ServiceLevelConfig
from tunip.snapshot_utils import SnapshotPathProvider
from tunip.spark_utils import SparkConnector

from inkling.feature import MultiFieldMerger


class ItemIntroLength(MultiFieldMerger):

    schema_type = "intro_len"
    field_names = ["intro_len"]

    def __init__(self, service_config: ServiceLevelConfig, domain_feature_path: DomainBasedFeaturePath, phase_type: str):
        self.service_config = service_config
        self.schema_feature_path = SchemaTypedFeaturePath(
            domain_path=domain_feature_path,
            schema_type=self.schema_type,
            phase_type="training"
        )
        self.latest_snapshot_dt = SnapshotPathProvider(service_config).latest_snapshot_dt(repr(self.schema_feature_path), force_fs=service_config.filesystem.upper())

    def apply(self, reco_sdf: DataFrame) -> DataFrame:
        spark: SparkSession = SparkConnector.getOrCreate(local=True)

        schema_snapshot_path = SchemaTypedSnapshotFeaturePath(
            snapshot_dt=self.latest_snapshot_dt,
            schema_path=self.schema_feature_path
        )

        t_intro_len_df = spark.read.parquet(
            repr(schema_snapshot_path)
        ).withColumnRenamed(
            "account_sid",
            "user_account_sid"
        )

        featured_sdf = reco_sdf.join(
            t_intro_len_df, on="user_account_sid",
            how="left"
        )

        for field_name in self.field_names:
            featured_sdf = featured_sdf.withColumn(
                field_name,
                F.when(F.isnull(F.col(field_name)), F.lit(0)).otherwise(F.col(field_name))
            )
        return featured_sdf

    def get_field_names(self):
        return self.field_names
        
