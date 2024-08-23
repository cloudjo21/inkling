import pandas as pd
import pyspark.pandas as ps

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import avg, col, when
from pyspark.sql.session import SparkSession

from tunip.feature_path_utils import (
    DomainBasedFeaturePath,
    SchemaTypedFeaturePath,
    SchemaTypedSnapshotFeaturePath
)
from tunip.service_config import ServiceLevelConfig
from tunip.snapshot_utils import SnapshotPathProvider
from tunip.spark_utils import SparkConnector


class PositiveRatio:
    schema_type = "positive_ratio"
    field_name = "positive_ratio"

    def __init__(self, service_config: ServiceLevelConfig, domain_feature_path: DomainBasedFeaturePath, phase_type: str):
        schema_feature_path = SchemaTypedFeaturePath(
            domain_path=domain_feature_path,
            schema_type=self.schema_type,
            phase_type="training"
        )
        latest_snapshot_dt = SnapshotPathProvider(service_config).latest_snapshot_dt(repr(schema_feature_path), force_fs=service_config.filesystem.upper())
        self.snapshot_feature_path = SchemaTypedSnapshotFeaturePath(snapshot_dt=latest_snapshot_dt, schema_path=schema_feature_path)
        self.fs_prefix = service_config.filesystem_prefix
    
    def apply(self, reco_sdf: DataFrame):
        _: SparkSession = SparkConnector.getOrCreate(local=True)

        feature_sdf = ps.from_pandas(
            pd.read_csv(
                f"{self.fs_prefix}{repr(self.snapshot_feature_path)}/data.csv"
            ).set_index("user_account_sid")
        ).to_spark(
            index_col="user_account_sid"
        )

        mean_user_positive_ratio = feature_sdf.select(self.field_name).agg(avg(col(self.field_name)).alias('avg_ratio'))
        avg_ratio = mean_user_positive_ratio.take(1)[0].avg_ratio
        featured_reco_sdf = reco_sdf.join(feature_sdf.select("user_account_sid", self.field_name), on="user_account_sid", how="left") \
            .withColumn(self.field_name, when(col(self.field_name).isNull(), avg_ratio).otherwise(col(self.field_name)))
        
        return featured_reco_sdf
    
    def get_field_names(self):
        return [self.field_name]

