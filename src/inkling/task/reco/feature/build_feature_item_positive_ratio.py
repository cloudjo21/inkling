import json

from pyspark.sql.functions import col, sum, when
from tunip.file_utils import services as file_services
from tunip.service_config import ServiceLevelConfig
from tunip.snapshot_utils import snapshot_now
from tunip.spark_utils import SparkConnector

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester
from inkling.utils.bigquery_utils import BigQuery2PandasDataFrame


class BuildFeatureItemPositiveRatio(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super(BuildFeatureItemFeedbackYnRatio, self).__init__(service_config, task_config.get("is_containerized", True))

        self.service_config = service_config
        self.task_config = task_config

    def __call__(self, context):
        LOGGER.info(f"==== START {__class__.__name__} ====")

        domain_name = self.task_config["ingress"]["domain_name"]
        schema_type = self.task_config["ingress"]["schema_type"]
        phase_type = self.task_config["ingress"]["phase_type"]

        pivot_schema_type = self.task_config["ingress"]["pivot_schema_type"]

        query = self.task_config["ingress"]["bigquery"]

        file_service = file_services(self.service_config)
        spark = SparkConnector.getOrCreate(local=True)

        snapshot_dt = snapshot_now()
        path = f"/user/{self.service_config.username}/lake/{domain_name}/{pivot_schema_type}/{phase_type}/{snapshot_dt}"

        bq2df = BigQuery2PandasDataFrame(self.service_config, query, path)
        bq2df.save()

        item_feedback_yn_df = spark.read.json(path)

        positive_df = item_feedback_yn_df.withColumn("positive_interaction", when(col("interaction").cast('float') >= 3.0, 1).otherwise(0)).groupBy("item_sid").agg(sum("positive_interaction").alias("positive_sum"))
        negative_df = item_feedback_yn_df.withColumn("negative_interaction", when(col("interaction").cast('float') < 3.0, 1).otherwise(0)).groupBy("item_sid").agg(sum("negative_interaction").alias("negative_sum"))
        positive_feedback_ratio_df = positive_df.join(negative_df, on="item_sid").withColumn("positive_ratio", col("positive_sum") / (col("positive_sum") + col("negative_sum")))

        positive_feedback_ratio_pdf = positive_feedback_ratio_df.toPandas()
        positive_feedback_ratio_pdf.set_index("item_sid", inplace=True)

        file_service.mkdirs(f"/user/{self.service_config.username}/lake/{domain_name}/positive_ratio/{phase_type}/{snapshot_dt}")

        with file_service.fs.open(f"{self.service_config.filesystem_prefix}/user/{self.service_config.username}/lake/{domain_name}/positive_ratio/{phase_type}/{snapshot_dt}/data.csv", 'w') as f:
            positive_feedback_ratio_pdf.loc[:, "positive_ratio"].to_csv(f, index=True)

        snapshot_dt = snapshot_now()

        file_service.mkdirs(f"/user/{self.service_config.username}/lake/{domain_name}/{schema_type}.info/{phase_type}/{snapshot_dt}")

        body = json.dumps({
            "mean": {
                "positive_ratio": positive_feedback_ratio_pdf.mean(),
            }
        })

        file_service.write(path=f"/user/{self.service_config.username}/lake/{domain_name}/{schema_type}.info/{phase_type}/{snapshot_dt}/data.json", contents=body)

        LOGGER.info(f"==== END {__class__.__name__} ====")
        
