import argparse

from pyspark.sql.functions import col, collect_list, udf
from pyspark.sql.types import StringType

from tunip.file_utils import services as file_services
from tunip.service_config import ServiceLevelConfig, get_service_config
from tunip.snapshot_utils import SnapshotPathProvider, snapshot_now
from tunip.spark_utils import SparkConnector
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.utils.airflow_utils import get_my_task


@udf(returnType=StringType())
def stringfy_tag_list(tag_list):
    if tag_list is not None:
        return " ".join(list(sorted(tag_list)))
    return ""


class CollectItemContentInitial:

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        self.service_config = service_config
        self.task_config = task_config

    def __call__(self, context):

        domain_name = self.task_config["ingress"]["domain_name"]
        source_type = self.task_config["ingress"]["source_type"]
        target_tag_condition = self.task_config["ingress"]["target_tag_condition"]
        category_target_codes = self.task_config["ingress"]["category_codes"]
        schema_type = self.task_config["egress"]["schema_type"]
        phase_type = self.task_config["egress"]["phase_type"]

        snapshot_provider = SnapshotPathProvider(self.service_config)
        spark = SparkConnector.getOrCreate(local=True)
        file_service = file_services(self.service_config)

        account_df = spark.read.parquet(
            snapshot_provider.latest(
                f"/user/{service_config.username}/lake/pool/{domain_name}/active_items/{source_type}",
                return_only_nauts_path=False
            )
        )
        account_sdf = account_df.withColumnRenamed("sid", "account_sid")
        tt_sdf = spark.read.parquet(snapshot_provider.latest(f"/user/{service_config.username}/lake/pool/{domain_name}/item_contents/{source_type}", return_only_nauts_path=True))
        tt_searchable_sdf = tt_sdf.filter(target_tag_condition)

        snapshot_dt = snapshot_now()
        for target_code in category_target_codes:
            tags_account_sdf = tt_searchable_sdf.join(
                account_sdf, on="account_sid", how="left"
            ).select(
                "account_sid", "code", "name"
            ).filter(
                f"code = {target_code}"
            ).withColumnRenamed(
                "name", "tag_name"
            ).withColumnRenamed(
                "account_sid", "item_sid"
            )
            tag_names_sdf = tags_account_sdf.groupBy(
                "item_sid"
            ).agg(
                collect_list("tag_name").alias("tag_list")
            ).withColumn(
                "tag_names", stringfy_tag_list(col("tag_list"))
            )

            egress_path = f"/user/{self.service_config.username}/lake/{domain_name}/{schema_type}/{phase_type}/{snapshot_dt}/{target_code}"
            file_service.mkdirs(egress_path)
            tag_names_sdf.write.format("parquet").mode("overwrite").save(egress_path)

            LOGGER.info(f"save tag_names to {egress_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="collect initial item contents")
    parser.add_argument(
        "-p",
        "--phase",
        help="the phase of dataset from the followings: training, test",
        type=str,
        required=True,
        default="training"
    )
    args = parser.parse_args()
    from inkling.dags.dag_build_item_content_vectors import DAG_IDS
    dag_id = DAG_IDS[args.phase]

    service_config = get_service_config()
    dag_config_filepath = f"{service_config.resource_path}/feature-store/{dag_id}/dag.yml"
    print(f"DAG CONFIG FILEPATH: {dag_config_filepath}")
    dag_config = YamlLoader(dag_config_filepath).load()
    task_config = get_my_task(dag_config, context=None, dag_id=dag_id, task_id="collect_item_content_initial")

    task = CollectItemContentInitial(service_config, task_config)
    task(context={})
