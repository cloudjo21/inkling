import argparse

import pyspark.sql.functions as F

from pyspark.sql.functions import udf
from pyspark.sql.types import TimestampType

from tunip.feature_path_utils import (
    DomainBasedFeaturePath,
    SchemaTypedFeaturePath,
    SchemaTypedSnapshotFeaturePath
)
from tunip.path.lake import LakePath
from tunip.service_config import (
    ServiceLevelConfig,
    get_service_config
)
from tunip.snapshot_utils import (
    SnapshotPathProvider,
    snapshot_now
)
from tunip.spark_utils import SparkConnector
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester
from inkling.utils.airflow_utils import get_my_task


class LoadLakeRootDatasetLabeled(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.domain_name = self.task_config["ingress"]["domain_name"]
        self.schema_type = self.task_config["ingress"]["schema_type"]
        self.source_type = self.task_config["ingress"]["source_type"]

        self.target_input_condition = self.task_config["ingress"]["target_input_condition"]
        self.target_label_condition = self.task_config["ingress"]["target_label_condition"]
        self.additional_columns = self.task_config["ingress"]["additional_columns"]
        self.subordinary_columns = self.task_config["ingress"]["subordinary_columns"]
        self.user_id_name = self.task_config["ingress"]["user_id_name"]

        self.egress_domain_name = self.task_config["egress"]["domain_name"]
        self.egress_schema_type = self.task_config["egress"]["schema_type"]

    def __call__(self, context):
        prev_dag_stat = self.fetch(context)

        run_snapshot_dt = (prev_dag_stat.header.run_snapshot_dt or None) if prev_dag_stat is not None else None
        save_snapshot_dt = run_snapshot_dt if run_snapshot_dt else snapshot_now()

        LOGGER.info(f"==== START {__class__.__name__} ====")

        snapshot_provider = SnapshotPathProvider(self.service_config)

        spark_config = self.task_config["ingress"].get(
            "spark_config",
            {"spark.executor.cores": 2, "spark.executor.memory": "2g", "spark.driver.memory": "4g"}
        )
        spark = SparkConnector.getOrCreate(local=True, spark_config=spark_config)

        domain_feature_path = DomainBasedFeaturePath(
            lake_path=LakePath(self.service_config.username),
            domain_name=self.domain_name
        )
        reco_log_dataset_path = SchemaTypedFeaturePath(
            domain_path=domain_feature_path,
            schema_type=self.schema_type,
            phase_type=self.source_type
        )
        latest_reco_log_dataset_path = snapshot_provider.latest(repr(reco_log_dataset_path), return_only_nauts_path=True)
        reco_dataset_df = spark.read.parquet(latest_reco_log_dataset_path)
        
        reco_subdataset_df = reco_dataset_df.select(
            self.user_id_name,
            *self.subordinary_columns
        ).dropDuplicates([self.user_id_name])

        reco_req_dataset_df = reco_dataset_df.filter(
            self.target_input_condition
        ).groupBy(
            *(
                [
                    "recommendation_sid",
                    "user_account_sid",
                    "item_sid"
                ] + self.additional_columns
            )
        ).agg(
            F.first("logged_at").alias("l_logged_at")
        ).withColumn(
            "req",
            F.lit(1)
        )

        reco_res_dataset_df = reco_dataset_df.filter(
            self.target_label_condition
        ).groupBy(
            *(
                [
                    "recommendation_sid",
                    "user_account_sid",
                    "item_sid"
                ] + self.additional_columns
            )
        ).agg(
            F.first("logged_at").alias("r_logged_at")
        ).withColumn(
            "res",
            F.lit(1)
        )

        @udf(returnType=TimestampType())
        def choose_logged_at(l_logged_at, r_logged_at):
            if not l_logged_at:
                return r_logged_at
            elif not r_logged_at:
                return l_logged_at
            elif l_logged_at and r_logged_at:
                return min(l_logged_at, r_logged_at)
            else:
                return l_logged_at
        
        reco_join_dataset_df = reco_req_dataset_df.join(
            reco_res_dataset_df,
            on=[
                *(
                    [
                        "recommendation_sid",
                        "user_account_sid",
                        "item_sid"
                    ] + self.additional_columns
                )
            ],
            how="outer"
        ).withColumn(
            "logged_at",
            choose_logged_at(
                F.col("l_logged_at"),
                F.col("r_logged_at")
            )
        ).select(
            *(
                [
                    "recommendation_sid", "user_account_sid", "item_sid"
                ] + self.additional_columns + [
                    "logged_at"
                ]
            )
        )

        reco_passed_dataset_df = reco_join_dataset_df.filter(
            "(req == 1) and (res is null)"
        ).drop("req", "res").withColumn("label", F.lit(0)).sample(fraction=0.1, withReplacement=False)

        reco_matched_dataset_df = reco_join_dataset_df.filter(
            "res == 1"
        ).drop(
            "req",
            "res"
        ).withColumn(
            "label",
            F.lit(1)
        )

        LOGGER.info(f"{reco_passed_dataset_df.count()} records of recommendation for requested but passed items' dataframe")
        LOGGER.info(f"{reco_matched_dataset_df.count()} records of recommendation for responsive items' dataframe")

        # JOIN input dataset and target dataset
        reco_union_dataset_df = reco_passed_dataset_df.union(reco_matched_dataset_df)

        LOGGER.info(f"{reco_union_dataset_df.count()} records of all the target recommendation dataframe ['requested but passed' + 'responsive']")

        only_matched_df = reco_union_dataset_df.where("label = 1").select("recommendation_sid").withColumnRenamed("recommendation_sid", "r_sid").select("r_sid").distinct()
        # the dataset filtered with recommendations containing at least one matched record
        matched_reco_dataset_df = reco_union_dataset_df.join(only_matched_df, reco_union_dataset_df.recommendation_sid == only_matched_df.r_sid).drop("r_sid")

        LOGGER.info(f"{matched_reco_dataset_df.count()} records of only matched recommendation dataframe")

        final_reco_dataset_df = matched_reco_dataset_df.join(
            reco_subdataset_df,
            on=self.user_id_name,
            how="left"
        )

        LOGGER.info(f"{final_reco_dataset_df.count()} of final dataset")

        egress_domain_feature_path = DomainBasedFeaturePath(
            lake_path=LakePath(self.service_config.username),
            domain_name=self.egress_domain_name
        )
        schema_feature_path = SchemaTypedSnapshotFeaturePath(
            schema_path=SchemaTypedFeaturePath(
                domain_path=egress_domain_feature_path,
                schema_type=self.egress_schema_type,
                phase_type=self.source_type
            ),
            snapshot_dt=save_snapshot_dt
        )

        final_reco_dataset_df.write.parquet(repr(schema_feature_path))

        LOGGER.info(f"written reco_dataset_df to {repr(schema_feature_path)}")

        LOGGER.info(f"==== END {__class__.__name__} ====")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load root dataset from lake data labeled by some comditions")
    parser.add_argument(
        "-d",
        "--dag_id",
        help="DAG id for this task, (dag_build_reco_v1_dataset, dag_build_reco_v1_dataset_once)",
        required=True,
        default="dag_build_reco_v1_dataset"
    )
    parser.add_argument(
        "-s",
        "--service_name",
        help="SERVICE_NAME for this task, (item-matching/reco-v1/dag-build-dataset-mart, item-matching/reco-v1/dag-build-dataset-mart-once)",
        required=True,
        default="item-matching/reco-v1/dag-build-dataset-mart"
    )
    args = parser.parse_args()

    deploy_envs = get_service_config().available_service_levels
    for deploy_env in deploy_envs[:1]:
        service_config = get_service_config(force_service_level=deploy_env)

        dag_config = YamlLoader(f"{service_config.resource_path}/{args.service_name}/dag.yml").load()
        task_config = get_my_task(
            dag_config=dag_config, 
            context=None, 
            dag_id=args.dag_id, 
            task_id="load_lake_root_dataset_labeled"
        )
        LoadLakeRootDatasetLabeled(service_config, task_config)(context=None)
