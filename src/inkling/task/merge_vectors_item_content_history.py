import argparse
import numpy as np

from typing import List, Optional

from pyspark.sql.functions import col, monotonically_increasing_id, udf
from pyspark.sql.types import ArrayType, FloatType

from tunip.file_utils import services as file_services
from tunip.path.warehouse import VECTORS_ARROW_FILEPATH
from tunip.path_utils import services as path_services
from tunip.service_config import get_service_config
from tunip.snapshot_utils import SnapshotPathProvider, snapshot_now
from tunip.spark_utils import SparkConnector
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.item_intro_constants import T_INTRO_SPECIALTY_CODES
from inkling.utils.airflow_utils import get_my_task


@udf(returnType=ArrayType(FloatType()))
def calc_mean_vector_or_get(vector_x: List[float], vector_y: Optional[list]):
    if vector_y is not None:
        vx = np.array(vector_x)
        vy = np.array(vector_y)
        v = np.sum([vx, vy], axis=0) / 2
        return v.tolist()
    else:
        return vector_x


class MergeVectorsItemContentHistory:

    def __init__(self, service_config, task_config):
        self.service_config = service_config
        self.task_config = task_config

    def __call__(self, context):

        domain_name = self.task_config["ingress"]["domain_name"]
        phase_type = self.task_config["ingress"]["phase_type"]
        schema_type_for_initial = self.task_config["ingress"]["schema_types"]["initial"]
        schema_type_for_history = self.task_config["ingress"]["schema_types"]["history"]
        id_field = self.task_config["ingress"]["id_field"]
        spark_config = self.task_config["ingress"]["spark_config"]

        egress_schema_type = self.task_config["egress"]["schema_type"]
        egress_task_name = f"{domain_name}-{egress_schema_type}"

        snapshot_provider = SnapshotPathProvider(self.service_config)
        spark = SparkConnector.getOrCreate(local=True, spark_config=spark_config)
        file_service = file_services(self.service_config)
        path_service = path_services(self.service_config)

        snapshot_dt = snapshot_now()
        for target_code in T_INTRO_SPECIALTY_CODES:
            ingress_task_name = f"{domain_name}_{schema_type_for_initial}_{target_code}"
            item_init_content_vector_path = f"/user/{self.service_config.username}/warehouse/vectors/{ingress_task_name}/{phase_type}"
            latest_initial_content_vectors_path = path_service.build(f"{snapshot_provider.latest(item_init_content_vector_path, return_only_nauts_path=True)}/arrow/{VECTORS_ARROW_FILEPATH}")
            item_initial_content_vector_sdf = spark.read.parquet(
                f"{snapshot_provider.latest(item_init_content_vector_path, return_only_nauts_path=True)}/arrow"
            ).withColumnRenamed(
                "content_names", "vector_x"
            )

            LOGGER.info(f"initial content vectors are read from {latest_initial_content_vectors_path}")

            item_history_content_vector_path = f"/user/{self.service_config.username}/lake/{domain_name}/{schema_type_for_history}/{phase_type}"
            latest_history_content_vectors_path = f"{snapshot_provider.latest(item_history_content_vector_path)}/{target_code}"
            item_history_content_vector_sdf = spark.read.parquet(latest_history_content_vectors_path).withColumnRenamed("vector", "vector_y")

            LOGGER.info(f"history content vectors are read from {latest_history_content_vectors_path}")

            item_content_vector_sdf = item_initial_content_vector_sdf.join(
                item_history_content_vector_sdf, on=id_field, how="left"
            ).withColumn(
                "vector", calc_mean_vector_or_get(col("vector_x"), col("vector_y"))
            ).select(
                id_field, "vector"
            ).coalesce(1).withColumn(
                "vid", monotonically_increasing_id()
            )

            egress_task_name = f"{domain_name}_{egress_schema_type}_{target_code}"
            egress_path = f"/user/{self.service_config.username}/warehouse/vectors/{egress_task_name}/{phase_type}/{snapshot_dt}"

            egress_vectors_path = f"{egress_path}/vectors"
            file_service.mkdirs(egress_vectors_path)
            item_content_vector_sdf.select(id_field, "vector").write.format("parquet").mode("overwrite").save(egress_vectors_path)

            LOGGER.info(f"merged content vectors are written to {egress_path}/vectors")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="merge vectors to mean vector of initial item content vectors and historical ones")
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
    dag_config = YamlLoader(dag_config_filepath).load()
    task_config = get_my_task(dag_config, context=None, dag_id=dag_id, task_id="merge_vectors_item_content_history")

    task = MergeVectorsItemContentHistory(service_config, task_config)
    task(context={})
