import argparse
import numpy as np
import pandas as pd
import pyspark.pandas as ps
import torch

from typing import List

from pyspark.sql.functions import col, collect_list, lit, size, udf
from pyspark.sql.types import ArrayType, FloatType, StringType

from tunip.file_utils import services as file_services
from tunip.iter_utils import chunked_iterators
from tunip.service_config import ServiceLevelConfig, get_service_config
from tunip.snapshot_utils import SnapshotPathProvider, snapshot_now
from tunip.spark_utils import SparkConnector
from tunip.yaml_loader import YamlLoader

from tweak.predict.predictors import SimplePredictorFactory

from inkling import LOGGER
from inkling.utils.airflow_utils import get_my_task


class LoadVectorsItemContentHistory:

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        self.service_config = service_config
        self.task_config = task_config

    def __call__(self, context):

        domain_name = self.task_config["ingress"]["domain_name"]
        schema_type = self.task_config["ingress"]["schema_type"]
        phase_type = self.task_config["ingress"]["phase_type"]
        plm_model_name = self.task_config["ingress"]["plm_model_name"]
        id_field = self.task_config["ingress"]["id_field"]
        content_field = self.task_config["ingress"]["content_field"]
        category_target_codes = self.task_config["ingress"]["category_codes"]

        egress_schema_type = self.task_config["egress"]["schema_type"]

        snapshot_provider = SnapshotPathProvider(self.service_config)
        spark = SparkConnector.getOrCreate(local=True)
        file_service = file_services(self.service_config)

        predictor = SimplePredictorFactory.create(
            model_name=plm_model_name,
            plm=True,
            encoder_only=True,
            max_length=128,
            zero_padding=False,
            predict_output_type="last_hidden.mean_pooling",
            device="cpu"
        )

        snapshot_dt = snapshot_now()
        for target_code in map(lambda x: str(x), category_target_codes):
            latest_ingress_path = snapshot_provider.latest(f"/user/{self.service_config.username}/lake/{domain_name}/{schema_type}/{phase_type}", return_only_nauts_path=True)
            ingress_path = f"{latest_ingress_path}/{target_code}"
            egress_path = f"/user/{self.service_config.username}/lake/{domain_name}/{egress_schema_type}/{phase_type}/{snapshot_dt}/{target_code}"

            LOGGER.info(f"The vectors would be written from the source of {ingress_path} to {egress_path}")

            content_history_sdf = spark.read.parquet(ingress_path).groupBy(id_field).agg(collect_list(content_field).alias("content_history"))

            file_service.mkdirs(egress_path)
            item_history_vectors = []
            # TODO spark job packaging with predictor
            for row in content_history_sdf.collect():
                iter_contents = iter(row.content_history)
                vectors = []
                for chunk in chunked_iterators(iter_contents, len(row.content_history), 16):
                    content_chunk = list(chunk)
                    vector = predictor.predict(content_chunk)
                    vectors.append(vector)
                history_vector: np.ndarray = np.mean(torch.vstack(vectors).cpu().numpy(), axis=0)
                item_history_vectors.append((row.item_account_sid, history_vector.tolist()))
            
            item_history_pdf = pd.DataFrame(
                {id_field: [r[0] for r in item_history_vectors], "vector": [r[1] for r in item_history_vectors]}
            )
            item_history_sdf = ps.from_pandas(item_history_pdf).to_spark().coalesce(1)
            item_history_sdf.select(id_field, "vector").write.format("parquet").mode("overwrite").save(egress_path)

            LOGGER.info(f"The vectors are written from the source of {ingress_path} to {egress_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load historical item content vectors")
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
    task_config = get_my_task(dag_config, context=None, dag_id=dag_id, task_id="load_vectors_item_content_history")

    task = LoadVectorsItemContentHistory(service_config, task_config)
    task(context={})
