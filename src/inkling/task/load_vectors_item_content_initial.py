import argparse
import os
import pyarrow as pa

from tunip.service_config import get_service_config
from tunip.snapshot_utils import SnapshotPathProvider, snapshot_now
from tunip.yaml_loader import YamlLoader

from tweak.predict.predictor import PredictorConfig
from tweak.vector.fetch.vector_fetcher import VectorFetcher
from tweak.vector.write.parquet_record_vector_writer import ParquetRecordVectorWriter

from inkling import LOGGER
from inkling.utils.airflow_utils import get_my_task


class LoadVectorsItemContentInitial:
    def __init__(self, service_config, task_config):
        self.service_config = service_config
        self.task_config = task_config

    def __call__(self, context):

        domain_name = self.task_config["ingress"]["domain_name"]
        schema_type = self.task_config["ingress"]["schema_type"]
        phase_type = self.task_config["ingress"]["phase_type"]
        id_field = self.task_config["ingress"]["id_field"]
        content_fields = self.task_config["ingress"]["content_fields"]
        category_target_codes = self.task_config["ingress"]["category_codes"]

        egress_schema_type = self.task_config["egress"]["schema_type"]

        snapshot_provider = SnapshotPathProvider(self.service_config)
        vector_fetcher = VectorFetcher(self.service_config, self.task_config)

        snapshot_dt = snapshot_now()
        for target_code in category_target_codes:
            ingress_path = snapshot_provider.latest(f"/user/{self.service_config.username}/lake/{domain_name}/{schema_type}/{phase_type}", return_only_nauts_path=True) + os.sep + str(target_code)
            
            egress_task_name = f"{domain_name}_{egress_schema_type}_{target_code}"
            egress_path = f"/user/{self.service_config.username}/warehouse/vectors/{egress_task_name}/{phase_type}/{snapshot_dt}"

            predictor_config_json: str = self.task_config["ingress"]["predictor_config"].replace("{USERNAME}", self.service_config.username)

            LOGGER.info(f"The vectors would be written from the source of {ingress_path} to {egress_path}")
            LOGGER.info(f"PredictorConfig: {predictor_config_json}")
            
            predictor_config = PredictorConfig.model_validate_json(predictor_config_json)
            vector_writer = ParquetRecordVectorWriter(
                service_config=self.service_config,
                task_config=self.task_config,
                predictor_config=predictor_config,
                schema=pa.schema(
                    [pa.field(id_field, type=pa.string())] + [pa.field(f_name, type=pa.list_(pa.float32())) for f_name in content_fields]
                ),
                id_field=id_field,
                content_fields=content_fields,
                vectors_path=egress_path,
                batch_size=256,
                build_id_mapping=False
            )

            vector_fetcher.fetch(vector_writer, ingress_latest_path=ingress_path, format="parquet")

            LOGGER.info(f"The vectors are written from the source of {ingress_path} to {egress_path}")
        

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load initial item content vectors")
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
    task_config = get_my_task(dag_config, context=None, dag_id=dag_id, task_id="load_vectors_item_content_initial")

    task = LoadVectorsItemContentInitial(service_config, task_config)
    task(context={})
