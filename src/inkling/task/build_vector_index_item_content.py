import argparse

from tunip.path.warehouse import (
    WarehouseVectorsTaskPhasePath,
    WarehouseVectorsTaskPhaseSnapshotPath
)
from tunip.service_config import get_service_config
from tunip.snapshot_utils import SnapshotPathProvider
from tunip.yaml_loader import YamlLoader

from tweak.vector.index import VectorIndexBuilder

from inkling import LOGGER
from inkling.utils.airflow_utils import get_my_task


class BuildVectorIndexItemContent:

    def __init__(self, service_config, task_config):
        self.service_config = service_config
        self.task_config = task_config

    def __call__(self, context):
        domain_name = self.task_config["ingress"]["domain_name"]
        schema_type = self.task_config["ingress"]["schema_type"]
        phase_type = self.task_config["ingress"]["phase_type"]
        id_field = self.task_config["ingress"]["id_field"]
        category_target_codes = self.taks_config["ingress"]["category_codes"]
        d_size = self.task_config["egress"]["d_size"]
        index_type = self.task_config["egress"].get("index_type", "flat_inner_product")

        snapshot_provider = SnapshotPathProvider(self.service_config)

        for target_code in category_target_codes:
            source_type = f"{domain_name}_{schema_type}_{target_code}_{phase_type}"
            index_builder = VectorIndexBuilder(
                service_config=self.service_config,
                source_type=source_type,
                d_size=d_size,
                index_type=index_type
            )

            ingress_task_name = f"{domain_name}_{schema_type}_{target_code}"
            # ingress_path = f"/user/{self.service_config.username}/warehouse/vectors/{ingress_task_name}/{phase_type}"
            vectors_path = WarehouseVectorsTaskPhasePath(self.service_config.username, ingress_task_name, phase_type)
            LOGGER.info(f"vectors directory of {str(vectors_path)} ...")
            vectors_snapshot_dt = snapshot_provider.latest_snapshot_dt(repr(vectors_path), force_fs=self.service_config.filesystem.upper())

            latest_vectors_root_path: WarehouseVectorsTaskPhaseSnapshotPath = WarehouseVectorsTaskPhaseSnapshotPath.from_parent(vectors_path, vectors_snapshot_dt)

            LOGGER.info(f"vectors parquet files from {str(latest_vectors_root_path)} are read to build vector indexes ...")

            index_builder(latest_vectors_root_path, id_field)

            LOGGER.info(f"vector indexes from {ingress_task_name} has been built complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="build vector-indexing for item content vector against requested content vector")
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
    task_config = get_my_task(dag_config, context=None, dag_id=dag_id, task_id="build_vector_index_item_content")

    task = BuildVectorIndexItemContent(service_config, task_config)
    task(context={})
