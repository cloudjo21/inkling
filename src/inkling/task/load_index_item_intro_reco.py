import json

from tunip.es_utils import (
    create_index,
    init_elastic_client
)
from tunip.file_utils import services as file_services
from tunip.service_config import get_service_config
from tunip.snapshot_utils import SnapshotPathProvider, snapshot_now
from tunip.spark_utils import SparkConnector
from tunip.yaml_loader import YamlLoader

from tweak.predict.predictor import PredictorConfig
from tweak.predict.predictors import PredictorFactory
from tweak.predict.predict_pretrained_encoder import PreTrainedEncoderPredictor
from tweak.vector.fetch.vector_fetcher import VectorFetcher
from tweak.vector.write.search_index_vector_writer import SearchIndexVectorWriter

from inkling import LOGGER
from inkling.utils.airflow_utils import get_my_task


class LoadIndexItemIntroReco:

    def __init__(self, service_config, task_config):
        self.service_config = service_config
        self.task_config = task_config

        self.domain_name = self.task_config["ingress"]["domain_name"]
        self.schema_type = self.task_config["ingress"]["schema_type"]
        self.source_type = self.task_config["ingress"]["source_type"]
        self.content_fields = self.task_config["ingress"]["content_fields"]

        self.index_name = self.task_config["egress"]["index_name"]
        self.content_vector_fields = self.task_config["egress"]["content_vector_fields"]
        self.predictor_config_json = task_config['egress']['predictor_config']

    def __call__(self, context):
        LOGGER.info(f"===== START {__class__.__name__} =====")

        predictor_config = PredictorConfig.parse_raw(self.predictor_config_json)
        predictor: PreTrainedEncoderPredictor = PredictorFactory.create(predictor_config)

        index_snapshot_dt = snapshot_now()
        recent_index_name = f"{self.index_name}-{index_snapshot_dt}"
        snapshot_provider = SnapshotPathProvider(self.service_config)
        file_service = file_services(self.service_config)
        self.search_client = init_elastic_client(self.service_config)

        index_template_path = f"{self.service_config.resource_path}/index_template/item_matching/item_intro_reco/index_template.json"
        with open(index_template_path, mode="r") as f:
            index_template = json.load(f)
            # create_index(es=self.search_client, index_name=recent_index_name, mappings=index_mapping)
            self.search_client.indices.create(index=recent_index_name, mappings=index_template["mappings"], settings=index_template["settings"])

        ingress_path = f"/user/{self.service_config.username}/lake/{self.domain_name}/{self.schema_type}/{self.source_type}"
        ingress_latest_path = snapshot_provider.latest(ingress_path)

        vector_writer = SearchIndexVectorWriter(
            service_config=self.service_config,
            supplier=predictor,
            index_name=recent_index_name,
            schema=index_template,
            content_fields=self.content_fields,
            content_vector_fields=self.content_vector_fields,
            batch_size=8
        )
        vector_fetcher = VectorFetcher(self.service_config, self.task_config)
        vector_fetcher.fetch(vector_writer, ingress_latest_path=ingress_latest_path)

        index_snapshot_path = f"/user/{self.service_config.username}/warehouse/index2alias/{self.index_name}/{index_snapshot_dt}"
        file_service.mkdirs(index_snapshot_path)
        file_service.write(f"{index_snapshot_path}/data.txt", f"{recent_index_name}")
        file_service.mkdirs(f"{index_snapshot_path}/_SUCCESS")

        LOGGER.info(f"===== END {__class__.__name__} =====")


if __name__ == "__main__":
    service_config = get_service_config()
    dag_config_filepath = f"{service_config.resource_path}/item-matching/dag-build-index-intro-reco/dag.yml"
    dag_config = YamlLoader(dag_config_filepath).load()

    task_config = get_my_task(dag_config, context=None, dag_id='dag_build_index_reco', task_id='load_index_item_intro_reco')

    task = LoadIndexItemIntroReco(service_config, task_config)
    task(context={})
