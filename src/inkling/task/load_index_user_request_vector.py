import json

from tunip.es_utils import (
    create_index,
    init_elastic_client
)
from tunip.file_utils import services as file_services
from tunip.service_config import get_service_config
from tunip.snapshot_utils import snapshot_now
from tunip.yaml_loader import YamlLoader

from tweak.predict.predict_seq2seq_lm_encoder import Seq2SeqLMEncoderPredictor
from tweak.predict.predictor import PredictorConfig
from tweak.predict.predictors import PredictorFactory
from tweak.vector.fetch.vector_fetcher import VectorFetcher
from tweak.vector.write.search_index_vector_writer import SearchIndexVectorWriter

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester
from inkling.utils.airflow_utils import get_my_task


class LoadIndexUserRequestVector(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.content_fields = task_config['ingress']['content_fields']
        assert "task_name" in task_config['ingress']
        assert "domain_name" in task_config['ingress']

        self.predictor_config_json = task_config['egress']['predictor_config']
        self.index_name = task_config['egress']['index_name']
        self.index_mapping = task_config['egress']['index_mapping']
        self.content_vector_fields = task_config['egress']['content_vector_fields']

    def __call__(self, context):

        LOGGER.info(f"===== START {__class__.__name__} =====")

        predictor_config = PredictorConfig.parse_raw(self.predictor_config_json)
        predictor: Seq2SeqLMEncoderPredictor = PredictorFactory.create(predictor_config)

        index_snapshot_dt = snapshot_now()
        recent_index_name = f"{self.index_name}-{index_snapshot_dt}"

        self.search_client = init_elastic_client(self.service_config)
        index_mapping_path = f"{self.service_config.resource_path}/index_mappings/user_req_item_intro_vecs/index_mapping.json"
        with open(index_mapping_path, mode="r") as f:
            index_mapping = json.load(f)
            create_index(es=self.search_client, index_name=recent_index_name, mappings=index_mapping)

        vector_writer = SearchIndexVectorWriter(
            service_config=self.service_config,
            supplier=predictor,
            index_name=recent_index_name,
            schema=self.index_mapping,
            content_fields=self.content_fields,
            content_vector_fields=self.content_vector_fields,
            batch_size=8
        )
        vector_fetcher = VectorFetcher(self.service_config, self.task_config)
        vector_fetcher.fetch(vector_writer)

        # self.load(context, body={"index_name_snapshot": f"{self.index_name}-{snapshot_now()}"})
        file_service = file_services(self.service_config)
        index_snapshot_dt = snapshot_now()
        index_snapshot_path = f"/user/{self.service_config.username}/warehouse/index2alias/{self.index_name}/{index_snapshot_dt}"
        file_service.mkdirs(index_snapshot_path)
        file_service.write(f"{index_snapshot_path}/data.txt", f"{recent_index_name}")
        file_service.mkdirs(f"{index_snapshot_path}/_SUCCESS")

        LOGGER.info(f"===== END {__class__.__name__} =====")

if __name__ == "__main__":
    service_config = get_service_config()
    dag_config_filepath = f"{service_config.resource_path}/item-reco-feedback/dag-build-corpus/dag.yml"
    dag_config = YamlLoader(dag_config_filepath).load()
    task_config = get_my_task(dag_config, context=None, dag_id='dag_build_corpus_item_reco_feedback', task_id='load_index_user_request_vector')

    task = LoadIndexUserRequestVector(service_config, task_config)
    task(context={})
