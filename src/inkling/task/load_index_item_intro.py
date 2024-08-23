import json

from tunip.es_utils import (
    create_index,
    init_elastic_client
)
from tunip.file_utils import services as file_services
from tunip.service_config import get_service_config
from tunip.snapshot_utils import snapshot_now
from tunip.yaml_loader import YamlLoader

from tweak.predict.predict_pretrained_encoder import PreTrainedEncoderPredictor
from tweak.predict.predictor import PredictorConfig
from tweak.predict.predictors import PredictorFactory
from tweak.vector.fetch.vector_fetcher import VectorFetcher
from tweak.vector.write.search_index_vector_writer import SearchIndexVectorWriter

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester
from inkling.utils.airflow_utils import get_my_task


class LoadIndexItemIntro(DAGStatusDigester):

    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.input_fields = task_config['ingress']['input_fields']

        self.index_name = task_config['egress']['index_name']
        self.index_mapping = task_config['egress']['index_mapping']
        self.predictor_config_json = task_config['egress']['predictor_config']
        self.vector_fields = task_config['egress']['vector_fields']

        # assertion keys for vector fetcher
        assert 'task_name' in task_config['ingress']
        assert 'domain_name' in task_config['ingress']
        assert 'group_by_fields' in task_config['ingress']
        assert 'group_select_field' in task_config['ingress']
        assert 'select_fields' in task_config['ingress']
        assert isinstance(task_config['ingress']['group_by_fields'], list)
        assert isinstance(task_config['ingress']['select_fields'], list)


    def __call__(self, context):

        LOGGER.info(f"===== START {__class__.__name__} =====")

        predictor_config = PredictorConfig.parse_raw(self.predictor_config_json)
        predictor: PreTrainedEncoderPredictor = PredictorFactory.create(predictor_config)

        index_snapshot_dt = snapshot_now()
        recent_index_name = f"{self.index_name}-{index_snapshot_dt}"

        self.search_client = init_elastic_client(self.service_config)
        index_mapping_path = f"{self.service_config.resource_path}/index_mappings/item_intro_vector/index_mapping.json"
        with open(index_mapping_path, mode="r") as f:
            index_mapping = json.load(f)
            create_index(es=self.search_client, index_name=recent_index_name, mappings=index_mapping)

        vector_writer = SearchIndexVectorWriter(
            service_config=self.service_config,
            supplier=predictor,
            index_name=recent_index_name,
            schema=self.index_mapping,
            content_fields=self.input_fields,
            content_vector_fields=self.vector_fields,
            batch_size=8
        )

        vector_fetcher = VectorFetcher(service_config=self.service_config, task_config=self.task_config)
        # ingress_path = f"/user/{service_config.username}/warehouse/top_k_vector_input/{task_name}/{domain_name}"
        vector_fetcher.fetch(vector_writer)

        # self.load(context, body={"index_name_snapshot": f"{self.index_name}-{snapshot_now()}"})
        file_service = file_services(self.service_config)
        index_snapshot_path = f"/user/{self.service_config.username}/warehouse/index2alias/{self.index_name}/{index_snapshot_dt}"
        file_service.mkdirs(index_snapshot_path)
        file_service.write(f"{index_snapshot_path}/data.txt", f"{recent_index_name}")
        file_service.mkdirs(f"{index_snapshot_path}/_SUCCESS")

        LOGGER.info(f"===== END {__class__.__name__} =====")

if __name__ == "__main__":
    service_config = get_service_config()
    dag_config_filepath = f"{service_config.resource_path}/item-reco-feedback/dag-build-corpus/dag.yml"
    dag_config = YamlLoader(dag_config_filepath).load()
    task_config = get_my_task(dag_config, context=None, dag_id='dag_build_corpus_item_reco_feedback', task_id='load_index_item_intro')

    task = LoadIndexItemIntro(service_config, task_config)
    task(context={})
