from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.task.builtin.alias_index import AliasIndex
from inkling.utils.airflow_utils import get_my_task


class AliasIndexUserRequestVector(AliasIndex):
    def __init__(self, service_config, task_config):
        super(AliasIndexUserRequestVector, self).__init__(service_config, task_config)

    def __call__(self, context):
        LOGGER.info(f"===== START {__class__.__name__} =====")

        super().__call__(context)

        LOGGER.info(f"===== END {__class__.__name__} =====")


if __name__ == "__main__":
    service_config = get_service_config()
    dag_config_filepath = f"{service_config.resource_path}/item-reco-feedback/dag-build-corpus/dag.yml"
    dag_config = YamlLoader(dag_config_filepath).load()
    task_config = get_my_task(dag_config, context=None, dag_id='dag_build_corpus_item_reco_feedback', task_id='alias_index_user_request_vector')

    task = AliasIndexUserRequestVector(service_config, task_config)
    task(context={})
