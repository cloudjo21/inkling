from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.task.builtin.alias_index import AliasIndex
from inkling.utils.airflow_utils import get_my_task


class AliasIndexNews(AliasIndex):
    def __init__(self, service_config, task_config):
        super(AliasIndexNews, self).__init__(service_config, task_config)

    def __call__(self, context=None):
        LOGGER.info(f"===== START {__class__.__name__} =====")
        super().__call__(context)
        LOGGER.info(f"===== END {__class__.__name__} =====")

if __name__ == "__main__":
    deploy_envs = get_service_config().available_service_levels
    for deploy_env in deploy_envs:
        service_config = get_service_config(force_service_level=deploy_env)

        dag_config = YamlLoader(f"{service_config.resource_path}/news/full/dag.yml").load()
        task_config = get_my_task(
            dag_config=dag_config, 
            context=None, 
            dag_id="dag_load_full_index_news", 
            task_id="alias_index_news"
        )
        AliasIndexNews(service_config, task_config)()
