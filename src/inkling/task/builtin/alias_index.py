from tunip.es_utils import (
    add_or_update_alias,
    delete_alias,
    delete_index,
    init_elastic_client,
    list_indexes_of
)
from tunip.file_utils import services as file_services
from tunip.snapshot_utils import SnapshotPathProvider
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
# from inkling.dag_stat_utils import DAGStatusDigester


class AliasIndex:
# class AliasIndex(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        self.service_config = service_config
        self.task_config = task_config

        self.index_alias = task_config["ingress"]["index_alias"]
        self.num_index_keep = task_config["ingress"]["num_index_keep"]


    def __call__(self, context):
        # prev_dag_stat = self.fetch(context)
        # target_index_name = prev_dag_stat.body.index_name_snapshot

        file_service = file_services(self.service_config)
        snapshot_provider = SnapshotPathProvider(self.service_config)
        latest_index_path = snapshot_provider.latest(f"/user/{self.service_config.username}/warehouse/index2alias/{self.index_alias}", return_only_nauts_path=True)
        target_index_name = file_service.load(f"{latest_index_path}/data.txt").decode("utf-8")

        es_client = init_elastic_client(self.service_config)
        
        success = add_or_update_alias(
            es=es_client,
            index_name=target_index_name,
            alias=self.index_alias
        )
        if success:
            index_names = list_indexes_of(es_client, alias=self.index_alias, elastic_host=self.service_config.elastic_host, desc=True)
            for name in index_names[1:]:
                delete_alias(es=es_client, index_name=name, alias=self.index_alias)
            for name in index_names[self.num_index_keep:]:
                delete_index(es=es_client, index_name=name)

        LOGGER.info(f"ADD alias:[{self.index_alias}] to index:[{target_index_name}] => {success}")
