import numpy as np
import orjson
import pandas as pd

from importlib import import_module
from sqlalchemy import create_engine

from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from tunip.es_utils import init_elastic_client, list_indexes_of
from tunip.file_utils import services as file_services
from tunip.service_config import ServiceLevelConfig
from tunip.snapshot_utils import snapshot_now

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester


def upsert_generator(df, index_name, mapping_id):
    df_iter = df.iterrows()
    for _, document in df_iter:
        yield {
            "_op_type": 'update',
            "_index": index_name,
            "_id": document[mapping_id],
            "doc": document.to_dict(),
            "doc_as_upsert": True
        }


def get_value_regarding_json(value):
    result = None
    try:
        result = orjson.loads(value)
    except orjson.JSONDecodeError:
        result = value
    finally:
        return result


class InstantUpdateIndexFields(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.task_id = self.task_config["task_id"]

        self.sid_type = self.task_config["ingress"]["sid_type"]
        self.delta_seconds = self.task_config["ingress"]["delta_seconds"]
        self.instant_fields: dict = self.task_config["ingress"].get("instant_fields") or None
        self.domains = self.instant_fields.keys()

        self.index_aliases: dict = self.task_config["egress"]["index_aliases"]
        assert set(self.domains) == set([idx["domain_name"] for idx in self.index_aliases])
        self.es_mapping_id = self.task_config["egress"]["es_mapping_id"]
        self.num_paths_to_left = self.task_config["egress"]["num_paths_to_left"]
        self.transform_fields = self.task_config["egress"].get("transform_fields") or None

        self.exclude_field_usage = bool(self.instant_fields) != bool(self.transform_fields)


    def __call__(self, context=None, snapshot_dt=None):
        LOGGER.info(f"==== START {__class__.__name__} - {self.task_id} ====")
        egress_path = f"/user/{self.service_config.username}/{self.task_config['egress']['repository_type']}/{{domain_name}}/{self.task_config['egress']['schema_type']}/{self.task_config['egress']['source_type']}/{{save_snapshot_dt}}"

        prev_dag_stat = self.fetch(context)
        target_run_datetime = prev_dag_stat.body["run_datetime"]
        delta_seconds = prev_dag_stat.body["delta_seconds"]

        query = self.task_config["ingress"]["query"].format(
            run_datetime=target_run_datetime,
            delta_seconds=delta_seconds
        )

        es_client = init_elastic_client(self.service_config)
        db_config = self.service_config.db_config
        engine = create_engine(f'mysql+mysqldb://{db_config.user}:{db_config.password}@127.0.0.1:{db_config.port}/{db_config.database}?charset=utf8')
        file_service = file_services(self.service_config)

        rows = pd.read_sql(con=engine, sql=query)

        save_snapshot_dt = snapshot_now()
        save_snapshot_paths = dict()

        for domain in self.domains:

            if (self.instant_fields is not None and domain in self.instant_fields) and self.exclude_field_usage:
                # just 1-to-1 projection of identity
                instant_fields = self.instant_fields[domain]
                domain_docs = rows.loc[:, (self.sid_type,) + tuple(instant_fields)]
                for field in instant_fields:
                    domain_docs[field] = rows[field].apply(lambda x: get_value_regarding_json(x))
            else:
                if self.exclude_field_usage is False:
                    LOGGER.warning("instant fields are filled in DAG setting at the same time while transform fields are also done.")
                # n-to-1 projection of transformation
                for transform_op in self.transform_fields[domain]["transform_ops"]:
                    package_name = transform_op["transform_package_name"]
                    ingress_fields = transform_op["ingress"]
                    egress_field = transform_op["egress"]
                    transform_module = import_module(package_name)
                    transform_apply = getattr(transform_module, "apply")

                    domain_docs = rows.loc[:, (self.sid_type,) + tuple(ingress_fields)]
                    domain_docs[egress_field] = rows.loc[:, ingress_fields].apply(transform_apply, axis=1)

            domain_docs = domain_docs.replace({np.nan: None})

            alias = list(filter(lambda index_alias: index_alias["domain_name"] == domain, self.index_aliases))
            LOGGER.info(f"==== WOULD BULK INSERT documents into {alias} ====")

            index_names = list_indexes_of(
                es_client, alias=alias[0]["alias_name"], elastic_host=self.service_config.elastic_host, desc=True
            ) if alias else None
            assert index_names is not None
            latest_index_name = index_names[0]
            try:
                helpers.bulk(es_client, upsert_generator(domain_docs, latest_index_name, self.es_mapping_id))
            except BulkIndexError as bie:
                LOGGER.error(f"==== FAILED DOCUMENTS TO WRITE {latest_index_name} ====")
                failed_documents = bie.errors
                for failed_doc in failed_documents:
                    LOGGER.error(failed_doc["update"]["error"]["reason"])

            LOGGER.info(f"==== {__class__.__name__} updates {len(domain_docs)} documents of {latest_index_name}")

            save_path = egress_path.format(domain_name=domain, save_snapshot_dt=save_snapshot_dt)
            file_service.mkdirs(save_path)
            file_service.write(f"{save_path}/data.json", domain_docs.to_json(orient="records"))

            LOGGER.info(f"==== {__class__.__name__} writes {len(domain_docs)} documents to {save_path}")

            save_snapshot_paths[domain] = save_path

        self.load(context, body={"query": query, "save_snapshot_dt": save_snapshot_dt, "save_snapshot_paths": save_snapshot_paths})

        LOGGER.info(f"==== END {__class__.__name__} ====")

