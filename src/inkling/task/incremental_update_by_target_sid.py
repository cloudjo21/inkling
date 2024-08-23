import json
import numpy as np
import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from tunip import file_utils
from tunip.es_utils import init_elastic_client, list_indexes_of
from tunip.snapshot_utils import snapshot_now, SnapshotCleaner
from tunip.datetime_utils import to_sql_strftime
from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.utils.airflow_utils import get_my_task
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

class IncrementalUpdateByTargetSid(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.task_id = self.task_config["task_id"]

        self.query = self.task_config["ingress"]["query"]

        self.target_sid_ingress_path = f"/user/{self.service_config.username}/{self.task_config['ingress']['repository_type']}/{self.task_config['ingress']['domain_name']}/{self.task_config['ingress']['schema_type']}/{self.task_config['ingress']['source_type']}"
        self.egress_path = f"/user/{self.service_config.username}/{self.task_config['egress']['repository_type']}/{self.task_config['egress']['domain_name']}/{self.task_config['egress']['schema_type']}/{self.task_config['egress']['source_type']}"

        self.num_paths_to_left = self.task_config["egress"].get("num_paths_to_left")

        self.split_fields = self.task_config["egress"].get("split_fields", None)
        self.merge_json_fields = self.task_config["egress"].get("merge_json_fields", None)
        self.json_string_array_fields = self.task_config["egress"].get("json_string_array_fields", None)
        self.json_string_fields = self.task_config["egress"].get("json_string_fields", None)

        self.es_mapping_id = self.task_config["egress"]["mapping_id"]
        self.index_alias = self.task_config["egress"]["index_alias"]

    def __call__(self, context=None):
        LOGGER.info(f"===== START {__class__.__name__} - {self.task_id} =====")

        prev_dag_stat = self.fetch(context)

        run_snapshot_dt = prev_dag_stat.header.run_snapshot_dt or None
        save_snapshot_dt = run_snapshot_dt if run_snapshot_dt else snapshot_now()

        save_path = f"{self.egress_path}/{save_snapshot_dt}"

        file_service = file_utils.services(self.service_config)

        target_sids = file_service.load(f"{self.target_sid_ingress_path}/{run_snapshot_dt}/sids.txt").decode("utf-8")
        target_sid_list = tuple(target_sids.split(', '))
        if target_sids:
            formatted_sids = ", ".join('"{}"'.format(sid) for sid in target_sid_list)

            run_datetime = to_sql_strftime(context["data_interval_end"])
            query_with_target_sids = self.query.format(target_sids=formatted_sids, run_datetime=run_datetime)

            db_user = self.service_config.config.get('db.user')
            db_password = self.service_config.config.get('db.password')
            db_port = self.service_config.config.get('db.port')
            db_database = self.service_config.config.get('db.database')

            engine = create_engine(f'mysql+mysqldb://{db_user}:{db_password}@127.0.0.1:{db_port}/{db_database}?charset=utf8')
        
            pd_docs = pd.read_sql(con=engine, sql=query_with_target_sids)
            es_client = init_elastic_client(self.service_config)

            index_names = list_indexes_of(es_client, alias=self.index_alias, elastic_host=self.service_config.elastic_host, desc=True)
            current_index_name = index_names[0]

            if len(pd_docs) > 0:
                if self.split_fields:
                    pd_docs = self._split_fields(pd_docs, self.split_fields)

                if self.merge_json_fields:
                    pd_docs = self._merge_fields_to_json(pd_docs, self.merge_json_fields)
            
                if self.json_string_array_fields:
                    pd_docs = self._json_string_array_to_dict_array(pd_docs, self.json_string_array_fields)

                if self.json_string_fields:
                    pd_docs = self._json_string_to_dict(pd_docs, self.json_string_fields)

                pd_docs = pd_docs.replace({np.nan: None})

                try:
                    helpers.bulk(es_client, upsert_generator(pd_docs, current_index_name, self.es_mapping_id))
                except BulkIndexError as e:
                    LOGGER.info(f"===== FAILED TO WRITE DOCUMENTS =====")
                    failed_documents = e.errors
                    for failed_doc in failed_documents:
                        LOGGER.info(failed_doc["update"]["error"]["reason"])

                json_docs = pd_docs.to_json(orient="records")
                file_service.write(save_path + "/" + "data.json", json_docs)
            
                LOGGER.info(f"===== Total {len(pd_docs)} data incremented at {save_path} =====")
            else:
                file_service.write(save_path + "/" + "data.json", "")
                LOGGER.info(f"===== No documents to write into {save_path} =====")
        else:
            file_service.write(save_path + "/" + "data.json", "")
            LOGGER.info(f"===== No Target Sids to Update at {save_snapshot_dt} =====")

        if self.num_paths_to_left:
            snapshot_cleaner = SnapshotCleaner(service_config=self.service_config, paths_left=self.num_paths_to_left)
            snapshot_cleaner.clean(root_path=self.egress_path)
            LOGGER.info(f"===== Cleaning old data... ({self.num_paths_to_left} snapshots will remain.) =====")
        else:
            LOGGER.info(f"===== Skip cleaning data... (All snapshots will remain.) =====")

        self.load(context, body={"query": self.query, "save_snapshot_dt": save_snapshot_dt})
        LOGGER.info(f"===== END {__class__.__name__} =====")

    def _split_fields(self, docs, split_columns, separator="|"):
        for col in split_columns:
            docs[col] = [c.split(separator) if c else None for c in docs[col]]
        return docs

    def _merge_fields_to_json(self, docs, merge_json_mappings):
        def create_json(row, cols):
            json_col = {}
            for col in cols:
                json_col[col] = row[col]
            return json_col
        
        for merge_json_mapping in merge_json_mappings:
            for merge_col, cols in merge_json_mapping.items():
                docs[merge_col] = docs.apply(lambda doc: create_json(doc, cols), axis=1)
                docs = docs.drop(cols, axis=1)

        return docs

    def _json_string_array_to_dict_array(self, docs, json_string_array_columns):
        for col in json_string_array_columns:
            docs[col] = docs[col].apply(lambda x: [json.loads(item) for item in x])
        
        return docs

    def _json_string_to_dict(self, docs, json_string_columns):
        for col in json_string_columns:
            docs[col] = docs[col].apply(lambda x: json.loads(x))
        
        return docs

