import json
import numpy as np

import pandas as pd
from sqlalchemy import create_engine
from elasticsearch import helpers
from elasticsearch.helpers import BulkIndexError

from tunip.nugget_api import Nugget
from tunip import file_utils
from tunip.es_utils import init_elastic_client, list_indexes_of
from tunip.snapshot_utils import snapshot_now, SnapshotCleaner
from tunip.datetime_utils import bitmap_to_time_range, to_sql_strftime
from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.utils.airflow_utils import get_my_task
from inkling.dag_stat_utils import DAGStatusDigester


MIN = 7
CHUNK_SIZE = 1

def upsert_generator(df, index_name, sid):
    df_iter = df.iterrows()
    for index, document in df_iter:
        yield {
            "_op_type": 'update',
            "_index": index_name,
            "_id": document[sid],
            "doc": document.to_dict(),
            "doc_as_upsert": True
        }

class IncrementalUpdateIndexBySid(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.task_id = self.task_config["task_id"]

        self.nugget = Nugget(split_sentence=True)

        self.query_format = self.task_config["ingress"]["query"]

        self.sid_type = self.task_config["ingress"]["sid_type"]

        self.tokenize_field = self.task_config["ingress"].get("tokenize_field", None)
        self.json_convert_fields = self.task_config["ingress"].get("json_convert_fields", None)
        self.merge_fields = self.task_config["ingress"].get("merge_fields", None)
        self.merge_json_fields = self.task_config["ingress"].get("merge_json_fields", None)
        self.split_fields = self.task_config["ingress"].get("split_fields", None)
        self.bit_to_time_range_fields = self.task_config["ingress"].get("bit_to_time_range_fields", None)

        self.egress_path = f"/user/{self.service_config.username}/{self.task_config['egress']['repository_type']}/{self.task_config['egress']['domain_name']}/{self.task_config['egress']['schema_type']}/{self.task_config['egress']['source_type']}"

        self.num_paths_to_left = self.task_config["egress"].get("num_paths_to_left")

        self.index_alias = self.task_config["egress"]["index_alias"]

    def __call__(self, context=None, snapshot_dt=None):
        LOGGER.info(f"===== START {__class__.__name__} - {self.task_id} =====")

        run_snapshot_dt = snapshot_dt if snapshot_dt else snapshot_now()
        save_snapshot_dt = run_snapshot_dt

        save_path = f"{self.egress_path}/{save_snapshot_dt}"

        db_user = self.service_config.config.get('db.user')
        db_password = self.service_config.config.get('db.password')
        db_port = self.service_config.config.get('db.port')
        db_database = self.service_config.config.get('db.database')

        engine = create_engine(f'mysql+mysqldb://{db_user}:{db_password}@127.0.0.1:{db_port}/{db_database}?charset=utf8')

        run_datetime = to_sql_strftime(context["data_interval_end"])
        query = self.query_format.format(run_datetime=run_datetime)
        
        file_service = file_utils.services(self.service_config)

        pd_docs = pd.read_sql(con=engine, sql=query).convert_dtypes().replace({pd.NA: None})

        es_client = init_elastic_client(self.service_config)

        index_names = list_indexes_of(es_client, alias=self.index_alias, elastic_host=self.service_config.elastic_host, desc=True)
        current_index_name = index_names[0]

        if len(pd_docs) > 0:
            if self.tokenize_field:
                for fld in self.tokenize_field:
                    LOGGER.info(f"Start tokenizing [{fld}]...")
                    pd_docs[f"{fld}_token"] = self.nugget.get_token_surfaces_by_chunk(pd_docs[fld], CHUNK_SIZE)

            if self.json_convert_fields:
                pd_docs = self._convert_to_json_column(pd_docs, self.json_convert_fields)

            if self.bit_to_time_range_fields:
                pd_docs = self._bit_to_time_range(pd_docs, self.bit_to_time_range_fields)

            if self.merge_fields:
                pd_docs = self._merge_fields(pd_docs, self.merge_fields)

            if self.merge_json_fields:
                pd_docs = self._merge_fields_to_json(pd_docs, self.merge_json_fields)

            if self.split_fields:
                pd_docs = self._split_fields(pd_docs, self.split_fields)

            pd_docs = pd_docs.replace({np.nan: None})

            try:
                helpers.bulk(es_client, upsert_generator(pd_docs, current_index_name, self.sid_type))
            except BulkIndexError as e:
                LOGGER.info(f"===== FAILED TO WRITE DOCUMENTS =====")
                failed_documents = e.errors
                for failed_doc in failed_documents:
                    LOGGER.info(failed_doc["update"]["error"]["reason"])
            
            json_docs = pd_docs.to_json(orient="records")
            file_service.write(save_path + "/" + "data.json", json_docs)

            LOGGER.info(f"===== Total {len(pd_docs)} data incremented to {current_index_name} at {save_snapshot_dt} =====")
        else:
            file_service.write(save_path + "/" + "data.json", "")
            LOGGER.info(f"===== No documents to write into {save_path} =====")

        if self.num_paths_to_left:
            snapshot_cleaner = SnapshotCleaner(service_config=self.service_config, paths_left=self.num_paths_to_left)
            snapshot_cleaner.clean(root_path=self.egress_path)
            LOGGER.info(f"===== Cleaning old data... ({self.num_paths_to_left} snapshots will remain.) =====")
        else:
            LOGGER.info(f"===== Skip cleaning data... (All snapshots will remain.) =====")

        self.load(context, body={"query": query, "save_snapshot_dt": save_snapshot_dt})

        LOGGER.info(f"===== END {__class__.__name__} =====")

    def _convert_to_json_column(self, docs, cols):
        for col in cols:
            docs[col] = docs[col].apply(json.loads)
        return docs

    def _merge_fields(self, docs, merge_mappings):
        for merge_mapping in merge_mappings:
            for merge_col, cols in merge_mapping.items():
                docs[merge_col] = docs[cols].values.tolist()
                docs = docs.drop(cols, axis=1)

        return docs

    def _merge_fields_to_json(self, docs, merge_json_mappings):
        def create_json(row):
            json_col = {}
            for col in cols:
                json_col[col] = row[col]
            return json_col
        
        for merge_json_mapping in merge_json_mappings:
            for merge_col, cols in merge_json_mapping.items():
                docs[merge_col] = docs.apply(create_json, axis=1)
                docs = docs.drop(cols, axis=1)

        return docs

    def _split_fields(self, docs, split_columns, separator="|"):
        for col in split_columns:
            docs[col] = [c.split(separator) if c else None for c in docs[col]]
        return docs

    def _bit_to_time_range(self, docs, fields: list):
        for fld in fields:
            time_range_list = []
            for bits in docs[fld]:
                if bits is None:
                    time_range_list.append(None)
                else:
                    time_range = bitmap_to_time_range(bits)
                    time_range_list.append(time_range)
            docs[fld] = time_range_list
        
        return docs


