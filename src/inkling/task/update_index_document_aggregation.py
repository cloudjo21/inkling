import json
import numpy as np
import pandas as pd
import pyspark.sql.functions as F

from elasticsearch import helpers
from requests import Session
from pyspark.sql.types import StructType

from tunip.es_utils import (
    init_elastic_client,
    list_indexes_of
)
from tunip.file_utils import services as file_services
from tunip.snapshot_utils import SnapshotPathProvider
from tunip.spark_utils import SparkConnector

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester
from inkling.utils.document_aggregation_utils import aggregate_document


def insert_generator(docs, index_name):
    for doc in docs:
        yield {
            "_op_type": "index",
            "_index": index_name,
            "_source": doc
        }

def insert_generator_with_single_id(docs, index_name, id_column):
    for doc in docs:
        yield {
            "_op_type": "update",
            "_index": index_name,
            "_id": doc[id_column],
            "doc": doc,
            "doc_as_upsert": True
        }

def update_generator(docs, index_name):
    for doc in docs:
        yield {
            "_op_type": 'update',
            "_index": index_name,
            "_id": doc["_id"],
            "doc": doc["_source"],
            "doc_as_upsert": True
        }

class UpdateIndexDocumentAggregation(DAGStatusDigester):
    def __init__(self, service_config, task_config):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.ingress_path = f"/user/{service_config.username}/{self.task_config['ingress']['repository_type']}/{self.task_config['ingress']['domain_name']}/{self.task_config['ingress']['schema_type']}/{self.task_config['ingress']['source_type']}"
        schema_type = self.task_config["ingress"]["schema_type"].split(".")[0]
        self.schema_root_path = f"/user/{self.service_config.username}/lake/{self.task_config['ingress']['domain_name']}/{schema_type}.schema"
        self.id_set_info_list = self.task_config["ingress"]["id_set_info_list"]
        self.convert_to_json_columns = self.task_config["ingress"].get("convert_to_json_columns")

        self.index_alias = self.task_config["egress"]["index_alias"]
        self.es_query = self.task_config["egress"]["es_query"]
        self.condition_fields = self.task_config["egress"]["condition_fields"]
        self.aggregate_type = self.task_config["egress"]["aggregate_type"]

    def __call__(self, context=None):
        LOGGER.info(f"===== START {__class__.__name__} =====")
        prev_dag_stat = self.fetch(context)

        run_snapshot_dt = prev_dag_stat.header.run_snapshot_dt or None

        spark = SparkConnector.getOrCreate(local=True)
        es_client = init_elastic_client(self.service_config)

        index_names = list_indexes_of(es_client, alias=self.index_alias, elastic_host=self.service_config.elastic_host, desc=True)
        current_index_name = index_names[0]

        snapshot_path_provider = SnapshotPathProvider(self.service_config)
        latest_schema_path = snapshot_path_provider.latest(self.schema_root_path, return_only_nauts_path=True)

        file_service = file_services(self.service_config)
        schema_dict = file_service.load(f"{latest_schema_path}/data.json").decode("utf-8")

        schema = StructType.fromJson(json.loads(schema_dict))

        # Get list of id set to join
        id_columns = [path_info["id_column"] for path_info in self.id_set_info_list]

        id_set_list = []
        for id_set_info in self.id_set_info_list:
            id_set_path = f"/user/{self.service_config.username}/{id_set_info['repository_type']}/{id_set_info['domain_name']}/{id_set_info['schema_type']}/{id_set_info['source_type']}"
            id_column = id_set_info["id_column"]
            id_set_list.append((id_column, id_set_path))

        docs_df = spark.read.schema(schema).json(f"{self.ingress_path}/{run_snapshot_dt}")
        if not docs_df.isEmpty():
            # 모든 id set에 대해 LEFT JOIN 실행
            for id_column, id_set_path in id_set_list:
                id_set_df = spark.read.json(f"{id_set_path}/{run_snapshot_dt}")
                docs_df = id_set_df.join(docs_df, [id_column], "INNER")

            save_df = docs_df.dropna(how="any", subset=id_columns)

            if self.convert_to_json_columns:
                for column in self.convert_to_json_columns:
                    save_df = save_df.withColumn(column, F.to_json(column))

            if not save_df.isEmpty():
                LOGGER.info(f"===== Start Updating {current_index_name} =====")

                docs_pdf = save_df.toPandas().convert_dtypes().replace({pd.NA: None})
                docs_pdf["exists"] = 0

                query_body = self.es_query["body"]
                query_clause = self.es_query["clause"]
                query_search_type = self.es_query["search_type"]
                query_fields = self.es_query["query_fields"]

                if query_search_type == "single_field":
                    clause_data = docs_pdf[query_fields].to_dict(orient="list")
                    for k, v in clause_data.items():
                        clause_data[k] = str(v).replace("'", '"')
                    clauses = query_clause.format(**clause_data)
                    query = query_body.format(clauses=clauses)

                elif query_search_type == "multiple_fields":
                    clause_data = docs_pdf[query_fields].applymap(lambda x: f'"{x}"').to_dict(orient="records")
                    clause_list = []
                    for row in clause_data:
                        clause_list.append(query_clause.format(**row))

                    query = query_body.format(clauses=", ".join(clause_list))

                # query incremental target documents
                response = None
                with Session() as sess:
                    response = sess.get(
                        f"{self.service_config.elastic_host}/{self.index_alias}/_search",
                        data=query,
                        headers={'Content-Type': 'application/json; charset=utf-8'}
                    )

                incremental_target_docs = []
                try:
                    if response is None:
                        raise Exception(f"{self.service_config.elastic_host}/{self.index_alias}/_search query: {query}")
                    incremental_target_docs = response.json()["hits"]["hits"]
                except KeyError as e:
                    LOGGER.info("FAILED TO SEARCH")
                    LOGGER.info(f"Response: \n{response.json()}\n")
                except Exception as e:
                    LOGGER.error(str(e))

                # update documents in index
                documents_to_update = []
                for i in range(len(incremental_target_docs)):
                    doc = incremental_target_docs[i]["_source"]

                    conditions = [docs_pdf[cond_fld] == doc[cond_fld] for cond_fld in self.condition_fields]

                    incremental_data = docs_pdf.loc[np.all(conditions, axis=0)]
                    if not incremental_data.empty:
                        docs_pdf.loc[np.all(conditions, axis=0), "exists"] = 1

                        updated_doc = aggregate_document(doc, incremental_data.to_dict(orient="records")[0], self.aggregate_type)
                        documents_to_update.append(incremental_target_docs[i])

                # documents not exist in index
                documents_to_insert_pdf = docs_pdf.loc[docs_pdf["exists"] == 0].drop(columns=["exists"])

                if self.convert_to_json_columns:
                    for column in self.convert_to_json_columns:
                        documents_to_insert_pdf[column] = documents_to_insert_pdf[column].apply(lambda x: json.loads(x) if x is not None else None)

                documents_to_insert = documents_to_insert_pdf.to_dict(orient="records")  # 기존에 없던 선생님 upsert 진행

                # upsert documents with bulk API
                if len(query_fields) == 1:
                    helpers.bulk(es_client, insert_generator_with_single_id(documents_to_insert, current_index_name, query_fields[0]))
                else:
                    helpers.bulk(es_client, insert_generator(documents_to_insert, current_index_name))
                helpers.bulk(es_client, update_generator(documents_to_update, current_index_name))

                LOGGER.info(f"===== Update {current_index_name} to Elasticsearch Done =====")
            else:
                LOGGER.info(f"===== No Documents to Update Elasticsearch =====")

            LOGGER.info(f"===== Update {current_index_name} to Elasticsearch Done =====")

        self.load(context, body={"run_snapshot_dt": run_snapshot_dt})

