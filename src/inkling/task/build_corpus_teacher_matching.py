import math

from typing import Optional

from pyspark.sql import DataFrame
from pyspark.sql.types import ArrayType, FloatType, Row, StructField

from tunip.snapshot_utils import SnapshotPathProvider, snapshot_now
from tunip.spark_utils import SparkConnector
from tunip.es_utils import (
    init_elastic_client,
    search_filter_many_values,
    search_filter_many_values_multi_field
)
from tunip.service_config import get_service_config
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.utils.airflow_utils import get_my_task
from inkling.utils.bigquery_utils import SparkBatchWriterForPandasRowIterator


class BuildCorpusItemMatching:
    def __init__(self, service_config, task_config):
        self.service_config = service_config
        self.task_config = task_config

        # task_name = 'generation'
        # domain_name = 'user_req_item_intro'
        self.vector_type = self.task_config["ingress"]["vector_type"]
        self.task_name = self.task_config["ingress"]["task_name"]
        self.domain_name = self.task_config["ingress"]["domain_name"]

	self.category_name = self.task_config["ingress"]["category_name"]
	self.item_category_names = self.task_config["ingress"]["item_category_names"]

        self.adhoc_index_name = self.task_config["ingress"]["adhoc_index_name"]
        self.adhoc_vector_field = self.task_config["ingress"]["adhoc_vector_field"]
        self.static_index_name = self.task_config["ingress"]["static_index_name"]
        self.static_vector_field = self.task_config["ingress"]["static_vector_field"]

        self.id_field = task_config["ingress"]["id_field"]
        self.reference_field = self.task_config["ingress"]["reference_field"]

        self.search_size = self.task_config["egress"]["search_size"]
        self.batch_size = self.task_config["egress"].get("batch_size", 5000)
        self.spark_config = self.task_config["egress"].get("spark_config", {"spark.executor.cores": 2, "spark.driver.memory": "4g"})


    @staticmethod
    def inflate_dataframe(corpus_df, categories, category_name) -> Optional[DataFrame]:
        per_count = corpus_df.count() / len(categories)
        inflated_df_list = []
        for category_value in categories:
            spec_df = corpus_df.where(f"{category_name} = {category_value}")
            spec_count = spec_df.count()
            spec_to_mean_rate = per_count / spec_count
            inflation_rate: int = math.floor(0.5 * spec_to_mean_rate) + 1

            inflated_spec_df = spec_df
            for _ in range(inflation_rate-1):
                inflated_spec_df = inflated_spec_df.union(spec_df)
            inflated_df_list.append(inflated_spec_df)

        inflated_df = inflated_df_list[0] if (inflated_df_list or None) else None
        for i in range(1, len(inflated_df_list)):
            inflated_df = inflated_df.union(inflated_df_list[i])
        return inflated_df


    def __call__(self, context):
        def search_vector_callback(schema, rows, search_client):

            adhoc_vectors = []
            static_vectors = []
            rows_map = dict([(row[self.id_field], row) for row in rows])
            distinct_rows = list(rows_map.items())
            sids_search = []
            sids = [row[0] for row in distinct_rows]
            ids = [row[1][self.reference_field] for row in distinct_rows]
            specialties = [row[1]['specialty'] for row in distinct_rows]
            
            prev_end = 0
            for batch_end in range(0, len(ids), self.search_size):
                if batch_end > 0:
                    adhoc_search_res = search_filter_many_values(search_client, self.adhoc_index_name, self.id_field, sids[prev_end:batch_end], [self.id_field, self.reference_field, self.adhoc_vector_field])
                    adhoc_batch = [r[self.adhoc_vector_field] for r in adhoc_search_res]
                    sids_batch = [r[self.id_field] for r in adhoc_search_res]

                    field2values = {
                        self.reference_field: ids[prev_end:batch_end],
                        'specialty': specialties[prev_end:batch_end]
                    }
                    search_res = search_filter_many_values_multi_field(search_client, self.static_index_name, field2values, [self.reference_field, self.static_vector_field])
                    static_batch = []
                    for tid in ids[prev_end:batch_end]:
                        item = list(filter(lambda r: r[self.reference_field] == tid, search_res))[0]
                        static_batch.append(item[self.static_vector_field])

                    if len(adhoc_batch) != len(static_batch):
                        x = set([r[self.reference_field] for r in adhoc_search_res])
                        y = set(ids[prev_end:batch_end])
                        LOGGER.error(x.symmetric_difference(y))
                        LOGGER.error(f"{len(sids[prev_end:batch_end])} != {len(ids[prev_end:batch_end])}")
                        assert len(adhoc_batch) == len(static_batch), LOGGER.error(f"{len(adhoc_batch)} != {len(static_batch)}")

                    assert len(adhoc_batch) == len(static_batch), LOGGER.error(f"{len(adhoc_batch)} != {len(static_batch)}")
                    
                    adhoc_vectors.extend(adhoc_batch)
                    static_vectors.extend(static_batch)
                    sids_search.extend(sids_batch)

                    prev_end = batch_end
            if self.adhoc_vector_field not in schema.fieldNames():
                schema.add(StructField(self.adhoc_vector_field, ArrayType(FloatType())))
            if self.static_vector_field not in schema.fieldNames():
                schema.add(StructField(self.static_vector_field, ArrayType(FloatType())))
            
            updated = []
            LOGGER.debug(f"#-of-batch-rows: {len(rows)}, #-of-batch-rows-by-sid {len(rows_map)}") # can differ, why?
            LOGGER.debug(f"ASSERT {len(sids_search)} == {len(adhoc_vectors)} == {len(static_vectors)}")
            assert len(sids_search) == len(adhoc_vectors)
            assert len(sids_search) == len(static_vectors)
            for sid, adhoc_vector, static_vector in zip(sids_search, adhoc_vectors, static_vectors):
                row = rows_map[sid]
                d = row.asDict()
                d[self.adhoc_vector_field] = adhoc_vector
                d[self.static_vector_field] = static_vector
                updated.append(Row(**d))
            return updated

        LOGGER.info(f"==== START {__class__.__name__} ====")

        ingress_path = f"/user/{self.service_config.username}/warehouse/{self.vector_type}/{self.task_name}/{self.domain_name}"
        egress_path = f"/user/{self.service_config.username}/mart/corpus.vec/{self.task_name}/{self.domain_name}/{snapshot_now()}"

        spark = SparkConnector.getOrCreate(local=True, spark_config=self.spark_config)
        search_client = init_elastic_client(self.service_config)

        snapshot_provider = SnapshotPathProvider(self.service_config)
        ingress_df = spark.read.json(snapshot_provider.latest(ingress_path))
        inflated_ingress_df = self.inflate_dataframe(ingress_df, self.item_category_names, self.category_name)

        test_df = inflated_ingress_df.sample(0.1, seed=42)
        train_dev_df = inflated_ingress_df.subtract(test_df)
        dev_df = train_dev_df.sample(0.09, seed=42)
        train_df = train_dev_df.subtract(dev_df)
        phase_df_dict = {
            "train": train_df,
            "validation": dev_df,
            "test": test_df
        }

        for phase, df in phase_df_dict.items():
            iter_writer = SparkBatchWriterForPandasRowIterator(
                service_config=self.service_config,
                save_path=egress_path + "/" + phase,
                batch_size=self.batch_size,
                user_callback=search_vector_callback
            )
            iter_writer(df.toPandas(), search_client=search_client)

        LOGGER.info(
            f"==== {__class__.__name__} have written corpus.vec to {egress_path} ===="
        )
        LOGGER.info(f"==== END {__class__.__name__} ====")

