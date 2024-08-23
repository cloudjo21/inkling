import json
import numpy as np
import re
from pydantic import BaseModel

from pyspark.sql.functions import avg, col, length, min, max, stddev, udf
from pyspark.sql.types import ArrayType, FloatType

from tunip import file_utils
from tunip.snapshot_utils import (
    SnapshotPathProvider,
    snapshot_now
)
from tunip.spark_utils import SparkConnector

from inkling import LOGGER
from inkling.refine.keyword.load_refinery import LoadConditionalEntropyRefinery


class CalcTermRefineIntervalRequest(BaseModel):
    keyword_path: str
    keyword_length: int


class CalcLastTermRefineInterval:

    def __init__(self, service_config):
        self.service_config = service_config

    def __call__(self, calc_req: CalcTermRefineIntervalRequest, udf_map: dict) -> float:

        spark = SparkConnector.getOrCreate(local=True)
        keyword_df = spark.read.json(calc_req.keyword_path)

        terms_df = keyword_df.filter(
            length('keyword') == calc_req.keyword_length
        ).withColumn(
            'entropy', udf_map['get_entropy']('keyword')
        ).withColumn(
            'terms', udf_map['get_terms']('keyword')
        )

        last_terms_df = terms_df.withColumn('term', udf_map['last_term'](col('terms')))
        term_avg = last_terms_df.agg(avg('term').alias('avg')).collect()[0].avg
        term_stddev = last_terms_df.agg(stddev('term').alias('stddev')).collect()[0].stddev
        term_min = last_terms_df.where('term < 0.0').agg(min('term').alias('min')).collect()[0].min
        term_max = last_terms_df.where('term < 0.0').agg(max('term').alias('max')).collect()[0].max

        if calc_req.keyword_length > 5:
            lower_bound_threshold = np.linspace(-term_max, -term_min, 100)[1]
        else:
            lower_bound_threshold = np.linspace(-term_max, -term_min, 100)[5]

        LOGGER.info(f"kwd_len: {calc_req.keyword_length}, term_avg: {term_avg}, term_stddev: {term_stddev}, term_min: {term_min}, term_max: {term_max}")
        LOGGER.info(f"lb_thld: {lower_bound_threshold}")

        return lower_bound_threshold


class CalcLastTermRefineIntervalObject:
    @classmethod
    def apply(cls, calc_req, refinery, service_config):
        @udf(returnType=FloatType())
        def get_entropy(keyword):
            entropy = refinery(keyword, verbose=False)
            return entropy

        @udf(returnType=ArrayType(FloatType()))
        def get_terms(keyword):
            _, terms = refinery(keyword, verbose=True)
            return  terms

        @udf(returnType=FloatType())
        def last_term(terms):
            return terms[-1]

        calc = CalcLastTermRefineInterval(service_config)
        udf_map = {
            'get_entropy': get_entropy,
            'get_terms': get_terms,
            'last_term': last_term,
        }

        lower_bound_threshold = calc(calc_req, udf_map=udf_map)
        return lower_bound_threshold


class CalcLastTermRefineIntervalJob:

    @classmethod
    def apply(cls, service_config, schema_type='keyword', source_type='mix', min_keyword_length=3) -> dict:

        refinery = LoadConditionalEntropyRefinery.apply(service_config, schema_type=schema_type, source_type=source_type, use_threshold=False)

        @udf
        def condense_space(keyword):
            return re.sub('\s', '', keyword)
        snapshot_provider = SnapshotPathProvider(service_config)
        keyword_path = f"/user/{service_config.username}/lake/keyword/{schema_type}/{source_type}"
        keyword_snapshot_path = snapshot_provider.latest(keyword_path, return_only_nauts_path=True)

        spark = SparkConnector.getOrCreate(local=True)
        keyword_df = spark.read.json(keyword_snapshot_path)
        max_keyword_length = keyword_df.select('keyword').withColumn('keyword_length', length(condense_space('keyword'))).agg(max('keyword_length').alias('max_length')).collect()[0].max_length

        len2thld = dict()
        for kwd_len in list(range(min_keyword_length, max_keyword_length+1)):
            calc_req = CalcTermRefineIntervalRequest(
                keyword_path=keyword_snapshot_path,
                keyword_length=kwd_len
            )
            lower_bound_threshold = CalcLastTermRefineIntervalObject.apply(calc_req, refinery, service_config)
            len2thld[kwd_len] = lower_bound_threshold
        
        # write down json dictionary of keyword-length to lower-bound-threshold
        file_service = file_utils.services(service_config)
        last_term_refine_interval_path = f"/user/{service_config.username}/lake/last_term_refine_threshold/{schema_type}/{source_type}/{snapshot_now()}"
        file_service.mkdirs(last_term_refine_interval_path)
        file_service.write(last_term_refine_interval_path + "/" + "data.json", json.dumps(len2thld, ensure_ascii=False))

        return len2thld
