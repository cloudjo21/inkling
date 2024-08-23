import json

from tunip import file_utils
from tunip.snapshot_utils import SnapshotPathProvider
from tunip.spark_utils import SparkConnector

from paani.refine.cond_entropy_refinery import ConditionalEntropyRefinery

from inkling import LOGGER


class LoadConditionalEntropyRefinery:
    @classmethod
    def apply(cls, service_config, schema_type, source_type, use_threshold=True):

        spark = SparkConnector.getOrCreate(local=True)

        username = service_config.username

        snapshot_provider = SnapshotPathProvider(service_config)
        token2count_path = f"/user/{username}/lake/token2count/{schema_type}/{source_type}"
        LOGGER.info(f"Will get latest path for lake/token2count: {token2count_path}")
        token2count_snapshot_path = snapshot_provider.latest(token2count_path, return_only_nauts_path=True)

        count2token_num_path = f"/user/{username}/lake/count2token_num/{schema_type}/{source_type}"
        LOGGER.info(f"Will get latest path for lake/count2token_num: {count2token_num_path}")
        count2token_num_snapshot_path = snapshot_provider.latest(count2token_num_path, return_only_nauts_path=True)

        cooccur2count_path = f"/user/{username}/lake/cooccur2count/{schema_type}/{source_type}"
        LOGGER.info(f"Will get latest path for lake/cooccur2count: {cooccur2count_path}")
        cooccur2count_snapshot_path = snapshot_provider.latest(cooccur2count_path, return_only_nauts_path=True)

        count2cooccur_num_path = f"/user/{username}/lake/count2cooccur_num/{schema_type}/{source_type}"
        LOGGER.info(f"Will get latest path for lake/count2cooccur_num: {count2cooccur_num_path}")
        count2cooccur_num_snapshot_path = snapshot_provider.latest(count2cooccur_num_path, return_only_nauts_path=True)

        token_count_sum_path = f"/user/{username}/lake/token_count_sum/{schema_type}/{source_type}"
        LOGGER.info(f"Will get latest path for lake/token_count_sum: {token_count_sum_path}")
        token_count_sum_snapshot_path = snapshot_provider.latest(token_count_sum_path, return_only_nauts_path=True)

        token2count = dict()
        for r in spark.read.json(token2count_snapshot_path).collect():
            token2count[r['token']] = r['count']

        count2token_num = dict()
        for r in spark.read.json(count2token_num_snapshot_path).collect():
            count2token_num[r['count']] = r['n_token']

        cooccur2count = dict()
        for r in spark.read.json(cooccur2count_snapshot_path).collect():
            cooccur2count[r['cooccur']] = r['count']

        count2cooccur_num = dict()
        for r in spark.read.json(count2cooccur_num_snapshot_path).collect():
            count2cooccur_num[r['count']] = r['n_occur']

        token_count_sum = spark.read.json(token_count_sum_snapshot_path).collect()[0]['token_count_sum']

        file_service = file_utils.services(service_config)

        if use_threshold is True:
            first_term_refine_interval_path = f"/user/{username}/lake/first_term_refine_threshold/{schema_type}/{source_type}"
            first_term_refine_interval_snapshot_path = snapshot_provider.latest(first_term_refine_interval_path, return_only_nauts_path=True)
            bt_threshold_first_term = file_service.load(first_term_refine_interval_snapshot_path + "/" + "data.json")
            if bt_threshold_first_term:
                first_term_length2threshold = json.loads(bt_threshold_first_term.decode('utf-8'))
            else:
                first_term_length2threshold = dict()

            last_term_refine_interval_path = f"/user/{username}/lake/last_term_refine_threshold/{schema_type}/{source_type}"
            last_term_refine_interval_snapshot_path = snapshot_provider.latest(last_term_refine_interval_path, return_only_nauts_path=True)
            bt_threshold_last_term = file_service.load(last_term_refine_interval_snapshot_path + "/" + "data.json")
            if bt_threshold_last_term:
                last_term_length2threshold = json.loads(bt_threshold_last_term.decode('utf-8'))
            else:
                last_term_length2threshold = dict()
        else:
            first_term_length2threshold = dict()
            last_term_length2threshold = dict()

        refinery = ConditionalEntropyRefinery(
            token2count=token2count, 
            count2token_num=count2token_num,
            cooccur2count=cooccur2count,
            count2cooccur_num=count2cooccur_num,
            N_token_count_sum=token_count_sum,
            boundary_terms_refine_threshold={
                'first': first_term_length2threshold,
                'last': last_term_length2threshold
            }
        )
        return refinery

