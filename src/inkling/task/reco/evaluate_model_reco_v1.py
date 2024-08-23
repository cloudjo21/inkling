import argparse
import joblib
import numpy as np
import orjson
import os
import pyspark.pandas as ps

from copy import deepcopy
from sklearn.metrics import precision_score, recall_score

from tunip.datetime_utils import KST, snapshot2dag_runid
from tunip.file_utils import services as file_services
from tunip.path.mart import MartCorpusDomainTrainingStepPath
from tunip.service_config import (
    ServiceLevelConfig,
    get_service_config
)
from tunip.snapshot_utils import (
    SnapshotPathProvider,
    snapshot_now
)
from tunip.spark_utils import SparkConnector
from tunip.yaml_loader import YamlLoader

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester
from inkling.utils.airflow_utils import get_my_task


class EvaluateModelRecoV1(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super(EvaluateModelRecoV1, self).__init__(service_config, task_config.get("is_containerized", True))

        self.service_config = service_config
        self.task_config = task_config

        self.task_name = self.task_config["ingress"]["task_name"]
        self.domain_name = self.task_config["ingress"]["domain_name"]

        self.label_name = self.task_config["ingress"]["label_name"]
        self.key_names = self.task_config["ingress"]["key_names"]

        self.spark_config = self.task_config["ingress"]["spark_config"]
        self.best_pos_pred_threshold = self.task_config["ingress"].get("threshold", 0.5)

    def __call__(self, context):

        LOGGER.info(f"==== START {__class__.__name__} ====")

        prev_dag_stat = self.fetch(context)

        save_snapshot_dt = prev_dag_stat["start_snapshot_dt"] or None

        assert save_snapshot_dt

        LOGGER.info(f"read run_snapshot_dt: {save_snapshot_dt} from prev DAG-status.")

        snapshot_provider = SnapshotPathProvider(self.service_config)
        spark = SparkConnector.getOrCreate(local=True, spark_config=self.spark_config)
        file_service = file_services(self.service_config)

        xgb_model = joblib.load('model.ubj')

        corpus_path = MartCorpusDomainTrainingStepPath(
            user_name=self.service_config.username,
            task_name=self.task_name,
            domain_name=self.domain_name,
            training_step="test"
        )

        if os.environ.get("TEST_DATASET_SNAPSHOT_DT"):
            test_corpus_path = f"{str(corpus_path)}/{os.environ.get('TEST_DATASET_SNAPSHOT_DT')}"
        else:
            test_corpus_path = snapshot_provider.latest(corpus_path, return_only_nauts_path=True)
        test_corpus_sdf = spark.read.parquet(test_corpus_path)

        feature_names = [c for c in deepcopy(test_corpus_sdf.columns) if c not in self.key_names and c != self.label_name]
        feature_names_string = '\n' + '\n'.join(list(map(lambda x: "  - " + x, feature_names)))
        LOGGER.info(f"Feature Names: {feature_names_string}")

        test_corpus_pdf = test_corpus_sdf.select(*(feature_names+[self.label_name])).to_pandas_on_spark().to_pandas()

        test_features = test_corpus_pdf.loc[:, feature_names]
        test_labels = test_corpus_pdf.loc[:, self.label_name]

        schemata_snapshot_path = snapshot_provider.latest(f"/user/{self.service_config.username}/mart/schemata/{self.task_name}/{self.domain_name}", return_only_nauts_path=True)
        feature_name_map = orjson.loads(file_service.load(f'{schemata_snapshot_path}/feature_name_map.json'))
        fprefixed_test_features = test_features.rename(columns=feature_name_map)
        test_predictions = xgb_model.predict_proba(fprefixed_test_features[:])

        test_pred_indexes = np.argmax(test_predictions, axis=1)
        # np.where(test_pred_indexes == 1)[0].size, len(test_predictions)
        test_label_samples = test_labels[:].to_numpy()

        LOGGER.info(f"#-of-positive-labels: {np.where(test_pred_indexes == 1)[0].size}")
        LOGGER.info(f"#-of-test-predictions: {len(test_predictions)}")
        LOGGER.info(f"#-of-test-labels: {len(test_label_samples)}")

        precision_result = precision_score(test_label_samples, test_pred_indexes)
        recall_result = recall_score(test_label_samples, test_pred_indexes)

        f1_score = 2/(1/precision_result + 1/recall_result)

        # TODO recording metrics
        LOGGER.info(
            f"""
            prediction: {precision_result}
            recoall: {recall_result}
            f1-score: {f1_score}
            """
        )

        end_snapshot_dt = snapshot_now()

        self.load(
            context,
            body={
                "start_snapshot_dt": prev_dag_stat.body["start_snapshot_dt"],
                "end_snapshot_dt": end_snapshot_dt,
                "precision": precision_result.item(),
                "recall": recall_result.item(),
                "threshold": self.best_pos_pred_threshold,

                "num_test_data": len(test_features),
                "num_test_data_raw": len(test_corpus_pdf), 
                "test_corpus_path": test_corpus_path,
                "num_test_features": len(feature_names),
                "test_feature_names": {", ".join(feature_names)},

                "num_training_data": prev_dag_stat.body["num_training_data"],
                "num_training_data_raw": prev_dag_stat.body["num_training_data_raw"],
                "training_corpus_path": prev_dag_stat.body["training_corpus_path"],
                "num_training_features": len(prev_dag_stat.body["feature_names"]),
                "training_feature_names": {", ".join(prev_dag_stat.body["feature_names"])},

                "training_time": prev_dag_stat.body["training_time"],
                "dataset_type": prev_dag_stat.body["dataset_type"],
                "training_data_sampling_factor": prev_dag_stat.body["training_data_sampling_factor"],
                "username": prev_dag_stat.body["username"],
                "domain_name": prev_dag_stat.body["domain_name"],
                "task_name": prev_dag_stat.body["task_name"],
                "spark_config": prev_dag_stat.body["spark_config"]
            }
        )

        LOGGER.info(f"==== END {__class__.__name__} ====")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="evaluate reco-v1 model")
    parser.add_argument(
        "-s",
        "--snapshot_dt",
        help="snapshot datetime string for dag_stat.[DAG_ID].[SNAPSHOT_DT].[PREV_TASK_ID].result.header.run_snapshot_dt",
        type=str,
        required=True,
    )
    args = parser.parse_args()

    service_config = get_service_config()
    dag_path = f"{service_config.resource_path}/item-matching/reco-v1/dag-build-model-mart/dag.yml"
    dag_config = YamlLoader(dag_path).load()
    task_config = get_my_task(
        dag_config=dag_config, 
        context=None,
        dag_id="dag_build_reco_v1_model", 
        task_id="evaluate_model_reco_v1"
    )
    task_config["is_containerized"] = False

    # $BUCKET_NAME/user/ed/dag_stat/dag_build_reco_v1_model/20231121_022237_000000
    run_snapshot_dt = args.snapshot_dt
    snapshot_dag_runid = snapshot2dag_runid(run_snapshot_dt, time_zone=KST)

    EvaluateModelRecoV1(
        service_config,
        task_config
    )(
        context={
            "header": {
                "dag_id": "dag_build_reco_v1_model",
                "task_id": "evaluate_model_reco_v1",
                "run_id": f"app_main__{snapshot_dag_runid}",
                "run_snapshot_dt": run_snapshot_dt
            },
            "body": {
                "prev_dag_id": "dag_build_reco_v1_model",
                "prev_task_id": "train_model_reco_v1",
                "start_snapshot_dt": run_snapshot_dt,
            }
        }
    )
