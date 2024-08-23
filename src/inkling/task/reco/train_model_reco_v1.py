import joblib
import numpy as np
import orjson
import os
import pyspark.pandas as ps
import time

from copy import deepcopy
from pathlib import Path
from xgboost import XGBClassifier

from tunip.datetime_utils import snapshot2dag_runid
from tunip.file_utils import services as file_services
from tunip.model.feature_schema import FeatureSchemaHandler
from tunip.path.mart import (
    MartCorpusDomainTrainingStepPath,
    MartModelPath,
)
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


class TrainModelRecoV1(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super(TrainModelRecoV1, self).__init__(service_config, task_config.get("is_containerized", True))

        self.service_config = service_config
        self.task_config = task_config

        self.task_name = self.task_config["ingress"]["task_name"]
        self.domain_name = self.task_config["ingress"]["domain_name"]
        self.dataset_type = self.task_config["ingress"]["dataset_type"]

        self.label_name = self.task_config["ingress"]["label_name"]
        self.key_names = self.task_config["ingress"]["key_names"]

        self.spark_config = self.task_config["ingress"]["spark_config"]

        self.training_data_sampling_factor = self.task_config["ingress"].get("training_sampling_factor", 1.0)

    def __call__(self, context):

        LOGGER.info(f"==== START {__class__.__name__} ====")

        snapshot_provider = SnapshotPathProvider(self.service_config)
        spark = SparkConnector.getOrCreate(local=True, spark_config=self.spark_config)
        file_service = file_services(self.service_config)

        np.random.seed(42)

        start_snapshot_dt = snapshot_now()

        corpus_path = MartCorpusDomainTrainingStepPath(
            user_name=self.service_config.username,
            task_name=self.task_name,
            domain_name=self.domain_name,
            training_step="training"
        )
        if os.environ.get("TRAINING_DATASET_SNAPSHOT_DT"):
            training_corpus_path = f"{str(corpus_path)}/{os.environ.get('TRAINING_DATASET_SNAPSHOT_DT')}"
        else:
            training_corpus_path = snapshot_provider.latest(corpus_path, return_only_nauts_path=True)
        training_corpus_sdf = spark.read.parquet(training_corpus_path)

        LOGGER.info(f"Mart Corpus Path: {training_corpus_path}")

        feature_names = [c for c in deepcopy(training_corpus_sdf.columns) if c not in self.key_names and c != self.label_name]
        feature_names_string = '\n' + '\n'.join(list(map(lambda x: "  - " + x, feature_names)))
        LOGGER.info(f"Feature Names: {feature_names_string}")

        training_corpus_pdf = training_corpus_sdf.select(*(feature_names+[self.label_name])).to_pandas_on_spark().to_pandas()

        training_sample_ratio = int(self.training_data_sampling_factor * len(training_corpus_pdf))
        indexes = np.random.choice(len(training_corpus_pdf), training_sample_ratio, replace=False)

        training_features = training_corpus_pdf.loc[indexes, feature_names]
        training_labels = training_corpus_pdf.loc[indexes, self.label_name]

        invalid_columns = []
        for column_name in training_features.columns:
            contain_na = training_features.loc[:, column_name].isna().any()
            if contain_na:
                invalid_columns.append(column_name)

        assert len(invalid_columns) == 0, LOGGER.error(f"invalid columns: {', '.join(invalid_columns)}")

        renamed_features = ['f%d' % i for i in range(len(training_features.columns))]
        feature_name_map = dict(zip(training_features.columns, renamed_features))
        fprefixed_training_features = training_features.rename(columns=feature_name_map)

        start_time = time.time()

        item_reco_xgb_regular = XGBClassifier(n_estimators=500, learning_rate=0.2, max_depth=4, random_state=32)
        item_reco_xgb_regular.fit(fprefixed_training_features, training_labels)

        training_time = time.time() - start_time
        LOGGER.info("{} 학습시간 : {:.4f}sec".format(self.dataset_type, training_time))

        joblib.dump(item_reco_xgb_regular, 'model.ubj')
        LOGGER.info(f"Model is saved at local current directory: {Path(os.curdir).absolute()}")

        # model_snapshot_path = f"/user/{self.service_config.username}/mart/models/{self.task_name}/{self.domain_name}/"
        model_snapshot_path = f"{repr(MartModelPath(self.service_config.username, self.task_name, self.domain_name))}/{start_snapshot_dt}"
        file_service.mkdirs(model_snapshot_path)

        model_bytes = open('model.ubj', 'rb').read()
        # model_bytes = joblib.load('model.ubj')
        LOGGER.info(f"{len(model_bytes)} bytes of ({self.task_name}/{self.domain_name}) model would be written to the path: {model_snapshot_path}/model.ubj")

        file_service.write_binary(f"{model_snapshot_path}/model.ubj", model_bytes)
        LOGGER.info(f"({self.task_name}/{self.domain_name}) model has written to the path: {model_snapshot_path}/model.ubj")

        schemata_snapshot_path = f"/user/{self.service_config.username}/mart/schemata/{self.task_name}/{self.domain_name}/{start_snapshot_dt}"
        file_service.mkdirs(schemata_snapshot_path)

        feature_schema_handler = FeatureSchemaHandler(self.service_config)
        feature_schema_handler.write(f"{schemata_snapshot_path}/schema.json", training_features)
        LOGGER.info(f"({self.task_name}/{self.domain_name}) model's schema has written to the path: {schemata_snapshot_path}/schema.json")

        file_service.write(f"{schemata_snapshot_path}/feature_name_map.json", orjson.dumps(feature_name_map).decode())
        LOGGER.info(f"({self.task_name}/{self.domain_name}) model's feature-name-to-fprefixed-name has written to the path: {schemata_snapshot_path}/feature_name_map.json")

        self.load(
            context,
            body={
                "start_snapshot_dt": start_snapshot_dt,
                "num_training_data": len(training_features),
                "training_corpus_path": training_corpus_path,
                "training_time": training_time,
                "dataset_type": self.dataset_type,
                "feature_names": {", ".join(feature_names)},
                "num_features": len(feature_names),
                "training_data_sampling_factor": self.training_data_sampling_factor,
                "num_training_data_raw": len(training_corpus_pdf),
                "username": self.service_config.username,
                "domain_name": self.domain_name,
                "task_name": self.task_name,
                "spark_config": self.spark_config
            }
        )

        LOGGER.info(f"==== END {__class__.__name__} ====")


if __name__ == "__main__":
    service_config = get_service_config()
    dag_path = f"{service_config.resource_path}/item-matching/reco-v1/dag-build-model-mart/dag.yml"
    dag_config = YamlLoader(dag_path).load()
    task_config = get_my_task(
        dag_config=dag_config, 
        context=None, 
        dag_id="dag_build_reco_v1_model", 
        task_id="train_model_reco_v1"
    )
    task_config["is_containerized"] = False

    snapshot_dt = snapshot_now()
    snapshot_dag_runid = snapshot2dag_runid(snapshot_dt)

    TrainModelRecoV1(service_config, task_config)(
        context={
            "header": {
                "dag_id": "dag_build_reco_v1_model",
                "task_id": "train_model_reco_v1",
                "run_id": f"app_main__{snapshot_dag_runid}",
                # "run_snapshot_dt": snapshot_dt
            },
            "body": {
                "prev_dag_id": None,
                "prev_task_id": None,
            }
        }
    )
