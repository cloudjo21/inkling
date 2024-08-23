import argparse
from copy import deepcopy

from tunip.datetime_utils import (
    DatetimeInterval,
    DatetimeIntervalItem,
    DateIntervalBuilder
)
from tunip.feature_path_utils import (
    DomainBasedFeaturePath,
    SchemaTypedFeaturePath,
    SchemaTypedSnapshotFeaturePath
)
from tunip.file_utils import services as file_services
from tunip.path.lake import LakePath
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
from inkling.feature.factory import GeneralFeatureFactory, PeriodicFeatureFactory
from inkling.utils.airflow_utils import (
    get_my_task,
    get_mart_dates
)


class LoadMartDataset(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super().__init__(service_config)

        self.service_config = service_config
        self.task_config = task_config

        self.domain_name = self.task_config["ingress"]["domain_name"]
        self.schema_type = self.task_config["ingress"]["schema_type"]
        self.source_type = self.task_config["ingress"]["source_type"]

        self.build_options = self.task_config["ingress"]["build_options"]
        self.date_period_days = self.task_config["ingress"]["date_period_days"]

        self.feature_domain_name = self.task_config["ingress"]["feature_domain_name"]
        self.label_name = self.task_config["ingress"]["label_name"]
        self.key_names = self.task_config["ingress"]["key_names"]
        self.datetime_name = self.task_config["ingress"]["datetime_name"]
        self.user_id_name = self.task_config["ingress"]["user_id_name"]
        self.item_id_name = self.task_config["ingress"]["item_id_name"]
        self.additional_columns = self.task_config["ingress"]["additional_columns"]

        self.target_periodic_features = task_config["ingress"]["target_periodic_features"]
        self.target_general_features = task_config["ingress"]["target_general_features"]

        self.egress_task_name = self.task_config["egress"]["task_name"]
        self.egress_domain_name = self.task_config["egress"]["domain_name"]

    def __call__(self, context):

        LOGGER.info(f"==== START {__class__.__name__} ====")

        prev_dag_stat = self.fetch(context)

        run_snapshot_dt = (prev_dag_stat.header.run_snapshot_dt or None) if prev_dag_stat is not None else None
        save_snapshot_dt = run_snapshot_dt if run_snapshot_dt else snapshot_now()

        snapshot_provider = SnapshotPathProvider(self.service_config)
        file_service = file_services(self.service_config)
        spark = SparkConnector.getOrCreate(
            local=True,
            spark_config={
                "spark.executor.cores": 4,
                "spark.executor.memory": "2g",
                "spark.driver.memory": "6g",
                "spark.executor.extraJavaOptions":"-XX:+UesG1GC",
                "spark.driver.extraJavaOptions":"-XX:+UesG1GC",
                "spark.sql.autoBroadcastJoinThreshold": "-1"
            }
        )

        LOGGER.info(f"periodic features:\n{self.target_periodic_features}")
        LOGGER.info(f"general features:\n{self.target_general_features}")

        root_dataset_domain_feature_path = DomainBasedFeaturePath(
            lake_path=LakePath(self.service_config.username),
            domain_name=self.domain_name
        )
        feature_domain_feature_path = DomainBasedFeaturePath(
            lake_path=LakePath(self.service_config.username),
            domain_name=self.feature_domain_name
        )
        schema_path=SchemaTypedFeaturePath(
            domain_path=root_dataset_domain_feature_path,
            schema_type=self.schema_type,
            phase_type=self.source_type
        )
        
        latest_snapshot_dt = snapshot_provider.latest_snapshot_dt(
            repr(schema_path),
            force_fs=self.service_config.filesystem.upper()
        )
        schema_feature_path = SchemaTypedSnapshotFeaturePath(
            schema_path=schema_path,
            snapshot_dt=latest_snapshot_dt
        )
        LOGGER.info("initial dataset path:")
        LOGGER.info(repr(schema_feature_path))

        resultive_date_intervals = dict()

        for build_opt in self.build_options:
            phase_type = build_opt["phase_type"]
            # end_date_interval = build_opt["end_date_interval"]
            start_date_interval, end_date_interval = get_mart_dates(build_opt)
            resultive_date_intervals[phase_type] = {
                "start_date": start_date_interval,
                "end_date": end_date_interval
            }

            date_interval_builder: DateIntervalBuilder = DateIntervalBuilder(
                interval_start_date=start_date_interval,
                interval_end_date=end_date_interval,
                date_period_days=self.date_period_days,
                use_datetime=True
            )
            datetime_interval: DatetimeInterval = date_interval_builder.apply()

            LOGGER.info("PERIODICS:")
            LOGGER.info(list(iter(datetime_interval)))

            start_date, end_date = datetime_interval.maximal_interval()
            
            init_dataset_df = spark.read.parquet(
                repr(schema_feature_path)
            ).filter(
                f"{self.datetime_name} >= '{start_date}' and {self.datetime_name} < '{end_date}'"
            )
            
            LOGGER.info("columns of inital dataset:")
            LOGGER.info(init_dataset_df.schema.fieldNames())

            LOGGER.info("merge PERIODIC features into the dataset")
            LOGGER.info(f"{init_dataset_df.count()} number of records before merging {phase_type} features")
            
            periodic_featured_sdf = None
            periodic_target_feature_names = []

            if self.target_periodic_features:
                for prev_start_date, prev_end_date, start_date, end_date in iter(datetime_interval):
                
                    LOGGER.info(f"merge the features for [{start_date}:{end_date}] by [{prev_start_date}:{prev_end_date}]")
                
                    featured_sdf = init_dataset_df.filter(f"{self.datetime_name} >= '{start_date}' and {self.datetime_name} < '{end_date}'")
                
                    for feature_name in self.target_periodic_features:
                        LOGGER.info(f"... merge the feature: {feature_name}")
                        LOGGER.info(f"CURRENT features: {featured_sdf.columns}")
                        feature_merger = PeriodicFeatureFactory.create(feature_name, self.service_config, feature_domain_feature_path, phase_type)
                        dt_interval_item = DatetimeIntervalItem(
                            source_start_date=prev_start_date,
                            source_end_date=prev_end_date,
                            target_start_date=start_date,
                            target_end_date=end_date
                        )
                        featured_sdf = feature_merger.apply(dt_interval_item, featured_sdf)
                        LOGGER.info(f"{featured_sdf.count()} number of records after merging {feature_name}")
                        periodic_target_feature_names.extend(feature_merger.get_field_names())
            
                    if periodic_featured_sdf:
                        periodic_featured_sdf = periodic_featured_sdf.union(featured_sdf)
                    else:
                        periodic_featured_sdf = featured_sdf

            LOGGER.info("merge GENERAL features into the dataset")
            
            general_target_feature_names = []
            general_featured_sdf = init_dataset_df

            if self.target_general_features:
                for feature_name in self.target_general_features:
                    LOGGER.info(f"... merge the feature: {feature_name}")
                    LOGGER.info(f"CURRENT features: {general_featured_sdf.columns}")
                    feature_merger = GeneralFeatureFactory.create(
                        feature_name,
                        self.service_config,
                        feature_domain_feature_path,
                        phase_type
                    )
                    general_featured_sdf = feature_merger.apply(general_featured_sdf)
                    LOGGER.info(f"{general_featured_sdf.count()} number of records after merging {feature_name}")
                    general_target_feature_names.extend(feature_merger.get_field_names())

            if general_target_feature_names:
                featured_sdf = periodic_featured_sdf.join(
                    general_featured_sdf.select(*([self.user_id_name, self.item_id_name] + general_target_feature_names)),
                    on=[self.user_id_name, self.item_id_name],
                    how="left"
                )
            else:
                featured_sdf = periodic_featured_sdf
        
            corpus_train_path = MartCorpusDomainTrainingStepPath(
                user_name=self.service_config.username,
                task_name=self.egress_task_name,
                domain_name=self.egress_domain_name,
                training_step=phase_type,
                # snapshot_dt=snapshot_now()
            )
            
            file_service.mkdirs(str(corpus_train_path))
            save_snapshot_path = f"{str(corpus_train_path)}/{save_snapshot_dt}"

            LOGGER.info(f"{phase_type} dataset is written into the following path:")
            LOGGER.info(f"\t{str(save_snapshot_path)}")

            columns_except_features = self.additional_columns
            feature_names = [
                c for c in deepcopy(featured_sdf.schema.fieldNames())
                if c not in self.key_names and c != self.label_name and c not in columns_except_features
            ]
            LOGGER.info(f"The {phase_type} dataset merged with the following features:")
            LOGGER.info("\n\t".join(feature_names))

            if phase_type == "test":
                featured_sdf = featured_sdf.sample(False, 0.333)

            featured_sdf.coalesce(8).drop(*columns_except_features).write.parquet(save_snapshot_path)

        LOGGER.info(f"==== END {__class__.__name__} ====")

        self.load(
            context,
            body={
                "save_snapshot_dt": save_snapshot_dt,
                "save_snapshot_path": save_snapshot_path,
                "features": feature_names,
                "date_intervals": resultive_date_intervals
            }
        )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="load mart dataset")
    parser.add_argument(
        "-d",
        "--dag_id",
        help="DAG id for this task, (dag_build_reco_v1_dataset, dag_build_reco_v1_dataset_once)",
        required=True,
        default="dag_build_reco_v1_dataset"
    )
    parser.add_argument(
        "-s",
        "--service_name",
        help="SERVICE_NAME for this task, (item-matching/reco-v1/dag-build-dataset-mart, item-matching/reco-v1/dag-build-dataset-mart-once)",
        required=True,
        default="item-matching/reco-v1/dag-build-dataset-mart"
    )
    args = parser.parse_args()

    deploy_envs = get_service_config().available_service_levels
    for deploy_env in deploy_envs[:1]:
        service_config = get_service_config(force_service_level=deploy_env)

        dag_config = YamlLoader(f"{service_config.resource_path}/{args.service_name}/dag.yml").load()
        task_config = get_my_task(
            dag_config=dag_config, 
            context=None, 
            dag_id=args.dag_id, 
            task_id="load_mart_dataset"
        )
        LoadMartDataset(service_config, task_config)(context=None)
