import joblib
import numpy as np
import pygsheets
import shap
import matplotlib.pyplot as plt

from copy import deepcopy
from shap import Explanation

from tunip import GSheet
from tunip.file_utils import FileHandler
from tunip.file_utils import services as file_services
from tunip.google_cloud_utils import GoogleDriveUploader
from tunip.path_utils import services as path_services
from tunip.service_config import ServiceLevelConfig
from tunip.snapshot_utils import SnapshotPathProvider
from tunip.spark_utils import SparkConnector

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester


class ExplainFeaturesRecoV1(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super(ExplainFeaturesRecoV1, self).__init__(service_config, task_config.get("is_containerized", True))

        self.service_config = service_config
        self.task_config = task_config

        self.repository_type = self.task_config["ingress"]["repository_type"]
        self.task_name = self.task_config["ingress"]["task_name"]
        self.domain_name = self.task_config["ingress"]["domain_name"]
        self.phase_type = self.task_config["ingress"]["phase_type"]

        self.label_name = self.task_config["ingress"]["label_name"]
        self.key_names = self.task_config["ingress"]["key_names"]

        self.spark_config = self.task_config["ingress"].get("spark_config", {"spark.executor.cores": 2, "spark.executor.memory": "2g", "spark.driver.memory": "4g"})
        self.n_sample: int = self.task_config["ingress"]["n_sample"]

        self.drive_target_folder_ids: dict = self.task_config["egress"]["drive_target_folder_ids"]
        self.leader_board_sheet_url = self.task_config["egress"]["leader_board_sheet_url"]

        self.mime_type = self.task_config["egress"].get("mime_type", "image/png")

    def __call__(self, context):

        LOGGER.info(f"==== START {__class__.__name__} ====")

        prev_dag_stat = self.fetch(context)
        start_snapshot_dt = prev_dag_stat.header.run_snapshot_dt or None
        save_snapshot_dt = start_snapshot_dt
        leader_board_wsheet_gid = prev_dag_stat.body["leader_board_wsheet_gid"]

        snapshot_provider = SnapshotPathProvider(self.service_config)
        spark = SparkConnector.getOrCreate(local=True, spark_config=self.spark_config)
        file_service = file_services(self.service_config)
        uploader = GoogleDriveUploader(self.service_config.config)

        corpus_path = f"/user/{self.service_config.username}/{self.repository_type}/corpus/{self.task_name}/{self.domain_name}/{self.phase_type}"
        latest_corpus_path = snapshot_provider.latest(corpus_path, return_only_nauts_path=True)
        model_path = f"/user/{self.service_config.username}/{self.repository_type}/models/{self.task_name}/{self.domain_name}"

        corpus_sdf = spark.read.parquet(latest_corpus_path)
        feature_names = [c for c in deepcopy(corpus_sdf.columns) if c not in self.key_names and c != self.label_name]
        corpus_pdf = corpus_sdf.select(*(feature_names + [self.label_name])).to_pandas_on_spark().to_pandas()
        LOGGER.info(f"feature_names: {feature_names}")
        LOGGER.info(f"label_name: {self.label_name}")
        features = corpus_pdf.loc[:, feature_names]
        LOGGER.info(f"features[:2]: {features[:2]}")

        latest_model_path = f"{snapshot_provider.latest(model_path, return_only_nauts_path=True)}/model.ubj"
        file_service.download(latest_model_path)

        path_service = path_services.get("LOCAL", config=self.service_config.config)
        xgb_model = joblib.load(path_service.build(latest_model_path))

        LOGGER.info("======== Explainer is computing SHAP Values / SHAP Interaction Values ========")

        explainer = shap.TreeExplainer(xgb_model)
        shap_values: np.ndarray = explainer.shap_values(features[:self.n_sample])
        shap_interaction_values: np.ndarray = explainer.shap_interaction_values(features[:self.n_sample])

        # for shap.plots.*
        shap_values_obj: Explanation = explainer(features[:self.n_sample])

        LOGGER.info("======== Plotting SHAP Global Plots ========")

        target_schema = "shap_global_plots"
        target_folder_id = self.drive_target_folder_ids[target_schema]
        snapshot_folder_id = uploader.create_folder(folder_name=save_snapshot_dt, parent_folder_id=target_folder_id)

        figure_filename = "shap_summary_beeswarm.png"
        shap.summary_plot(shap_values, features[:self.n_sample], max_display=len(features.columns), show=False)
        self._save_shap_figure(
            figure_filename,
            snapshot_folder_id,
            file_service,
            uploader,
            save_snapshot_dt,
            target_schema=target_schema
        )

        figure_filename = "shap_summary_violin.png"
        shap.plots.violin(shap_values, features[:self.n_sample], max_display=len(features.columns), show=False)
        self._save_shap_figure(
            figure_filename,
            snapshot_folder_id,
            file_service,
            uploader,
            save_snapshot_dt,
            target_schema=target_schema
        )

        # figure_filename = "shap_summary_layered_violin.png"
        # shap.plots.violin(shap_values_obj, features=features[:self.n_sample], feature_names=features.columns, plot_type="layered_violin", show=False)
        # self._save_shap_figure(
        #     figure_filename,
        #     snapshot_folder_id,
        #     file_service,
        #     uploader,
        #     save_snapshot_dt,
        #     target_schema=target_schema
        # )

        figure_filename = "shap_summary_bar_mean_shap_values.png"
        shap.plots.bar(shap_values_obj, max_display=len(features.columns))
        self._save_shap_figure(
            figure_filename,
            snapshot_folder_id,
            file_service,
            uploader,
            save_snapshot_dt,
            target_schema=target_schema
        )

        figure_filename = "shap_summary_interaction_beeswarm_best_features.png"
        shap.summary_plot(
            shap_interaction_values[:self.n_sample],
            features[:self.n_sample],
            plot_size=(1, 1.2),
            show=False
        )
        self._save_shap_figure(
            figure_filename,
            snapshot_folder_id,
            file_service,
            uploader,
            save_snapshot_dt,
            target_schema=target_schema
        )

        figure_filename = "shap_summary_interaction_beeswarm_samples.png"
        shap.summary_plot(
            shap_interaction_values[:self.n_sample],
            features[:self.n_sample],
            max_display=len(features.columns),
            plot_size=(1, 1.2),
            show=False
        )
        self._save_shap_figure(
            figure_filename,
            snapshot_folder_id,
            file_service,
            uploader,
            save_snapshot_dt,
            target_schema=target_schema
        )

        LOGGER.info("======== Noting the Memo of URL to SHAP Global Plots in the Record of Leaderboard ========")

        # leader_board_wsheet_name = f"{self.domain_name}-shap_plots_global-{save_snapshot_dt}"
        service_filepath = f"{self.service_config.resource_path}/{self.service_config.config.get('gcs.project_id')}.json"

        gsheet = GSheet(service_file=service_filepath)
        sheet: pygsheets.spreadsheet.Spreadsheet = gsheet.get_sheet(self.leader_board_sheet_url)
        wsheet = sheet.worksheet(property="id", value=leader_board_wsheet_gid)
        wsheet.cell("A1").note = f"SHAP Explanation Figures: https://drive.google.com/drive/folders/{snapshot_folder_id}"

        LOGGER.info(f"==== END {__class__.__name__} ====")
    
    def _save_shap_figure(
            self,
            figure_filename: str,
            snapshot_folder_id: str,
            file_service: FileHandler,
            uploader: GoogleDriveUploader,
            save_snapshot_dt: str,
            target_schema: str,
            dpi: int=700
        ):
        plt.savefig(figure_filename, dpi=dpi)
        self._write_to_file_service(figure_filename, file_service, save_snapshot_dt, target_schema)
        uploader.upload(filepath=figure_filename, mime_type=self.mime_type, target_folder_id=snapshot_folder_id)

    def _write_to_file_service(self, figure_filename: str, file_service: FileHandler, save_snapshot_dt: str, target_schema: str):
        with open(figure_filename, "rb") as f:
            summary_plot_bytes = f.read()
            plot_dir_path = f"/user/{self.service_config.username}/{self.repository_type}/{target_schema}/{self.task_name}/{self.domain_name}/{self.phase_type}/{save_snapshot_dt}"
            file_service.write_binary(f"{plot_dir_path}/{figure_filename}", summary_plot_bytes)


if __name__ == "__main__":
    import argparse
    from tunip.datetime_utils import KST, snapshot2dag_runid
    from tunip.service_config import get_service_config
    from tunip.yaml_loader import YamlLoader
    from inkling.utils.airflow_utils import get_my_task

    parser = argparse.ArgumentParser(description="explain the main/interaction effects of features of reco-v1 model via SHAP plots")
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
        task_id="explain_features_reco_v1"
    )
    task_config["is_containerized"] = False

    # $BUCKET_NAME/user/ed/dag_stat/dag_build_reco_v1_model/20231121_022237_000000
    run_snapshot_dt = args.snapshot_dt
    snapshot_dag_runid = snapshot2dag_runid(run_snapshot_dt, time_zone=KST)

    ExplainFeaturesRecoV1(
        service_config,
        task_config
    (
        context={
            "header": {
                "dag_id": "dag_build_reco_v1_model",
                "task_id": "explain_features_reco_v1",
                "run_id": f"app_main__{snapshot_dag_runid}",
                "run_snapshot_dt": run_snapshot_dt
            },
            "body": {
                "prev_dag_id": "dag_build_reco_v1_model",
                "prev_task_id": "record_leaderboard_reco_v1",
                "start_snapshot_dt": run_snapshot_dt,
            }
        }
    )
