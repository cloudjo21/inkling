import joblib
import orjson
import pandas as pd
import pygsheets
import requests

from tunip import GSheet
from tunip.service_config import ServiceLevelConfig

from inkling import LOGGER
from inkling.dag_stat_utils import DAGStatusDigester


class RecordLeaderboardRecoV1(DAGStatusDigester):

    def __init__(self, service_config: ServiceLevelConfig, task_config: dict):
        super(RecordLeaderboardRecoV1, self).__init__(service_config, task_config.get("is_containerized", True))

        self.service_config = service_config
        self.task_config = task_config

        self.task_name = self.task_config["ingress"]["task_name"]
        self.domain_name = self.task_config["ingress"]["domain_name"]

        self.training_record_slack_url = self.task_config["egress"]["training_record_slack_url"]
        self.leader_board_sheet_url = self.task_config["egress"]["leader_board_sheet_url"]
        self.top_k_leaders = self.task_config["egress"]["top_k_leaders"]

        # $BUCKET_NAME/user/ed/dag_stat/dag_build_reco_v1_model/20231115_102231_636039

    def __call__(self, context):

        LOGGER.info(f"==== START {__class__.__name__} ====")

        prev_dag_stat = self.fetch(context)

        start_snapshot_dt = prev_dag_stat.body["start_snapshot_dt"] or None
        end_snapshot_dt = prev_dag_stat.body["end_snapshot_dt"] or None

        save_snapshot_dt = start_snapshot_dt

        assert save_snapshot_dt

        LOGGER.info(f"read run_snapshot_dt: {save_snapshot_dt} from prev DAG-status.")

        xgb_model = joblib.load('model.ubj')

        recording_wsheet_name = self.domain_name
        leader_board_wsheet_name = f"{self.domain_name}-top-{self.top_k_leaders}-records-{save_snapshot_dt}"
        service_filepath = f"{self.service_config.resource_path}/{self.service_config.config.get('gcs.project_id')}.json"

        gsheet = GSheet(service_file=service_filepath)
        sheet: pygsheets.spreadsheet.Spreadsheet = gsheet.get_sheet(self.leader_board_sheet_url)
        wsheet = sheet.worksheet_by_title(recording_wsheet_name)
        LOGGER.info(f"model: {self.domain_name} leader_board_sheet: {wsheet.url}")

        stat_body = prev_dag_stat.body
        training_record = {
            "snapshots": {"start_time": start_snapshot_dt, "end_time": end_snapshot_dt},
            "models": {
                "model_configs": [
                    {
                        "model_name": str(type(xgb_model)),
                        "model_config": {}
                    }
                ]
            },
            "evaluations": {
                "precision": stat_body["precision"],
                "recall": stat_body["recall"],
                "threshold": stat_body["threshold"]
            },
            "feature_names": ", ".join(stat_body["training_feature_names"]),
            "num_training_data": stat_body["num_training_data"],
            "num_test_data": stat_body["num_test_data"],
            "training_time": stat_body["training_time"],
            "dataset_type": stat_body["dataset_type"],
            "training_data_sampling_factor": stat_body["training_data_sampling_factor"],
            "num_training_data_raw": stat_body["num_training_data_raw"],
            "num_test_data_raw": stat_body["num_test_data_raw"], 
            "username": stat_body["username"],
            "domain_name": stat_body["domain_name"],
            "task_name": stat_body["task_name"],
            "spark_config": stat_body["spark_config"],
        }

        header_ranges = [((1,1), (1,len(list(training_record))+1))]
        header_values = [[['record_id'] + list(training_record)]]
        wsheet.update_values_batch(ranges=header_ranges, values=header_values, majordim='ROWS')

        record_tuple = [
            (
                k,
                orjson.dumps(v, option=orjson.OPT_INDENT_2).decode() if isinstance(v, dict) else v
            ) for (k, v) in training_record.items()
        ]
        record_tuple = [(k, '\n'.join(v.split(", "))) if k == 'feature_names' else (k, v) for k, v in record_tuple]
        record_values = [str(x[1]) for x in record_tuple]

        record_ids = list(map(lambda x: int(x),  filter(lambda x: x != 'record_id' and x, wsheet.get_col(1, returnas="matrix"))))
        next_row_id = record_ids[-1] + 1 if record_ids else 1
        wsheet.update_row(index=next_row_id, values=[next_row_id]+ record_values)
        wsheet.adjust_column_width(start=2, end=2, pixel_size=300)
        wsheet.adjust_column_width(start=3, end=3, pixel_size=300)
        wsheet.adjust_column_width(start=4, end=4, pixel_size=300)
        wsheet.adjust_column_width(start=5, end=5, pixel_size=300)
        wsheet.adjust_column_width(start=6, end=6, pixel_size=500)

        slack_record_log = dict(filter(lambda x: x[0] in ["snapshots", "models", "evaluations", "feature_names"], training_record.items()))
        slack_record_log = dict([(k.upper(), v) for k, v in slack_record_log.items()])
        slack_record_log = dict([("*" + k + "*", v) if k in ["EVALUATIONS"] else (k, v) for k, v in slack_record_log.items()])

        slack_message = orjson.dumps(slack_record_log, option=orjson.OPT_INDENT_2).decode() + "\n================================================================"

        session = requests.Session()
        session.post(url=self.training_record_slack_url, json={"text": slack_message})

        recording_df = wsheet.get_as_df()
        if recording_df is None or len(recording_df) < 1:
            LOGGER.info(f"Worksheet for {recording_wsheet_name} would have been filled the first record")
            record_tuple.append(("record_id", 1))
            recording_df = pd.DataFrame([dict(record_tuple)])
        else:
            LOGGER.info(f"Worksheet for {recording_wsheet_name} has {len(recording_df)} records")

        def get_f1(prec, recl):
            return 2/(1/prec + 1/recl)

        def get_f1_from_evaluations(eval_str):
            ev = orjson.loads(eval_str)
            f1 = get_f1(ev["precision"], ev["recall"])
            return f1

        eval_df = recording_df.set_index("record_id").evaluations.apply(lambda x: get_f1_from_evaluations(x)).sort_values(ascending=False)
        merged_leading_record_df = recording_df.set_index("record_id").merge(eval_df.iloc[:self.top_k_leaders], left_index=True, right_index=True)

        sheet.add_worksheet(leader_board_wsheet_name, index=1)

        def get_start_time_from_snapshots(snapshots: str):
            return orjson.loads(snapshots)["start_time"] or None

        def get_model_name(models: str):
            return "|".join([m_config["model_name"] for m_config in orjson.loads(models)["model_configs"]])

        def get_precision(evaluations_x: str):
            eval_x = orjson.loads(evaluations_x)
            return format(eval_x["precision"], '.3f')

        def get_recall(evaluations_x: str):
            eval_x = orjson.loads(evaluations_x)
            return format(eval_x["recall"], '.3f')

        def get_threshold(evaluations_x: str):
            eval_x = orjson.loads(evaluations_x)
            if "threshold" in eval_x:
                return format(eval_x["threshold"], '.3f')
            else:
                return format(.5, '.3f')

        leading_record_df = merged_leading_record_df.loc[:, ["task_name", "domain_name", "snapshots", "models", "evaluations_y", "evaluations_x"]]
        leading_record_df["record_time"] = leading_record_df.loc[:, "snapshots"].apply(lambda x: get_start_time_from_snapshots(x))
        leading_record_df["model_name"] = leading_record_df.loc[:, "models"].apply(lambda x: get_model_name(x))
        leading_record_df["precision"] = leading_record_df.loc[:, "evaluations_x"].apply(lambda x: get_precision(x))
        leading_record_df["recall"] = leading_record_df.loc[:, "evaluations_x"].apply(lambda x: get_recall(x))
        leading_record_df["threshold"] = leading_record_df.loc[:, "evaluations_x"].apply(lambda x: get_threshold(x))
        leading_record_df = leading_record_df.rename(columns={"evaluations_y": "f1-score"})
        leading_record_df = leading_record_df.drop(columns=["snapshots", "models", "evaluations_x"])
        leading_record_df = leading_record_df.loc[leading_record_df.loc[:, "f1-score"].sort_values(ascending=False).index, :]

        lb_wsheet = sheet.worksheet_by_title(leader_board_wsheet_name)
        lb_wsheet.set_dataframe(leading_record_df.reset_index(), start=(0, 0), fit=True)
        lb_wsheet.adjust_column_width(start=3, end=3, pixel_size=100)
        lb_wsheet.adjust_column_width(start=4, end=4, pixel_size=200)
        lb_wsheet.adjust_column_width(start=5, end=5, pixel_size=300)

        LOGGER.info(f"{lb_wsheet.url}")
        LOGGER.info(f"{lb_wsheet.id}")

        self.load(
            context,
            body={
                "leader_board_wsheet_url": lb_wsheet.url,
                "leader_board_wsheet_gid": lb_wsheet.id,
                "leader_board_wsheet_title": lb_wsheet.title
            }
        )

        LOGGER.info(f"==== END {__class__.__name__} ====")


if __name__ == "__main__":
    import argparse
    from tunip.datetime_utils import KST, snapshot2dag_runid
    from tunip.service_config import get_service_config
    from tunip.yaml_loader import YamlLoader
    from inkling.utils.airflow_utils import get_my_task

    parser = argparse.ArgumentParser(description="record leaderboard of reco-v1 model")
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
        task_id="record_leaderboard_reco_v1"
    )
    task_config["is_containerized"] = False

    # $BUCKET_NAME/user/ed/dag_stat/dag_build_reco_v1_model/20231121_022237_000000
    run_snapshot_dt = args.snapshot_dt
    snapshot_dag_runid = snapshot2dag_runid(run_snapshot_dt, time_zone=KST)

    RecordLeaderboardRecoV1(
        service_config,
        task_config
    )(
        context={
            "header": {
                "dag_id": "dag_build_reco_v1_model",
                "task_id": "record_leaderboard_reco_v1",
                "run_id": f"app_main__{snapshot_dag_runid}",
                "run_snapshot_dt": run_snapshot_dt
            },
            "body": {
                "prev_dag_id": "dag_build_reco_v1_model",
                "prev_task_id": "evaluate_model_reco_v1",
                "start_snapshot_dt": run_snapshot_dt,
            }
        }
    )
