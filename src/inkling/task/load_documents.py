from datetime import datetime
from pathlib import Path

from tunip.env import NAUTS_HOME
from tunip.datetime_utils import DATETIME_DT_FORMAT, dag_runid2snapshot, to_sql_strftime
from tunip.path.lake.document import LakeDocumentSourcePath
from tunip.snapshot_utils import snapshot_now

from inkling import LOGGER
from inkling.utils.bigquery_utils import BigQuery2PandasDataFrame


class LoadDocuments:
    def __init__(self, service_config, task_config):
        self.service_config = service_config
        self.task_config = task_config
        self.source_type = self.task_config['ingress']['source_type']

        self.start_date = self.task_config['ingress']['start_date']
        self.end_date = self.task_config['ingress']['end_date']

        self.batch_size = 20000

        self.username = service_config.username

        self.gcs_keypath = str(Path(NAUTS_HOME) / 'resources' / f"{service_config.config.get('gcs.project_id')}.json")

        doc_nautspath = LakeDocumentSourcePath(
            user_name=service_config.username,
            schema_type=task_config['ingress']['schema_type'],
            source_type=task_config['ingress']['source_type']
        )
        self.doc_path = f"{service_config.filesystem_prefix}{str(doc_nautspath)}"


    def __call__(self, context):

        LOGGER.info(f'==== START {__class__.__name__} ====')
        
        LOGGER.info(self.task_config['ingress']['source_type'])

        run_id = context['dag_run'].run_id
        run_snapshot_dt = dag_runid2snapshot(run_id)

        try:
            dagrun_conf_bigquery = context['dag_run'].conf['bigquery']
        except KeyError:
            dagrun_conf_bigquery = None

        if dagrun_conf_bigquery:
            query = dagrun_conf_bigquery

            LOGGER.info(f"We got the BigQuery from airflow REST API to trigger dagrun: {dagrun_conf_bigquery}")
        else:
            query = self.task_config['ingress'][self.source_type]
            query = query.replace(
                'CREATED_AT_START', to_sql_strftime(datetime.strptime(self.start_date, DATETIME_DT_FORMAT))
            ).replace(
                'CREATED_AT_END', to_sql_strftime(datetime.strptime(self.end_date, DATETIME_DT_FORMAT))
            )

        LOGGER.info(f"querying for {self.source_type}: {query}")

        self.doc_path = self.doc_path + "/" + (run_snapshot_dt if run_snapshot_dt else snapshot_now())
        LOGGER.info(f"Load documents to the following path: {self.doc_path}")

        bq2pdf = BigQuery2PandasDataFrame(self.service_config, query, self.doc_path)
        _ = bq2pdf.save_and_load()

        LOGGER.info(f'==== END {__class__.__name__} ====')

