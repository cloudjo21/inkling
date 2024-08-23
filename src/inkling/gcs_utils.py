import os

from pathlib import Path
from google.cloud import bigquery
from google.oauth2 import service_account

from tunip.config import Config
from tunip.env import NAUTS_HOME


# TODO deprecated
# alter to tunip.google_cloud_utils
class GcsConnector:

    def __init__(self, config: Config):
        credentials = service_account.Credentials.from_service_account_file(
            str(Path(NAUTS_HOME) / 'resources' / f"{config.get('gcs.project_id')}.json")
        )
        self.client = bigquery.Client(credentials.project_id, credentials)
