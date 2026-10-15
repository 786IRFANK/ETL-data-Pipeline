from celery import Task
from sqlalchemy import text
from app.utils.logger import get_logger
from app.config.settings import settings
from gcp import storage_client
from io import StringIO
import pandas as pd
from app.database.database_pool import get_sqlalchemy_engine

logger = get_logger(__name__)


class ForianClaimsSilverTask(Task):
    abstract = True

    def __init__(self):
        super().__init__()
        self.storage_service = storage_client.bucket(
            settings.GCP_BUCKET_NAME
        )
        self._logger = get_logger(self.__class__.__name__)
        self.schema_name = "forian_claims_silver"
        self.landing_forian_claims_bucket_path = settings.FORIAN_CLAIMS_LANDING_PATH
        self.forian_claims_bucket_path = settings.FORIAN_CLAIMS_BRONZE_PATH

        self.engine = get_sqlalchemy_engine()  # Get shared engine
        # Schema must be pre-created by DBA - do not auto-create
        # self.create_silver_schema()

    def log_info(self, msg: str):
        self._logger.info(msg)

    def create_silver_schema(self):
        """Create silver schema if it doesn't exist"""
        try:
            with self.engine.begin() as conn:  # Use begin() for transactions
                conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {self.schema_name}"))
            self.log_info(f"Schema {self.schema_name} ready")
        except Exception:
            self._logger.exception(
                f"Failed to create silver schema '{self.schema_name}'"
            )
            raise

    def forian_claims_list_bronze_files(self):
        #List all blobs under the given folder prefix
        blobs = self.storage_service.list_blobs(prefix=self.forian_claims_bucket_path)
        file_list = [blob.name for blob in blobs if not blob.name.endswith('/')]
        return file_list

    def forian_claims_find_codebook_blob(self, filename: str = "codebook.csv"):
        # Find codebook.csv under landing path
        blobs = self.storage_service.list_blobs(prefix=self.landing_forian_claims_bucket_path)
        for blob in blobs:
            if blob.name.lower().endswith(f"/{filename}"):
                return blob.name
        return None

    def open_csv_stream_from_gcs(self, blob_path):
        blob = self.storage_service.blob(blob_path)
        return blob.open("rb")

