from celery import Task
from google.cloud import storage
from app.config.settings import settings
from app.utils.logger import get_logger
from gcp import storage_client
import time


logger = get_logger(__name__)

class ForianClaimsBronzeTask(Task):
    abstract = True

    def __init__(self):
        self.bucket = storage_client.bucket(settings.GCP_BUCKET_NAME)
        self.bucket_name = settings.GCP_BUCKET_NAME
        self._logger = get_logger(self.__class__.__name__)

        self.landing_forian_claims_bucket_path = settings.FORIAN_CLAIMS_LANDING_PATH
        self.bronze_forian_claims_bucket_path = settings.FORIAN_CLAIMS_BRONZE_PATH


    def log_info(self, msg: str):
        self._logger.info(msg)

    def forian_claims_list_landing_files(self):
        #List all blobs under the given folder prefix
        blobs = self.bucket.list_blobs(prefix=self.landing_forian_claims_bucket_path)
        
        file_list = [blob.name for blob in blobs if not blob.name.endswith('/')]
        return file_list

    def bronze_file_exists(self, bronze_blob_name):
        blob = self.bucket.blob(bronze_blob_name)
        return blob.exists()

    def upload_to_bronze(self, landing_blob_name):

        file_name = landing_blob_name.split("/")[-1]
        bronze_blob_name = f"{self.bronze_forian_claims_bucket_path}/{file_name}"

        if self.bronze_file_exists(bronze_blob_name):
            logger.info(f"File exists in bronze, replacing: {bronze_blob_name}")
        else:
            logger.info(f"File does not exist in bronze, inserting: {bronze_blob_name}")

        source_blob = self.bucket.blob(landing_blob_name)
        destination_blob = self.bucket.blob(bronze_blob_name)

        self.bucket.copy_blob(
            source_blob,
            self.bucket,
            destination_blob.name
        )

        logger.info(f"Loaded file to bronze: {bronze_blob_name}")