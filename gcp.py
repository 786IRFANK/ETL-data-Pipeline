import os
from google.cloud import storage

from app.config.settings import settings


## locally run code

BUCKET_NAME = ""
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
storage_client = storage.Client(project=settings.GCP_PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)


ADHOC_GCP_BUCKET_NAME = ""
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
adhoc_storage_client = storage.Client(project=settings.GCP_PROJECT_ID)
adhoc_bucket = adhoc_storage_client.bucket(ADHOC_GCP_BUCKET_NAME)

