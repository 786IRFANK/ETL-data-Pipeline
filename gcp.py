import os
from google.cloud import storage

from app.config.settings import settings

# Use bucket name from settings (set via GCP_BUCKET_NAME env var)
# On VM: uses the VM's service account
# In Cloud Run: uses the service's identity
# Locally: uses `gcloud auth application-default login`
# BUCKET_NAME = settings.GCP_BUCKET_NAME
# ADHOC_GCP_BUCKET_NAME = settings.ADHOC_GCP_BUCKET_NAME
# Initialize GCS client using ADC (Application Default Credentials)
# storage_client = storage.Client(project=settings.GCP_PROJECT_ID)
# bucket = storage_client.bucket(BUCKET_NAME)

# adhoc_storage_client = storage.Client(project=settings.GCP_PROJECT_ID)
# adhoc_bucket = adhoc_storage_client.bucket(ADHOC_GCP_BUCKET_NAME)


## locally run code

BUCKET_NAME = ""
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
storage_client = storage.Client(project=settings.GCP_PROJECT_ID)
bucket = storage_client.bucket(BUCKET_NAME)


ADHOC_GCP_BUCKET_NAME = ""
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = ""
adhoc_storage_client = storage.Client(project=settings.GCP_PROJECT_ID)
adhoc_bucket = adhoc_storage_client.bucket(ADHOC_GCP_BUCKET_NAME)

