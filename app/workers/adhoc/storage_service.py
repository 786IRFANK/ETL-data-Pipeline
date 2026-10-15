
import os
from google.cloud import storage
from gcp import adhoc_storage_client
from google.oauth2 import service_account
from app.config.settings import settings

def upload_bytes_to_gcp(file_bytes: bytes, destination_blob_name: str,content_type: str = "application/octet-stream"):


    bucket = adhoc_storage_client.bucket(settings.ADHOC_GCP_BUCKET_NAME)

    blob = bucket.blob(destination_blob_name)

    blob.upload_from_string(file_bytes,content_type=content_type)

    return f"gs://{settings.ADHOC_GCP_BUCKET_NAME}/{destination_blob_name}"
