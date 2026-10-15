"""
GCP Cloud Storage Service
"""
from google.cloud import storage
from google.oauth2 import service_account
from typing import Optional, BinaryIO
import os
from app.config.settings import settings


class GCPStorageService:
    def __init__(self):
        """Initialize GCP Storage client"""
        if settings.GCP_CREDENTIALS_PATH and os.path.exists(settings.GCP_CREDENTIALS_PATH):
            credentials = service_account.Credentials.from_service_account_file(
                settings.GCP_CREDENTIALS_PATH
            )
            self.client = storage.Client(
                project=settings.GCP_PROJECT_ID,
                credentials=credentials
            )
        else:
            # Use default credentials
            self.client = storage.Client(project=settings.GCP_PROJECT_ID)
        
        self.bucket_name = settings.GCP_BUCKET_NAME
        self.bucket = self.client.bucket(self.bucket_name)
    
    def upload_file(
        self,
        source_file_path: str,
        destination_blob_name: str,
        content_type: Optional[str] = None
    ) -> str:

        blob = self.bucket.blob(destination_blob_name)
        
        if content_type:
            blob.upload_from_filename(source_file_path, content_type=content_type)
        else:
            blob.upload_from_filename(source_file_path)
        
        return f"gs://{self.bucket_name}/{destination_blob_name}"
    
    def upload_from_string(
        self,
        data: str,
        destination_blob_name: str,
        content_type: str = "text/plain"
    ) -> str:

        blob = self.bucket.blob(destination_blob_name)
        blob.upload_from_string(data, content_type=content_type)
        
        return f"gs://{self.bucket_name}/{destination_blob_name}"
    
    def upload_from_file_object(
        self,
        file_obj: BinaryIO,
        destination_blob_name: str,
        content_type: Optional[str] = None
    ) -> str:

        blob = self.bucket.blob(destination_blob_name)
        
        if content_type:
            blob.upload_from_file(file_obj, content_type=content_type)
        else:
            blob.upload_from_file(file_obj)
        
        return f"gs://{self.bucket_name}/{destination_blob_name}"
    
    def download_file(
        self,
        source_blob_name: str,
        destination_file_path: str
    ) -> str:

        blob = self.bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_path)
        
        return destination_file_path
    
    def download_as_string(self, source_blob_name: str) -> str:

        blob = self.bucket.blob(source_blob_name)
        return blob.download_as_text()
    
    def list_blobs(self, prefix: Optional[str] = None) -> list:

        blobs = self.client.list_blobs(self.bucket_name, prefix=prefix)
        return [blob.name for blob in blobs]
    
    def delete_blob(self, blob_name: str) -> bool:

        blob = self.bucket.blob(blob_name)
        blob.delete()
        return True
    
    def blob_exists(self, blob_name: str) -> bool:

        blob = self.bucket.blob(blob_name)
        return blob.exists()