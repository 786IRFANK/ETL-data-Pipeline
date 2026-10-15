import os
import pandas as pd
from io import BytesIO
from app.workers.nhanes.silver.base import SilverTask


class SilverFileProcessor(SilverTask):

    def list_gcs_csv_files(self, component):
        blobs = self.storage_service.list_blobs(prefix=f"{self.bucket_path}/CSV/{component}_Data")
        return [b.name for b in blobs if b.name.lower().endswith(".csv")]

    def read_csv_from_gcs(self, blob_path):
        blob = self.storage_service.blob(blob_path)
        return pd.read_csv(BytesIO(blob.download_as_bytes()))

    def read_csv_from_local(self, file_path):
        return pd.read_csv(file_path, low_memory=False)
