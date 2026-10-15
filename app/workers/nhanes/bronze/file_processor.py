import io, os, csv, requests
import pandas as pd
from app.workers.nhanes.bronze.base import BronzeTask
from app.utils.retry import create_retry_session

class BronzeFileProcessor(BronzeTask):
    def __init__(self):
        super().__init__()
        self.session = create_retry_session()

    def xpt_to_csv_gcs(
        self,
        xpt_url: str,
        bucket_name: str,
        xpt_blob_path: str,
        csv_blob_path: str,
        component: str,
    ):

        try:

            response = self.session.get(xpt_url, timeout=(5, 30))
            response.raise_for_status()

            content_type = response.headers.get("Content-Type", "")
            if "text/html" in content_type.lower():
                self.log_info(f"Skipping HTML page (not XPT): {xpt_url}")
                return None

            xpt_bytes = response.content

            xpt_blob = self.bucket.blob(xpt_blob_path)
            xpt_blob.upload_from_string(
                xpt_bytes,
                content_type="application/octet-stream"
            )

            self.log_info(f"XPT uploaded to GCS: gs://{self.bucket_name}/{xpt_blob_path}")

            xpt_buffer = io.BytesIO(xpt_blob.download_as_bytes())

            try:
                df = pd.read_sas(xpt_buffer, format="xport")
            except ValueError as e:
                self.log_info(f"Invalid XPORT file: {xpt_url} | {e}")
                xpt_blob.delete()
                return None

            
            self.upload_dataframe_to_gcs(df, csv_blob_path)
            # csv_blob.upload_from_string(
            #     csv_buffer.getvalue(),
            #     content_type="text/csv"
            # )

            self.log_info(f"CSV uploaded to GCS: gs://{bucket_name}/{csv_blob_path}")


            xpt_blob.delete()
            self.log_info(f"XPT deleted from GCS: gs://{bucket_name}/{xpt_blob_path}")

            return f"gs://{bucket_name}/{csv_blob_path}"

        except Exception as e:
            self.log_info(f"Failed to process XPT {xpt_url}: {e}")
            return None



    def upload_dataframe_to_gcs(self, df, gcs_path):
        try:
            # Convert DataFrame to CSV in memory
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)

            # Upload to GCS
            blob = self.bucket.blob(gcs_path)
            blob.upload_from_string(csv_buffer.getvalue(), content_type="text/csv")

            # Verify upload by checking if blob exists
            if blob.exists():
                self.log_info(f"Upload successful: {gcs_path}")
                return gcs_path
            else:
                self.log_info(f" Upload attempted but blob not found: {gcs_path}")
                return None

        except Exception as e:
            self.log_info(f" Upload failed: {e}")
            return None

    def is_file_size_allowed(self, url, max_size_gb=1):
        """
        Check remote file size using HEAD request.
        Returns True if file size <= max_size_gb, else False.
        """
        try:
            response = requests.head(url, allow_redirects=True, timeout=10)
            response.raise_for_status()

            content_length = response.headers.get("Content-Length")
            if not content_length:
                # If size unknown, allow processing (or change policy if needed)
                self.log_info(f" File size unknown, allowing download: {url}")
                return True

            size_bytes = int(content_length)
            size_gb = size_bytes / (1024 ** 3)

            if size_gb > max_size_gb:
                self.log_info(
                    f" Skipping large file ({size_gb:.2f} GB > {max_size_gb} GB): {url}"
                )
                return False

            return True

        except Exception as e:
            self.log_info(
                f" Failed to check file size, skipping for safety: {url} | {type(e).__name__}: {e}"
            )
            return False
