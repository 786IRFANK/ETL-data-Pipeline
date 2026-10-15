import os
import pandas as pd
from datetime import datetime, timezone
from celery_app import celery_app
from app.service.redis.job_service import RedisService
from app.workers.forian_claims.silver.base import ForianClaimsSilverTask
from app.workers.forian_claims.silver.processor import ForianClaimsSilverProcessor
from sqlalchemy import DateTime, String


@celery_app.task(name="forian_claims_silver_task", bind=True, base=ForianClaimsSilverTask)
def forian_claims_silver_task(self, params: dict, job_id: str):
    """Process bronze files and load into silver layer using streaming ETL"""

    RedisService.update_job_status(job_id, "FORIAN CLAIMS - Silver Task Started")

    processor = ForianClaimsSilverProcessor(log_info=self.log_info)

    codebook_blob_path = self.forian_claims_find_codebook_blob()
    if not codebook_blob_path:
        raise FileNotFoundError(
            "codebook.csv not found in landing/forian_claims. "
            "Please upload the codebook before running the silver task."
        )

    with self.open_csv_stream_from_gcs(codebook_blob_path) as cb_stream:
        codebook_df = pd.read_csv(cb_stream)

    processor.create_codebook_table(
        codebook_df,
        schema_name=self.schema_name,
        engine=self.engine
    )
    codebook_table_defs = processor.build_codebook_table_defs(codebook_df)

    file_list = self.forian_claims_list_bronze_files()
    if not file_list:
        self.log_info("No files found in bronze layer.")
        return {
            "status": "no_files",
            "job_id": job_id,
            "message": "No files found in bronze layer."
        }

    processed_files = []
    failed_files = []
    metadata_columns = ["processed_at", "data_source", "source_file"]
    metadata_sqlalchemy = [
        ("processed_at", DateTime),
        ("data_source", String(255)),
        ("source_file", String(255)),
    ]

    for file in file_list:
        try:
            self.log_info(f"Processing file: {file}")

            raw_table_name = os.path.basename(file).replace('.csv', '')
            table_name = processor.normalize_table_key(raw_table_name)

            filename = os.path.basename(file)
            if filename.lower() == "codebook.csv":
                self.log_info("Skipping codebook.csv in bronze layer.")
                continue

            table_key = processor.normalize_table_key(table_name)
            if table_key not in codebook_table_defs:
                raise ValueError(
                    f"Codebook does not contain schema for table: {table_name}"
                )

            first_chunk = True

            with self.open_csv_stream_from_gcs(file) as f:
                for chunk in pd.read_csv(f, chunksize=50000):

                    chunk["processed_at"] = datetime.now(timezone.utc)
                    chunk["data_source"] = "FORIAN_CLAIMS"
                    chunk["source_file"] = filename

                    expected_columns = (
                        codebook_table_defs[table_key]["columns"] + metadata_columns
                    )
                    chunk = processor.align_chunk_to_codebook(
                        chunk,
                        expected_columns
                    )

                    if first_chunk:
                        processor.generate_table(
                            table_name,
                            chunk,
                            schema_name=self.schema_name,
                            engine=self.engine,
                            table_defs=codebook_table_defs,
                            extra_columns=metadata_sqlalchemy
                        )
                        processor.truncate_table_safe(
                            table_name,
                            self.schema_name,
                            self.engine
                        )
                        first_chunk = False

                    processor.load_chunk_to_postgres(
                        chunk,
                        table_name,
                        schema_name=self.schema_name,
                        engine=self.engine
                    )

            processed_files.append(file)
            self.log_info(f"Successfully processed file: {file}")

        except Exception as e:
            failed_files.append(file)
            self.log_error(f"Error processing file {file}: {str(e)}")

    if failed_files:
        RedisService.update_job_status(
            job_id,
            f"FORIAN CLAIMS - Silver Task Partial Success. Failed files: {failed_files}"
        )
        status = "partial_success"
    else:
        RedisService.update_job_status(
            job_id,
            "FORIAN CLAIMS - Silver Task Completed"
        )
        status = "success"

    return {
        "status": status,
        "job_id": job_id,
        "processed_files": processed_files,
        "failed_files": failed_files,
        "file_count": len(processed_files)
    }
