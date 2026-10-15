import os, re
import pandas as pd
from datetime import datetime
from celery_app import celery_app
from app.service.redis.job_service import RedisService
from app.workers.nhanes.silver.database import SilverDatabase
from app.workers.nhanes.silver.file_processor import SilverFileProcessor
from app.workers.nhanes.silver.categorizer import SilverCategorizer


class SilverProcessor(
    SilverDatabase,
    SilverFileProcessor,
    SilverCategorizer,
):

    def process_component_silver(self, component,job_id: str):

        if not component or not isinstance(component, str) or not component.strip():
            return {"status": "skipped", "reason": "Component must be a non-empty string"}

        RedisService.update_job_status(job_id, f"SILVER - {component} Task started")
        # Get CSVs from GCP
        csv_files = self.list_gcs_csv_files(component)

        if not csv_files:
            return {"status": "skipped", "reason": f"No CSV files found for {component}"}
 
        # Categorize files
        categorized = self.categorize_and_group_files(csv_files)

        if not categorized:
            return {"status": "skipped", "reason": "No files to process"}

        # Process each category
        processed_tables = []
        total_rows_processed = 0

        for domain, categories in categorized.items():
            for category, blob_paths in categories.items():

                table_name = f"nhanes_{domain[:4]}_{category}"
                clean_table_name = re.sub(r'\W', '_', table_name).lower()

                _ = self.check_if_table_exists(clean_table_name)
                chunk_size = 10000

                for blob_path in blob_paths:
                    filename = os.path.basename(blob_path).replace(".csv", "")
                    self.log_info(f"Processing file: {filename}")

                    if self.check_if_filename_code_processed(filename, component):
                        self.log_info(f"Skipping already processed file: {filename}")
                        continue

                    try:
                        df = self.read_csv_from_gcs(blob_path)
                        df["source_file"] = filename

                        total_rows = len(df)

                        table_already_exists = self.check_if_table_exists(clean_table_name)

                        # Create table only once
                        if not table_already_exists:
                            created = self.create_table_from_df(df, clean_table_name)
                            if not created:
                                self.log_info(f"Failed to create table: {clean_table_name}")
                                continue

                        # Insert ONLY via chunk loop
                        for i in range(0, total_rows, chunk_size):
                            chunk = df.iloc[i:i + chunk_size]
                            self.insert_to_database(chunk, clean_table_name, "append")

                        # Update data_check AFTER successful insert
                        self.update_data_check(
                            filename_code=filename,
                            component=component,
                            domain=domain,
                            category=category,
                            total_rows=total_rows
                        )

                        processed_tables.append({
                            "table": clean_table_name,
                            "domain": domain,
                            "category": category,
                            "rows": total_rows,
                            "status": "created" if not table_already_exists else "appended"
                        })

                        total_rows_processed += total_rows
                        self.log_info(f"[SUCCESS] {filename} processed for table {clean_table_name}")

                    except Exception as e:
                        self.log_info(
                            f" Failed processing file {filename} "
                            f"for table {clean_table_name} {str(e)}"
                        )
                        continue

        RedisService.update_job_status(job_id, f"SILVER - {component} Task Completed")
        return {
            "status": "success",
            "component": component,
            "tables_processed": len(processed_tables),
            "total_rows": total_rows_processed,
            "details": processed_tables
        }


@celery_app.task(bind=True, base=SilverProcessor, name="nhanes.silver.demographic")
def demographic_silver_task(self,bronze_results,job_id: str):
    return self.process_component_silver("Demographics",job_id)


@celery_app.task(bind=True, base=SilverProcessor, name="nhanes.silver.dietary")
def dietary_silver_task(self,bronze_results,job_id: str):
    return self.process_component_silver("Dietary",job_id)


@celery_app.task(bind=True, base=SilverProcessor, name="nhanes.silver.examination")
def examination_silver_task(self,bronze_results,job_id: str):
    return self.process_component_silver("Examination",job_id)


@celery_app.task(bind=True, base=SilverProcessor, name="nhanes.silver.laboratory")
def laboratory_silver_task(self,bronze_results,job_id: str):
    return self.process_component_silver("Laboratory",job_id)


@celery_app.task(bind=True, base=SilverProcessor, name="nhanes.silver.question")
def question_silver_task(self,bronze_results,job_id: str):
    return self.process_component_silver("Questionnaire",job_id)