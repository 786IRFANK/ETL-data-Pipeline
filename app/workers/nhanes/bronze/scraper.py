import os, requests, pandas as pd
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import io
from app.service.redis.job_service import RedisService
from app.workers.nhanes.bronze.database import BronzeDatabase
from app.workers.nhanes.bronze.file_processor import BronzeFileProcessor
from app.workers.nhanes.bronze.codebook import BronzeCodebook


class BronzeScraper(
    BronzeDatabase,
    BronzeFileProcessor,
    BronzeCodebook,
):

    def process_component(self, component,job_id: str):
        if not component or not isinstance(component, str):
            self.log_info(" Invalid component provided (None or empty)")
            return

        search_url = f"{self.base_url}/nchs/nhanes/search/datapage.aspx?Component={component}"

        RedisService.update_job_status(job_id, f"BRONZE - {component} Task Started")
        response = self.fetch_with_retry(search_url, max_attempts=3, initial_delay=2)  # (connect_timeout, read_timeout)
        if response is None:
            self.log_info(f" Failed to fetch search URL for component: {component}")
            return

        try:
            html = response.text
            soup = BeautifulSoup(html, "html.parser")
        except Exception as e:
            self.log_info(f" Failed to parse HTML: {type(e).__name__} - {str(e)}")
            return

        tbody = soup.find("tbody")

        if not tbody:
            self.log_info(" No datasets found (tbody missing)")
            return

        processed_files = []
        skipped_files = []

        for row in tbody.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) < 4:
                continue

            year = cells[0].get_text(strip=True)

            doc_tag = cells[2].find("a", href=True)
            xpt_tag = cells[3].find("a", href=True)

            if not doc_tag or not xpt_tag:
                continue

            doc_href = doc_tag["href"]
            xpt_href = xpt_tag["href"]

            doc = self.base_url + doc_href if doc_href.startswith("/") else doc_href
            xpt = self.base_url + xpt_href if xpt_href.startswith("/") else xpt_href


            dataset = (
                xpt.split("/")[-1]
                .replace(".XPT", "")
                .replace(".xpt", "")
            )

            csv_name = f"{dataset}.csv"

            #  Prevent re-processing
            if self.record_exists(dataset, component):
                skipped_files.append(dataset)
                continue
            
            #  NEW: Skip XPT files larger than 1 GB
            if not self.is_file_size_allowed(xpt, max_size_gb=1):
                skipped_files.append(dataset)
                self.insert_data_check_log(dataset, year, component, "Not Processed", "File Too Large")
                continue

            csv_path = self.xpt_to_csv_gcs(
                xpt_url=xpt,
                bucket_name=self.bucket_name,
                xpt_blob_path=f"{self.bucket_path}/XPT/{dataset}.XPT",
                csv_blob_path=f"{self.bucket_path}/CSV/{component}_Data/{dataset}.csv",
                component="NHANES"
            )


            self.log_info(f"\n{'='*60}")
            self.log_info(f" Processing: {dataset} ({year})")

            if not csv_path:
                self.log_info(
                    f"Skipping file (CSV not created) | Dataset={dataset}, Year={year}, URL={xpt}"
                )
                continue  # Skip to next file

            chunk_size = 10_000
            processed_rows = 0

            # Extract bucket + blob path from gs://...
            # gs://bucket-name/path/to/file.csv
            _, _, _, *blob_parts = csv_path.split("/")
            blob_path = "/".join(blob_parts)

            blob = self.bucket.blob(blob_path)

            csv_bytes = blob.download_as_bytes()
            csv_buffer = io.BytesIO(csv_bytes)

            for chunk in pd.read_csv(csv_buffer, chunksize=chunk_size):
                chunk["extracted_at"] = datetime.now(timezone.utc)
                chunk["source"] = f"NHANES_{year}"
                chunk["component"] = component

                processed_rows += len(chunk)


            codebook = self.parse_codebook(doc)
            self.log_info(f" Parsed codebook for {dataset} ({len(codebook)} variables)")
            
            self.save_codebook_to_postgres(codebook, dataset, year, component)


            domain, category = self.categorize_file(csv_name)

            self.insert_data_check_log(dataset, year, component, domain, category)

            
            processed_files.append({
                "dataset": dataset,
                "year": year,
                "rows": processed_rows,
                "local_path": csv_path,
                #"gcs_path": uploaded_path
            })
        
        RedisService.update_job_status(job_id, f"BRONZE - {component} Task Completed")
        return {
            "status": "success",
            "component": component,
            "processed": len(processed_files),
            "skipped": len(skipped_files),
            "files": processed_files
        }
