import json
import requests
from bs4 import BeautifulSoup
from app.database.connection import get_db_connection
from app.workers.nhanes.bronze.base import BronzeTask
from app.utils.retry import create_retry_session

# Codebook field names (used in parsing and DB mapping)
KEY_VARIABLE_NAME = "Variable Name"
KEY_SAS_LABEL = "SAS Label"
KEY_ENGLISH_TEXT = "English Text"


class BronzeCodebook(BronzeTask):

    def __init__(self):
        super().__init__()
        self.session = create_retry_session()

    def parse_codebook(self, doc_url):
        try:
            response = self.session.get(doc_url, timeout=(5, 30))

            if response.status_code == 404:
                self.log_info(f"Codebook not found (404), skipping: {doc_url}")
                return []

            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            result = []

            for block in soup.select("div.pagebreak"):
                var_info = {
                    KEY_VARIABLE_NAME: None,
                    KEY_SAS_LABEL: None,
                    KEY_ENGLISH_TEXT: None,
                    "Target": None,
                    "title": None,
                    "codevalue": []
                }

                title_tag = block.find("h3", class_="vartitle")
                if title_tag:
                    var_info["title"] = title_tag.get_text(strip=True)

                for dt, dd in zip(block.find_all("dt"), block.find_all("dd")):
                    key = dt.get_text(strip=True).replace(":", "")
                    value = dd.get_text(" ", strip=True)
                    if key in [KEY_VARIABLE_NAME, KEY_SAS_LABEL, KEY_ENGLISH_TEXT, "Target"]:
                        var_info[key] = value

                table = block.find("table", class_="values")
                if table:
                    headers = [th.get_text(strip=True) for th in table.find_all("th")]
                    for tr in table.select("tbody tr"):
                        cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
                        if len(cells) == len(headers):
                            var_info["codevalue"].append(dict(zip(headers, cells)))

                result.append(var_info)

            return result

        except requests.exceptions.RequestException as e:
            self.log_info(f"Failed to fetch codebook after retries: {doc_url} | {e}")
            return []

    def save_codebook_to_postgres(self, codebook, dataset_name, year, component):

        if not codebook:
            self.log_info(f"No codebook data for {dataset_name}")
            return

        insert_sql = """
            INSERT INTO codebook (
                dataset_name,
                dataset_year,
                variable_name,
                sas_label,
                english_text,
                target,
                title,
                codevalue,
                component
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
        """

        inserted = 0
        skipped = 0

        with get_db_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    for entry in codebook:

                        #  support BOTH formats
                        variable_name = (
                            entry.get(KEY_VARIABLE_NAME)
                            or entry.get(f"{KEY_VARIABLE_NAME}:")
                        )

                        if not variable_name:
                            skipped += 1
                            continue

                        cursor.execute(
                            insert_sql,
                            (
                                dataset_name,
                                year,
                                variable_name,
                                entry.get(KEY_SAS_LABEL) or entry.get(f"{KEY_SAS_LABEL}:"),
                                entry.get(KEY_ENGLISH_TEXT) or entry.get(f"{KEY_ENGLISH_TEXT}:"),
                                entry.get("Target") or entry.get("Target:"),
                                entry.get("title"),
                                json.dumps(entry.get("codevalue", [])),
                                component
                            )
                        )

                        inserted += 1

                conn.commit()

                self.log_info(
                    f"Codebook insert completed for {dataset_name} | "
                    f"Inserted: {inserted}, Skipped: {skipped}"
                )

            except Exception as e:
                conn.rollback()
                self.log_info(f"Error saving codebook {dataset_name}: {str(e)}")
                raise
