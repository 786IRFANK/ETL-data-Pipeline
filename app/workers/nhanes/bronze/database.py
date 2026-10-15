# app/workers/nhanes/bronze/database.py
import re
from psycopg2.errors import UndefinedTable

from app.database.connection import get_db_connection
from app.config.category_mapping import DOMAIN_CATEGORIES
from app.workers.nhanes.bronze.base import BronzeTask
from app.utils.logger import get_logger

logger = get_logger(__name__)

class BronzeDatabase(BronzeTask):

    def ensure_bronze_tables_exist(self):
        """
        Verify that Bronze core tables exist.
        Must be created via Alembic.
        """
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                try:
                    cursor.execute("SELECT 1 FROM data_check LIMIT 1")
                    cursor.execute("SELECT 1 FROM codebook LIMIT 1")
                    logger.info("Bronze tables verified successfully")
                except UndefinedTable as e:
                    raise RuntimeError(
                        "Bronze core tables are missing. "
                        "Run Alembic migrations before pipeline execution."
                    ) from e

    def categorize_file(self, filename):
        """Categorize file based on filename pattern"""
        for domain, categories in DOMAIN_CATEGORIES.items():
            for category, pattern in categories.items():
                if re.match(pattern, filename):
                    return domain, category
        return "UNCATEGORIZED", "Unknown"

    def record_exists(self, filename_code, component):
        """Check if record exists in data_check table"""
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT COUNT(*) FROM data_check WHERE filename_code=%s AND component=%s",
                    (filename_code, component)
                )
                count = cursor.fetchone()[0]
                return count > 0

    def insert_data_check_log(self, filename_code, year, component, domain, category):
        """Insert data check log entry (with automatic transaction handling)"""
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO data_check (filename_code, year, component, domain, category)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                """, (filename_code, year, component, domain, category))

                rows_affected = cursor.rowcount
                if rows_affected > 0:
                    logger.info(f"Inserted data check log for {filename_code}")
                else:
                    logger.info(f"Data check log already exists for {filename_code}")
                
                return True
