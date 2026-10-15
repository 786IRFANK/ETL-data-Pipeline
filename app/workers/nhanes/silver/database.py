# app/workers/nhanes/silver/database.py
import re

from sqlalchemy import text, inspect, bindparam
import pandas as pd
from app.workers.nhanes.silver.base import SilverTask



class SilverDatabase(SilverTask):

    def check_if_filename_code_processed(self, filename_code,component):
        with self.engine.connect() as conn:
            query = text("""
                SELECT EXISTS (
                    SELECT 1 FROM data_check
                    WHERE filename_code = :code AND silver_process = TRUE AND component = :component
                )
            """)
            return conn.execute(query, {"code": filename_code, "component": component}).scalar()

    def check_if_table_exists(self, table_name):
        """Check if table exists in schema"""
        clean = re.sub(r'\W', '_', table_name).lower()
        with self.engine.connect() as conn:
            query = text("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_schema = :schema AND table_name = :table
                )
            """)
            result = conn.execute(query, {"schema": self.schema_name, "table": clean}).scalar()
            return result

    def get_table_row_count(self, table_name):
        """Get row count for a table"""
        clean = re.sub(r'\W', '_', table_name).lower()
        with self.engine.connect() as conn:
            stmt = text("SELECT COUNT(*) FROM :schema_name.:table_name").bindparams(
                bindparam("schema_name", self.schema_name, literal_execute=True),
                bindparam("table_name", clean, literal_execute=True),
            )
            count = conn.execute(stmt).scalar()
            return count

    def insert_to_database(self, df, table_name, if_exists="append"):
        if self.engine is None:
            self.log_info("No database connection")
            return False

        try:
            clean_table_name = re.sub(r"\W", "_", table_name).lower()
            schema = self.schema_name

            inspector = inspect(self.engine)

            table_exists = inspector.has_table(clean_table_name, schema=schema)

            if not table_exists:
                self.log_info(f"Creating table {schema}.{clean_table_name}")
                self.create_table_from_df(df, clean_table_name)
            else:
                db_columns = {
                    col["name"]: col["type"]
                    for col in inspector.get_columns(clean_table_name, schema=schema)
                }

                new_columns = [c for c in df.columns if c not in db_columns]

                if new_columns:
                    self.log_info(
                        f"New columns detected in {clean_table_name}: {new_columns}"
                    )

                    with self.engine.begin() as conn:
                        for col in new_columns:
                            sql_type = self._infer_sql_type(df[col])
                            alter_sql = text(
                                f'ALTER TABLE {schema}.{clean_table_name} '
                                f'ADD COLUMN "{col}" {sql_type}'
                            )
                            conn.execute(alter_sql)

            with self.engine.begin() as conn:
                df.to_sql(
                    name=clean_table_name,
                    con=conn,         
                    schema=schema,
                    if_exists=if_exists,
                    index=False,
                    method="multi",
                    chunksize=1000
                )

            self.log_info(f"Inserted {len(df)} rows into {schema}.{clean_table_name}")
            return True

        except Exception as e:
            self.log_info(f"Insert failed for {table_name}: {e}")
            raise

    def update_data_check(self, filename_code, component, domain, category, total_rows):
        """Update data check table with processing status"""
        try:
            with self.engine.begin() as conn:
                conn.execute(
                    text("""
                        UPDATE data_check
                        SET
                            total_rows = :total_rows,
                            silver_process = TRUE,
                            last_processed_date = CURRENT_TIMESTAMP
                            WHERE filename_code = :code
                            AND component = :component
                            AND domain = :domain
                            AND category = :category
                        """), {
                            "code": filename_code,
                            "component": component,
                            "domain": domain,
                            "category": category,
                            "total_rows": total_rows
                })

            self.log_info(f"Updated data check for {filename_code}")
        except Exception as e:
            self.log_error(f"Failed to update data check for {filename_code}: {e}")
            raise

    def create_table_from_df(self, df, table_name):
        try:
            if df is None or df.empty:
                return False

            clean_table_name = re.sub(r"\W", "_", table_name).lower()

            column_defs = []
            for col in df.columns:
                sql_type = self._infer_sql_type(df[col])
                column_defs.append(f'"{col}" {sql_type}')

            columns_sql = ",\n".join(column_defs)

            create_stmt = f"""
            CREATE TABLE IF NOT EXISTS {self.schema_name}.{clean_table_name} (
                {columns_sql}
            )
            """

            with self.engine.begin() as conn:
                conn.execute(text(create_stmt))

            self.log_info(f"Table created: {self.schema_name}.{clean_table_name}")
            return True

        except Exception as e:
            self.log_info(f"Error creating table {table_name}: {e}")
            raise

    def _infer_sql_type(self, series: pd.Series) -> str:
        """
        Infer PostgreSQL column type from pandas Series
        """
        if pd.api.types.is_integer_dtype(series):
            return "BIGINT"
        if pd.api.types.is_float_dtype(series):
            return "DOUBLE PRECISION"
        if pd.api.types.is_bool_dtype(series):
            return "BOOLEAN"
        if pd.api.types.is_datetime64_any_dtype(series):
            return "TIMESTAMP"
        return "TEXT"