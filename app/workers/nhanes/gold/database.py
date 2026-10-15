import pandas as pd
import numpy as np
from psycopg2.extras import execute_values
from psycopg2 import Error as Psycopg2Error
from app.workers.nhanes.gold.base import GoldTask
from app.database.connection import get_db_connection
from app.database.database_pool import get_sqlalchemy_engine


class GoldDatabase(GoldTask):
    """Database operations for Gold layer with SQL pushdown optimization"""
    def __init__(self):
        super().__init__()
        self._engine = get_sqlalchemy_engine()

    def _validate_table_name(self, table_name):
        """Validate table name to prevent SQL injection"""
        if not table_name:
            raise ValueError("Table name cannot be empty")
        
        if not isinstance(table_name, str):
            raise TypeError(f"Table name must be string, got {type(table_name)}")
        
        # Check for suspicious characters
        invalid_chars = [';', '--', '/*', '*/', 'DROP', 'DELETE', 'TRUNCATE']
        table_upper = table_name.upper()
        
        for char in invalid_chars:
            if char in table_upper:
                raise ValueError(f"Invalid table name: contains '{char}'")
        
        return table_name.strip()

    def _validate_schema_name(self, schema_name):
        """Validate schema name"""
        if not schema_name:
            raise ValueError("Schema name cannot be empty")
        
        if not isinstance(schema_name, str):
            raise TypeError(f"Schema name must be string, got {type(schema_name)}")
        
        return schema_name.strip()

    def table_exists(self, table_name):
        """Check if a table exists in the source schema"""
        try:
            validated_table = self._validate_table_name(table_name)
            validated_schema = self._validate_schema_name(self.source_schema)
            
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.tables
                            WHERE table_schema = %s
                            AND table_name = %s
                        )
                    """, (validated_schema, validated_table))
                    return cursor.fetchone()[0]

        except Exception as e:
            self.log_info(f"Unexpected error in table_exists: {e}")
            return False

    def has_seqn_column(self, table_name):
        """Check if table has SEQN column"""
        try:
            validated_table = self._validate_table_name(table_name)
            validated_schema = self._validate_schema_name(self.source_schema)
            
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT EXISTS (
                            SELECT 1 FROM information_schema.columns
                            WHERE table_schema = %s
                            AND table_name = %s
                            AND LOWER(column_name) = 'seqn'
                        )
                    """, (validated_schema, validated_table))
                    result = cursor.fetchone()
                    return result[0] if result else False

        except Exception as e:
            self.log_info(f"Unexpected error in has_seqn_column: {e}")
            return False

    def get_table_columns(self, table_name, schema=None):
        """Get all column names for a table"""
        try:
            validated_table = self._validate_table_name(table_name)
            validated_schema = self._validate_schema_name(schema or self.source_schema)
            
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = %s
                        AND table_name = %s
                        ORDER BY ordinal_position
                    """, (validated_schema, validated_table))
                    return [(row[0], row[1]) for row in cursor.fetchall()]

        except Exception as e:
            self.log_info(f"Error getting table columns: {e}")
            return []

    def get_missing_gold_tables(self, table_names, source_component, category="GOLD"):
        if not table_names:
            return []

        try:
            validated_component = self._validate_table_name(source_component)
            validated_category = self._validate_table_name(category) if category else "GOLD"

            validated_tables = []
            for table in table_names:
                try:
                    validated_tables.append(self._validate_table_name(table))
                except Exception as e:
                    self.log_info(f"Skipping invalid table name '{table}': {e}")

            if not validated_tables:
                return []

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT table_name
                        FROM public.data_check_gold
                        WHERE table_name = ANY(%s)
                        AND source_component = %s
                        AND category = %s
                    """, (
                        validated_tables,
                        validated_component,
                        validated_category
                    ))

                    existing_tables = {row[0] for row in cursor.fetchall()}

            missing_tables = [t for t in validated_tables if t not in existing_tables]

            self.log_info(
                f"Missing gold tables: {missing_tables} "
                f"for {validated_component}/{validated_category}"
            )

            return missing_tables

        except Exception as e:
            self.log_info(f"Error getting missing gold tables: {e}")
            return []

    def insert_gold_record(self, table_name, source_component="NHANES", category="GOLD"):
        try:
            validated_table = self._validate_table_name(table_name)
            validated_component = self._validate_table_name(source_component)
            validated_category = self._validate_table_name(category) if category else "GOLD"

            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                     INSERT INTO public.data_check_gold
                        (table_name, source_component, category, status)
                        VALUES (%s, %s, %s, 'SUCCESS')
                    """, (
                        validated_table,
                        validated_component,
                        validated_category
                    ))

                conn.commit()

            self.log_info(
                f"Gold record ensured: {validated_table} "
                f"({validated_component}/{validated_category})"
            )

        except Exception as e:
            self.log_info(f"Error inserting gold record for '{table_name}': {e}")
            raise

    def get_table_row_count(self, table_name, schema=None):
        """Get row count for a table using SQL"""
        try:
            validated_table = self._validate_table_name(table_name)
            validated_schema = self._validate_schema_name(schema or self.target_schema)
            
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM {validated_schema}."{validated_table}"
                    """)
                    return cursor.fetchone()[0]
        except Exception as e:
            self.log_info(f"Error getting row count: {e}")
            return 0

    def create_gold_table_from_query(self, table_name, select_query):
        """
        Create or replace gold table directly from a SQL query.
        This is the KEY optimization - we never load data into pandas.
        """
        try:
            validated_table = self._validate_table_name(table_name)
            
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Drop existing table if it exists
                    cursor.execute(f"""
                        DROP TABLE IF EXISTS {self.target_schema}."{validated_table}"
                    """)
                    
                    # Create new table from query
                    cursor.execute(f"""
                        CREATE TABLE {self.target_schema}."{validated_table}" AS
                        {select_query}
                    """)
                    
                    # Get row and column count
                    cursor.execute(f"""
                        SELECT COUNT(*) FROM {self.target_schema}."{validated_table}"
                    """)
                    row_count = cursor.fetchone()[0]
                    
                    cursor.execute("""
                        SELECT COUNT(*)
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                    """, (self.target_schema, validated_table))
                    col_count = cursor.fetchone()[0]
                    
                conn.commit()
            
            self.log_info(
                f"✓ Gold table created: {self.target_schema}.{validated_table} "
                f"({row_count} rows, {col_count} columns)"
            )
            
            return {"rows": row_count, "columns": col_count}

        except Exception as e:
            self.log_info(f"Error creating table '{table_name}': {e}")
            raise

    def evolve_gold_table_schema(self, table_name, source_query):
        """
        Evolve existing gold table schema by adding new columns.
        Insert data using SQL INSERT...SELECT for efficiency.
        """
        try:
            validated_table = self._validate_table_name(table_name)
            
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    # Get columns from source query
                    cursor.execute(f"""
                        SELECT * FROM ({source_query}) AS src LIMIT 0
                    """)
                    source_columns = {desc[0] for desc in cursor.description}
                    
                    # Get existing table columns
                    cursor.execute("""
                        SELECT column_name, data_type
                        FROM information_schema.columns
                        WHERE table_schema = %s AND table_name = %s
                    """, (self.target_schema, validated_table))
                    
                    existing_cols = {row[0]: row[1] for row in cursor.fetchall()}
                    
                    # Find new columns
                    new_columns = source_columns - set(existing_cols.keys())
                    
                    if new_columns:
                        # Add new columns (default to TEXT type)
                        for col in new_columns:
                            cursor.execute(f"""
                                ALTER TABLE {self.target_schema}."{validated_table}"
                                ADD COLUMN "{col}" TEXT
                            """)
                        
                        self.log_info(f"Added {len(new_columns)} new columns")
                    
                    # Insert data using SQL
                    cursor.execute(f"""
                        INSERT INTO {self.target_schema}."{validated_table}"
                        SELECT * FROM ({source_query}) AS src
                    """)
                    
                conn.commit()
            
            return True

        except Exception as e:
            self.log_info(f"Error evolving table schema: {e}")
            raise