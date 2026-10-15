
import pandas as pd
import numpy as np
from app.database.database_pool import get_sqlalchemy_engine
from app.database.connection import get_db_connection
from app.workers.nhanes.gold.database import GoldDatabase


class GoldTransformations(GoldDatabase):
    """Transformation operations for Gold layer using SQL pushdown"""

    def get_table_full_name(self, short_name):
        """Convert short table name to full table name with prefix."""
        try:
            if not short_name or not isinstance(short_name, str):
                raise ValueError(f"Invalid short name: {short_name}")
            
            return f"{self.table_prefix}{short_name.strip()}"
        except Exception as e:
            self.log_info(f"Error getting full table name: {e}")
            raise

    def build_merge_query(self, table_names):
        """
        Build SQL query to merge tables using FULL OUTER JOIN.
        This is the KEY optimization - merge happens in database, not in pandas.
        """
        valid_tables = []
        status = {}
        
        # Validate tables
        for short in table_names:
            full = self.get_table_full_name(short)
            
            if not self.table_exists(full):
                status[short] = "NOT_FOUND"
                self.log_info(f"   Table not found: {full}")
                continue
            
            if not self.has_seqn_column(full):
                status[short] = "NO_SEQN"
                self.log_info(f"   No SEQN column: {full}")
                continue
            
            valid_tables.append((short, full))
            status[short] = "SUCCESS"
        
        if not valid_tables:
            self.log_info("  No valid tables to merge")
            return None, status
        
        # Build the merge query
        query_parts = []
        
        for idx, (short, full) in enumerate(valid_tables):
            # Get columns for this table
            columns = self.get_table_columns(full)
            
            if idx == 0:
                # First table - select all columns, normalize seqn to SEQN
                col_list = []
                for col_name, col_type in columns:
                    if col_name.lower() == 'seqn':
                        col_list.append(f'"{col_name}" AS "SEQN"')
                    else:
                        col_list.append(f'"{col_name}"')
                
                query_parts.append(f"""
                    SELECT {', '.join(col_list)}
                    FROM {self.source_schema}."{full}"
                """)
            else:
                # Subsequent tables - handle column conflicts by renaming
                base_alias = f"t{idx}"
                col_list = [f'{base_alias}."SEQN"']
                
                for col_name, col_type in columns:
                    if col_name.lower() == 'seqn':
                        continue  # Skip SEQN, we'll use it for join
                    
                    # Rename column to avoid conflicts
                    new_name = f"{col_name}_{short}"
                    col_list.append(f'{base_alias}."{col_name}" AS "{new_name}"')
                
                # Build the join
                prev_alias = "t0" if idx == 1 else "merged"
                
                join_query = f"""
                    SELECT {', '.join([f'{prev_alias}.*'] + col_list)}
                    FROM ({''.join(query_parts)}) AS {prev_alias}
                    FULL OUTER JOIN (
                        SELECT 
                            "{columns[0][0]}" AS "SEQN",
                            {', '.join([f'"{c[0]}"' for c in columns if c[0].lower() != 'seqn'])}
                        FROM {self.source_schema}."{full}"
                    ) AS {base_alias}
                    ON {prev_alias}."SEQN" = {base_alias}."SEQN"
                """
                
                query_parts = [join_query]
        
        final_query = ''.join(query_parts)
        
        self.log_info(f"  Built merge query for {len(valid_tables)} tables")
        return final_query, status

    def build_optimized_merge_query(self, table_names):
        """
        Build an optimized SQL merge query using CTEs and COALESCE.
        Handles column conflicts automatically and efficiently.
        """
        valid_tables = []
        status = {}
        
        # Validate tables first
        for short in table_names:
            full = self.get_table_full_name(short)
            
            if not self.table_exists(full):
                status[short] = "NOT_FOUND"
                self.log_info(f"   Table not found: {full}")
                continue
            
            if not self.has_seqn_column(full):
                status[short] = "NO_SEQN"
                self.log_info(f"   No SEQN column: {full}")
                continue
            
            valid_tables.append((short, full))
            status[short] = "SUCCESS"
        
        if not valid_tables:
            return None, status
        
        # Build CTEs for each table
        cte_parts = []
        all_columns = {}  # Track all unique columns across tables
        
        for short, full in valid_tables:
            columns = self.get_table_columns(full)
            
            # Normalize SEQN column
            col_selects = []
            for col_name, col_type in columns:
                if col_name.lower() == 'seqn':
                    col_selects.append(f'"{col_name}" AS seqn')
                else:
                    # Track column with source table
                    if col_name not in all_columns:
                        all_columns[col_name] = []
                    all_columns[col_name].append(short)
                    col_selects.append(f'"{col_name}"')
            
            cte_parts.append(f"""
                {short}_cte AS (
                    SELECT {', '.join(col_selects)}
                    FROM {self.source_schema}."{full}"
                )
            """)
        
        # Build the main SELECT with FULL OUTER JOINs
        if len(valid_tables) == 1:
            # Single table - just select from CTE
            short, _ = valid_tables[0]
            query = f"""
                WITH {cte_parts[0]}
                SELECT * FROM {short}_cte
            """
        else:
            # Multiple tables - build joins
            first_short, _ = valid_tables[0]

            # Start with first table
            join_query = f"{first_short}_cte"

            # Track all previous table seqn columns for COALESCE
            previous_seqns = [f'{first_short}_cte.seqn']

            # Add subsequent tables - use COALESCE to chain through accumulated results
            for idx in range(1, len(valid_tables)):
                short, _ = valid_tables[idx]

                # Join on COALESCE of all previous seqns to handle sparse overlaps
                coalesce_expr = f"COALESCE({', '.join(previous_seqns)})"

                join_query = f"""
                    {join_query}
                    FULL OUTER JOIN {short}_cte
                    ON {coalesce_expr} = {short}_cte.seqn
                """

                # Add this table's seqn for next iteration
                previous_seqns.append(f'{short}_cte.seqn')
            
            # Build column list for final SELECT
            final_columns = ['COALESCE(' + ', '.join([f'{s}_cte.seqn' for s, _ in valid_tables]) + ') AS "SEQN"']
            
            for col_name, sources in all_columns.items():
                if len(sources) == 1:
                    # No conflict - use column as-is
                    short = sources[0]
                    final_columns.append(f'{short}_cte."{col_name}"')
                else:
                    # Conflict - rename with suffix
                    for short in sources:
                        final_columns.append(f'{short}_cte."{col_name}" AS "{col_name}_{short}"')
            
            query = f"""
                WITH {', '.join(cte_parts)}
                SELECT {', '.join(final_columns)}
                FROM {join_query}
            """
        
        self.log_info(f"   Built optimized merge query for {len(valid_tables)} tables")
        return query, status

    def merge_tables_sql(self, table_names):
        """
        Merge tables using pure SQL - NO pandas loading.
        Returns the SQL query and status, not a DataFrame.
        """
        self.log_info(f"  Building SQL merge for {len(table_names)} tables...")
        
        # Use optimized query builder
        merge_query, status = self.build_optimized_merge_query(table_names)
        
        if merge_query is None:
            self.log_info(" Failed to build merge query")
            return None, status
        
        # Validate query returns data
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute(f"SELECT COUNT(*) FROM ({merge_query}) AS count_check")
                    row_count = cursor.fetchone()[0]
                    
                    if row_count == 0:
                        self.log_info("  Merge query returns 0 rows")
                        return None, status
                    
                    self.log_info(f"  Merge query validated: {row_count} rows")
        except Exception as e:
            self.log_info(f"  Error validating merge query: {e}")
            raise
        
        return merge_query, status

    def process_merge_config(self, merge_config):
        """
        Process the entire merge configuration using SQL pushdown.
        MAJOR CHANGE: We create tables directly from SQL queries, never loading into pandas.
        """
        # Input validation
        if not merge_config:
            self.log_info("Empty merge configuration provided")
            return {}
        
        if not isinstance(merge_config, dict):
            raise TypeError("merge_config must be a dictionary")
        
        self.log_info("=" * 70)
        self.log_info("NHANES TABLE MERGER - SQL PUSHDOWN MODE")
        self.log_info("=" * 70)
        
        results = {}
        
        for output_table, input_tables in merge_config.items():
            try:
                if not output_table or not isinstance(output_table, str):
                    self.log_info(f"Invalid output table name: {output_table}, skipping")
                    continue
                
                if not input_tables:
                    self.log_info(f"No input tables for '{output_table}', skipping")
                    results[output_table] = {
                        "status": "NO_INPUT_TABLES",
                        "merge_status": {},
                        "new_files": {}
                    }
                    continue
                
                self.log_info(f"\n{'='*70}")
                self.log_info(f"Processing: {output_table}")
                self.log_info(f"Input tables: {input_tables}")
                self.log_info(f"{'='*70}\n")

                # Check which tables need processing
                missing_tables = self.get_missing_gold_tables(
                    table_names=input_tables,
                    source_component=output_table
                )

                if not missing_tables:
                    self.log_info("All source tables already processed. Skipping.")
                    continue
                
                # Build SQL merge query (NO pandas loading!)
                merge_query, status = self.merge_tables_sql(input_tables)
                
                if merge_query is None:
                    self.log_info(f" No valid merge query for '{output_table}'")
                    results[output_table] = {
                        "status": "NO_VALID_TABLES",
                        "merge_status": status,
                        "new_files": {}
                    }
                    continue
                
                # Create gold table directly from SQL query
                self.log_info(f"Creating table '{output_table}' from SQL query...")
                table_stats = self.create_gold_table_from_query(output_table, merge_query)
                
                # Record success in tracking table
                for t in missing_tables:
                    self.insert_gold_record(t, output_table)
                
                results[output_table] = {
                    "status": "SUCCESS",
                    "rows": table_stats["rows"],
                    "columns": table_stats["columns"],
                    "merge_status": status,
                    "new_files": {}
                }
                
            except Exception as e:
                self.log_info(f" Error processing '{output_table}': {e}")
                results[output_table] = {
                    "status": "ERROR",
                    "error": str(e),
                    "merge_status": {},
                    "new_files": {}
                }
        
        return results
 
    def print_category_summary(self):
        """Print summary of successfully tracked categories"""
        try:
            with get_db_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("""
                        SELECT 
                            table_name,
                            source_component,
                            COUNT(DISTINCT category) AS category_count
                        FROM public.data_check_gold
                        WHERE status = 'SUCCESS'
                        GROUP BY table_name, source_component
                        ORDER BY table_name, source_component
                    """)
                    rows = cursor.fetchall()
                    columns = [desc[0] for desc in cursor.description]

            df = pd.DataFrame(rows, columns=columns)

            if not df.empty:
                self.log_info("\n" + "=" * 70)
                self.log_info("CATEGORY TRACKING SUMMARY")
                self.log_info("=" * 70)
                self.log_info(f"\n{df.to_string(index=False)}\n")
                self.log_info(
                    f"Total tracked categories: {df['category_count'].sum()}"
                )
            else:
                self.log_info("No categories tracked yet.")

        except Exception as e:
            self.log_error(f"Error printing category summary: {e}", exc_info=True)