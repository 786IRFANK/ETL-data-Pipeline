import json
import re
import pandas as pd
from sqlalchemy import text
from app.database.sqlalchemy_engine import engine_adhoc


BATCH_SIZE = 5000

def _safe_value(v):
    try:
        return None if pd.isna(v) else v
    except (TypeError, ValueError):
        return v

def list_tables(schema: str) -> set[str]:
    with engine_adhoc.begin() as conn:
        rows = conn.execute(
            text(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = :schema"
            ),
            {"schema": schema},
        ).fetchall()
    return {r[0] for r in rows}



def insert_upload_registry(
    *,
    org_id: str,
    workspace_id: str,
    uploaded_by: str,
    table_name: str | None,       # None is valid for FAILED rows
    file_name: str,
    source_path: str | None,
    column_schema: dict,
    row_count: int,
    job_status: str = "COMPLETED",
    error_message: str | None = None,
    file_hash: str | None = None,
) -> None:


    # For FAILED rows, generate a unique sentinel instead of NULL
    if table_name is None:
        hash_prefix = (file_hash or "nohash")[:16]
        # Sanitise file_name so it's safe inside the sentinel string
        safe_fname = re.sub(r"[^a-z0-9_]", "_", file_name.lower())[:40]
        table_name = f"__failed__{safe_fname}__{hash_prefix}"

    with engine_adhoc.begin() as conn:
        conn.execute(
            text("""
                INSERT INTO upload_registry
                    (org_id, workspace_id, uploaded_by, table_name,
                     file_name, source_path, column_schema, row_count,
                     job_status, error_message, file_hash)
                VALUES
                    (:org_id, :workspace_id, :uploaded_by, :table_name,
                     :file_name, :source_path, CAST(:column_schema AS jsonb),
                     :row_count, :job_status,
                     :error_message, :file_hash)
                ON CONFLICT (table_name)
                DO UPDATE SET
                    job_status    = EXCLUDED.job_status,
                    error_message = EXCLUDED.error_message,
                    row_count     = EXCLUDED.row_count,
                    file_hash     = EXCLUDED.file_hash,  
                    column_schema = jsonb_set(
                                        upload_registry.column_schema,
                                        '{current}',
                                        COALESCE(
                                            EXCLUDED.column_schema->'current',
                                            '{}'::jsonb
                                        )
                                    ),
                    updated_at    = NOW()
            """),
            {
                "org_id":         org_id,
                "workspace_id":   workspace_id,
                "uploaded_by":    uploaded_by,
                "table_name":     table_name,
                "file_name":      file_name,
                "source_path":    source_path,
                "column_schema":  json.dumps(column_schema),
                "row_count":      row_count,
                "job_status":     job_status,
                "error_message":  error_message,
                "file_hash":      file_hash,
            },
        )


def get_existing_registry_entry(file_hash: str, file_name: str, org_id: str) -> list:

    query = text("""
        SELECT
            id,
            org_id,
            table_name,
            file_name,
            file_hash
        FROM upload_registry
        WHERE org_id    = :org_id
          AND (file_hash = :file_hash OR file_name = :file_name)
          AND job_status IN ('COMPLETED', 'OVERWRITTEN')
          AND table_name NOT LIKE '__failed__%'
        ORDER BY created_at DESC
    """)

    with engine_adhoc.connect() as conn:
        rows = conn.execute(query, {
            "file_hash": file_hash,
            "file_name": file_name,
            "org_id":    org_id,
        }).fetchall()

    return rows
    

def create_or_upsert_table(
    *,
    df: pd.DataFrame,
    schema: str,
    table_name: str,
    overwrite: bool = False,
) -> dict:

    with engine_adhoc.begin() as conn:

        if overwrite:
            df.to_sql(
                name=table_name,
                con=conn,
                schema=schema,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000
            )
            return {
                "table_name": f"{schema}.{table_name}",
                "rows_inserted": len(df),
                "current_columns": list(df.columns),
                "is_new_version": False,
                "is_overwritten": True,
            }

        # find all tables with same base name
        query = text("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = :schema
            AND table_name LIKE :pattern
        """)

        result = conn.execute(
            query,
            {"schema": schema, "pattern": f"{table_name}%"}
        ).fetchall()

        existing_tables = [r[0] for r in result]

        # determine new table name
        if table_name not in existing_tables:
            final_table = table_name
        else:
            versions = [1]

            for t in existing_tables:
                match = re.match(rf"{table_name}_v(\d+)", t)
                if match:
                    versions.append(int(match.group(1)))

            next_version = max(versions) + 1
            final_table = f"{table_name}_v{next_version}"

        full_table = f"{schema}.{final_table}"

        # create table
        df.head(0).to_sql(
            name=final_table,
            con=conn,
            schema=schema,
            if_exists="fail",
            index=False
        )

        # insert data
        df.to_sql(
            name=final_table,
            con=conn,
            schema=schema,
            if_exists="append",
            index=False,
            method="multi",
            chunksize=1000
        )

    return {
        "table_name": full_table,
        "rows_inserted": len(df),
        "current_columns": list(df.columns),
        "is_new_version": final_table != table_name,
        "is_overwritten": False,
    }


def get_latest_version_number(base_table_name: str, schema: str) -> int:

    query = text("""
        SELECT table_name FROM upload_registry
        WHERE table_name = :base
           OR table_name ~ (:pattern)
    """)
    with engine_adhoc.connect() as conn:
        rows = conn.execute(query, {
            "base": f"{schema}.{base_table_name}",
            "pattern": f"^{re.escape(schema)}\\.{re.escape(base_table_name)}_v\\d+$"
        }).fetchall()

    if not rows:
        return 1  # no existing versions

    max_v = 1
    for row in rows:
        match = re.search(r"_v(\d+)$", row[0])
        if match:
            max_v = max(max_v, int(match.group(1)))
    return max_v
