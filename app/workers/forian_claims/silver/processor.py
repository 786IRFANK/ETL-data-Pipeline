import re
import pandas as pd
from sqlalchemy import (text,MetaData,Table,Column,Integer,BigInteger,Float,String,Date,DateTime,Text,Boolean,Numeric,
)

class ForianClaimsSilverProcessor():

    def __init__(self, log_info=None):
        self._log_info = log_info

    def log_info(self, msg: str):
        if self._log_info:
            self._log_info(msg)

    def pandas_dtype_to_sql(self, dtype, col_name=None):
        dtype_str = str(dtype)

        if "datetime" in dtype_str:
            return DateTime
        if col_name and col_name in ["ndc", "prescriber_npi"]:
            return BigInteger
        if "int" in dtype_str:
            return Integer
        if "float" in dtype_str:
            return Float
        return String(255)

    def normalize_header(self, name: str) -> str:
        cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", str(name).strip().lower())
        cleaned = re.sub(r"_+", "_", cleaned).strip("_")
        return cleaned

    def normalize_table_key(self, name: str) -> str:
        return self.normalize_header(name)

    def parse_sql_type(self, type_value, length=None, precision=None, scale=None):
        if type_value is None or (isinstance(type_value, float) and pd.isna(type_value)):
            return None

        type_str = str(type_value).strip().lower()
        if not type_str:
            return None

        match = re.match(r"([a-zA-Z_]+)\s*\(([^)]+)\)", type_str)
        base_type = type_str
        params = []
        if match:
            base_type = match.group(1).strip()
            params = [p.strip() for p in match.group(2).split(",")]

        def to_int(val):
            try:
                return int(float(val))
            except Exception:
                return None

        if base_type in ["varchar", "nvarchar", "char", "nchar", "string", "text"]:
            length_val = to_int(params[0]) if params else to_int(length)
            return String(length_val or 255)

        if base_type in ["int", "integer", "int4", "smallint", "int2"]:
            return Integer

        if base_type in ["bigint", "int8", "long"]:
            return BigInteger

        if base_type in ["float", "float4", "float8", "double", "double_precision", "real"]:
            return Float

        if base_type in ["decimal", "numeric", "number"]:
            precision_val = to_int(params[0]) if len(params) > 0 else to_int(precision)
            scale_val = to_int(params[1]) if len(params) > 1 else to_int(scale)
            if precision_val:
                return Numeric(precision_val, scale_val or 0)
            return Numeric

        if base_type in ["date"]:
            return Date

        if base_type in ["datetime", "timestamp", "timestamptz", "timestampz"]:
            return DateTime

        if base_type in ["bool", "boolean"]:
            return Boolean

        return String(255)

    def build_codebook_table_defs(self, codebook_df: pd.DataFrame):
        if codebook_df is None or codebook_df.empty:
            raise ValueError("Codebook is empty or missing.")

        codebook_df = codebook_df.copy()
        codebook_df.columns = [self.normalize_header(c) for c in codebook_df.columns]

        table_col_candidates = [
            "table_name",
            "table",
            "dataset",
            "dataset_name",
            "source_table",
            "file",
            "filename",
        ]
        column_col_candidates = [
            "column_name",
            "column",
            "variable",
            "variable_name",
            "field",
            "field_name",
        ]
        type_col_candidates = [
            "data_type",
            "datatype",
            "type",
            "sql_type",
            "db_type",
            "variable_type",
        ]
        length_col_candidates = ["length", "max_length", "size", "width", "varchar_length"]
        precision_col_candidates = ["precision"]
        scale_col_candidates = ["scale"]

        def find_col(candidates):
            for c in candidates:
                if c in codebook_df.columns:
                    return c
            return None

        table_col = find_col(table_col_candidates)
        column_col = find_col(column_col_candidates)
        type_col = find_col(type_col_candidates)
        length_col = find_col(length_col_candidates)
        precision_col = find_col(precision_col_candidates)
        scale_col = find_col(scale_col_candidates)

        if not table_col or not column_col or not type_col:
            raise ValueError(
                "Codebook must include table name, column name, and data type columns."
            )

        table_defs = {}
        for _, row in codebook_df.iterrows():
            table_name = row.get(table_col)
            column_name = row.get(column_col)
            data_type = row.get(type_col)

            if pd.isna(table_name) or pd.isna(column_name):
                continue

            table_key = self.normalize_table_key(str(table_name))
            column_name = str(column_name).strip()
            if not column_name:
                continue

            length_val = row.get(length_col) if length_col else None
            precision_val = row.get(precision_col) if precision_col else None
            scale_val = row.get(scale_col) if scale_col else None

            sql_type = self.parse_sql_type(
                data_type, length=length_val, precision=precision_val, scale=scale_val
            )
            if sql_type is None:
                sql_type = String(255)

            if table_key not in table_defs:
                table_defs[table_key] = {"columns": [], "types": {}}

            if column_name not in table_defs[table_key]["types"]:
                table_defs[table_key]["columns"].append(column_name)
                table_defs[table_key]["types"][column_name] = sql_type

        return table_defs

    def generate_table(
        self,
        table_name,
        df,
        schema_name,
        engine,
        table_defs=None,
        extra_columns=None
    ):
        metadata = MetaData(schema=schema_name)
        table_key = self.normalize_table_key(table_name)
        if table_defs and table_key in table_defs:
            columns = [
                Column(col, table_defs[table_key]["types"][col])
                for col in table_defs[table_key]["columns"]
            ]
        else:
            columns = [
                Column(col, self.pandas_dtype_to_sql(dtype, col))
                for col, dtype in df.dtypes.items()
            ]

        if extra_columns:
            existing_cols = {col.name for col in columns}
            for col_name, col_type in extra_columns:
                if col_name not in existing_cols:
                    columns.append(Column(col_name, col_type))

        table = Table(table_name, metadata, *columns)
        metadata.create_all(engine)
        return table

    def align_chunk_to_codebook(self, chunk_df, expected_columns):
        for col in expected_columns:
            if col not in chunk_df.columns:
                chunk_df[col] = pd.NA

        extra_columns = [c for c in chunk_df.columns if c not in expected_columns]
        if extra_columns:
            self.log_info(f"Dropping unexpected columns: {extra_columns}")
            chunk_df = chunk_df.drop(columns=extra_columns)

        chunk_df = chunk_df[expected_columns]
        return chunk_df

    def create_codebook_table(self, codebook_df, schema_name, engine):
        if codebook_df is None or codebook_df.empty:
            raise ValueError("Codebook is empty or missing.")

        codebook_df = codebook_df.copy()
        codebook_df.columns = [self.normalize_header(c) for c in codebook_df.columns]

        codebook_df.to_sql(
            name="codebook",
            con=engine,
            schema=schema_name,
            if_exists="replace",
            index=False,
            chunksize=1000,
            method="multi",
        )
    
    def truncate_table_safe(self, table_name, schema_name, engine):
        if not re.match(r"^[a-zA-Z_]+$", table_name):
            raise ValueError(f"Invalid table name: {table_name}")

        full_table_name = f"{schema_name}.{table_name}"
        with engine.begin() as conn:
            conn.execute(text(f"TRUNCATE TABLE {full_table_name}"))
    
    def load_chunk_to_postgres(self, chunk_df, table_name, schema_name, engine):

        if "date_of_service" in chunk_df.columns:
            chunk_df["date_of_service"] = pd.to_datetime(
                chunk_df["date_of_service"], errors="coerce"
            )

        if "processed_at" in chunk_df.columns:
            chunk_df["processed_at"] = pd.to_datetime(
                chunk_df["processed_at"], errors="coerce"
            )

        chunk_df.to_sql(
            name=table_name,
            con=engine,
            schema=schema_name,
            if_exists="append",
            index=False,
            chunksize=1000,
            method="multi"
        )
