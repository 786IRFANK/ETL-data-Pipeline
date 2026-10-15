import io
from pathlib import Path
import pandas as pd
import re
from enum import Enum
from pandas.errors import EmptyDataError

_PG_MAX = 63

def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:

    # Drop fully-null columns
    df = df.dropna(axis=1, how="all")

    # Drop unnamed columns that are still fully null after above
    unnamed_null_cols = [
        col for col in df.columns
        if str(col).strip().startswith("Unnamed:") and df[col].isna().all()
    ]
    df = df.drop(columns=unnamed_null_cols)

    # Strip whitespace from string column names
    df.columns = [
        col.strip() if isinstance(col, str) else col
        for col in df.columns
    ]

    return df


def parse_csv_file(file_bytes: bytes, filename: str):

    try:
        df = pd.read_csv(io.BytesIO(file_bytes))
    except EmptyDataError:
        raise ValueError(f"'{filename}' is empty - no columns found.")

    df = _clean_dataframe(df)
    if df.empty or len(df.columns) == 0:
        raise ValueError(f"'{filename}' contains no usable data.")

    return [("sheet1", df)]


def parse_excel_file(file_bytes: bytes, filename: str):

    try:
        xls = pd.ExcelFile(io.BytesIO(file_bytes))
    except Exception as exc:
        raise ValueError(f"'{filename}' could not be opened: {exc}")

    sheets = []
    skipped = []

    for sheet_name in xls.sheet_names:
        df = xls.parse(sheet_name)
        df = _clean_dataframe(df)

        if df.empty or len(df.columns) == 0:
            skipped.append(sheet_name)
            continue

        sheets.append((sheet_name, df))

    if not sheets:
        skipped_list = ", ".join(skipped)
        raise ValueError(
            f"'{filename}' has no usable sheets. "
            f"Skipped (empty): {skipped_list}"
        )

    return sheets


def parse_file(file_bytes: bytes, filename: str):

    lower_name = filename.lower()
    if lower_name.endswith('.csv'):
        return parse_csv_file(file_bytes, filename)
    if lower_name.endswith(('.xlsx', '.xls')):
        return parse_excel_file(file_bytes, filename)

    raise ValueError(
        f"'{filename}' has unsupported extension. "
        f"Only .csv, .xlsx, .xls are allowed."
    )


def _slugify(text: str) -> str:
    
    text = text.lower().strip()
    text = re.sub(r"[^a-z0-9]+", "_", text)
    text = re.sub(r"_+", "_", text)
    return text.strip("_") or "unnamed"


def _truncate_parts(parts: list[str], max_len: int = _PG_MAX, sep: str = "__") -> str:

    full = sep.join(parts)
    if len(full) <= max_len:
        return full

    # Hard truncate last resort – keeps prefix + sheet intact
    overhead = len(sep) * (len(parts) - 1)
    budget = max_len - overhead
    per_part = max(4, budget // len(parts))
    truncated = [p[:per_part] for p in parts]
    return sep.join(truncated)[:max_len]


def make_schema_name(org_id: str) -> str:
    """org_id  →  safe PostgreSQL schema name."""
    return f"upload_data_{org_id}".lower()


def make_table_name(
    org_id: str,
    filename_stem: str,
    sheet_name: str | None = None,
) -> str:

    parts = []

    # filename
    fname = _slugify(filename_stem)[:15]
    parts.append(fname)

    # sheet name (only for excel)
    if sheet_name:
        sname = _slugify(sheet_name)[:12]
        parts.append(sname)

    # org name/id
    org = _slugify(org_id)[:8]
    parts.append(org)

    table_name = "_".join(parts)

    return _truncate_parts([table_name])



def resolve_versioned_name(
    base_name: str,
    existing_tables: set[str],
) -> str:

    if base_name not in existing_tables:
        return base_name
    for v in range(2, 100):
        candidate = f"{base_name}_v{v}"[:_PG_MAX]
        if candidate not in existing_tables:
            return candidate
    raise ValueError(f"Cannot find a free versioned name for '{base_name}'")

def _validate_dataframe(df: pd.DataFrame, source_name: str) -> bool:

    if df is None or df.empty:
        raise ValueError(f"'{source_name}' is empty — no data to ingest.")

    if df.shape[1] == 0:
        raise ValueError(f"'{source_name}' has no valid columns after cleanup.")

    rows, cols = df.shape
    if rows < 2:
        raise ValueError(f"'{source_name}' must contain at least one data row.")

    if cols < 2:
        raise ValueError(f"'{source_name}' must contain at least two columns.")

    total_cells = rows * cols
    non_empty = df.count().sum()
    if total_cells > 0 and (non_empty / total_cells) < 0.15:
        raise ValueError(f"'{source_name}' structure invalid (data too sparse).")

    return True
