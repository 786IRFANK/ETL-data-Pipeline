import hashlib
import logging
import re
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
import pandas as pd
from google.api_core.exceptions import NotFound
from sqlalchemy.exc import SQLAlchemyError

from app.database.database_pool import get_sqlalchemy_engine
from app.workers.adhoc.pgservices import (
    get_existing_registry_entry,
    insert_upload_registry,
    create_or_upsert_table
)
from app.workers.adhoc.file_ingestion_utils import (
    make_schema_name,
    _validate_dataframe,
)

from celery_app import celery_app
from app.config.settings import settings
from gcp import adhoc_storage_client, storage_client
from requests.exceptions import ConnectionError as RequestsConnectionError, Timeout as RequestsTimeout
from sqlalchemy.exc import OperationalError
from google.api_core.exceptions import (
    DeadlineExceeded,
    GatewayTimeout,
    InternalServerError,
    ServiceUnavailable,
)

logger = logging.getLogger(__name__)

def _is_transient_error(exc: Exception) -> bool:
    return isinstance(
        exc,
        (
            OperationalError,
            RequestsConnectionError,
            RequestsTimeout,
            ServiceUnavailable,
            InternalServerError,
            DeadlineExceeded,
            GatewayTimeout,
        ),
    )



def _parse_gcs_path(gcs_url: str) -> tuple[str, str]:
    if not gcs_url:
        raise ValueError("GCS URL cannot be empty")
    if not isinstance(gcs_url, str):
        raise TypeError("GCS URL must be a string")

    if gcs_url.startswith("gs://"):
        path = gcs_url[len("gs://"):]
    elif gcs_url.startswith("https://storage.googleapis.com/"):
        path = gcs_url[len("https://storage.googleapis.com/"):]
    else:
        raise ValueError(
            f"Unsupported GCS URL format: '{gcs_url}'. "
            "Expected 'gs://' or 'https://storage.googleapis.com/' prefix."
        )

    parts = path.split("/", 1)
    if len(parts) != 2 or not parts[1]:
        raise ValueError(f"Could not extract blob path from GCS URL: '{gcs_url}'")

    bucket_name, blob_name = parts
    if not bucket_name or not blob_name:
        raise ValueError("Bucket name and blob name cannot be empty")

    return bucket_name, blob_name


def _download_bytes_from_gcs(gcs_url: str) -> tuple[bytes, str]:
    bucket_name, blob_name = _parse_gcs_path(gcs_url)

    lower = blob_name.lower()
    if not (lower.endswith(".csv") or lower.endswith(".xlsx") or lower.endswith(".xls")):
        raise ValueError(
            f"Unsupported file type for '{blob_name}'. "
            "Only .csv / .xlsx / .xls are accepted."
        )

    # bucket = storage_client.bucket(settings.GCP_BUCKET_NAME)
    bucket = storage_client.bucket(settings.GCP_BUCKET_NAME)
    blob = bucket.blob(blob_name)

    logger.info("Downloading gs://%s/%s …", bucket_name, blob_name)
    file_bytes = blob.download_as_bytes()

    if not file_bytes:
        raise ValueError(f"Downloaded file is empty: gs://{bucket_name}/{blob_name}")

    return file_bytes, blob_name


def _delete_from_gcs(gcs_url: str) -> None:
    """Delete a blob from GCS; logs a warning if already gone (idempotent)."""
    bucket_name, blob_name = _parse_gcs_path(gcs_url)

    #bucket = storage_client.bucket(settings.GCP_BUCKET_NAME)
    bucket = storage_client.bucket(settings.GCP_BUCKET_NAME)

    blob = bucket.blob(blob_name)

    try:
        blob.delete()
        logger.info("Deleted gs://%s/%s", bucket_name, blob_name)
    except NotFound:
        logger.warning(
            "Blob already gone (skipping delete): gs://%s/%s", bucket_name, blob_name
        )


def _get_file_hash(file_bytes: bytes) -> str:
    """Return the SHA-256 hex digest of file_bytes."""
    return hashlib.sha256(file_bytes).hexdigest()



def parse_file(file_bytes: bytes, filename: str) -> list[tuple[str | None, pd.DataFrame]]:

    lower_name = filename.lower()

    if lower_name.endswith(".csv"):
        df = pd.read_csv(BytesIO(file_bytes))
        logger.info("Parsed CSV '%s': %d rows × %d cols", filename, len(df), len(df.columns))
        # FIX: wrap in list-of-tuple, not a bare DataFrame
        return [(None, df)]

    if lower_name.endswith((".xlsx", ".xls")):
        xl = pd.ExcelFile(BytesIO(file_bytes))
        sheets = []
        for sheet_name in xl.sheet_names:
            df = xl.parse(sheet_name)
            logger.info(
                "Parsed sheet '%s' in '%s': %d rows × %d cols",
                sheet_name, filename, len(df), len(df.columns),
            )
            sheets.append((sheet_name, df))
        return sheets

    raise ValueError(
        f"'{filename}' has unsupported extension. "
        "Only .csv, .xlsx, .xls are allowed."
    )


def _slugify(value: str) -> str:
    """Lowercase, replace non-alphanumerics with underscores, collapse runs."""
    value = value.lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value


def _truncate_parts(parts: list[str], max_len: int = 63) -> str:
    name = "_".join(parts)
    return name[:max_len]


def make_table_name(
    org_id: str,
    filename_stem: str,
    sheet_name: str | None = None,
) -> str:
    parts = []

    fname = _slugify(filename_stem)[:15]
    parts.append(fname)

    if sheet_name:
        sname = _slugify(sheet_name)[:12]
        parts.append(sname)

    org = _slugify(org_id)[:8]
    parts.append(org)

    table_name = "_".join(parts)
    return _truncate_parts([table_name])



def duplicate_outcome(
    existing_rows: list,
    file_hash: str,
    filename: str,
    org_id: str,
) -> tuple[dict | None, bool]:
    org_rows = [r for r in existing_rows if r.org_id == org_id]
    same_hash_rows = [r for r in org_rows if r.file_hash == file_hash]
    same_filename_rows = [r for r in org_rows if r.file_name == filename]

    # Exact duplicate: same name + same content
    same_hash_same_name = [r for r in same_hash_rows if r.file_name == filename]
    if same_hash_same_name:
        return {
            "status": "skipped",
            "reason": "Exact duplicate: same filename and same content",
            "filename": filename,
            "existing_table": same_hash_same_name[0].table_name,
        }, False

    # Same content, different name
    same_hash_diff_name = [r for r in same_hash_rows if r.file_name != filename]
    if same_hash_diff_name:
        return {
            "status": "skipped",
            "reason": "Duplicate content: different filename but identical content",
            "filename": filename,
            "existing_file": same_hash_diff_name[0].file_name,
            "existing_table": same_hash_diff_name[0].table_name,
        }, False

    # Same name, new content → overwrite
    overwrite_existing = bool(same_filename_rows)
    if overwrite_existing:
        logger.info(
            "Same filename '%s' with new content — will overwrite existing table.",
            filename,
        )

    return None, overwrite_existing




def process_sheets(
    *,
    sheets: list[tuple[str | None, pd.DataFrame]],
    filename: str,
    filename_stem: str,
    org_id: str,
    workspace_id: str,
    uploaded_by: str,
    gcs_uri: str,
    file_hash: str,
    overwrite_existing: bool,
) -> tuple[list, list]:
    schema = make_schema_name(org_id)
    table_results = []
    skipped_sheets = []

    for sheet_name, df in sheets:   # FIX: sheets is now always list[(str|None, df)]
        source_label = f"{filename}:{sheet_name}" if sheet_name else filename

        try:
            _validate_dataframe(df, source_label)
        except ValueError as sheet_exc:
            logger.warning("Skipping sheet — %s", sheet_exc)
            skipped_sheets.append({"sheet": sheet_name, "reason": str(sheet_exc)})
            continue

        pg_table = make_table_name(
            org_id=org_id,
            filename_stem=filename_stem,
            sheet_name=sheet_name,
        )

        result = create_or_upsert_table(
            df=df,
            schema=schema,
            table_name=pg_table,
            overwrite=overwrite_existing,
        )

        insert_upload_registry(
            org_id=org_id,
            workspace_id=workspace_id,
            uploaded_by=uploaded_by,
            table_name=result["table_name"],
            file_name=filename,
            source_path=gcs_uri,
            column_schema={"current": result["current_columns"]},
            row_count=result["rows_inserted"],
            job_status="OVERWRITTEN" if overwrite_existing else "COMPLETED",
            file_hash=file_hash,
        )
        table_results.append(result)

    return table_results, skipped_sheets




@celery_app.task(
    bind=True,                          # FIX: bind=True so `self` is available for retry
    autoretry_for=(OperationalError,),
    retry_backoff=5,
    retry_backoff_max=60,
    retry_kwargs={"max_retries": 3},
    acks_late=True,
    reject_on_worker_lost=True,
)
def process_gcs_file_task(
    self,                               # FIX: `self` added (required when bind=True)
    gcs_url: str,
    org_id: str,
    workspace_id: str,
    uploaded_by: str,
) -> dict:

    file_hash: str | None = None
    filename: str | None = None

    try:
        # ── 1. Download ───────────────────────────────────────────────────
        file_bytes, blob_name = _download_bytes_from_gcs(gcs_url)
        filename = blob_name.split("/")[-1]
        filename_stem = Path(filename).stem

        # ── 2. Hash ───────────────────────────────────────────────────────
        file_hash = _get_file_hash(file_bytes)
        logger.info("SHA-256 %s  →  %s", filename, file_hash)

        # ── 3. Duplicate detection ────────────────────────────────────────
        existing_rows = get_existing_registry_entry(file_hash, filename, org_id)
        duplicate_response, overwrite_existing = duplicate_outcome(
            existing_rows=existing_rows,
            file_hash=file_hash,
            filename=filename,
            org_id=org_id,
        )
        if duplicate_response:
            return duplicate_response

        # ── 4. Parse ──────────────────────────────────────────────────────
        # FIX: removed the redundant _parse_bytes_to_df call; parse_file handles
        # both CSV and Excel and now always returns list[(sheet_name, df)].
        sheets = parse_file(file_bytes, filename)

        if not sheets:
            raise ValueError(f"File produced no sheets after parsing: '{gcs_url}'")

        # ── 5. Process sheets ─────────────────────────────────────────────
        table_results, skipped_sheets = process_sheets(
            sheets=sheets,
            filename=filename,
            filename_stem=filename_stem,
            org_id=org_id,
            workspace_id=workspace_id,
            uploaded_by=uploaded_by,
            gcs_uri=gcs_url,
            file_hash=file_hash,
            overwrite_existing=overwrite_existing,
        )

        # ── 6. Delete from GCS after successful ingestion ─────────────────
        _delete_from_gcs(gcs_url)

        # If every sheet was skipped, treat the whole file as a failure
        if not table_results:
            skipped_summary = "; ".join(
                f"{s['sheet']}: {s['reason']}" for s in skipped_sheets
            )
            raise ValueError(
                f"'{filename}' has no usable sheets. Skipped: {skipped_summary}"
            )

        # ── Success response ───────────────────────────────────────────────
        return {
            "status": "success",
            "filename": filename,
            "org_id": org_id,
            "workspace_id": workspace_id,
            "uploaded_by": uploaded_by,
            "gcs_uri": gcs_url,             # FIX: was referencing undefined `gcs_uri`
            "tables": table_results,
            "skipped_sheets": skipped_sheets,
            "uploaded_at": datetime.now(timezone.utc).isoformat(),
        }

    except ValueError as exc:
        # Permanent failure — log to registry and do not retry
        logger.warning("Permanent failure for '%s': %s", filename, exc)

        insert_upload_registry(
            org_id=org_id,
            workspace_id=workspace_id,
            uploaded_by=uploaded_by,
            table_name=None,
            file_name=filename,
            source_path=gcs_url,
            column_schema={},
            row_count=0,
            job_status="FAILED",
            error_message=str(exc),
            file_hash=file_hash,
        )

        raise

    except Exception as exc:
        # Transient failure — retry with exponential back-off if applicable
        if _is_transient_error(exc):
            next_retry = self.request.retries + 1
            if next_retry <= self.max_retries:
                raise self.retry(exc=exc, countdown=5 * (2 ** (next_retry - 1)))
        raise