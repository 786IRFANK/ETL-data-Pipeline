import base64
import mimetypes
from datetime import datetime, timezone
from pathlib import Path
import hashlib
from requests.exceptions import ConnectionError as RequestsConnectionError, Timeout as RequestsTimeout
from sqlalchemy.exc import OperationalError
from google.api_core.exceptions import (
    DeadlineExceeded,
    GatewayTimeout,
    InternalServerError,
    ServiceUnavailable,
)
from celery_app import celery_app

from app.workers.adhoc.file_ingestion_utils import (
    make_schema_name,
    make_table_name,
    parse_file,_validate_dataframe
)
from app.workers.adhoc.pgservices import (
    create_or_upsert_table,
    insert_upload_registry,
    get_existing_registry_entry
)
from app.workers.adhoc.storage_service import upload_bytes_to_gcp


def _gcs_bronze_path(org_id: str, filename: str) -> str:
    return f"{org_id}/upload_data/bronze/{filename}"


def _content_type(filename: str) -> str:
    mime, _ = mimetypes.guess_type(filename)
    return mime or "application/octet-stream"


def get_file_hash(file_bytes: bytes) -> str:

    sha256 = hashlib.sha256()
    sha256.update(file_bytes)
    return sha256.hexdigest()


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


def _retry_delay_seconds(retry_number: int) -> int:
    # retry_number is 1-based
    return {1: 5, 2: 20, 3: 60}.get(retry_number, 60)


def duplicate_outcome(existing_rows: list, file_hash: str, filename: str, org_id: str) -> tuple[dict | None, bool]:
    # Defensive org scoping
    org_rows = [r for r in existing_rows if r.org_id == org_id]
    same_hash_rows = [r for r in org_rows if r.file_hash == file_hash]
    same_filename_rows = [r for r in org_rows if r.file_name == filename]

    same_hash_same_name = [r for r in same_hash_rows if r.file_name == filename]
    if same_hash_same_name:
        return {
            "status": "skipped",
            "reason": "Exact duplicate: same filename and same content",
            "filename": filename,
            "existing_table": same_hash_same_name[0].table_name,
        }, False

    same_hash_diff_name = [r for r in same_hash_rows if r.file_name != filename]
    if same_hash_diff_name:
        return {
            "status": "skipped",
            "reason": "Duplicate content: different filename but identical content",
            "filename": filename,
            "existing_file": same_hash_diff_name[0].file_name,
            "existing_table": same_hash_diff_name[0].table_name,
        }, False

    same_name_diff_hash = [r for r in same_filename_rows if r.file_hash != file_hash]
    overwrite_existing = bool(same_filename_rows)
    if same_name_diff_hash:
        print(
            f"Same filename '{filename}' with new content detected. "
            f"Proceeding to overwrite existing table."
        )

    return None, overwrite_existing


def process_sheets(
    *,
    sheets: list,
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

    for sheet_name, df in sheets:
        source_label = f"{filename}:{sheet_name}" if sheet_name else filename

        try:
            _validate_dataframe(df, source_label)
        except ValueError as sheet_exc:
            print(f"Skipping sheet â€” {sheet_exc}")
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
    bind=True,
    name="upload_and_ingest_file",
    max_retries=3,
)
def upload_and_ingest_file(
    self,
    *,
    file_b64: str,
    filename: str,
    org_id: str,
    workspace_id: str,
    uploaded_by: str,
) -> dict:

    # Initialise these early so the except blocks can always reference them
    # safely even if an error fires before they are assigned.
    file_hash: str | None = None
    gcs_uri:   str | None = None

    try:
        # ── Decode ────────────────────────────────────────────────────────
        file_bytes    = base64.b64decode(file_b64)
        filename_stem = Path(filename).stem

        # ── Hash ──────────────────────────────────────────────────────────
        file_hash = get_file_hash(file_bytes)
        print(f"SHA256 file hash: {file_hash} for {filename}")

        # ── Duplicate detection (scoped strictly to this org) ────────────
        existing_rows = get_existing_registry_entry(file_hash, filename, org_id)
        duplicate_response, overwrite_existing = duplicate_outcome(
            existing_rows=existing_rows,
            file_hash=file_hash,
            filename=filename,
            org_id=org_id,
        )
        if duplicate_response:
            return duplicate_response

        # ── STEP 1: Parse & validate BEFORE uploading to GCS ─────────────
        # Avoids wasting a GCS upload if the file turns out to be empty/invalid.
        sheets = parse_file(file_bytes, filename)

        if not sheets:
            raise ValueError(
                f"'{filename}' produced no sheets or is completely empty."
            )

        # ── STEP 2: Upload to GCS (only after we know the file is valid) ──
        gcs_uri = upload_bytes_to_gcp(
            file_bytes=file_bytes,
            destination_blob_name=_gcs_bronze_path(org_id, filename),
            content_type=_content_type(filename),
        )

        table_results, skipped_sheets = process_sheets(
            sheets=sheets,
            filename=filename,
            filename_stem=filename_stem,
            org_id=org_id,
            workspace_id=workspace_id,
            uploaded_by=uploaded_by,
            gcs_uri=gcs_uri,
            file_hash=file_hash,
            overwrite_existing=overwrite_existing,
        )

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
            "status":       "success",
            "filename":     filename,
            "org_id":       org_id,
            "workspace_id": workspace_id,
            "uploaded_by":  uploaded_by,
            "gcs_uri":      gcs_uri,
            "tables":       table_results,
            "skipped_sheets": skipped_sheets,
            "uploaded_at":  datetime.now(timezone.utc).isoformat(),
        }

    except ValueError as exc:
        # Permanent failure — log to registry and do not retry
        print(f"Permanent failure for '{filename}': {exc}")

        insert_upload_registry(
            org_id=org_id,
            workspace_id=workspace_id,
            uploaded_by=uploaded_by,
            table_name=None,           # sentinel will be generated inside insert_upload_registry
            file_name=filename,
            source_path=gcs_uri,       # may be None if failure was before GCS step
            column_schema={},
            row_count=0,
            job_status="FAILED",
            error_message=str(exc),    # store the actual reason for failure
            file_hash=file_hash,       # safe: initialised to None at top of function
        )

        raise

    except Exception as exc:
        # Transient failure — retry with exponential back-off if applicable
        if _is_transient_error(exc):
            next_retry = self.request.retries + 1
            if next_retry <= self.max_retries:
                countdown = _retry_delay_seconds(next_retry)
                raise self.retry(exc=exc, countdown=countdown)
        raise

