import base64
from typing import Annotated, List
from fastapi import APIRouter, Depends, File, Form, HTTPException, UploadFile
from sqlalchemy import text
from sqlalchemy.orm import Session
from app.api.dependencies.auth import get_current_user, CurrentUser
from app.api.models.db_models import AdhocUser
from app.database.session import get_adhoc_db
from app.workers.adhoc.tasks import upload_and_ingest_file
from app.config.settings import settings
router = APIRouter()


MAX_FILES     = settings.MAX_FILES
MAX_FILE_SIZE = settings.MAX_FILE_SIZE
ALLOWED_EXTS  = settings.ALLOWED_EXTS


def _get_active_org(org_id: str, db: Session) -> AdhocUser:

    user = db.query(AdhocUser).filter(AdhocUser.org_id == org_id).first()
    if not user:
        raise HTTPException(
            status_code=404,
            detail=f"Organisation '{org_id}' is not registered.",
        )
    if user.status != "active":
        raise HTTPException(
            status_code=403,
            detail=f"Organisation '{org_id}' is not active.",
        )
    return user


def _assert_schema_exists(schema: str, db: Session) -> None:
    
    row = db.execute(
        text(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = :schema"
        ),
        {"schema": schema},
    ).fetchone()
    if not row:
        raise HTTPException(
            status_code=404,
            detail=f"Schema '{schema}' does not exist in the database.",
        )


def simple_validate_file(file_bytes: bytes, filename: str):
    if len(file_bytes) == 0:
        raise ValueError("Uploaded file is empty")

    if not filename.lower().endswith((".csv", ".xlsx", ".xls")):
        raise ValueError("Unsupported file extension")

    return True
    
@router.post(
    "/",
    responses={
        400: {"description": "Invalid request or file validation failed."},
        403: {"description": "Organisation is not active."},
        404: {"description": "Organisation or schema not found."},
    },
)
async def upload_files(
    org_id: Annotated[str, Form(...)],
    files:  Annotated[List[UploadFile], File(...)],
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
    db: Annotated[Session, Depends(get_adhoc_db)],
):

    # authentication middleware should ensure these are always present:
    uploaded_by  = current_user.uid            # Firebase UID
    workspace_id = current_user.workspace_id   # custom claim

   # org_id must be valid and active to proceed; also confirms DB connectivity
    _get_active_org(org_id, db)

    # Schema convention: "upload_data_<org_id>"
    schema = f"upload_data_{org_id}".lower()
    _assert_schema_exists(schema, db)

    # max files per request to prevent abuse; individual file size is checked in the loop
    if len(files) > MAX_FILES:
        raise HTTPException(
            status_code=400,
            detail=f"Maximum {MAX_FILES} files per request.",
        )

    dispatched = []

    for upload in files:
        content = await upload.read()
        await upload.close()

        if not upload.filename:
            raise HTTPException(status_code=400, detail="A file is missing its filename.")

        if len(content) > MAX_FILE_SIZE:
            raise HTTPException(
                status_code=400,
                detail=f"'{upload.filename}' exceeds the 200 MB limit.",
            )

        if not upload.filename.lower().endswith(ALLOWED_EXTS):
            raise HTTPException(
                status_code=400,
                detail=f"'{upload.filename}' must be .csv, .xlsx, or .xls.",
            )

        # validate file content early to fail fast on bad inputs; this also protects downstream processing and provides quicker feedback to users
        try:
            simple_validate_file(content, upload.filename)
        except ValueError as e:
            raise HTTPException(
                status_code=400,
                detail=f"'{upload.filename}' validation failed: {str(e)}"
            )
        # All validations passed; queue the file for processing by Celery
        task = upload_and_ingest_file.delay(
            file_b64=base64.b64encode(content).decode("utf-8"),
            filename=upload.filename,
            org_id=org_id,
            workspace_id=workspace_id,
            uploaded_by=uploaded_by,
        )

        dispatched.append(
            {
                "filename":   upload.filename,
                "size_bytes": len(content),
                "task_id":    task.id,
            }
        )

    return {
        "message":           "Files queued for upload and ingestion.",
        "org_id":            org_id,
        "workspace_id":      workspace_id,
        "uploaded_by":       uploaded_by,
        "files":             dispatched,
    }



@router.get(
    "/status/{task_id}",
    responses={
        401: {"description": "Authentication is required."},
    },
)
async def upload_status(
    task_id: str,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
):
    
    task = upload_and_ingest_file.AsyncResult(task_id)

    state_map = {
        "PENDING": "pending",
        "STARTED": "in_progress",
        "SUCCESS": "completed",
    }

    status = state_map.get(task.state, "failed")
    payload: dict = {"task_id": task_id, "status": status}

    if task.state == "SUCCESS":
        payload["result"] = task.result
    elif status == "failed":
        payload["error"] = str(task.result)

    return payload


