from fastapi import APIRouter, Depends, HTTPException, status
from app.api.dependencies.auth import get_current_user, CurrentUser
from app.api.models.pipeline_models import ConnectorSyncRequest
from app.workers.connector_task.task import process_gcs_file_task
from typing import Annotated
from sqlalchemy import text
from app.api.models.db_models import AdhocUser
from sqlalchemy.orm import Session
from app.database.session import get_adhoc_db
from app.config.settings import settings

router = APIRouter()

MAX_FILES     = settings.MAX_FILES
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

def _filename_from_gcs_url(gcs_url: str) -> str:
    """Extract bare filename from a GCS URL."""
    return gcs_url.rstrip("/").split("/")[-1]

def _validate_file_extension(gcs_url: str) -> None:
    filename = _filename_from_gcs_url(gcs_url)
    if not filename.lower().endswith(ALLOWED_EXTS):
        raise HTTPException(
            status_code=400,
            detail=(
                f"'{filename}' has an unsupported extension. "
                f"Allowed: {', '.join(ALLOWED_EXTS)}."
            ),
        )

@router.post("/connectors/sync", status_code=status.HTTP_201_CREATED)
async def sync_connectors(
    org_id: str,
    request: ConnectorSyncRequest,
    db: Annotated[Session, Depends(get_adhoc_db)],
    current_user: CurrentUser = Depends(get_current_user),
):
    

    try:
        uploaded_by  = current_user.uid            # Firebase UID
        workspace_id = current_user.workspace_id   # custom claim

    # org_id must be valid and active to proceed; also confirms DB connectivity
        _get_active_org(org_id, db)

        # Schema convention: "upload_data_<org_id>"
        schema = f"upload_data_{org_id}".lower()
        _assert_schema_exists(schema, db)

        # max files per request to prevent abuse; individual file size is checked in the loop
        if len(request.gcs_urls) > MAX_FILES:
            raise HTTPException(
                status_code=400,
                detail=f"Maximum {MAX_FILES} files per request.",
            )

        tasks = []

        for gcs_url in request.gcs_urls:
            _validate_file_extension(gcs_url)

        for gcs_url in request.gcs_urls:

            task = process_gcs_file_task.delay(
                gcs_url=gcs_url,
                org_id=org_id,
                workspace_id=workspace_id,
                uploaded_by=uploaded_by,
            )

            tasks.append({
                "gcs_url": gcs_url,
                "task_id": task.id,
                "status": "queued"
            })

        return {
            "message": "Connector sync tasks queued successfully",
            "total_files": len(tasks),
            "tasks": tasks
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Error queuing connector sync tasks: {str(e)}"
        )


@router.get(
    "/connectors/sync/status/{task_id}",
    responses={401: {"description": "Authentication required."}},
)
async def sync_status(
    task_id: str,
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
):
    """Poll the processing status of a single queued task."""
    task = process_gcs_file_task.AsyncResult(task_id)
 
    state_map = {
        "PENDING": "pending",
        "STARTED": "in_progress",
        "SUCCESS": "completed",
    }
 
    status_str = state_map.get(task.state, "failed")
    payload: dict = {"task_id": task_id, "status": status_str}
 
    if task.state == "SUCCESS":
        payload["result"] = task.result
    elif status_str == "failed":
        payload["error"] = str(task.result)
 
    return payload