
from typing import Annotated
from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from app.api.dependencies.auth import get_current_user, CurrentUser
from app.api.models.db_models import AdhocUser
from app.api.models.pipeline_models import CreateUserRequest, UpdateStatusRequest
from app.database.session import get_adhoc_db

router = APIRouter()

@router.post(
    "/create",
    status_code=201,
    responses={
        401: {"description": "Authentication is required."},
        409: {"description": "Organisation is already registered."},
    },
)
def create_user(
    request: CreateUserRequest,
    db: Annotated[Session, Depends(get_adhoc_db)],
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
):

    existing = db.query(AdhocUser).filter(AdhocUser.org_id == request.org_id).first()
    if existing:
        raise HTTPException(
            status_code=409,
            detail=f"Organisation '{request.org_id}' is already registered.",
        )

    user = AdhocUser(username=request.username, org_id=request.org_id)
    db.add(user)
    db.commit()
    db.refresh(user)

    return {
        "message": "Organisation registered successfully.",
        "data": {
            "id":         user.id,
            "username":   user.username,
            "org_id":     user.org_id,
            "status":     user.status,
            "created_at": user.created_at.isoformat(),
        },
    }


@router.put(
    "/update-status",
    responses={
        401: {"description": "Authentication is required."},
        404: {"description": "Organisation not found."},
    },
)
def update_user_status(
    request: UpdateStatusRequest,
    db: Annotated[Session, Depends(get_adhoc_db)],
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
):
    
    user = (
        db.query(AdhocUser)
        .filter(
            AdhocUser.username == request.username,
            AdhocUser.org_id   == request.org_id,
        )
        .first()
    )

    if not user:
        raise HTTPException(status_code=404, detail="Organisation not found.")

    user.status = "inactive" if user.status == "active" else "active"
    db.commit()
    db.refresh(user)

    return {
        "message": "Status updated successfully.",
        "data": {
            "username": user.username,
            "org_id":   user.org_id,
            "status":   user.status,
        },
    }



@router.get(
    "/{org_id}",
    responses={
        401: {"description": "Authentication is required."},
        404: {"description": "Organisation not found."},
    },
)
def get_user(
    org_id: str,
    db: Annotated[Session, Depends(get_adhoc_db)],
    current_user: Annotated[CurrentUser, Depends(get_current_user)],
):

    user = db.query(AdhocUser).filter(AdhocUser.org_id == org_id).first()
    if not user:
        raise HTTPException(
            status_code=404,
            detail=f"Organisation '{org_id}' not found.",
        )
    return {
        "id":         user.id,
        "username":   user.username,
        "org_id":     user.org_id,
        "status":     user.status,
        "created_at": user.created_at.isoformat(),
        "updated_at": user.updated_at.isoformat(),
    }
