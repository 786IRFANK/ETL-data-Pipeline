from dataclasses import dataclass
from typing import Annotated
from fastapi import HTTPException, Header, status
from app.api.models.pipeline_models import CreateUserRequest, UpdateStatusRequest
from app.config.settings import settings

# Uncomment in production:
# import firebase_admin
# from firebase_admin import auth as firebase_auth

@dataclass
class CurrentUser:
    uid:          str
    workspace_id: str
    role:         str = "user"


def get_current_user(
    x_dev_user: Annotated[str | None, Header(alias="X-Dev-User")] = None,
    x_dev_workspace: Annotated[str | None, Header(alias="X-Dev-Workspace")] = None,
    x_dev_role: Annotated[str | None, Header(alias="X-Dev-Role")] = None,
) -> CurrentUser:

    # ── PRODUCTION (uncomment) ───────────────────────────────────────────────
    # if not credentials:
    #     raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
    #                         detail="Missing Bearer token.")
    # try:
    #     decoded = firebase_auth.verify_id_token(credentials.credentials)
    # except Exception as exc:
    #     raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED,
    #                         detail=f"Invalid token: {exc}") from exc
    # return CurrentUser(
    #     uid=decoded["uid"],
    #     workspace_id=decoded.get("workspace_id", "default"),
    #     role=decoded.get("role", "user"),
    # )

    # ── DEVELOPMENT STUB ─────────────────────────────────────────────────────
    env = (settings.ENVIRONMENT or "").lower()
    if env in {"development", "dev", "local"}:
        if not settings.AUTH_USER or not settings.AUTH_WORKSPACE:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Dev auth is not configured. Set AUTH_USER and AUTH_WORKSPACE.",
            )
        if not x_dev_user or not x_dev_workspace:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Missing dev auth headers (X-Dev-User, X-Dev-Workspace).",
            )
        if x_dev_user != settings.AUTH_USER or x_dev_workspace != settings.AUTH_WORKSPACE:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Invalid dev auth headers.",
            )
        return CurrentUser(
            uid=x_dev_user,
            workspace_id=x_dev_workspace,
            role=x_dev_role or settings.AUTH_ROLE or "user",
        )

    raise HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Authentication is required.",
    )
