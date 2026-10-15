
"""
Pydantic Models for Pipeline API
"""
from pydantic import BaseModel, Field, ConfigDict, field_validator,validator
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum


class RunType(str, Enum):
    NOW = "now"
    SCHEDULED = "scheduled"


class PipelineStatus(str, Enum):
    PENDING = "PENDING"
    STARTED = "STARTED"
    SUCCESS = "SUCCESS"
    FAILURE = "FAILURE"
    RETRY = "RETRY"
    REVOKED = "REVOKED"
    SCHEDULED = "SCHEDULED"


class PipelineStartRequest(BaseModel):
    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})
    
    pipeline_code: str = Field(..., description="Pipeline code (e.g., 'nhanes')")
    run_type: RunType = Field(default=RunType.NOW, description="Run type: 'now' or 'scheduled'")
    schedule_at: Optional[str] = Field(None, description="Schedule time for future execution")
    params: Dict[str, Any] = Field(default_factory=dict, description="Additional parameters")


class PipelineStartResponse(BaseModel):
    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})
    
    task_id: str
    pipeline_code: str
    run_type: str
    status: str
    message: str
    created_at: datetime


class TaskInfo(BaseModel):
    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})
    
    task_id: str
    task_name: str
    status: str
    result: Optional[Any] = None
    error: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None


class JobDetailResponse(BaseModel):
    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})
    
    job_id: str
    pipeline_code: str
    status: str
    created_at: datetime
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    tasks: List[TaskInfo] = []
    error: Optional[str] = None


class JobListResponse(BaseModel):
    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})
    
    jobs: List[Dict[str, Any]]
    total: int
    page: int
    page_size: int


class DeleteResponse(BaseModel):
    model_config = ConfigDict(json_encoders={datetime: lambda v: v.isoformat()})
    
    job_id: str
    status: str
    message: str


class CreateUserRequest(BaseModel):
    username: str = Field(..., min_length=5, max_length=15)
    org_id:   str = Field(..., min_length=5, max_length=8,
                          description="Unique org identifier, max 8 chars.")

    @field_validator("org_id")
    @classmethod
    def org_id_no_spaces(cls, v: str) -> str:
        if " " in v:
            raise ValueError("org_id must not contain spaces.")
        return v.lower()


class UpdateStatusRequest(BaseModel):
    username: str = Field(..., min_length=5, max_length=15)
    org_id:   str = Field(..., min_length=5, max_length=8,)

class ConnectorSyncRequest(BaseModel):
    gcs_urls: List[str] = Field(
        ...,
        description="List of GCS file URLs ",
        example=[
            "gs://bucket-name/file1.csv",
            "gs://bucket-name/file2.xlsx"
        ]
    )

    @validator("gcs_urls")
    def validate_urls(cls, v):
        if not v:
            raise ValueError("gcs_urls list cannot be empty")

        for url in v:
            if not (
                url.startswith("gs://") or 
                url.startswith("https://storage.googleapis.com/")
            ):
                raise ValueError(f"Invalid GCS URL format: {url}")

        return v