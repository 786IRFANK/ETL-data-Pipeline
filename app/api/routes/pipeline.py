"""
Pipeline API Routes
"""
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException
from app.api.models.pipeline_models import (
    PipelineStartRequest,
    PipelineStartResponse,
    DeleteResponse,
    PipelineStatus
)
from app.service.redis.job_service import RedisService
from app.pipeline.registry import PIPELINE_REGISTRY


router = APIRouter()
redis_service = RedisService()

@router.post(
    "/start",
    response_model=PipelineStartResponse,
    responses={
        400: {"description": "Pipeline not found. The given pipeline_code is not registered."},
    },
)
async def start_pipeline(request: PipelineStartRequest):

    pipeline_code = request.pipeline_code.strip().lower()

    pipeline = PIPELINE_REGISTRY.get(pipeline_code)
    if not pipeline:
        raise HTTPException(status_code=400, detail=f"Pipeline '{pipeline_code}' not found")

    if request.run_type.value == "scheduled" and request.schedule_at:
        result = pipeline.schedule(request.schedule_at, request.params)
        message = "Pipeline scheduled successfully"
        pipline_status = PipelineStatus.SCHEDULED.value
    else:
        result = pipeline.start(request.params)
        message = "Pipeline started successfully"
        pipline_status = PipelineStatus.STARTED.value

    if isinstance(result, (list, tuple)):
        job_id = result[0]
        task_id = result[1] if len(result) > 1 else None
    else:
        job_id = result
        task_id = None


    return PipelineStartResponse(
        task_id=task_id or job_id,
        pipeline_code=pipeline_code,
        run_type=request.run_type.value,
        status=pipline_status,
        message=message,
        created_at=datetime.now(timezone.utc)
    )

@router.get(
    "/job/{job_id}",
    responses={
        404: {"description": "Job not found. No job exists with the given job_id."},
        500: {"description": "Internal server error while fetching job metadata."},
    },
)
async def get_job_detail(job_id: str):
    try:
        job_detail = redis_service.get_job_metadata(job_id)


        if not job_detail:
            raise HTTPException(status_code=404, detail=f"Job {job_id} not found")

        return job_detail
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/jobs/{job_id}",
    response_model=DeleteResponse,
    responses={
        404: {"description": "Job not found. No job exists with the given job_id."},
        500: {"description": "Internal server error while deleting the job."},
    },
)
async def delete_job(job_id: str):
    try:
        result = redis_service.delete_job(job_id)
        
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["message"])
        
        return DeleteResponse(
            job_id=job_id,
            status="revoked",
            message=result["message"]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.delete(
    "/tasks/{task_id}",
    response_model=DeleteResponse,
    responses={
        404: {"description": "Task not found. No task exists with the given task_id."},
        500: {"description": "Internal server error while revoking the task."},
    },
)
async def delete_task(task_id: str):
    """
    Delete/revoke a specific task
    """
    try:
        result = redis_service.delete_task(task_id)
        
        if not result["success"]:
            raise HTTPException(status_code=404, detail=result["message"])
        
        return DeleteResponse(
            job_id=task_id,
            status="revoked",
            message=result["message"]
        )
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


