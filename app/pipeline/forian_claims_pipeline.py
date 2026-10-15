import uuid
from celery import chain
from datetime import datetime, timezone
from app.service.redis.job_service import RedisService
from app.workers.forian_claims.bronze.tasks import forian_claims_pipeline_task
from app.workers.forian_claims.silver.tasks import forian_claims_silver_task


class FORIANCLAIMSPipeline:
    def __init__(self):
        self.pipeline_code = "forian_claims"

    def start(self, params: dict) -> str:
        """Run pipeline immediately (Bronze → Silver)"""
        job_id = str(uuid.uuid4())
        params["job_id"] = job_id

        RedisService.store_job_metadata(
            job_id,
            {
                "pipeline_code": self.pipeline_code,
                "run_type": "now",
                "params": params,
                "status": "RUNNING",
            },
        )


        chain(
            forian_claims_pipeline_task.s(params,job_id),
            forian_claims_silver_task.s(job_id),
        ).apply_async()

        return job_id

    def schedule(self, schedule_at: str, params: dict) -> str:
        """Schedule pipeline """
        job_id = str(uuid.uuid4())
        params["job_id"] = job_id

        scheduled_utc = datetime.fromisoformat(
            schedule_at.replace("Z", "+00:00")
        )

        if scheduled_utc.tzinfo is None:
            scheduled_utc = scheduled_utc.replace(tzinfo=timezone.utc)

        if scheduled_utc <= datetime.now(timezone.utc):
            raise ValueError("Scheduled time must be in the future (UTC)")

        RedisService.store_job_metadata(
            job_id,
            {
                "pipeline_code": self.pipeline_code,
                "run_type": "scheduled",
                "schedule_at": schedule_at,
                "params": params,
                "status": "SCHEDULED",
            },
        )

        chain(
            forian_claims_pipeline_task.s(params, job_id),
            forian_claims_silver_task.s(job_id),
        ).apply_async(eta=scheduled_utc)

        return job_id
