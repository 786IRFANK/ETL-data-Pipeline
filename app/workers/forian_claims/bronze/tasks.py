from datetime import datetime, timezone

from celery_app import celery_app
from app.workers.forian_claims.bronze.base import ForianClaimsBronzeTask
from app.service.redis.job_service import RedisService
from app.utils.logger import get_logger

logger = get_logger(__name__, component="forian")

@celery_app.task(name="forian_claims_pipeline_task", bind=True, base=ForianClaimsBronzeTask)
def forian_claims_pipeline_task(self, params: dict,job_id: str):
    try:
        task_id = self.request.id

        logger.info("Forian claims pipeline task started")
        RedisService.update_job_status(job_id, "FORIAN CLAIMS - Bronze Task Started")

        # Get scheduled time if available
        if "scheduled_for" in params:
            logger.info("Task scheduled_for", scheduled_for=params["scheduled_for"])

        # Your actual pipeline logic goes here
        landing_files = self.forian_claims_list_landing_files()
        logger.info(f"Found {len(landing_files)} files in landing zone")
        if not landing_files:
            logger.info("No files found in landing zone to process.")
            return {
                "status": "no_files",   
                "task_id": task_id,
                "message": "No files found in landing zone to process.",
            }
        for file in landing_files:
            logger.info(f"Processing file: {file}")
            self.upload_to_bronze(file)

            logger.info(f"Processed file: {file}")
        
        RedisService.update_job_status(job_id, "FORIAN CLAIMS - Bronze Task completed")
        result = {
            "status": "success",
            "task_id": task_id,
            "executed_at": datetime.now(timezone.utc).isoformat(),
            # "params": params,
            "message": "Forian claims pipeline completed successfully",
        }

        logger.info("Forian claims pipeline task completed", task_id=task_id)

        return result

    except Exception as exc:  
        logger.exception("Forian claims pipeline task failed", task_id=self.request.id, error=str(exc))
        raise