from datetime import datetime, timezone

from celery import shared_task

from app.config.merge_config import MERGE_CONFIG
from app.service.redis.job_service import RedisService
from app.utils.logger import get_logger
from app.workers.nhanes.gold.transformations import GoldTransformations

logger = get_logger(__name__)


@shared_task(bind=True, base=GoldTransformations, name="nhanes.gold.demographic")
def gold_task(self, params=None,job_id=None):

    try:
        if params is not None and not isinstance(params, dict):
            self.log_info("params must be a dictionary or None")
        
        self.log_info("=" * 70)
        self.log_info("Starting Gold Layer Processing - Demographic")
        self.log_info("=" * 70)
        
        # Validate merge config
        if not MERGE_CONFIG:
            self.log_info("MERGE_CONFIG is empty")
            return {
                "component": "demographic",
                "layer": "gold",
                "status": "skipped",
                "reason": "Empty merge configuration",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "details": {}
            }

        if not isinstance(MERGE_CONFIG, dict):
            raise TypeError("MERGE_CONFIG must be a dictionary")

        RedisService.update_job_status(job_id, "Gold Task Started")
        
        # Process merge configuration
        result = self.process_merge_config(MERGE_CONFIG)

        # Update job status
        RedisService.update_job_status(job_id, "SUCCESS")

        
        if not result:
            self.log_info("No tables were created or merged.")
            return {
                "component": "demographic",
                "layer": "gold",
                "status": "skipped",
                "reason": "No tables created or merged",
                "completed_at": datetime.now(timezone.utc).isoformat(),
                "details": {}
            }
        
        # Print summary
        #self.print_summary(result)
        
        # Print category tracking summary
        #self.print_category_summary()

        # Count successful tables
        successful_tables = [
            table for table, info in result.items()
            if isinstance(info, dict) and info.get("status") == "SUCCESS"
        ]

        
        # Count tables with errors
        failed_tables = [
            table for table, info in result.items()
            if info.get("status") == "ERROR"
        ]
        
        # Count tables with no new data
        no_data_tables = [
            table for table, info in result.items()
            if info.get("status") == "NO_NEW_DATA"
        ]
        
        self.log_info("\n" + "=" * 70)
        self.log_info(" Gold task completed!")
        self.log_info(f"  - Created: {len(successful_tables)} tables")
        self.log_info(f"  - No new data: {len(no_data_tables)} tables")
        self.log_info(f"  - Failed: {len(failed_tables)} tables")
        self.log_info("=" * 70)
        
        # Determine overall status
        if successful_tables:
            overall_status = "success"
        elif failed_tables:
            overall_status = "partial_failure"
        else:
            overall_status = "no_new_data"
        
        return {
            "component": "demographic",
            "layer": "gold",
            "status": overall_status,
            "tables_created": len(successful_tables),
           # "tables_failed": len(failed_tables),
            "tables_no_data": len(no_data_tables),
            "successful_tables": successful_tables,
            #"failed_tables": failed_tables,
            "no_data_tables": no_data_tables,
            "completed_at": datetime.now(timezone.utc).isoformat(),
            "details": result
        }

    except TypeError as e:
        self.log_info(f" Type error in gold task: {e}")
        logger.exception("Gold task type error details:")
        raise
    except ValueError as e:
        self.log_info(f"Value error in gold task: {e}")
        logger.exception("Gold task value error details:")
        raise
    except Exception as e:
        self.log_info(f" Gold task failed: {e}")
        RedisService.update_job_status(job_id, "Gold Task Failed")
        logger.exception("Gold task exception details:")
        raise


