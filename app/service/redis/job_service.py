import json
from datetime import datetime, timezone
from app.service.redis.connection import get_redis_client
from app.service.redis.exceptions import JobNotFoundError

JOBS_QUEUE_KEY = "jobs:queue"


class RedisService:

    @staticmethod
    def _utc_now():
        return datetime.now(timezone.utc).isoformat()

    @staticmethod
    def store_job_metadata(job_id: str, payload: dict):
        redis = get_redis_client()
        key = f"job:{job_id}"

        for field, value in payload.items():
            if value is None:
                continue

            if isinstance(value, (dict, list)):
                value = json.dumps(value)

            redis.hset(key, field, str(value))

        # timestamps
        redis.hset(key, "created_at", RedisService._utc_now())

        # queue
        redis.lpush(JOBS_QUEUE_KEY, job_id)

    @staticmethod
    def get_job_metadata(job_id: str) -> dict:
        redis = get_redis_client()

        data = redis.hgetall(f"job:{job_id}")
        if not data:
            raise JobNotFoundError(f"Job {job_id} not found")

        return data

    @staticmethod
    def update_job_status(job_id: str, status: str):
        redis = get_redis_client()
        key = f"job:{job_id}"

        redis.hset(key, "status", str(status))
        redis.hset(key, "updated_at", RedisService._utc_now())

    @staticmethod
    def delete_job(job_id: str) -> dict:
        redis = get_redis_client()
        key = f"job:{job_id}"

        if not redis.exists(key):
            return {"success": False, "message": f"Job {job_id} not found"}

        redis.delete(key)
        redis.lrem(JOBS_QUEUE_KEY, 0, job_id)
        
        return {"success": True, "message": f"Job {job_id} deleted successfully"}

    @staticmethod
    def delete_task(task_id: str) -> dict:
        redis = get_redis_client()
        key = f"job:{task_id}"

        if not redis.exists(key):
            return {"success": False, "message": f"Task {task_id} not found"}

        redis.delete(key)
        redis.lrem(JOBS_QUEUE_KEY, 0, task_id)
        
        return {"success": True, "message": f"Task {task_id} deleted successfully"}
