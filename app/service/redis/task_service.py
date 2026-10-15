from app.service.redis.connection import get_redis_client


class RedisTaskService:

    @staticmethod
    def push_task(task_name: str, payload: dict):
        redis = get_redis_client()
        redis.lpush("tasks:queue", f"{task_name}:{payload}")

    @staticmethod
    def pop_task():
        redis = get_redis_client()
        return redis.rpop("tasks:queue")
