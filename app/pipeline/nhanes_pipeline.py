import uuid
from datetime import datetime, timezone

from celery import group,chain

from app.service.redis.job_service import RedisService
from app.service.redis.task_service import RedisTaskService
from app.workers.nhanes.bronze.tasks import (
    demographic_bronze_task,
    dietary_bronze_task,
    examination_bronze_task,
    laboratory_bronze_task,
    question_bronze_task,
)
from app.workers.nhanes.silver.tasks import (
    demographic_silver_task,
    dietary_silver_task,
    laboratory_silver_task,
    examination_silver_task,
    question_silver_task
)
from app.workers.nhanes.gold.tasks import (gold_task)


class NHANESPipeline:
    TASK_MAP = {
        "demographic": {
            "bronze": demographic_bronze_task,
            "silver": demographic_silver_task,
        },
        "dietary": {
            "bronze": dietary_bronze_task,
            "silver": dietary_silver_task,
        },
        "examination": {
            "bronze": examination_bronze_task,
            "silver": examination_silver_task,
        },
        "laboratory": {
            "bronze": laboratory_bronze_task,
            "silver": laboratory_silver_task,
        },
        "question": {
            "bronze": question_bronze_task,
            "silver": question_silver_task,
        },
    }
    ALL_TASKS = ["demographic", "dietary", "examination", "laboratory", "question"]

    def __init__(self):
        self.pipeline_code = "nhanes"

    def _generate_job_id(self) -> str:
        return str(uuid.uuid4())

    def _build_task_groups(self, job_id: str, requested_tasks: list):
        """Build bronze and silver task groups based on requested tasks."""
        bronze_tasks = []
        silver_tasks = []

        for task_name in requested_tasks:
            if task_name in self.TASK_MAP:
                bronze_tasks.append(self.TASK_MAP[task_name]["bronze"].s(job_id))
                silver_tasks.append(self.TASK_MAP[task_name]["silver"].s(job_id))

        bronze_group = group(*bronze_tasks) if bronze_tasks else None
        silver_group = group(*silver_tasks) if silver_tasks else None

        return bronze_group, silver_group

    def start(self, params: dict):
        job_id = self._generate_job_id()

        requested_tasks = params.get("tasks", self.ALL_TASKS) if params else self.ALL_TASKS
        skip_gold = params.get("skip_gold", False) if params else False

        RedisService.store_job_metadata(
            job_id,
            {
                "pipeline_code": self.pipeline_code,
                "run_type": "now",
                "params": params,
                "status": "Running",
                "requested_tasks": requested_tasks,
                "skip_gold": skip_gold,
            },
        )

        RedisTaskService.push_task("nhanes_start", {"job_id": job_id})

        bronze_group, silver_group = self._build_task_groups(job_id, requested_tasks)

        if not bronze_group:
            return job_id

        if skip_gold:
            pipeline_chain = bronze_group | silver_group
        else:
            pipeline_chain = chain(
                bronze_group | silver_group,
                gold_task.s(job_id),
            )

        pipeline_chain.apply_async()

        return job_id

    def schedule(self, schedule_at: str, params: dict):
        job_id = self._generate_job_id()

        scheduled_utc = datetime.fromisoformat(
            schedule_at.replace("Z", "+00:00")
        )

        if scheduled_utc.tzinfo is None:
            scheduled_utc = scheduled_utc.replace(tzinfo=timezone.utc)

        if scheduled_utc <= datetime.now(timezone.utc):
            raise ValueError("Scheduled time must be in the future (UTC)")

        requested_tasks = params.get("tasks", self.ALL_TASKS) if params else self.ALL_TASKS
        skip_gold = params.get("skip_gold", False) if params else False

        RedisService.store_job_metadata(
            job_id,
            {
                "pipeline_code": self.pipeline_code,
                "run_type": "scheduled",
                "schedule_at": schedule_at,
                "params": params,
                "status": "SCHEDULED",
                "requested_tasks": requested_tasks,
                "skip_gold": skip_gold,
            },
        )

        RedisTaskService.push_task(
            "nhanes_start",
            {"job_id": job_id, "schedule_at": scheduled_utc.isoformat()},
        )

        bronze_group, silver_group = self._build_task_groups(job_id, requested_tasks)

        if not bronze_group:
            return job_id

        if skip_gold:
            pipeline_chain = bronze_group | silver_group
        else:
            pipeline_chain = chain(
                bronze_group | silver_group,
                gold_task.s(job_id),
            )

        pipeline_chain.apply_async(eta=scheduled_utc)

        return job_id

