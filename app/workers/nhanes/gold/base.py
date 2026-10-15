from celery import Task
from app.utils.logger import get_logger
from app.database.connection import get_db_connection

logger = get_logger(__name__)


class GoldTask(Task):
    """Base task for Gold layer (pooled psycopg2 connections)"""

    abstract = True

    def __init__(self):
        super().__init__()
        self._logger = get_logger(self.__class__.__name__)
        self.source_schema = "nhanes_silver"
        self.target_schema = "nhanes_gold"
        self.table_prefix = "nhanes_"
        # Schema must be pre-created by DBA - do not auto-create

    def log_info(self, msg: str):
        self._logger.info(msg)

    def before_start(self, task_id, args, kwargs):
        """Called before task execution"""
        self.log_info(
            f"Starting Gold task {self.name} (task_id={task_id})"
        )
        return super().before_start(task_id, args, kwargs)