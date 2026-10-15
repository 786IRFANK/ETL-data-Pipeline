from celery import Task
from sqlalchemy import text
from app.utils.logger import get_logger
from app.config.settings import settings
from gcp import storage_client
from app.database.database_pool import get_sqlalchemy_engine

logger = get_logger(__name__)


class SilverTask(Task):
    """Base task for silver layer (uses shared SQLAlchemy engine)"""

    abstract = True

    def __init__(self):
        super().__init__()
        self.storage_service = storage_client.bucket(
            settings.GCP_BUCKET_NAME
        )
        self._logger = get_logger(self.__class__.__name__)
        self.schema_name = "nhanes_silver"
        self.bucket_path = settings.BRONZE_PATH

        self.engine = get_sqlalchemy_engine()  # Get shared engine
        # Schema must be pre-created by DBA - do not auto-create

    def log_info(self, msg: str):
        self._logger.info(msg)
