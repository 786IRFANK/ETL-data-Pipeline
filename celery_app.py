
"""
Celery Application Configuration
"""
import os
import sys

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from celery import Celery

# Import settings
try:
    from app.config.settings import settings
except ImportError:
    # Fallback for when running from different directory
    import importlib.util
    settings_path = os.path.join(os.path.dirname(__file__), 'app', 'config', 'settings.py')
    spec = importlib.util.spec_from_file_location("settings", settings_path)
    settings_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(settings_module)
    settings = settings_module.settings

celery_app = Celery(
    "axonaldata",
    broker=settings.REDIS_URL,
    backend=settings.REDIS_URL,
    include=[
        "app.workers.nhanes.bronze.tasks",
        "app.workers.nhanes.silver.tasks",
        "app.workers.nhanes.gold.tasks",
        "app.workers.forian_claims.bronze.tasks",
        "app.workers.forian_claims.silver.tasks",
        "app.workers.adhoc.tasks",
        "app.workers.connector_task.task",

    ]
)

# Celery configuration
# Note: SSL is configured via rediss:// URL scheme and ?ssl_cert_reqs=CERT_NONE query param
# set in build_env.py when port 6378 (Memorystore with TLS) is detected
celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_time_limit = 5 * 60 * 60,       # 5 hours (hard limit)
    task_soft_time_limit = 2 * 60 * 60,  # 2 hours (soft limit)
    worker_prefetch_multiplier = 1,
    worker_max_tasks_per_child = 1000,
    result_expires = 24 * 60 * 60,       # 1 day
    task_acks_late = True,
    task_reject_on_worker_lost = True,
)

# from kombu import Queue

# celery_app.conf.task_queues = (
#     Queue("default"),
#     Queue("adhoc_queue"),
# )
# celery_app.conf.task_default_queue = "default"

if __name__ == "__main__":
    celery_app.start()
 
# Provide a compatibility alias so other modules can import `celery`
# (some workers expect `from celery_app import celery`).
celery = celery_app