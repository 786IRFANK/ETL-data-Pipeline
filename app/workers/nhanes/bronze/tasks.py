from celery_app import celery_app
from datetime import datetime, timezone
from app.workers.nhanes.bronze.scraper import BronzeScraper



@celery_app.task(bind=True, base=BronzeScraper, name="nhanes.bronze.demographic")
def demographic_bronze_task(self,job_id: str):
    self.ensure_bronze_tables_exist()
    result =  self.process_component("Demographics",job_id)
    return {
            "component": "Demographics",
            "layer": "bronze",
            "status":"completed",
            **result,
            "completed_at": datetime.now(timezone.utc).isoformat()
        }



@celery_app.task(bind=True, base=BronzeScraper, name="nhanes.bronze.dietary")
def dietary_bronze_task(self,job_id: str):
    self.ensure_bronze_tables_exist()
    result =  self.process_component("Dietary",job_id)
    return {
            "component": "Dietary",
            "layer": "bronze",
            "status":"completed",
            **result,
            "completed_at": datetime.now(timezone.utc).isoformat()
        }



@celery_app.task(bind=True, base=BronzeScraper, name="nhanes.bronze.examination")
def examination_bronze_task(self,job_id: str):
    self.ensure_bronze_tables_exist()
    result =  self.process_component("Examination",job_id)
    return {
            "component": "Examination",
            "layer": "bronze",
            "status":"completed",
            **result,
            "completed_at": datetime.now(timezone.utc).isoformat()
        }



@celery_app.task(bind=True, base=BronzeScraper, name="nhanes.bronze.laboratory")
def laboratory_bronze_task(self,job_id: str):
    self.ensure_bronze_tables_exist()
    result =  self.process_component("Laboratory",job_id)
    return {
            "component": "Laboratory",
            "layer": "bronze",
            "status":"completed",
            **result,
            "completed_at": datetime.now(timezone.utc).isoformat()
        }



@celery_app.task(bind=True, base=BronzeScraper, name="nhanes.bronze.question")
def question_bronze_task(self,job_id: str):
    self.ensure_bronze_tables_exist()
    result =  self.process_component("Questionnaire",job_id)
    return {
            "component": "Questionnaire",
            "layer": "bronze",
            "status":"completed",
            **result,
            "completed_at": datetime.now(timezone.utc).isoformat()
        }


