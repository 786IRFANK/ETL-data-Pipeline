"""
Application Settings Configuration
"""
from pydantic_settings import BaseSettings
from typing import List


class Settings(BaseSettings):
    # Application
    DEBUG: bool = True
    HOST: str = "0.0.0.0"
    PORT: int = 8000
    ENVIRONMENT: str = "development"
    
    # Redis
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_DB: int = 0
    REDIS_PASSWORD: str = ""
    REDIS_URL: str = "redis://localhost:6379/0"
    
    # GCP Storage
    GCP_PROJECT_ID: str = ""
    GCP_BUCKET_NAME: str = ""
    GCP_CREDENTIALS_PATH: str = ""
    
    # NHANES
    NHANES_BASE_URL: str = "https://wwwn.cdc.gov"
    NHANES_YEARS: str = "2017-2018,2019-2020"
    
    # Storage Paths
    BRONZE_PATH: str = "bronze/nhanes"
    SILVER_PATH: str = "silver/nhanes"
    GOLD_PATH: str = "gold/nhanes"
    
    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()