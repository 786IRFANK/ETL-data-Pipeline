from pydantic_settings import BaseSettings
from pydantic import Field, field_validator
from typing import List,Any,ClassVar, Tuple


class Settings(BaseSettings):

    APP_NAME: str = "Axonal Data Pipeline API"
    APP_DESCRIPTION: str = "Data pipeline orchestration system with Celery"
    APP_VERSION: str = "1.0.0"

    ENVIRONMENT: str = Field(..., description="Environment name (development/staging/production)")
    DEBUG: bool = False
    HOST: str = "0.0.0.0"
    PORT: int = 8000

    CORS_ORIGINS: List[str] = []
    @field_validator("CORS_ORIGINS", mode="before")
    @classmethod
    def parse_cors_origins(cls, v: Any) -> List[str]:
        if v is None:
            return []
        if isinstance(v, str):
            return [origin.strip() for origin in v.split(",") if origin.strip()]
        if isinstance(v, list):
            return v
        raise ValueError("Invalid CORS_ORIGINS format")

    REDIS_URL: str = Field(..., description="Redis connection URL")
    REDIS_HOST: str | None = None
    REDIS_PORT: int | None = None
    REDIS_PASSWORD: str | None = None
    REDIS_DB: int = 0

    POSTGRES_HOST: str = Field(...)
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = Field(...)
    POSTGRES_PASSWORD: str = Field(...)
    POSTGRES_DB: str = Field(...)
    ADHOC_POSTGRES_DB: str = Field(...)


    GCP_PROJECT_ID: str = Field(...)
    GCP_BUCKET_NAME: str = Field(...)
    ADHOC_GCP_BUCKET_NAME: str = Field(...)
    GCP_CREDENTIALS_PATH: str = ""  # Empty for ADC

    ADHOC_GCS_BASE_PATH: str = "adhoc_uploads"

    # Dev mock auth (required in dev/local)
    AUTH_USER: str | None = None
    AUTH_WORKSPACE: str | None = None
    AUTH_ROLE: str = "user"

    MAX_FILES: ClassVar[int] = 5
    MAX_FILE_SIZE: ClassVar[int] = 200 * 1024 * 1024   # 200 MB
    ALLOWED_EXTS: ClassVar[Tuple[str, ...]] = (".csv", ".xlsx", ".xls")

    NHANES_BASE_URL: str = "https://wwwn.cdc.gov"
    NHANES_YEARS_STR: str = Field(default="2017-2018,2019-2020", alias="NHANES_YEARS")

    @property
    def NHANES_YEARS(self) -> List[str]:
        return [year.strip() for year in self.NHANES_YEARS_STR.split(",") if year.strip()]

    
    BRONZE_PATH: str = "bronze/NHANES"
    SILVER_PATH: str = "silver/NHANES"
    GOLD_PATH: str = "gold/NHANES"
    TEST_PATH: str = "test2/NHANES"

    FORIAN_CLAIMS_LANDING_PATH: str = "landing/forian_claims"
    FORIAN_CLAIMS_BRONZE_PATH: str = "test/bronze/forian/forian_claims"

    class Config:
        env_file = ".env"
        case_sensitive = True


settings = Settings()
