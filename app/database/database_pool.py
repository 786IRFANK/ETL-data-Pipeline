# app/db/database_pool.py
from psycopg2.pool import ThreadedConnectionPool
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool

from app.config.settings import settings
from app.utils.logger import get_logger

logger = get_logger(__name__)

DATABASE_URL = (
    f"postgresql+psycopg2://{settings.POSTGRES_USER}:"
    f"{settings.POSTGRES_PASSWORD}@"
    f"{settings.POSTGRES_HOST}:"
    f"{settings.POSTGRES_PORT}/"
    f"{settings.POSTGRES_DB}"
)

# Psycopg2 connection pool for Bronze/Gold layers
PSYCOPG2_POOL = None

def get_psycopg2_pool():
    """Get or create the psycopg2 connection pool (singleton)"""
    global PSYCOPG2_POOL
    if PSYCOPG2_POOL is None:
        try:
            PSYCOPG2_POOL = ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                host=settings.POSTGRES_HOST,
                database=settings.POSTGRES_DB,
                user=settings.POSTGRES_USER,
                password=settings.POSTGRES_PASSWORD,
                port=settings.POSTGRES_PORT,
            )
            logger.info("Psycopg2 connection pool created successfully")
        except Exception as e:
            logger.error(f"Failed to create psycopg2 pool: {e}")
            raise
    return PSYCOPG2_POOL

# SQLAlchemy engine for Silver layer (singleton)
SQLALCHEMY_ENGINE = None

def get_sqlalchemy_engine():
    """Get or create the SQLAlchemy engine (singleton)"""
    global SQLALCHEMY_ENGINE
    if SQLALCHEMY_ENGINE is None:
        try:
            SQLALCHEMY_ENGINE = create_engine(
                DATABASE_URL,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_timeout=30,
                pool_recycle=1800,
                pool_pre_ping=True,
                echo=False,
            )
            logger.info("SQLAlchemy engine created successfully")
        except Exception as e:
            logger.error(f"Failed to create SQLAlchemy engine: {e}")
            raise
    return SQLALCHEMY_ENGINE

def close_all_pools():
    """Close all database pools - call on application shutdown"""
    global PSYCOPG2_POOL, SQLALCHEMY_ENGINE
    
    if PSYCOPG2_POOL:
        PSYCOPG2_POOL.closeall()
        logger.info("Psycopg2 pool closed")
        PSYCOPG2_POOL = None
    
    if SQLALCHEMY_ENGINE:
        SQLALCHEMY_ENGINE.dispose()
        logger.info("SQLAlchemy engine disposed")
        SQLALCHEMY_ENGINE = None