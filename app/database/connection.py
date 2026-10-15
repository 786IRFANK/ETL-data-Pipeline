# app/db/connection.py
from contextlib import contextmanager

from app.database.database_pool import get_psycopg2_pool, get_sqlalchemy_engine
from app.utils.logger import get_logger

logger = get_logger(__name__)

@contextmanager
def get_db_connection():

    pool = get_psycopg2_pool()
    conn = pool.getconn()
    try:
        yield conn
        conn.commit()  # Commit on success
    except Exception as e:
        conn.rollback()  # Rollback on failure
        logger.error(f"Database transaction failed: {e}")
        raise
    finally:
        pool.putconn(conn)  # Always return to pool

@contextmanager
def get_sqlalchemy_connection():

    engine = get_sqlalchemy_engine()
    conn = engine.connect()
    try:
        yield conn
    finally:
        conn.close()

@contextmanager
def get_sqlalchemy_transaction():

    engine = get_sqlalchemy_engine()
    conn = engine.begin()
    try:
        yield conn
        conn.commit()  # Explicit commit
    except Exception as e:
        conn.rollback()  # Rollback on failure
        logger.error(f"SQLAlchemy transaction failed: {e}")
        raise
    finally:
        conn.close()