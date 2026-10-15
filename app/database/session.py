
from sqlalchemy.orm import sessionmaker
from app.database.sqlalchemy_engine import engine_main, engine_adhoc

SessionMain = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine_main
)

SessionAdhoc = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine_adhoc
)


def get_main_db():
    db = SessionMain()
    try:
        yield db
    finally:
        db.close()


def get_adhoc_db():
    db = SessionAdhoc()
    try:
        yield db
    finally:
        db.close()