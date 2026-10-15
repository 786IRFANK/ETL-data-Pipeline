from sqlalchemy import (
    Column,
    Integer,
    String,
    DateTime,
    BigInteger,
    JSON,
    Text,
    TIMESTAMP,
    UniqueConstraint,
    JSON,
    func,BOOLEAN,
)
from app.database.base import Base
from sqlalchemy.sql import func
from sqlalchemy.dialects.postgresql import JSONB

# ------------------------------------------------------------------
# SCHEMA: nhanes_silver
# ------------------------------------------------------------------

class DataCheck(Base):
    __tablename__ = "data_check"

    id = Column(Integer, primary_key=True, index=True)
    filename_code = Column(String(255))
    year = Column(String(255))
    component = Column(String(255))
    domain = Column(String(50), nullable=False)
    category = Column(String(200), nullable=False)
    processed_files_count = Column(Integer, default=0)
    total_rows = Column(Integer, default=0)
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())
    gcp_merged_path = Column(Text)
    silver_process = Column(BOOLEAN, default=False)
    last_processed_date = Column(
        TIMESTAMP,
        server_default=func.current_timestamp(),
        onupdate=func.current_timestamp()
    )


class Codebook(Base):
    __tablename__ = "codebook"

    id = Column(Integer, primary_key=True, index=True)
    dataset_name = Column(String(255))
    dataset_year = Column(String(255))
    variable_name = Column(String(255))
    sas_label = Column(Text)
    english_text = Column(Text)
    target = Column(Text)
    title = Column(Text)
    codevalue = Column(JSON)
    component = Column(String(255))
    created_at = Column(TIMESTAMP, server_default=func.current_timestamp())


# ------------------------------------------------------------------
# SCHEMA: nhanes_gold
# ------------------------------------------------------------------

class DataCheckGold(Base):
    __tablename__ = "data_check_gold"

    id = Column(Integer, primary_key=True, index=True)
    table_name = Column(Text, nullable=False)
    source_component = Column(Text, nullable=False)
    category = Column(Text, nullable=False)
    status = Column(Text, default="SUCCESS")
    inserted_at = Column(TIMESTAMP, server_default=func.now())

# ------------------------------------------------------------------
# SCHEMA: adhoc metadata
# ------------------------------------------------------------------
class UploadRegistry(Base):

    __tablename__ = "upload_registry"

    id = Column(Integer,  primary_key=True, autoincrement=True)
    org_id = Column(String(64),  nullable=False, index=True)
    workspace_id  = Column(String(64), nullable=False, index=True)
    uploaded_by = Column(String(64), nullable=False, index=True)   # firebase user_id
    table_name = Column(String(255), nullable=False, unique=True)
    file_name = Column(String(255), nullable=False)
    source_path = Column(Text, nullable=True)              
    column_schema = Column(JSONB, nullable=True)
    row_count = Column(BigInteger, nullable=True)
    is_append = Column(BOOLEAN, nullable=False, default=False)
    job_status = Column(String(16),  nullable=False, default="COMPLETED")
    error_message = Column(Text,  nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    updated_at = Column(DateTime(timezone=True), nullable=False,default=func.now(), onupdate=func.now())
    file_hash  = Column(Text, nullable=True) 
# ------------------------------------------------------------------
# SCHEMA: adhoc user management
# ------------------------------------------------------------------

class AdhocUser(Base):
    __tablename__ = "adhoc_user"

    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(String, nullable=False)
    org_id = Column(String, nullable=False, unique=True)
    status = Column(String, default="active")
    created_at = Column(DateTime(timezone=True), default=func.now())
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), default=func.now())



