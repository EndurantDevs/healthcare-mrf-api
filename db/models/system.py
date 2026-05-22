# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from sqlalchemy import DATE, JSON, TEXT, TIMESTAMP, Column, DateTime, Integer, PrimaryKeyConstraint, String

from db.connection import Base
from db.json_mixin import JSONOutputMixin

__all__ = (
    "ImportHistory",
    "ImportLog",
    "PartDImportRun",
    "PartDFormularySnapshot",
)


class ImportHistory(Base, JSONOutputMixin):
    __tablename__ = 'history'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('import_id'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['import_id']
    import_id = Column(String)
    json_status = Column(JSON)
    when = Column(DateTime)


class ImportLog(Base, JSONOutputMixin):
    __tablename__ = 'log'
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint('issuer_id', 'checksum'),
        {'schema': os.getenv('HLTHPRT_DB_SCHEMA') or 'mrf', 'extend_existing': True},
    )
    __my_index_elements__ = ['issuer_id', 'checksum']
    issuer_id = Column(Integer)
    checksum = Column(Integer)
    type = Column(String(4))
    text = Column(String)
    url = Column(String)
    source = Column(String)  # plans, index, providers, etc.
    level = Column(String)  # network, json, etc.


class PartDImportRun(Base, JSONOutputMixin):
    __tablename__ = "partd_import_run_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("status",), "name": "partd_import_run_v2_status_idx"},
        {"index_elements": ("started_at",), "name": "partd_import_run_v2_started_at_idx"},
    ]

    run_id = Column(String(64), nullable=False)
    import_id = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False)
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    source_summary = Column(JSON)
    error_text = Column(TEXT)


class PartDFormularySnapshot(Base, JSONOutputMixin):
    __tablename__ = "partd_formulary_snapshot_v2"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("snapshot_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["snapshot_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("run_id",), "name": "partd_formulary_snapshot_v2_run_idx"},
        {"index_elements": ("source_type", "release_date"), "name": "partd_formulary_snapshot_v2_source_release_idx"},
    ]

    snapshot_id = Column(String(128), nullable=False)
    run_id = Column(String(64), nullable=False)
    source_type = Column(String(16), nullable=False)
    source_url = Column(TEXT, nullable=False)
    artifact_name = Column(String(256))
    release_date = Column(DATE)
    cutoff_month = Column(DATE)
    status = Column(String(32), nullable=False)
    row_count_activity = Column(Integer, nullable=False, default=0)
    row_count_pricing = Column(Integer, nullable=False, default=0)
    imported_at = Column(TIMESTAMP)
    metadata_json = Column(JSON)
