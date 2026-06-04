# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from sqlalchemy import DATE, JSON, TEXT, TIMESTAMP, Column, DateTime, Integer, PrimaryKeyConstraint, String

from db.connection import Base
from db.json_mixin import JSONOutputMixin

__all__ = (
    "ImportHistory",
    "ImportLog",
    "ImportRun",
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


class ImportRun(Base, JSONOutputMixin):
    __tablename__ = "import_run"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("status", "heartbeat_at"), "name": "import_run_status_heartbeat_idx"},
        {"index_elements": ("importer", "created_at"), "name": "import_run_importer_created_idx"},
        {
            "index_elements": ("idempotency_key",),
            "name": "import_run_active_idempotency_idx",
            "unique": True,
            "where": "status IN ('queued', 'starting', 'running', 'finalizing', 'canceling')",
        },
        {"index_elements": ("schedule_id",), "name": "import_run_schedule_idx"},
        {"index_elements": ("subscription_id",), "name": "import_run_subscription_idx"},
        {"index_elements": ("source_file_import_id",), "name": "import_run_source_file_import_idx"},
    ]

    run_id = Column(String(64), nullable=False)
    engine = Column(String(64), nullable=False, default="healthcare-mrf-api")
    node_id = Column(String(64))
    importer = Column(String(64), nullable=False)
    family = Column(String(64))
    status = Column(String(32), nullable=False)
    phase_detail = Column(String(128))
    params = Column(JSON)
    idempotency_key = Column(String(160))
    triggered_by = Column(String(32))
    schedule_id = Column(String(64))
    subscription_id = Column(String(64))
    source_file_import_id = Column(String(64))
    created_at = Column(TIMESTAMP)
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    heartbeat_at = Column(TIMESTAMP)
    progress = Column(JSON)
    metrics = Column(JSON)
    error = Column(JSON)
    snapshot_id = Column(String(96))
    import_id = Column(String(64))
    retry_of_run_id = Column(String(64))


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
