# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

from sqlalchemy import DATE, JSON, TEXT, TIMESTAMP, BigInteger, Boolean, Column, DateTime, Integer, PrimaryKeyConstraint, String

from db.connection import Base
from db.json_mixin import JSONOutputMixin

__all__ = (
    "ImportHistory",
    "ImportLog",
    "ImportRun",
    "MRFCrawlRun",
    "MRFFile",
    "MRFPayer",
    "MRFPayerScorecard",
    "MRFPlan",
    "MRFSource",
    "MRFUrlObservation",
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


class MRFPayer(Base, JSONOutputMixin):
    __tablename__ = "mrf_payer"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("payer_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["payer_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("canonical_name",), "name": "mrf_payer_name_idx"},
        {"index_elements": ("parent_group",), "name": "mrf_payer_parent_group_idx"},
        {"index_elements": ("entity_type",), "name": "mrf_payer_entity_type_idx"},
    ]

    payer_id = Column(String(64), nullable=False)
    canonical_name = Column(String(256), nullable=False)
    aliases = Column(JSON)
    parent_group = Column(String(128))
    entity_type = Column(String(64))
    states = Column(JSON)
    eins = Column(JSON)
    lifecycle = Column(String(32), nullable=False, default="active")
    source_coverage = Column(JSON)
    metadata_json = Column(JSON)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class MRFSource(Base, JSONOutputMixin):
    __tablename__ = "mrf_source"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["source_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("payer_id",), "name": "mrf_source_payer_idx"},
        {"index_elements": ("source_key",), "name": "mrf_source_key_idx", "unique": True},
        {"index_elements": ("canonical_url",), "name": "mrf_source_canonical_url_idx"},
        {"index_elements": ("status",), "name": "mrf_source_status_idx"},
        {"index_elements": ("hosting_platform",), "name": "mrf_source_hosting_platform_idx"},
        {"index_elements": ("seed_provider",), "name": "mrf_source_seed_provider_idx"},
    ]

    source_id = Column(String(64), nullable=False)
    payer_id = Column(String(64))
    source_key = Column(String(96), nullable=False)
    display_name = Column(String(256), nullable=False)
    source_type = Column(String(64))
    hosting_platform = Column(String(64))
    access_model = Column(String(32))
    index_url = Column(TEXT)
    human_url = Column(TEXT)
    canonical_url = Column(TEXT)
    domain = Column(String(256))
    status = Column(String(32), nullable=False, default="needs_review")
    schema_version = Column(String(32))
    etag = Column(String(512))
    last_modified = Column(String(256))
    content_version = Column(String(128))
    last_crawled_at = Column(TIMESTAMP)
    latest_index_date = Column(String(32))
    num_plans = Column(Integer)
    num_files = Column(Integer)
    num_indices = Column(Integer)
    total_compressed_size = Column(BigInteger)
    provenance_url = Column(TEXT)
    seed_provider = Column(String(64))
    confidence = Column(Integer)
    license_status = Column(String(64))
    review_status = Column(String(32))
    metadata_json = Column(JSON)
    created_at = Column(TIMESTAMP)
    updated_at = Column(TIMESTAMP)


class MRFPlan(Base, JSONOutputMixin):
    __tablename__ = "mrf_plan"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("mrf_plan_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["mrf_plan_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("payer_id",), "name": "mrf_plan_payer_idx"},
        {"index_elements": ("source_id",), "name": "mrf_plan_source_idx"},
        {"index_elements": ("plan_id",), "name": "mrf_plan_plan_id_idx"},
        {"index_elements": ("market_type",), "name": "mrf_plan_market_idx"},
        {"index_elements": ("reporting_entity_name",), "name": "mrf_plan_reporting_entity_idx"},
    ]

    mrf_plan_id = Column(String(64), nullable=False)
    payer_id = Column(String(64))
    source_id = Column(String(64))
    plan_id = Column(String(128))
    plan_id_type = Column(String(64))
    plan_name = Column(String(512))
    market_type = Column(String(64))
    reporting_entity_name = Column(String(512))
    reporting_entity_type = Column(String(128))
    metadata_json = Column(JSON)
    first_seen_at = Column(TIMESTAMP)
    last_seen_at = Column(TIMESTAMP)


class MRFFile(Base, JSONOutputMixin):
    __tablename__ = "mrf_file"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("mrf_file_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["mrf_file_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("payer_id",), "name": "mrf_file_payer_idx"},
        {"index_elements": ("source_id",), "name": "mrf_file_source_idx"},
        {"index_elements": ("file_type",), "name": "mrf_file_type_idx"},
        {"index_elements": ("canonical_url",), "name": "mrf_file_canonical_url_idx"},
        {"index_elements": ("last_seen_at",), "name": "mrf_file_last_seen_idx"},
    ]

    mrf_file_id = Column(String(64), nullable=False)
    payer_id = Column(String(64))
    source_id = Column(String(64))
    file_type = Column(String(64), nullable=False)
    url = Column(TEXT, nullable=False)
    canonical_url = Column(TEXT)
    from_index_url = Column(TEXT)
    description = Column(TEXT)
    network_name = Column(String(512))
    plan_ids = Column(JSON)
    plan_names = Column(JSON)
    market_types = Column(JSON)
    is_signed_url = Column(Boolean, nullable=False, default=False)
    size_bytes = Column(BigInteger)
    etag = Column(String(512))
    last_modified = Column(String(256))
    schema_version = Column(String(32))
    metadata_json = Column(JSON)
    first_seen_at = Column(TIMESTAMP)
    last_seen_at = Column(TIMESTAMP)


class MRFCrawlRun(Base, JSONOutputMixin):
    __tablename__ = "mrf_crawl_run"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("crawl_run_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["crawl_run_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("run_id",), "name": "mrf_crawl_run_control_run_idx"},
        {"index_elements": ("status",), "name": "mrf_crawl_run_status_idx"},
        {"index_elements": ("started_at",), "name": "mrf_crawl_run_started_idx"},
    ]

    crawl_run_id = Column(String(64), nullable=False)
    run_id = Column(String(64))
    provider = Column(String(128))
    mode = Column(String(64))
    status = Column(String(32), nullable=False)
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)
    params = Column(JSON)
    sources_discovered = Column(Integer, nullable=False, default=0)
    urls_checked = Column(Integer, nullable=False, default=0)
    etag_skipped = Column(Integer, nullable=False, default=0)
    plans_discovered = Column(Integer, nullable=False, default=0)
    files_discovered = Column(Integer, nullable=False, default=0)
    bytes_streamed = Column(BigInteger, nullable=False, default=0)
    errors = Column(JSON)


class MRFPayerScorecard(Base, JSONOutputMixin):
    __tablename__ = "mrf_payer_scorecard"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("scorecard_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["scorecard_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("payer_id",), "name": "mrf_payer_scorecard_payer_idx"},
        {"index_elements": ("source",), "name": "mrf_payer_scorecard_source_idx"},
    ]

    scorecard_id = Column(String(64), nullable=False)
    payer_id = Column(String(64))
    source = Column(String(64), nullable=False)
    score = Column(String(32))
    update_cadence = Column(String(64))
    file_accessibility_pct = Column(Integer)
    notes = Column(TEXT)
    payload = Column(JSON)
    observed_at = Column(TIMESTAMP)


class MRFUrlObservation(Base, JSONOutputMixin):
    __tablename__ = "mrf_url_observation"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("observation_id"),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "extend_existing": True},
    )
    __my_index_elements__ = ["observation_id"]
    __my_additional_indexes__ = [
        {"index_elements": ("source_id",), "name": "mrf_url_observation_source_idx"},
        {"index_elements": ("canonical_url",), "name": "mrf_url_observation_url_idx"},
        {"index_elements": ("checked_at",), "name": "mrf_url_observation_checked_idx"},
        {"index_elements": ("status",), "name": "mrf_url_observation_status_idx"},
    ]

    observation_id = Column(String(64), nullable=False)
    source_id = Column(String(64))
    url = Column(TEXT, nullable=False)
    canonical_url = Column(TEXT)
    url_type = Column(String(64))
    status = Column(String(64), nullable=False)
    http_status = Column(Integer)
    etag = Column(String(512))
    last_modified = Column(String(256))
    content_length = Column(BigInteger)
    content_type = Column(String(256))
    final_url = Column(TEXT)
    checked_at = Column(TIMESTAMP)
    error = Column(TEXT)
    metadata_json = Column(JSON)


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
