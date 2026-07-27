# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Source-neutral provider profile facts and materialized public projections."""

from __future__ import annotations

import os

from sqlalchemy import (
    JSON,
    TEXT,
    TIMESTAMP,
    BigInteger,
    Boolean,
    Column,
    Integer,
    Index,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)

from db.connection import Base
from db.json_mixin import JSONOutputMixin

__all__ = (
    "ProviderProfileArtifact",
    "ProviderProfileFact",
    "ProviderProfileImportRun",
    "ProviderProfileProjection",
    "ProviderProfileSourceRecord",
)

_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"


class ProviderProfileImportRun(Base, JSONOutputMixin):
    __tablename__ = "provider_profile_import_run"
    __table_args__ = (
        PrimaryKeyConstraint("run_id"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    run_id = Column(String(64), nullable=False)
    source_key = Column(String(96), nullable=False)
    jurisdiction = Column(String(16), nullable=False)
    schema_version = Column(String(32), nullable=False)
    status = Column(String(32), nullable=False)
    source_manifest = Column(JSON)
    metrics = Column(JSON)
    error = Column(JSON)
    started_at = Column(TIMESTAMP)
    finished_at = Column(TIMESTAMP)


class ProviderProfileArtifact(Base, JSONOutputMixin):
    __tablename__ = "provider_profile_artifact"
    __table_args__ = (
        PrimaryKeyConstraint("artifact_id"),
        UniqueConstraint("run_id", "source_key", name="provider_profile_artifact_run_source_uq"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    artifact_id = Column(String(64), nullable=False)
    run_id = Column(String(64), nullable=False)
    source_key = Column(String(96), nullable=False)
    file_name = Column(String(256), nullable=False)
    source_url = Column(TEXT, nullable=False)
    category = Column(String(64), nullable=False)
    content_sha256 = Column(String(64), nullable=False)
    content_bytes = Column(BigInteger, nullable=False)
    header = Column(JSON)
    downloaded_at = Column(TIMESTAMP)
    metadata_json = Column(JSON)


class ProviderProfileSourceRecord(Base, JSONOutputMixin):
    __tablename__ = "provider_profile_source_record"
    __table_args__ = (
        PrimaryKeyConstraint("record_id"),
        UniqueConstraint(
            "run_id",
            "source_key",
            "source_record_key",
            name="provider_profile_source_record_run_key_uq",
        ),
        Index(
            "provider_profile_source_record_npi_idx",
            "matched_npi",
            "match_status",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    record_id = Column(String(64), nullable=False)
    run_id = Column(String(64), nullable=False)
    artifact_id = Column(String(64), nullable=False)
    source_key = Column(String(96), nullable=False)
    source_record_key = Column(String(256), nullable=False)
    profession_code = Column(String(32))
    license_id = Column(String(64))
    license_number = Column(String(96))
    raw_payload = Column(JSON, nullable=False)
    normalized_payload = Column(JSON)
    matched_npi = Column(BigInteger)
    match_status = Column(String(32), nullable=False, default="unmatched")
    match_evidence = Column(JSON)
    row_number = Column(Integer)


class ProviderProfileFact(Base, JSONOutputMixin):
    __tablename__ = "provider_profile_fact"
    __table_args__ = (
        PrimaryKeyConstraint("fact_id"),
        Index("provider_profile_fact_npi_category_idx", "npi", "category"),
        Index("provider_profile_fact_run_npi_idx", "run_id", "npi"),
        Index(
            "provider_profile_fact_logical_key_idx",
            "logical_fact_key",
            "npi",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    fact_id = Column(String(64), nullable=False)
    run_id = Column(String(64), nullable=False)
    npi = Column(BigInteger)
    source_record_id = Column(String(64), nullable=False)
    logical_fact_key = Column(String(64), nullable=False)
    category = Column(String(64), nullable=False)
    fact_type = Column(String(96), nullable=False)
    display = Column(TEXT, nullable=False)
    value_json = Column(JSON, nullable=False)
    availability = Column(String(32), nullable=False, default="available")
    assertion_type = Column(String(32), nullable=False)
    verification_status = Column(String(32), nullable=False)
    effective_start = Column(String(32))
    effective_end = Column(String(32))
    source_json = Column(JSON, nullable=False)
    sensitive = Column(Boolean, nullable=False, default=False)
    public_default = Column(Boolean, nullable=False, default=True)
    published_at = Column(TIMESTAMP)


class ProviderProfileProjection(Base, JSONOutputMixin):
    __tablename__ = "provider_profile_projection"
    __table_args__ = (
        PrimaryKeyConstraint("npi"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    npi = Column(BigInteger, nullable=False, autoincrement=False)
    generation_id = Column(String(64), nullable=False)
    schema_version = Column(String(32), nullable=False)
    profile_json = Column(JSON, nullable=False)
    evidence_json = Column(JSON)
    source_keys = Column(JSON, nullable=False)
    published_at = Column(TIMESTAMP, nullable=False)
