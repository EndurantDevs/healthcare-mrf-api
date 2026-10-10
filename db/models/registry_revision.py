# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable draft history and independently approved custom composition heads."""

import os

from sqlalchemy import TIMESTAMP, BigInteger, CheckConstraint, Column, Index, Integer, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("RegistryRevisionControl", "RegistryRecordHistory")
_SCHEMA = registry_schema()


class RegistryRevisionControl(Base):
    __tablename__ = "registry_revision_control"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("id = 1", name="registry_revision_singleton"),
        CheckConstraint(
            "draft_revision >= approved_revision AND approved_revision >= 0", name="registry_revision_order"
        ),
        {"schema": _SCHEMA},
    )

    id = Column(Integer, primary_key=True)
    draft_revision = Column(BigInteger, nullable=False, server_default="0")
    approved_revision = Column(BigInteger, nullable=False, server_default="0")


class RegistryRecordHistory(Base):
    """Full manual versions; source observation and serving snapshots are separate."""

    __tablename__ = "registry_record_history"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "record_kind IN ('group','company','network','company_links','provider','location','membership','site_binding','network_binding')",
            name="registry_history_kind",
        ),
        CheckConstraint("revision > 0 AND custom_revision > 0", name="registry_history_revision"),
        CheckConstraint(
            "jsonb_typeof(record_json) = 'object' AND jsonb_typeof(actor_json) = 'object'", name="registry_history_json"
        ),
        CheckConstraint("btrim(reason) <> '' AND btrim(idempotency_key) <> ''", name="registry_history_reason"),
        CheckConstraint("request_sha256 ~ '^[0-9a-f]{64}$'", name="registry_history_digest"),
        Index("registry_history_custom_revision_idx", "custom_revision"),
        UniqueConstraint("record_kind", "record_key", "idempotency_key", name="registry_history_replay"),
        {"schema": _SCHEMA},
    )

    record_kind = Column(String(16), primary_key=True)
    record_key = Column(String(36), primary_key=True)
    revision = Column(BigInteger, primary_key=True)
    custom_revision = Column(BigInteger, nullable=False)
    record_json = Column(JSONB, nullable=False)
    actor_json = Column(JSONB, nullable=False)
    reason = Column(String(1000), nullable=False)
    idempotency_key = Column(String(128), nullable=False)
    request_sha256 = Column(String(64), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
