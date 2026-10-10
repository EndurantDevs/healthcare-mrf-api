# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit approved record selections, independent of pending draft versions."""

import os

from sqlalchemy import TIMESTAMP, BigInteger, CheckConstraint, Column, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import JSONB

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("RegistryApprovalHistory", "RegistryApprovedRecord")
_SCHEMA = registry_schema()


class RegistryApprovalHistory(Base):
    """An approval records an explicit selection and the prior approved snapshot."""

    __tablename__ = "registry_approval_history"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "approved_revision > 0 AND previous_approved_revision >= 0 AND approved_revision > previous_approved_revision",
            name="registry_approval_order",
        ),
        CheckConstraint(
            "expected_draft_revision >= previous_approved_revision AND approved_revision=expected_draft_revision+1",
            name="registry_approval_draft",
        ),
        CheckConstraint(
            "jsonb_typeof(actor_json)='object' AND jsonb_typeof(selection_json)='array'", name="registry_approval_json"
        ),
        CheckConstraint("btrim(reason)<>'' AND btrim(idempotency_key)<>''", name="registry_approval_reason"),
        CheckConstraint("request_sha256 ~ '^[0-9a-f]{64}$'", name="registry_approval_digest"),
        UniqueConstraint("actor_key", "idempotency_key", name="registry_approval_replay"),
        {"schema": _SCHEMA},
    )
    approved_revision = Column(BigInteger, primary_key=True)
    previous_approved_revision = Column(BigInteger, nullable=False)
    expected_draft_revision = Column(BigInteger, nullable=False)
    actor_key = Column(String(128), nullable=False)
    actor_json = Column(JSONB, nullable=False)
    selection_json = Column(JSONB, nullable=False)
    reason = Column(String(1000), nullable=False)
    idempotency_key = Column(String(128), nullable=False)
    request_sha256 = Column(String(64), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class RegistryApprovedRecord(Base):
    """Copy the previous approved selection, replacing only explicitly approved items."""

    __tablename__ = "registry_approved_record"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "record_kind IN ('group','company','network','company_links','provider','location','membership','site_binding','network_binding')",
            name="registry_approved_kind",
        ),
        CheckConstraint(
            "approved_revision>0 AND record_revision>0 AND custom_revision>0 AND custom_revision<approved_revision",
            name="registry_approved_revision",
        ),
        CheckConstraint("jsonb_typeof(record_json)='object'", name="registry_approved_json"),
        {"schema": _SCHEMA},
    )
    approved_revision = Column(BigInteger, primary_key=True)
    record_kind = Column(String(16), primary_key=True)
    record_key = Column(String(36), primary_key=True)
    record_revision = Column(BigInteger, nullable=False)
    custom_revision = Column(BigInteger, nullable=False)
    record_json = Column(JSONB, nullable=False)
