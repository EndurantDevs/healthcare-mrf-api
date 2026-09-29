# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed links from exact CMS Organizations to existing payer identities."""

from __future__ import annotations

import os

from sqlalchemy import (
    TIMESTAMP,
    CheckConstraint,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import UUID

from db.connection import Base

__all__ = ("ProviderDirectoryMRFPayerReviewDecision", "ProviderDirectoryMRFPayerBinding")

_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


class ProviderDirectoryMRFPayerReviewDecision(Base):
    """An immutable insurer-company approval or closure with reviewed source facts."""

    __tablename__ = "provider_directory_mrf_payer_review_decision"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("decision_id"),
        UniqueConstraint("decision_id", "source_id", "resource_type", "resource_id", "payer_id"),
        UniqueConstraint("prior_decision_id"),
        CheckConstraint(
            "resource_type = 'Organization' AND source_id = 'cms-npd'", name="pd_mrf_payer_review_source_check"
        ),
        CheckConstraint(
            "(action = 'bind' AND prior_decision_id IS NULL) OR (action = 'close' AND prior_decision_id IS NOT NULL)",
            name="pd_mrf_payer_review_action_check",
        ),
        CheckConstraint(
            "review_receipt_sha256 ~ '^[0-9a-f]{64}$'",
            name="pd_mrf_payer_review_sha256_check",
        ),
        CheckConstraint(
            "source_payload_sha256 ~ '^[0-9a-f]{64}$'",
            name="pd_mrf_payer_source_sha256_check",
        ),
        ForeignKeyConstraint(
            ("source_id", "resource_type", "resource_id", "release_id"),
            (
                f"{_SCHEMA}.provider_directory_entity_release_evidence.source_id",
                f"{_SCHEMA}.provider_directory_entity_release_evidence.resource_type",
                f"{_SCHEMA}.provider_directory_entity_release_evidence.resource_id",
                f"{_SCHEMA}.provider_directory_entity_release_evidence.release_id",
            ),
            ondelete="RESTRICT",
            name="pd_mrf_payer_review_release_fkey",
        ),
        ForeignKeyConstraint(
            ("prior_decision_id", "source_id", "resource_type", "resource_id", "payer_id"),
            (
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.decision_id",
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.source_id",
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.resource_type",
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.resource_id",
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.payer_id",
            ),
            ondelete="RESTRICT",
            name="pd_mrf_payer_review_prior_fkey",
        ),
        Index("pd_mrf_payer_review_source_idx", "source_id", "resource_id", "reviewed_at"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    decision_id = Column(UUID(as_uuid=True), nullable=False)
    action = Column(String(8), nullable=False)
    source_id = Column(String(64), nullable=False)
    resource_type = Column(String(16), nullable=False)
    resource_id = Column(String(256), nullable=False)
    release_id = Column(String(256), nullable=False)
    source_payload_sha256 = Column(String(64), nullable=False)
    payer_id = Column(String(64), ForeignKey(f"{_SCHEMA}.mrf_payer.payer_id", ondelete="RESTRICT"), nullable=False)
    prior_decision_id = Column(UUID(as_uuid=True))
    review_receipt_id = Column(String(160), nullable=False)
    review_receipt_sha256 = Column(String(64), nullable=False)
    review_actor = Column(String(160), nullable=False)
    reviewed_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryMRFPayerBinding(Base):
    """The active confirmed-company pointer to an existing payer identity."""

    __tablename__ = "provider_directory_mrf_payer_binding"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        UniqueConstraint("binding_decision_id"),
        CheckConstraint(
            "resource_type = 'Organization' AND source_id = 'cms-npd'", name="pd_mrf_payer_binding_source_check"
        ),
        ForeignKeyConstraint(
            ("source_id", "resource_type", "resource_id"),
            (
                f"{_SCHEMA}.provider_directory_entity_source_binding.source_id",
                f"{_SCHEMA}.provider_directory_entity_source_binding.resource_type",
                f"{_SCHEMA}.provider_directory_entity_source_binding.resource_id",
            ),
            ondelete="RESTRICT",
            name="pd_mrf_payer_binding_organization_fkey",
        ),
        ForeignKeyConstraint(
            ("binding_decision_id", "source_id", "resource_type", "resource_id", "payer_id"),
            (
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.decision_id",
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.source_id",
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.resource_type",
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.resource_id",
                f"{_SCHEMA}.provider_directory_mrf_payer_review_decision.payer_id",
            ),
            ondelete="RESTRICT",
            name="pd_mrf_payer_binding_decision_fkey",
        ),
        Index("pd_mrf_payer_binding_payer_idx", "payer_id"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    source_id = Column(String(64), nullable=False)
    resource_type = Column(String(16), nullable=False)
    resource_id = Column(String(256), nullable=False)
    payer_id = Column(String(64), ForeignKey(f"{_SCHEMA}.mrf_payer.payer_id", ondelete="RESTRICT"), nullable=False)
    binding_decision_id = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
