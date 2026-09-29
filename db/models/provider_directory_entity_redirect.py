# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed, source-scoped organization/site aliases without rewriting source facts."""

import os

from sqlalchemy import TIMESTAMP, CheckConstraint, Column, ForeignKeyConstraint, Index, String, UniqueConstraint
from sqlalchemy.dialects.postgresql import UUID

from db.connection import Base

__all__ = ("ProviderDirectoryEntityRedirectDecision", "ProviderDirectoryEntityRedirect")
_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


class ProviderDirectoryEntityRedirectDecision(Base):
    """Immutable review of an exact pair of source-bound entity identities."""

    __tablename__ = "provider_directory_entity_redirect_decision"
    __main_table__ = __tablename__
    __table_args__ = (
        UniqueConstraint("decision_id", "source_id", "resource_type", "old_entity_id", "canonical_entity_id"),
        UniqueConstraint("prior_decision_id"),
        CheckConstraint("resource_type IN ('Organization', 'Location')", name="pd_entity_redirect_kind_check"),
        CheckConstraint("old_entity_id <> canonical_entity_id", name="pd_entity_redirect_distinct_check"),
        CheckConstraint(
            "(action = 'redirect' AND prior_decision_id IS NULL) OR "
            "(action = 'close' AND prior_decision_id IS NOT NULL)",
            name="pd_entity_redirect_action_check",
        ),
        CheckConstraint(
            "old_payload_sha256 ~ '^[0-9a-f]{64}$' AND canonical_payload_sha256 ~ '^[0-9a-f]{64}$' "
            "AND review_receipt_sha256 ~ '^[0-9a-f]{64}$'",
            name="pd_entity_redirect_hash_check",
        ),
        CheckConstraint(
            "source_id <> '' AND source_id = btrim(source_id) "
            "AND old_resource_id <> '' AND old_resource_id = btrim(old_resource_id) "
            "AND canonical_resource_id <> '' AND canonical_resource_id = btrim(canonical_resource_id) "
            "AND old_release_id <> '' AND old_release_id = btrim(old_release_id) "
            "AND canonical_release_id <> '' AND canonical_release_id = btrim(canonical_release_id) "
            "AND review_receipt_id <> '' AND review_receipt_id = btrim(review_receipt_id) "
            "AND review_actor <> '' AND review_actor = btrim(review_actor)",
            name="pd_entity_redirect_text_check",
        ),
        *(
            ForeignKeyConstraint(
                ("source_id", "resource_type", f"{side}_resource_id", f"{side}_release_id"),
                tuple(
                    f"{_SCHEMA}.provider_directory_entity_release_evidence.{column}"
                    for column in ("source_id", "resource_type", "resource_id", "release_id")
                ),
                ondelete="RESTRICT",
                name=f"pd_entity_redirect_{side}_evidence_fkey",
            )
            for side in ("old", "canonical")
        ),
        ForeignKeyConstraint(
            ("prior_decision_id", "source_id", "resource_type", "old_entity_id", "canonical_entity_id"),
            tuple(
                f"{_SCHEMA}.provider_directory_entity_redirect_decision.{column}"
                for column in ("decision_id", "source_id", "resource_type", "old_entity_id", "canonical_entity_id")
            ),
            ondelete="RESTRICT",
            name="pd_entity_redirect_prior_fkey",
        ),
        Index("pd_entity_redirect_old_evidence_idx", "source_id", "resource_type", "old_resource_id", "old_release_id"),
        Index(
            "pd_entity_redirect_canonical_evidence_idx",
            "source_id",
            "resource_type",
            "canonical_resource_id",
            "canonical_release_id",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    decision_id = Column(UUID(as_uuid=True), primary_key=True)
    action = Column(String(8), nullable=False)
    source_id = Column(String(64), nullable=False)
    resource_type = Column(String(16), nullable=False)
    old_entity_id = Column(UUID(as_uuid=True), nullable=False)
    canonical_entity_id = Column(UUID(as_uuid=True), nullable=False)
    old_resource_id = Column(String(256), nullable=False)
    canonical_resource_id = Column(String(256), nullable=False)
    old_release_id = Column(String(256), nullable=False)
    canonical_release_id = Column(String(256), nullable=False)
    old_payload_sha256 = Column(String(64), nullable=False)
    canonical_payload_sha256 = Column(String(64), nullable=False)
    prior_decision_id = Column(UUID(as_uuid=True))
    review_receipt_id = Column(String(160), nullable=False)
    review_receipt_sha256 = Column(String(64), nullable=False)
    review_actor = Column(String(160), nullable=False)
    reviewed_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryEntityRedirect(Base):
    """One active, one-hop redirect within an exact source and entity kind."""

    __tablename__ = "provider_directory_entity_redirect"
    __main_table__ = __tablename__
    __table_args__ = (
        UniqueConstraint("decision_id"),
        CheckConstraint("resource_type IN ('Organization', 'Location')", name="pd_entity_redirect_active_kind_check"),
        CheckConstraint("old_entity_id <> canonical_entity_id", name="pd_entity_redirect_active_distinct_check"),
        ForeignKeyConstraint(
            ("decision_id", "source_id", "resource_type", "old_entity_id", "canonical_entity_id"),
            tuple(
                f"{_SCHEMA}.provider_directory_entity_redirect_decision.{column}"
                for column in ("decision_id", "source_id", "resource_type", "old_entity_id", "canonical_entity_id")
            ),
            ondelete="RESTRICT",
            name="pd_entity_redirect_active_decision_fkey",
        ),
        Index("pd_entity_redirect_target_idx", "source_id", "resource_type", "canonical_entity_id"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    source_id = Column(String(64), primary_key=True)
    resource_type = Column(String(16), primary_key=True)
    old_entity_id = Column(UUID(as_uuid=True), primary_key=True)
    canonical_entity_id = Column(UUID(as_uuid=True), nullable=False)
    decision_id = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
