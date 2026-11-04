# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-only immutable audit bindings and append-only structural checkpoints."""

import uuid

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Index,
    LargeBinary,
    SmallInteger,
    String,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.models.custom_import import _CustomImportModel, _table_args

__all__ = ("CustomImportPostSealAudit", "CustomImportPostSealAuditCheckpoint")


class CustomImportPostSealAudit(_CustomImportModel):
    """One attempt binds fixed SQL and the complete frozen producer/base identity."""

    __tablename__ = "custom_import_post_seal_audit"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        Index("custom_import_post_seal_audit_family_idx", "family_id", "started_at", "audit_id"),
        CheckConstraint(
            "family_id>0 AND build_id>0 AND generation_id>0 AND dataset_id>0 AND "
            "engine_id ~ '^[a-z][a-z0-9_]{0,62}$' AND "
            "octet_length(sql_sha256)=32 AND octet_length(binding_sha256)=32",
            name="custom_import_post_seal_audit_identity",
        ),
        CheckConstraint(
            "jsonb_typeof(binding)='object' AND "
            "binding->>'contract' IS NOT DISTINCT FROM 'custom-import/post-seal-structure/v1' AND "
            "pg_column_size(binding)<=262144",
            name="custom_import_post_seal_audit_binding",
        ),
    )

    audit_id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    family_id = Column(BigInteger, nullable=False)
    engine_id = Column(String(63), nullable=False)
    build_id = Column(BigInteger, nullable=False)
    generation_id = Column(BigInteger, nullable=False)
    dataset_id = Column(BigInteger, nullable=False)
    sql_sha256 = Column(LargeBinary(32), nullable=False)
    binding_sha256 = Column(LargeBinary(32), nullable=False)
    binding = Column(JSONB, nullable=False)
    started_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("clock_timestamp()"))


class CustomImportPostSealAuditCheckpoint(_CustomImportModel):
    """Only INSERT is used; a failed or stale prefix never becomes a complete audit."""

    __tablename__ = "custom_import_post_seal_audit_checkpoint"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint(
            "checkpoint>=0 AND arm_ordinal BETWEEN 0 AND 62 AND next_ordinal BETWEEN 1 AND 63 AND "
            "(root_after_id IS NULL OR root_after_id>0) AND "
            "octet_length(previous_sha256)=32 AND octet_length(checkpoint_sha256)=32",
            name="custom_import_post_seal_checkpoint_identity",
        ),
        CheckConstraint(
            "(state='scanning' AND failure_code IS NULL AND counts IS NULL AND next_ordinal<=62) OR "
            "(state='failed' AND failure_code IS NOT NULL AND counts IS NULL AND next_ordinal<=62) OR "
            "(state='complete' AND failure_code IS NULL AND arm_ordinal=62 AND next_ordinal=63 "
            "AND root_after_id IS NULL AND root_eof AND counts IS NOT NULL AND jsonb_typeof(counts)='object')",
            name="custom_import_post_seal_checkpoint_state",
        ),
        CheckConstraint(
            "state='failed' OR (next_ordinal=arm_ordinal+1 AND root_after_id IS NULL AND root_eof) OR "
            "(arm_ordinal IN (41,42) AND next_ordinal=arm_ordinal AND root_after_id IS NOT NULL AND NOT root_eof)",
            name="custom_import_post_seal_checkpoint_progress",
        ),
        CheckConstraint(
            "page_root_count BETWEEN 0 AND 16384 AND "
            "outcome IN ('started','passed','range_page','range_eof','not_applicable','counts','failed') AND "
            "(range_sha256 IS NULL OR octet_length(range_sha256)=32)",
            name="custom_import_post_seal_checkpoint_outcome",
        ),
    )

    audit_id = Column(UUID(as_uuid=True), primary_key=True)
    checkpoint = Column(BigInteger, primary_key=True, autoincrement=False)
    arm_ordinal = Column(SmallInteger, nullable=False)
    next_ordinal = Column(SmallInteger, nullable=False)
    root_after_id = Column(BigInteger)
    root_eof = Column(Boolean, nullable=False)
    outcome = Column(String(24), nullable=False)
    page_root_count = Column(SmallInteger, nullable=False)
    range_sha256 = Column(LargeBinary(32))
    state = Column(String(16), nullable=False)
    failure_code = Column(String(96))
    counts = Column(JSONB(none_as_null=True))
    previous_sha256 = Column(LargeBinary(32), nullable=False)
    checkpoint_sha256 = Column(LargeBinary(32), nullable=False)
    recorded_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=text("clock_timestamp()"))
