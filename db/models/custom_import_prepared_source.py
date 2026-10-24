# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded replay metadata and parts, separate from immutable original objects."""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Column,
    Computed,
    Index,
    Integer,
    LargeBinary,
    SmallInteger,
    String,
    Text,
)

from db.models.custom_import import _CustomImportModel, _sha256_check, _table_args, _timestamp_column

__all__ = ("CustomImportPreparedSource", "CustomImportPreparedSourcePart")

PREPARED_SOURCE_CONTRACT = "custom-import/prepared-source-rows/v1"
PREPARED_PART_CONTRACT = "custom-import/prepared-source-part/v1"
MAX_PREPARED_PART_BYTES = 64 * 1024 * 1024
MAX_PREPARED_PARTS = 131_072
MAX_PREPARED_MANIFEST_BYTES = 2 * 1024 * 1024


class CustomImportPreparedSource(_CustomImportModel):
    """One preparation state; SOURCE may consume only its sealed representation."""

    __tablename__ = "custom_import_prepared_source"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint(
            "execution_id > 0 AND capture_bundle_id > 0 AND dataset_id > 0 AND "
            "definition_revision_id > 0 AND schema_revision_id > 0 AND source_binding_revision_id > 0 AND "
            "producer_fence > 0 AND version > 0",
            name="custom_import_prepared_source_identity_check",
        ),
        CheckConstraint(
            f"encoding_contract = '{PREPARED_SOURCE_CONTRACT}' AND "
            "octet_length(canonical_processing_policy) BETWEEN 2 AND 1048576 AND "
            f"octet_length(canonical_progress) BETWEEN 2 AND {MAX_PREPARED_MANIFEST_BYTES} AND "
            f"part_count BETWEEN 0 AND {MAX_PREPARED_PARTS} AND record_count >= 0 AND "
            "byte_count >= 0 AND manifest_byte_count >= 0 AND "
            + " AND ".join(
                _sha256_check(name)
                for name in (
                    "source_binding_sha256",
                    "request_identity_sha256",
                    "raw_manifest_sha256",
                    "processing_policy_sha256",
                    "producer_token_sha256",
                )
            ),
            name="custom_import_prepared_source_shape_check",
        ),
        CheckConstraint(
            "(state IN ('pending', 'closed') AND canonical_summary IS NULL AND "
            "prepared_sha256 IS NULL AND sealed_at IS NULL) OR "
            "(state = 'sealed' AND canonical_summary IS NOT NULL AND prepared_sha256 IS NOT NULL AND "
            f"octet_length(canonical_summary) BETWEEN 2 AND {MAX_PREPARED_MANIFEST_BYTES} AND "
            "octet_length(prepared_sha256) = 32 AND sealed_at IS NOT NULL AND "
            "prepared_sha256 = pg_catalog.sha256(convert_to(encoding_contract || ':' || canonical_summary, 'UTF8')))",
            name="custom_import_prepared_source_state_check",
        ),
        Index("custom_import_prepared_source_dataset_idx", "dataset_id"),
    )

    execution_id = Column(BigInteger, primary_key=True, autoincrement=False)
    capture_bundle_id = Column(BigInteger, nullable=False)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    source_binding_revision_id = Column(BigInteger, nullable=False)
    source_binding_sha256 = Column(LargeBinary(32), nullable=False)
    request_identity_sha256 = Column(LargeBinary(32), nullable=False)
    raw_manifest_sha256 = Column(LargeBinary(32), nullable=False)
    canonical_processing_policy = Column(Text, nullable=False)
    processing_policy_sha256 = Column(LargeBinary(32), nullable=False)
    encoding_contract = Column(String(63), nullable=False)
    state = Column(String(8), nullable=False)
    version = Column(BigInteger, nullable=False)
    producer_fence = Column(BigInteger, nullable=False)
    producer_token_sha256 = Column(LargeBinary(32), nullable=False)
    preparation_deadline_at = Column(TIMESTAMP(timezone=True), nullable=False)
    canonical_progress = Column(Text, nullable=False)
    part_count = Column(Integer, nullable=False)
    record_count = Column(BigInteger, nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    manifest_byte_count = Column(BigInteger, nullable=False)
    retained_byte_count = Column(
        BigInteger,
        Computed(
            "byte_count + manifest_byte_count + octet_length(canonical_processing_policy) + "
            "octet_length(canonical_progress) + coalesce(octet_length(canonical_summary), 0)",
            persisted=True,
        ),
        nullable=False,
    )
    canonical_summary = Column(Text)
    prepared_sha256 = Column(LargeBinary(32))
    sealed_at = Column(TIMESTAMP(timezone=True))
    created_at = _timestamp_column()


class CustomImportPreparedSourcePart(_CustomImportModel):
    """One immutable, bounded part; relationships are checked before sealing."""

    __tablename__ = "custom_import_prepared_source_part"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint(
            "execution_id > 0 AND dataset_id > 0 AND definition_revision_id > 0 AND schema_revision_id > 0 AND "
            f"stream_slot BETWEEN 1 AND 9 AND part_ordinal BETWEEN 1 AND {MAX_PREPARED_PARTS} AND "
            "first_source_ordinal > 0 AND record_count BETWEEN 0 AND 1000000 AND "
            "((record_count > 0 AND byte_count > 0) OR "
            "(record_count = 0 AND part_ordinal = 1 AND first_source_ordinal = 1 AND byte_count = 0))",
            name="custom_import_prepared_part_identity_check",
        ),
        CheckConstraint(
            f"payload_contract = '{PREPARED_SOURCE_CONTRACT}' AND byte_count BETWEEN 0 AND {MAX_PREPARED_PART_BYTES} AND "
            "octet_length(payload) = byte_count AND octet_length(payload_sha256) = 32 AND "
            "payload_sha256 = pg_catalog.sha256(payload) AND "
            f"octet_length(canonical_manifest) BETWEEN 2 AND {MAX_PREPARED_MANIFEST_BYTES} AND "
            "octet_length(manifest_sha256) = 32 AND "
            "manifest_sha256 = pg_catalog.sha256(convert_to(canonical_manifest, 'UTF8'))",
            name="custom_import_prepared_part_payload_check",
        ),
        Index("custom_import_prepared_part_dataset_idx", "dataset_id"),
    )

    execution_id = Column(BigInteger, primary_key=True, autoincrement=False)
    stream_slot = Column(SmallInteger, primary_key=True, autoincrement=False)
    part_ordinal = Column(Integer, primary_key=True, autoincrement=False)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    first_source_ordinal = Column(BigInteger, nullable=False)
    record_count = Column(Integer, nullable=False)
    payload_contract = Column(String(63), nullable=False)
    byte_count = Column(BigInteger, nullable=False)
    payload = Column(LargeBinary, nullable=False)
    payload_sha256 = Column(LargeBinary(32), nullable=False)
    canonical_manifest = Column(Text, nullable=False)
    manifest_sha256 = Column(LargeBinary(32), nullable=False)
