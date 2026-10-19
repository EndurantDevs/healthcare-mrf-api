# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Exact-build authorizations that retain immutable processing provenance."""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    ForeignKey,
    Index,
    LargeBinary,
    String,
    Text,
    UniqueConstraint,
)

from db.models.custom_import import _CustomImportModel, _reference, _sha256_check, _table_args, _timestamp_column

__all__ = ("CustomImportBuildContinuation",)


class CustomImportBuildContinuation(_CustomImportModel):
    """One immutable processing authorization; the lease selects its writer epoch."""

    __tablename__ = "custom_import_build_continuation"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        UniqueConstraint("authorization_id", name="custom_import_continuation_authorization_key"),
        UniqueConstraint("authorization_sha256", name="custom_import_continuation_digest_key"),
        UniqueConstraint("nonce_sha256", name="custom_import_continuation_nonce_key"),
        Index("custom_import_continuation_build_idx", "build_id", "continuation_id"),
        CheckConstraint(
            "contract = 'custom-import/build-continuation/v1' AND "
            "authorization_id ~ '^[0-9a-f]{64}$' AND producer_fence > 0 AND "
            "processing_deadline_removed AND "
            "((base_generation_id IS NULL AND base_pointer_version = 0) OR "
            "(base_generation_id > 0 AND base_pointer_version > 0)) AND "
            "(expected_authority_id IS NULL OR expected_authority_id <> continuation_id) AND "
            "octet_length(canonical_authorization) BETWEEN 1 AND 16384 AND "
            + " AND ".join(
                _sha256_check(name)
                for name in (
                    "producer_token_sha256",
                    "request_identity_sha256",
                    "worker_identity_sha256",
                    "predecessor_identity_sha256",
                    "nonce_sha256",
                    "authorization_sha256",
                )
            ),
            name="custom_import_continuation_shape",
        ),
    )

    continuation_id = Column(BigInteger, primary_key=True, autoincrement=True)
    contract = Column(String(48), nullable=False)
    authorization_id = Column(String(64), nullable=False)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    execution_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_execution", "execution_id"), ondelete="RESTRICT"),
        nullable=False,
    )
    build_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_build_attempt", "build_id"), ondelete="RESTRICT"),
        nullable=False,
    )
    snapshot_family_id = Column(
        BigInteger,
        ForeignKey(_reference("custom_import_snapshot_family", "family_id"), ondelete="RESTRICT"),
        nullable=False,
    )
    capture_bundle_id = Column(BigInteger, nullable=False)
    producer_fence = Column(BigInteger, nullable=False)
    producer_token_sha256 = Column(LargeBinary(32), nullable=False)
    request_identity_sha256 = Column(LargeBinary(32), nullable=False)
    expected_authority_id = Column(
        BigInteger, ForeignKey(_reference(__tablename__, "continuation_id"), ondelete="RESTRICT")
    )
    base_generation_id = Column(BigInteger)
    base_pointer_version = Column(BigInteger, nullable=False)
    worker_identity_sha256 = Column(LargeBinary(32), nullable=False)
    predecessor_identity_sha256 = Column(LargeBinary(32), nullable=False)
    nonce_sha256 = Column(LargeBinary(32), nullable=False)
    processing_deadline_removed = Column(Boolean, nullable=False)
    original_deadline_at = Column(TIMESTAMP(timezone=True), nullable=False)
    launch_expires_at = Column(TIMESTAMP(timezone=True), nullable=False)
    canonical_authorization = Column(Text, nullable=False)
    authorization_sha256 = Column(LargeBinary(32), nullable=False)
    created_at = _timestamp_column()
