# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Owner-only aggregate evidence for verified snapshot scratch cleanup."""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    CheckConstraint,
    Column,
    LargeBinary,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB

from db.models.custom_import import _CustomImportModel, _table_args, _timestamp_column

__all__ = ("CustomImportBuildScratchCleanup",)


class CustomImportBuildScratchCleanup(_CustomImportModel):
    """One exact snapshot's transactional cleanup receipt; no relationship grants."""

    __tablename__ = "custom_import_build_scratch_cleanup"
    __main_table__ = __tablename__
    __table_args__ = _table_args(
        CheckConstraint(
            "family_id>0 AND build_id>0 AND generation_id>0 AND dataset_id>0 AND "
            "definition_revision_id>0 AND schema_revision_id>0 AND execution_id>0 AND "
            "capture_bundle_id>0 AND producing_fence>0 AND "
            "engine_id ~ '^[a-z][a-z0-9_]{0,62}$' AND retained_bytes>=0 AND remaining_bytes>=0",
            name="custom_import_scratch_cleanup_identity",
        ),
        CheckConstraint(
            "octet_length(producing_token_sha256)=32 AND octet_length(materialization_sha256)=32 "
            "AND octet_length(effective_output_sha256)=32 AND octet_length(source_bundle_sha256)=32 "
            "AND octet_length(candidate_sha256)=32",
            name="custom_import_scratch_cleanup_digests",
        ),
        CheckConstraint(
            "jsonb_typeof(aggregate_evidence)='object' AND "
            "aggregate_evidence->>'contract' IS NOT DISTINCT FROM 'custom-import/build-scratch-cleanup/v1' "
            "AND pg_column_size(aggregate_evidence)<=65536",
            name="custom_import_scratch_cleanup_evidence",
        ),
    )

    family_id = Column(BigInteger, primary_key=True, autoincrement=False)
    engine_id = Column(String(63), nullable=False)
    build_id = Column(BigInteger, nullable=False)
    generation_id = Column(BigInteger, nullable=False)
    dataset_id = Column(BigInteger, nullable=False)
    definition_revision_id = Column(BigInteger, nullable=False)
    schema_revision_id = Column(BigInteger, nullable=False)
    execution_id = Column(BigInteger, nullable=False)
    capture_bundle_id = Column(BigInteger, nullable=False)
    producing_fence = Column(BigInteger, nullable=False)
    producing_token_sha256 = Column(LargeBinary(32), nullable=False)
    source_frozen_at = Column(TIMESTAMP(timezone=True), nullable=False)
    graph_frozen_at = Column(TIMESTAMP(timezone=True), nullable=False)
    output_frozen_at = Column(TIMESTAMP(timezone=True), nullable=False)
    verified_at = Column(TIMESTAMP(timezone=True), nullable=False)
    sealed_at = Column(TIMESTAMP(timezone=True), nullable=False)
    materialization_sha256 = Column(LargeBinary(32), nullable=False)
    effective_output_sha256 = Column(LargeBinary(32), nullable=False)
    source_bundle_sha256 = Column(LargeBinary(32), nullable=False)
    candidate_sha256 = Column(LargeBinary(32), nullable=False)
    retained_bytes = Column(BigInteger, nullable=False)
    remaining_bytes = Column(BigInteger, nullable=False)
    aggregate_evidence = Column(JSONB, nullable=False)
    cleaned_at = _timestamp_column()
