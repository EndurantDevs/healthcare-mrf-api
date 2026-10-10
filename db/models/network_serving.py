# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Low-volume controls for isolated network membership generations."""

import os

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    ForeignKey,
    Identity,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("NetworkMembershipCandidate", "NetworkMembershipBatch", "NetworkServingManifest", "NetworkServingControl")
_SCHEMA = registry_schema()


class NetworkMembershipCandidate(Base):
    """An admitted producer's isolated snapshot, never a mutable serving table."""

    __tablename__ = "network_membership_candidate"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "state IN ('open','sealed','validated','ready','published','rejected')", name="network_candidate_state"
        ),
        CheckConstraint(
            "schema_name = 'network_candidate_' || replace(candidate_id::text,'-','')", name="network_candidate_schema"
        ),
        CheckConstraint(
            "schema_revision > 0 AND approved_custom_revision >= 0 AND expected_head >= 0",
            name="network_candidate_revisions",
        ),
        CheckConstraint(
            "expected_rows >= 0 AND accepted_rows >= 0 AND accepted_rows <= expected_rows",
            name="network_candidate_accounting",
        ),
        CheckConstraint("jsonb_typeof(source_generations) = 'object'", name="network_candidate_sources"),
        CheckConstraint(
            "jsonb_typeof(source_recipes_json)='array' AND jsonb_array_length(source_recipes_json)<=100 "
            "AND octet_length(source_recipes_json::text)<=1048576",
            name="network_candidate_source_recipes",
        ),
        CheckConstraint(
            "state NOT IN ('validated','ready','published') OR validation_json IS NOT NULL",
            name="network_candidate_validated",
        ),
        CheckConstraint("state NOT IN ('ready','published') OR index_ready", name="network_candidate_indexes"),
        CheckConstraint(
            "validation_json IS NULL OR jsonb_typeof(validation_json) = 'object'", name="network_candidate_validation"
        ),
        CheckConstraint(
            "candidate_id <> '00000000-0000-0000-0000-000000000000'::uuid AND dataset_id <> '00000000-0000-0000-0000-000000000000'::uuid AND schema_id <> '00000000-0000-0000-0000-000000000000'::uuid AND producer_id <> '00000000-0000-0000-0000-000000000000'::uuid",
            name="network_candidate_identity",
        ),
        UniqueConstraint("schema_name", name="network_candidate_schema_unique"),
        {"schema": _SCHEMA},
    )

    candidate_id = Column(UUID(as_uuid=True), primary_key=True)
    dataset_id = Column(UUID(as_uuid=True), nullable=False)
    schema_id = Column(UUID(as_uuid=True), nullable=False)
    producer_id = Column(UUID(as_uuid=True), nullable=False)
    schema_name = Column(String(64), nullable=False)
    state = Column(String(16), nullable=False, server_default="open")
    schema_revision = Column(BigInteger, nullable=False, server_default="1")
    source_generations = Column(JSONB, nullable=False)
    approved_custom_revision = Column(BigInteger, nullable=False)
    expected_head = Column(BigInteger, nullable=False)
    expected_rows = Column(BigInteger, nullable=False)
    accepted_rows = Column(BigInteger, nullable=False, server_default="0")
    validation_json = Column(JSONB)
    index_ready = Column(Boolean, nullable=False, server_default="false")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    source_recipes_json = Column(JSONB, nullable=False, server_default="[]")


class NetworkMembershipBatch(Base):
    """Only complete COPY receipts enter this immutable batch ledger."""

    __tablename__ = "network_membership_batch"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("row_count BETWEEN 0 AND 5000", name="network_batch_rows"),
        CheckConstraint(
            "input_bytes BETWEEN 0 AND 8388608 AND copy_bytes BETWEEN 21 AND 16777216", name="network_batch_bytes"
        ),
        CheckConstraint(
            "input_sha256 ~ '^[0-9a-f]{64}$' AND copy_sha256 ~ '^[0-9a-f]{64}$'", name="network_batch_digests"
        ),
        {"schema": _SCHEMA},
    )

    candidate_id = Column(
        UUID(as_uuid=True), ForeignKey(f"{_SCHEMA}.network_membership_candidate.candidate_id"), primary_key=True
    )
    batch_id = Column(UUID(as_uuid=True), primary_key=True)
    row_count = Column(Integer, nullable=False)
    input_sha256 = Column(String(64), nullable=False)
    copy_sha256 = Column(String(64), nullable=False)
    input_bytes = Column(Integer, nullable=False)
    copy_bytes = Column(Integer, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class NetworkServingManifest(Base):
    """Retained manifests bind a sealed candidate and approved custom data."""

    __tablename__ = "network_serving_manifest"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "generation_id > 0 AND schema_revision > 0 AND approved_custom_revision >= 0",
            name="network_manifest_revisions",
        ),
        CheckConstraint("manifest_sha256 ~ '^[0-9a-f]{64}$'", name="network_manifest_digest"),
        CheckConstraint("jsonb_typeof(source_generations) = 'object'", name="network_manifest_sources"),
        UniqueConstraint("candidate_id", name="network_manifest_candidate"),
        {"schema": _SCHEMA},
    )

    generation_id = Column(BigInteger, Identity(always=True, minvalue=1, cycle=False), primary_key=True)
    candidate_id = Column(
        UUID(as_uuid=True), ForeignKey(f"{_SCHEMA}.network_membership_candidate.candidate_id"), nullable=False
    )
    schema_revision = Column(BigInteger, nullable=False)
    source_generations = Column(JSONB, nullable=False)
    approved_custom_revision = Column(BigInteger, nullable=False)
    manifest_sha256 = Column(String(64), nullable=False)
    eligible = Column(Boolean, nullable=False, server_default="true")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class NetworkServingControl(Base):
    __tablename__ = "network_serving_control"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("id = 1", name="network_serving_singleton"),
        CheckConstraint("generation_id IS NULL OR generation_id > 0", name="network_serving_generation"),
        {"schema": _SCHEMA},
    )

    id = Column(Integer, primary_key=True, autoincrement=False)
    generation_id = Column(BigInteger, ForeignKey(f"{_SCHEMA}.network_serving_manifest.generation_id"))
