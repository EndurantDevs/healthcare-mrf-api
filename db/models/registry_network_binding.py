# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed source bindings retain complete edition scope beside import swaps."""

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("RegistryNetworkBinding", "RegistryNetworkBindingBatch")
_SCHEMA = registry_schema()


class RegistryNetworkBinding(Base):
    """A scoped source key has one durable reviewed identity, including when closed."""

    __tablename__ = "registry_network_binding"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "binding_id<>'00000000-0000-0000-0000-000000000000'::uuid", name="registry_network_binding_identity"
        ),
        CheckConstraint("source_system IN ('aca','ptg','fhir')", name="registry_network_binding_system"),
        CheckConstraint(
            "btrim(source_id)<>'' AND btrim(dataset_schema)<>'' AND btrim(dataset_id)<>'' "
            "AND btrim(producer_id)<>'' AND btrim(edition_id)<>'' AND btrim(source_key)<>''",
            name="registry_network_binding_scope",
        ),
        CheckConstraint(
            "jsonb_typeof(source_scope_json)='object' AND octet_length(source_scope_json::text)<=16384",
            name="registry_network_binding_scope_json",
        ),
        CheckConstraint(
            "binding_key ~ '^[0-9a-f]{64}$' AND evidence_sha256 ~ '^[0-9a-f]{64}$' AND btrim(evidence_id)<>''",
            name="registry_network_binding_evidence",
        ),
        CheckConstraint("network_id>0 AND revision>0", name="registry_network_binding_revision"),
        UniqueConstraint("binding_key", name="registry_network_binding_key"),
        Index("registry_network_binding_network_idx", "network_id", "source_system", "archived"),
        {"schema": _SCHEMA},
    )

    binding_id = Column(UUID(as_uuid=True), primary_key=True)
    source_system = Column(String(16), nullable=False)
    source_id = Column(String(128), nullable=False)
    dataset_schema = Column(String(63), nullable=False)
    dataset_id = Column(String(128), nullable=False)
    producer_id = Column(String(128), nullable=False)
    edition_id = Column(String(128), nullable=False)
    source_key = Column(String(512), nullable=False)
    source_scope_json = Column(JSONB, nullable=False)
    binding_key = Column(String(64), nullable=False)
    network_id = Column(Integer, nullable=False)
    evidence_id = Column(String(512), nullable=False)
    evidence_sha256 = Column(String(64), nullable=False)
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class RegistryNetworkBindingBatch(Base):
    """Actor-bound immutable whole-batch replay evidence; never a serving head."""

    __tablename__ = "registry_network_binding_batch"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "btrim(actor_key)<>'' AND btrim(idempotency_key)<>''", name="registry_network_binding_batch_key"
        ),
        CheckConstraint("request_sha256 ~ '^[0-9a-f]{64}$'", name="registry_network_binding_batch_digest"),
        CheckConstraint(
            "jsonb_typeof(receipt_json)='object' AND octet_length(receipt_json::text)<=1048576",
            name="registry_network_binding_batch_receipt",
        ),
        {"schema": _SCHEMA},
    )

    actor_key = Column(String(128), primary_key=True)
    idempotency_key = Column(String(128), primary_key=True)
    request_sha256 = Column(String(64), nullable=False)
    receipt_json = Column(JSONB, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
