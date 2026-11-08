# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Canonical integer network allocation, editable heads and typed aliases."""

import os

from sqlalchemy import (
    TIMESTAMP,
    BigInteger,
    Boolean,
    CheckConstraint,
    Column,
    Identity,
    Index,
    Integer,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("NetworkRegistryIdentity", "NetworkRegistryRecord", "NetworkRegistryAlias")
_SCHEMA = registry_schema()


class NetworkRegistryIdentity(Base):
    """Never delete or reassign these IDs; archive the editable record instead."""

    __tablename__ = "network_registry_identity"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("network_id > 0", name="registry_network_positive"),
        UniqueConstraint("allocation_key", name="registry_network_allocation_key"),
        {"schema": _SCHEMA},
    )

    network_id = Column(
        Integer,
        Identity(always=True, minvalue=1, maxvalue=2147483647, cycle=False),
        primary_key=True,
    )
    allocation_key = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class NetworkRegistryRecord(Base):
    """Management head; public reads require an approved serving generation."""

    __tablename__ = "network_registry_record"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("network_id > 0", name="registry_network_record_positive"),
        CheckConstraint("btrim(display_name) <> ''", name="registry_network_name"),
        CheckConstraint("revision > 0", name="registry_network_revision"),
        CheckConstraint("jsonb_typeof(aliases) = 'array'", name="registry_network_aliases"),
        CheckConstraint(
            "catalog_evidence_json IS NULL OR (jsonb_typeof(catalog_evidence_json) = 'object' "
            "AND octet_length(catalog_evidence_json::text) <= 32768)",
            name="registry_network_catalog_evidence",
        ),
        {"schema": _SCHEMA},
    )

    network_id = Column(Integer, primary_key=True)
    display_name = Column(String(512), nullable=False)
    aliases = Column(JSONB, nullable=False, server_default="[]")
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
    catalog_evidence_json = Column(JSONB(none_as_null=True), nullable=True)


class NetworkRegistryAlias(Base):
    """An exact reviewed source alias; name equality is never an identity key."""

    __tablename__ = "network_registry_alias"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("network_id > 0", name="registry_network_alias_positive"),
        CheckConstraint("btrim(source_system) <> '' AND btrim(source_id) <> ''", name="registry_alias_source"),
        CheckConstraint("btrim(alias_type) <> '' AND btrim(alias_value) <> ''", name="registry_alias_value"),
        CheckConstraint("btrim(scope_key) <> '' AND btrim(evidence_id) <> ''", name="registry_alias_evidence"),
        Index("registry_network_alias_network_idx", "network_id"),
        {"schema": _SCHEMA},
    )

    source_system = Column(String(64), primary_key=True)
    source_id = Column(String(128), primary_key=True)
    alias_type = Column(String(64), primary_key=True)
    alias_value = Column(String(512), primary_key=True)
    scope_key = Column(String(512), primary_key=True)
    network_id = Column(Integer, nullable=False)
    evidence_id = Column(String(512), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
