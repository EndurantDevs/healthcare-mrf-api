# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Manual provider and site identities live outside replaceable source tables."""

import os

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, CheckConstraint, Column, String, UniqueConstraint, func, text
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("ManualProviderRegistry", "ManualLocationRegistry", "ManualProviderLocationBinding")
_SCHEMA = registry_schema()


class ManualProviderRegistry(Base):
    """A manually maintained provider may have no national identifier."""

    __tablename__ = "manual_provider_registry"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("provider_id<>'00000000-0000-0000-0000-000000000000'::uuid", name="manual_provider_identity"),
        CheckConstraint("provider_kind IN ('individual','organization')", name="manual_provider_kind"),
        CheckConstraint("btrim(display_name)<>'' AND revision>0", name="manual_provider_record"),
        CheckConstraint("npi IS NULL OR npi ~ '^[12][0-9]{9}$'", name="manual_provider_npi"),
        CheckConstraint(
            "cardinality(aliases)<=100 AND array_position(aliases,NULL) IS NULL", name="manual_provider_aliases"
        ),
        UniqueConstraint("npi", name="manual_provider_npi_unique"),
        {"schema": _SCHEMA},
    )

    provider_id = Column(UUID(as_uuid=True), primary_key=True)
    display_name = Column(String(256), nullable=False)
    provider_kind = Column(String(16), nullable=False)
    aliases = Column(ARRAY(String(512)), nullable=False, server_default=text("'{}'::varchar[]"))
    npi = Column(String(10))
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class ManualLocationRegistry(Base):
    """Site UUIDs remain distinct even when the canonical street address agrees."""

    __tablename__ = "manual_location_registry"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("location_id<>'00000000-0000-0000-0000-000000000000'::uuid", name="manual_location_identity"),
        CheckConstraint("btrim(display_name)<>'' AND revision>0", name="manual_location_record"),
        CheckConstraint(
            "jsonb_typeof(address_json)='object' AND jsonb_typeof(canonical_address_json)='object'",
            name="manual_location_address",
        ),
        CheckConstraint(
            "cardinality(aliases)<=100 AND array_position(aliases,NULL) IS NULL", name="manual_location_aliases"
        ),
        {"schema": _SCHEMA},
    )

    location_id = Column(UUID(as_uuid=True), primary_key=True)
    display_name = Column(String(256), nullable=False)
    aliases = Column(ARRAY(String(512)), nullable=False, server_default=text("'{}'::varchar[]"))
    address_json = Column(JSONB, nullable=False)
    canonical_address_json = Column(JSONB, nullable=False)
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class ManualProviderLocationBinding(Base):
    """An explicit provider/site pair has its own unified location key."""

    __tablename__ = "manual_provider_location_binding"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "provider_system IN ('npi','provider_directory','manual')", name="manual_binding_provider_system"
        ),
        CheckConstraint(
            "btrim(provider_id)<>'' AND btrim(entity_type)<>'' AND btrim(entity_id)<>''", name="manual_binding_provider"
        ),
        CheckConstraint("location_id<>'00000000-0000-0000-0000-000000000000'::uuid", name="manual_binding_location"),
        CheckConstraint("location_key ~ '^[0-9a-f]{64}$' AND revision>0", name="manual_binding_record"),
        UniqueConstraint("location_key", name="manual_binding_location_key"),
        {"schema": _SCHEMA},
    )

    provider_system = Column(String(32), primary_key=True)
    provider_id = Column(String(1024), primary_key=True)
    location_id = Column(UUID(as_uuid=True), primary_key=True)
    location_key = Column(String(64), nullable=False)
    entity_type = Column(String(64), nullable=False)
    entity_id = Column(String(128), nullable=False)
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
