# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Reviewed exact provider/site bindings retain their server-resolved source proof."""

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, CheckConstraint, Column, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("RegistrySiteBinding",)


class RegistrySiteBinding(Base):
    """A draft binding head is separate from its retained immutable source rows."""

    __tablename__ = "registry_site_binding"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "binding_id<>'00000000-0000-0000-0000-000000000000'::uuid", name="registry_site_binding_identity"
        ),
        CheckConstraint(
            "location_id<>'00000000-0000-0000-0000-000000000000'::uuid", name="registry_site_binding_location"
        ),
        CheckConstraint("source_generation>0 AND revision>0", name="registry_site_binding_revision"),
        CheckConstraint("provider_system IN ('npi','provider_directory')", name="registry_site_binding_system"),
        CheckConstraint(
            "provider_id<>'' AND provider_id=btrim(provider_id) AND provider_id !~ '[[:cntrl:]]'",
            name="registry_site_binding_provider",
        ),
        CheckConstraint("provider_system<>'npi' OR provider_id ~ '^[12][0-9]{9}$'", name="registry_site_binding_npi"),
        CheckConstraint(
            "location_key ~ '^[0-9a-f]{64}$' AND address_row_sha256 ~ '^[0-9a-f]{64}$'",
            name="registry_site_binding_hashes",
        ),
        CheckConstraint(
            "jsonb_typeof(source_receipt_json)='object' AND octet_length(source_receipt_json::text)<=65536",
            name="registry_site_binding_receipt",
        ),
        {"schema": registry_schema()},
    )

    binding_id = Column(UUID(as_uuid=True), primary_key=True)
    source_generation = Column(BigInteger, nullable=False)
    provider_system = Column(String(32), nullable=False)
    provider_id = Column(String(128), nullable=False)
    location_id = Column(UUID(as_uuid=True), nullable=False)
    location_key = Column(String(64), nullable=False)
    address_row_sha256 = Column(String(64), nullable=False)
    source_receipt_json = Column(JSONB, nullable=False)
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
