# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Companies may have several roles and need no regulatory identifier."""

import os

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, CheckConstraint, Column, String, func
from sqlalchemy.dialects.postgresql import ARRAY, JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("CompanyRegistry", "HIOSIssuerRegistry")
_SCHEMA = registry_schema()


class CompanyRegistry(Base):
    __tablename__ = "company_registry"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("btrim(display_name) <> ''", name="registry_company_name"),
        CheckConstraint("revision > 0", name="registry_company_revision"),
        CheckConstraint("jsonb_typeof(aliases) = 'array'", name="registry_company_aliases"),
        CheckConstraint(
            "cardinality(roles) > 0 AND array_position(roles, NULL) IS NULL "
            "AND roles <@ ARRAY['insurer','employer','network_operator']::varchar[]",
            name="registry_company_roles",
        ),
        {"schema": _SCHEMA},
    )

    company_id = Column(UUID(as_uuid=True), primary_key=True)
    display_name = Column(String(512), nullable=False)
    roles = Column(ARRAY(String(32)), nullable=False)
    aliases = Column(JSONB, nullable=False, server_default="[]")
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class HIOSIssuerRegistry(Base):
    __tablename__ = "hios_issuer_registry"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("hios_issuer_id ~ '^[0-9]{5}$' AND hios_issuer_id<>'00000'", name="registry_hios_id"),
        CheckConstraint("business_state ~ '^[A-Z]{2}$'", name="registry_hios_state"),
        {"schema": _SCHEMA},
    )

    hios_issuer_id = Column(String(5), primary_key=True)
    business_state = Column(String(2), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
