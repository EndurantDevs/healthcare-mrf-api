# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Durable group identities and editable heads, independent of source swaps."""

import os

from sqlalchemy import TIMESTAMP, BigInteger, Boolean, CheckConstraint, Column, String, func
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("CompanyGroupRegistry",)


class CompanyGroupRegistry(Base):
    __tablename__ = "company_group_registry"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("group_kind IN ('corporate_parent', 'naic_group')", name="registry_group_kind"),
        CheckConstraint("btrim(display_name) <> ''", name="registry_group_name"),
        CheckConstraint("revision > 0", name="registry_group_revision"),
        CheckConstraint("jsonb_typeof(aliases) = 'array'", name="registry_group_aliases"),
        {"schema": registry_schema()},
    )

    group_id = Column(UUID(as_uuid=True), primary_key=True)
    group_kind = Column(String(32), nullable=False)
    display_name = Column(String(512), nullable=False)
    aliases = Column(JSONB, nullable=False, server_default="[]")
    archived = Column(Boolean, nullable=False, server_default="false")
    revision = Column(BigInteger, nullable=False, server_default="1")
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
