# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Indexed source-scoped identities for retained plans and practitioner roles."""

import os

from sqlalchemy import TIMESTAMP, CheckConstraint, Column, Index, PrimaryKeyConstraint, String
from sqlalchemy.dialects.postgresql import UUID

from db.connection import Base

__all__ = ("ProviderDirectoryResourceIdentity",)


class ProviderDirectoryResourceIdentity(Base):
    """Continuity of an exact source resource, independent of release acceptance."""

    __runtime_schema_sync__ = False
    __tablename__ = "provider_directory_resource_identity"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_type", "resource_id"),
        CheckConstraint(
            "resource_type IN ('InsurancePlan', 'PractitionerRole')", name="pd_resource_identity_type_check"
        ),
        Index("pd_resource_identity_seek_idx", "source_id", "resource_type", "entity_id", unique=True),
        {"schema": os.getenv("HLTHPRT_DB_SCHEMA") or "mrf", "info": {"managed_immutable_storage": True}},
    )
    source_id = Column(String(64), nullable=False)
    resource_type = Column(String(64), nullable=False)
    resource_id = Column(String(64), nullable=False)
    entity_id = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
