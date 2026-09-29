# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable entity IDs bound to exact source-scoped FHIR resource identities."""

from __future__ import annotations

import os

from sqlalchemy import (
    TIMESTAMP,
    CheckConstraint,
    Column,
    ForeignKey,
    ForeignKeyConstraint,
    Index,
    PrimaryKeyConstraint,
    String,
    UniqueConstraint,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base

__all__ = (
    "ProviderDirectoryOrganizationIdentity",
    "ProviderDirectorySiteIdentity",
    "ProviderDirectoryEntitySourceBinding",
    "ProviderDirectoryEntityReleaseEvidence",
    "ProviderDirectoryCMSDoctorsGroupBinding",
    "ProviderDirectoryCMSDoctorsSiteBinding",
)

_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


class ProviderDirectoryOrganizationIdentity(Base):
    __tablename__ = "provider_directory_organization_identity"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("organization_id"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    organization_id = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectorySiteIdentity(Base):
    __tablename__ = "provider_directory_site_identity"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("site_id"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    site_id = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryEntitySourceBinding(Base):
    """One enduring binding per source/type/resource, independent of labels."""

    __tablename__ = "provider_directory_entity_source_binding"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_type", "resource_id"),
        CheckConstraint(
            "(resource_type = 'Organization' AND organization_id IS NOT NULL AND site_id IS NULL) OR "
            "(resource_type = 'Location' AND site_id IS NOT NULL AND organization_id IS NULL)",
            name="provider_directory_entity_binding_kind_check",
        ),
        Index("provider_directory_entity_binding_organization_idx", "organization_id"),
        Index("provider_directory_entity_binding_site_idx", "site_id"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    source_id = Column(String(64), nullable=False)
    resource_type = Column(String(16), nullable=False)
    resource_id = Column(String(256), nullable=False)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{_SCHEMA}.provider_directory_organization_identity.organization_id", ondelete="RESTRICT"),
    )
    site_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{_SCHEMA}.provider_directory_site_identity.site_id", ondelete="RESTRICT"),
    )
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryEntityReleaseEvidence(Base):
    """Immutable source facts for each observed publication release."""

    __tablename__ = "provider_directory_entity_release_evidence"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_type", "resource_id", "release_id"),
        ForeignKeyConstraint(
            ("source_id", "resource_type", "resource_id"),
            (
                f"{_SCHEMA}.provider_directory_entity_source_binding.source_id",
                f"{_SCHEMA}.provider_directory_entity_source_binding.resource_type",
                f"{_SCHEMA}.provider_directory_entity_source_binding.resource_id",
            ),
            ondelete="RESTRICT",
            name="provider_directory_entity_evidence_binding_fkey",
        ),
        Index("provider_directory_entity_evidence_release_idx", "source_id", "release_id"),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    source_id = Column(String(64), nullable=False)
    resource_type = Column(String(16), nullable=False)
    resource_id = Column(String(256), nullable=False)
    release_id = Column(String(256), nullable=False)
    payload_sha256 = Column(String(64), nullable=False)
    payload_json = Column(JSONB, nullable=False)
    observed_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryCMSDoctorsGroupBinding(Base):
    """Exact CMS Doctors Org_PAC_ID binding, separate from FHIR resources."""

    __tablename__ = "provider_directory_cms_doctors_group_binding"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("org_pac_id"),
        UniqueConstraint("organization_id"),
        CheckConstraint(
            "org_pac_id <> '' AND org_pac_id = btrim(org_pac_id)",
            name="provider_directory_cms_doctors_group_pac_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    org_pac_id = Column(String(64), nullable=False)
    organization_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{_SCHEMA}.provider_directory_organization_identity.organization_id", ondelete="RESTRICT"),
        nullable=False,
    )
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryCMSDoctorsSiteBinding(Base):
    """Exact CMS Doctors adrs_id binding, separate from FHIR Locations."""

    __tablename__ = "provider_directory_cms_doctors_site_binding"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("adrs_id"),
        UniqueConstraint("site_id"),
        CheckConstraint(
            "adrs_id <> '' AND adrs_id = btrim(adrs_id)",
            name="provider_directory_cms_doctors_site_address_check",
        ),
        {"schema": _SCHEMA, "extend_existing": True},
    )

    adrs_id = Column(String(256), nullable=False)
    site_id = Column(
        UUID(as_uuid=True),
        ForeignKey(f"{_SCHEMA}.provider_directory_site_identity.site_id", ondelete="RESTRICT"),
        nullable=False,
    )
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)
