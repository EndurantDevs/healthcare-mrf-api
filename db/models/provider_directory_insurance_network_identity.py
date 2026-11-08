# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Stable network IDs and release-specific FHIR InsurancePlan evidence."""

from __future__ import annotations

import os

from sqlalchemy import (
    TEXT,
    TIMESTAMP,
    CheckConstraint,
    Column,
    Index,
    PrimaryKeyConstraint,
    String,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base

__all__ = (
    "ProviderDirectoryInsuranceNetworkIdentity",
    "ProviderDirectoryInsuranceNetworkSourceBinding",
    "ProviderDirectoryInsuranceNetworkPlanEvidence",
)

_SCHEMA = os.getenv("HLTHPRT_DB_SCHEMA") or "mrf"


class ProviderDirectoryInsuranceNetworkIdentity(Base):
    __runtime_schema_sync__ = False
    __tablename__ = "provider_directory_insurance_network_identity"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("network_id"),
        {"schema": _SCHEMA, "extend_existing": True, "info": {"managed_immutable_storage": True}},
    )

    network_id = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryInsuranceNetworkSourceBinding(Base):
    """A network role for one exact source-scoped FHIR Organization."""

    __runtime_schema_sync__ = False

    __tablename__ = "provider_directory_insurance_network_source_binding"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "resource_id"),
        CheckConstraint("resource_type = 'Organization'", name="pd_insurance_network_org_type_check"),
        Index("pd_insurance_network_binding_id_idx", "network_id"),
        {"schema": _SCHEMA, "extend_existing": True, "info": {"managed_immutable_storage": True}},
    )

    source_id = Column(String(64), nullable=False)
    resource_type = Column(String(16), nullable=False)
    resource_id = Column(String(256), nullable=False)
    network_id = Column(UUID(as_uuid=True), nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), nullable=False)


class ProviderDirectoryInsuranceNetworkPlanEvidence(Base):
    """Exact plan/network and insurer references as seen in one release."""

    __runtime_schema_sync__ = False

    __tablename__ = "provider_directory_insurance_network_plan_evidence"
    __main_table__ = __tablename__
    __table_args__ = (
        PrimaryKeyConstraint("source_id", "release_id", "network_resource_id", "insurance_plan_resource_id"),
        CheckConstraint("network_resource_type = 'Organization'", name="pd_insurance_network_plan_org_type_check"),
        Index(
            "pd_insurance_network_plan_release_idx",
            "source_id",
            "release_id",
            "insurance_plan_resource_id",
        ),
        {"schema": _SCHEMA, "extend_existing": True, "info": {"managed_immutable_storage": True}},
    )

    source_id = Column(String(64), nullable=False)
    release_id = Column(String(256), nullable=False)
    network_resource_type = Column(String(16), nullable=False)
    network_resource_id = Column(String(256), nullable=False)
    insurance_plan_resource_id = Column(String(256), nullable=False)
    network_refs = Column(JSONB, nullable=False)
    owned_by_ref = Column(TEXT)
    administered_by_ref = Column(TEXT)
    plan_payload_sha256 = Column(String(64), nullable=False)
    plan_payload_json = Column(JSONB, nullable=False)
    observed_at = Column(TIMESTAMP(timezone=True), nullable=False)
