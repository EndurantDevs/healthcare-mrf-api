# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable source assertions and explicit durable identifier resolutions."""

import os

from sqlalchemy import (
    TIMESTAMP,
    CheckConstraint,
    Column,
    Date,
    Index,
    Integer,
    SmallInteger,
    String,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = (
    "RegistrySourceSnapshot",
    "RegistrySourceObservation",
    "RegistryIdentifierObservation",
    "RegistryIdentifierBinding",
    "RegistryIssuerCompanyAssertion",
    "RegistryCompanyGroupAssertion",
)
_SCHEMA = registry_schema()
_STATUS = "resolution_status IN ('resolved','unresolved','conflicting')"
_IDENTIFIER = (
    "(entity_kind='company' AND ((identifier_system='ein' AND identifier_value ~ '^[0-9]{9}$') "
    "OR (identifier_system='naic_company' AND identifier_value ~ '^[0-9]{5}$' AND identifier_value<>'00000'))) "
    "OR (entity_kind='group' AND identifier_system='naic_group' AND identifier_value ~ '^[1-9][0-9]{0,63}$')"
)


class RegistrySourceSnapshot(Base):
    """Reporting period, publication and retrieval dates are separate evidence."""

    __tablename__ = "registry_source_snapshot"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "btrim(source_system)<>'' AND btrim(source_id)<>'' AND btrim(edition_id)<>'' AND btrim(parser_version)<>''",
            name="registry_snapshot_source",
        ),
        CheckConstraint(
            "artifact_sha256 ~ '^[0-9a-f]{64}$' AND input_sha256 ~ '^[0-9a-f]{64}$'", name="registry_snapshot_digests"
        ),
        CheckConstraint(
            "reporting_year IS NULL OR reporting_year BETWEEN 2010 AND 2100", name="registry_snapshot_year"
        ),
        CheckConstraint(
            "snapshot_id<>'00000000-0000-0000-0000-000000000000'::uuid AND btrim(source_url)<>''",
            name="registry_snapshot_identity",
        ),
        UniqueConstraint(
            "source_system",
            "source_id",
            "edition_id",
            "input_sha256",
            "parser_version",
            name="registry_snapshot_edition",
        ),
        {"schema": _SCHEMA},
    )

    snapshot_id = Column(UUID(as_uuid=True), primary_key=True)
    source_system = Column(String(64), nullable=False)
    source_id = Column(String(128), nullable=False)
    edition_id = Column(String(128), nullable=False)
    source_url = Column(String(2048), nullable=False)
    artifact_sha256 = Column(String(64), nullable=False)
    input_sha256 = Column(String(64), nullable=False)
    parser_version = Column(String(128), nullable=False)
    reporting_year = Column(SmallInteger)
    published_at = Column(TIMESTAMP(timezone=True))
    retrieved_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class RegistrySourceObservation(Base):
    """Retain every row, including missing identifiers and unresolved evidence."""

    __tablename__ = "registry_source_observation"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("source_row_number > 0 AND btrim(source_record_key)<>''", name="registry_observation_key"),
        CheckConstraint("status IN ('accepted','unresolved','rejected')", name="registry_observation_status"),
        CheckConstraint(
            "jsonb_typeof(observation_json)='object' AND jsonb_typeof(issues_json)='array'",
            name="registry_observation_json",
        ),
        Index("registry_observation_status_idx", "snapshot_id", "status"),
        {"schema": _SCHEMA},
    )

    snapshot_id = Column(UUID(as_uuid=True), primary_key=True)
    source_record_key = Column(String(128), primary_key=True)
    source_row_number = Column(Integer, nullable=False)
    status = Column(String(16), nullable=False)
    observation_json = Column(JSONB, nullable=False)
    issues_json = Column(JSONB, nullable=False)


class RegistryIdentifierObservation(Base):
    __tablename__ = "registry_identifier_observation"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("entity_kind IN ('group','company')", name="registry_identifier_kind"),
        CheckConstraint("identifier_system IN ('ein','naic_company','naic_group')", name="registry_identifier_system"),
        CheckConstraint("btrim(identifier_value)<>'' AND btrim(raw_value)<>''", name="registry_identifier_value"),
        CheckConstraint(_IDENTIFIER, name="registry_identifier_format"),
        CheckConstraint(_STATUS, name="registry_identifier_status"),
        CheckConstraint(
            "(resolution_status='resolved') = (entity_id IS NOT NULL)", name="registry_identifier_resolution"
        ),
        Index("registry_identifier_lookup_idx", "identifier_system", "identifier_value"),
        {"schema": _SCHEMA},
    )

    snapshot_id = Column(UUID(as_uuid=True), primary_key=True)
    source_record_key = Column(String(128), primary_key=True)
    identifier_system = Column(String(32), primary_key=True)
    identifier_value = Column(String(64), primary_key=True)
    entity_kind = Column(String(16), primary_key=True)
    raw_value = Column(String(128), nullable=False)
    entity_id = Column(UUID(as_uuid=True))
    resolution_status = Column(String(16), nullable=False)


class RegistryIdentifierBinding(Base):
    """A reviewed identifier binding supplies stable allocation across editions."""

    __tablename__ = "registry_identifier_binding"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint("entity_kind IN ('group','company')", name="registry_binding_kind"),
        CheckConstraint("identifier_system IN ('ein','naic_company','naic_group')", name="registry_binding_system"),
        CheckConstraint("btrim(identifier_value)<>'' AND btrim(evidence_key)<>''", name="registry_binding_evidence"),
        CheckConstraint(_IDENTIFIER, name="registry_binding_format"),
        CheckConstraint(
            "entity_id<>'00000000-0000-0000-0000-000000000000'::uuid",
            name="registry_binding_identity",
        ),
        Index("registry_binding_entity_idx", "entity_kind", "entity_id"),
        {"schema": _SCHEMA},
    )

    entity_kind = Column(String(16), primary_key=True)
    identifier_system = Column(String(32), primary_key=True)
    identifier_value = Column(String(64), primary_key=True)
    entity_id = Column(UUID(as_uuid=True), nullable=False)
    snapshot_id = Column(UUID(as_uuid=True), nullable=False)
    evidence_key = Column(String(128), nullable=False)


class RegistryIssuerCompanyAssertion(Base):
    """HIOS is state scoped; a source observation never becomes an ownership date."""

    __tablename__ = "registry_issuer_company_assertion"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "hios_issuer_id ~ '^[0-9]{5}$' AND hios_issuer_id<>'00000' AND state ~ '^[A-Z]{2}$'",
            name="registry_issuer_assertion_identity",
        ),
        CheckConstraint(_STATUS, name="registry_issuer_assertion_status"),
        CheckConstraint(
            "(resolution_status='resolved') = (company_id IS NOT NULL)", name="registry_issuer_assertion_resolution"
        ),
        CheckConstraint(
            "valid_to IS NULL OR valid_from IS NULL OR valid_to>=valid_from", name="registry_issuer_assertion_period"
        ),
        Index("registry_issuer_assertion_lookup_idx", "hios_issuer_id", "state"),
        {"schema": _SCHEMA},
    )

    snapshot_id = Column(UUID(as_uuid=True), primary_key=True)
    source_record_key = Column(String(128), primary_key=True)
    hios_issuer_id = Column(String(5), nullable=False)
    state = Column(String(2), nullable=False)
    company_id = Column(UUID(as_uuid=True))
    resolution_status = Column(String(16), nullable=False)
    valid_from = Column(Date)
    valid_to = Column(Date)


class RegistryCompanyGroupAssertion(Base):
    """Separate source-reported affiliation from independently verified ownership."""

    __tablename__ = "registry_company_group_assertion"
    __runtime_schema_sync__ = False
    __table_args__ = (
        CheckConstraint(
            "relationship_kind IN ('reported_affiliation','verified_ownership')", name="registry_group_assertion_kind"
        ),
        CheckConstraint(_STATUS, name="registry_group_assertion_status"),
        CheckConstraint(
            "(resolution_status='resolved') = (group_id IS NOT NULL)", name="registry_group_assertion_resolution"
        ),
        CheckConstraint(
            "valid_to IS NULL OR valid_from IS NULL OR valid_to>=valid_from", name="registry_group_assertion_period"
        ),
        Index("registry_group_assertion_lookup_idx", "company_id", "valid_from"),
        {"schema": _SCHEMA},
    )

    snapshot_id = Column(UUID(as_uuid=True), primary_key=True)
    source_record_key = Column(String(128), primary_key=True)
    company_id = Column(UUID(as_uuid=True), nullable=False)
    group_id = Column(UUID(as_uuid=True))
    relationship_kind = Column(String(32), nullable=False)
    resolution_status = Column(String(16), nullable=False)
    valid_from = Column(Date)
    valid_to = Column(Date)
