# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Inactive company revision assertions; source observations remain separate.

Registration, migration, protected persistence and approval are caller-owned.
Rows reuse CompanyRegistry.company_id and do not independently establish approval.
"""

from sqlalchemy import TIMESTAMP, BigInteger, CheckConstraint, Column, Date, String, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import UUID

from db.connection import Base
from db.registry_schema import registry_schema

__all__ = ("CompanyRegistryRoleAssertion", "CompanyRegistryIdentifierAssertion")
_SCHEMA = registry_schema()
_ZERO_UUID = "'00000000-0000-0000-0000-000000000000'::uuid"


def _text_check(column, maximum):
    return (
        f"{column}<>'' AND {column}=btrim({column}) AND {column} !~ '[[:cntrl:]]' AND octet_length({column})<={maximum}"
    )


def _common_checks(prefix):
    return (
        CheckConstraint(f"assertion_id<>{_ZERO_UUID} AND company_id<>{_ZERO_UUID}", name=prefix + "_identity"),
        CheckConstraint("company_revision>0", name=prefix + "_revision"),
        CheckConstraint("valid_to IS NULL OR valid_to>=valid_from", name=prefix + "_period"),
        CheckConstraint(_text_check("evidence_ref", 128), name=prefix + "_evidence"),
        CheckConstraint(
            "(provenance_kind='manual_reference' AND source_snapshot_id IS NULL AND source_record_key IS NULL) "
            "OR (provenance_kind='source_reference' AND source_snapshot_id IS NOT NULL "
            f"AND source_snapshot_id<>{_ZERO_UUID} AND source_record_key IS NOT NULL "
            f"AND {_text_check('source_record_key', 128)})",
            name=prefix + "_provenance",
        ),
    )


class CompanyRegistryRoleAssertion(Base):
    """Explicit dated role values belonging to one whole company draft revision."""

    __tablename__ = "company_registry_role_assertion"
    __runtime_schema_sync__ = False
    __table_args__ = (
        *_common_checks("company_role_assertion"),
        CheckConstraint("role IN ('insurer','employer','network_operator')", name="company_role_assertion_role"),
        UniqueConstraint(
            "company_id", "company_revision", "role", "valid_from", name="company_role_assertion_revision_key"
        ),
        {"schema": _SCHEMA},
    )

    assertion_id = Column(UUID(as_uuid=True), primary_key=True)
    company_id = Column(UUID(as_uuid=True), primary_key=True)
    company_revision = Column(BigInteger, primary_key=True)
    role = Column(String(32), nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date)
    provenance_kind = Column(String(32), nullable=False)
    evidence_ref = Column(String(128), nullable=False)
    source_snapshot_id = Column(UUID(as_uuid=True))
    source_record_key = Column(String(128))
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())


class CompanyRegistryIdentifierAssertion(Base):
    """Scoped declared references; external conflicts need persisted validation.

    Opaque references carry source evidence, without a canonical CMS namespace.
    HIOS issuer identity remains in its existing independent registry model.
    """

    __tablename__ = "company_registry_identifier_assertion"
    __runtime_schema_sync__ = False
    __table_args__ = (
        *_common_checks("company_identifier_assertion"),
        CheckConstraint(
            "identifier_system IN ('ein','naic_company','external_reference')",
            name="company_identifier_assertion_system",
        ),
        CheckConstraint(
            _text_check("identifier_scope", 128) + " AND " + _text_check("identifier_value", 64),
            name="company_identifier_assertion_value",
        ),
        CheckConstraint(
            "(identifier_system='ein' AND identifier_value ~ '^[0-9]{9}$' AND identifier_value<>'000000000') "
            "OR (identifier_system='naic_company' AND identifier_value ~ '^[0-9]{5}$' AND identifier_value<>'00000') "
            "OR (identifier_system='external_reference' AND provenance_kind='source_reference')",
            name="company_identifier_assertion_format",
        ),
        UniqueConstraint(
            "company_id",
            "company_revision",
            "identifier_system",
            "identifier_scope",
            "identifier_value",
            name="company_identifier_assertion_revision_key",
        ),
        {"schema": _SCHEMA},
    )

    assertion_id = Column(UUID(as_uuid=True), primary_key=True)
    company_id = Column(UUID(as_uuid=True), primary_key=True)
    company_revision = Column(BigInteger, primary_key=True)
    identifier_system = Column(String(32), nullable=False)
    identifier_scope = Column(String(128), nullable=False)
    identifier_value = Column(String(64), nullable=False)
    valid_from = Column(Date, nullable=False)
    valid_to = Column(Date)
    provenance_kind = Column(String(32), nullable=False)
    evidence_ref = Column(String(128), nullable=False)
    source_snapshot_id = Column(UUID(as_uuid=True))
    source_record_key = Column(String(128))
    created_at = Column(TIMESTAMP(timezone=True), nullable=False, server_default=func.now())
