# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Company revision group evidence stays immutable and separate from source rows."""

from sqlalchemy import CheckConstraint, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateIndex, CreateTable

from db.models import company_registry_assertions as assertions
from db.models.company_registry import CompanyRegistry
from db.models.registry_evidence import RegistryCompanyGroupAssertion

_EXPECTED_COLUMNS = {
    "assertion_id": ("UUID", False),
    "company_id": ("UUID", False),
    "company_revision": ("BIGINT", False),
    "group_id": ("UUID", True),
    "group_kind": ("VARCHAR(32)", False),
    "relationship_kind": ("VARCHAR(32)", False),
    "valid_from": ("DATE", True),
    "valid_to": ("DATE", True),
    "provenance_kind": ("VARCHAR(32)", False),
    "evidence_ref": ("VARCHAR(128)", False),
    "source_snapshot_id": ("UUID", True),
    "source_record_key": ("VARCHAR(128)", True),
    "created_at": ("TIMESTAMP WITH TIME ZONE", False),
}


def test_group_evidence_keeps_revision_identity_nullable_periods_and_distinct_rows():
    model = assertions.CompanyRegistryGroupAssertion
    table = model.__table__
    dialect = postgresql.dialect()
    assert model.__name__ in assertions.__all__
    assert model.__runtime_schema_sync__ is False
    assert table.name == "company_registry_group_assertion"
    assert table.schema == CompanyRegistry.__table__.schema
    assert table is not RegistryCompanyGroupAssertion.__table__
    assert tuple(table.primary_key.columns.keys()) == ("assertion_id", "company_id", "company_revision")
    assert {column.name: (str(column.type.compile(dialect=dialect)), column.nullable) for column in table.c} == (
        _EXPECTED_COLUMNS
    )
    assert all(table.c[name].type.as_uuid for name in ("assertion_id", "company_id", "group_id", "source_snapshot_id"))
    assert str(table.c.created_at.server_default.arg) == "now()"
    assert not table.foreign_keys
    assert not any(isinstance(constraint, UniqueConstraint) for constraint in table.constraints)
    checks_by_name = {
        constraint.name: str(constraint.sqltext)
        for constraint in table.constraints
        if isinstance(constraint, CheckConstraint)
    }
    zero = "'00000000-0000-0000-0000-000000000000'::uuid"
    assert checks_by_name == {
        "company_group_assertion_identity": f"assertion_id<>{zero} AND company_id<>{zero}",
        "company_group_assertion_revision": "company_revision>0",
        "company_group_assertion_period": "valid_to IS NULL OR valid_to>=valid_from",
        "company_group_assertion_evidence": "evidence_ref<>'' AND evidence_ref=btrim(evidence_ref) AND evidence_ref !~ '[[:cntrl:]]' AND octet_length(evidence_ref)<=128",
        "company_group_assertion_provenance": "(provenance_kind='manual_reference' AND source_snapshot_id IS NULL AND source_record_key IS NULL) OR (provenance_kind='source_reference' AND source_snapshot_id IS NOT NULL AND source_snapshot_id<>"
        + zero
        + " AND source_record_key IS NOT NULL AND source_record_key<>'' AND source_record_key=btrim(source_record_key) AND source_record_key !~ '[[:cntrl:]]' AND octet_length(source_record_key)<=128)",
        "company_group_assertion_group_identity": f"group_id IS NULL OR group_id<>{zero}",
        "company_group_assertion_group_kind": "group_kind IN ('corporate_parent','naic_group')",
        "company_group_assertion_relationship": "relationship_kind IN ('reported_affiliation','verified_ownership','verified_no_affiliation')",
        "company_group_assertion_manual_group": "provenance_kind<>'manual_reference' OR group_id IS NOT NULL OR relationship_kind='verified_no_affiliation'",
        "company_group_assertion_no_affiliation": "relationship_kind<>'verified_no_affiliation' OR (group_id IS NULL AND valid_from IS NOT NULL AND provenance_kind='manual_reference')",
    }
    ddl = str(CreateTable(table).compile(dialect=dialect))
    assert "PRIMARY KEY (assertion_id, company_id, company_revision)" in ddl
    assert all(f"CONSTRAINT {name} CHECK" in ddl for name in checks_by_name)
    index = next(iter(table.indexes))
    assert len(table.indexes) == 1 and not index.unique
    assert tuple(index.columns.keys()) == ("company_id", "company_revision")
    assert "ON " + str(table.fullname) + " (company_id, company_revision)" in str(
        CreateIndex(index).compile(dialect=dialect)
    )
