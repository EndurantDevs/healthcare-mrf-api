# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure company proposals and native DDL metadata; no persisted authority claim."""

from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from datetime import datetime
from uuid import UUID

import pytest
from sqlalchemy import CheckConstraint, UniqueConstraint
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.models.company_registry import CompanyRegistry, HIOSIssuerRegistry
from db.models.company_registry_assertions import CompanyRegistryIdentifierAssertion, CompanyRegistryRoleAssertion
from db.models.registry_evidence import RegistryIdentifierBinding, RegistryIdentifierObservation
from process import company_registry_assertion_values as values

COMPANY = "11111111-1111-4111-8111-111111111111"
SNAPSHOT = "22222222-2222-4222-8222-222222222222"


def _provenance(kind="manual_reference"):
    return {
        "kind": kind,
        "evidence_ref": "reviewed-reference-a",
        "snapshot_id": SNAPSHOT if kind == "source_reference" else None,
        "source_record_key": "record-a" if kind == "source_reference" else None,
    }


def _role(ordinal=1, **changes):
    return {
        "assertion_id": str(UUID(int=ordinal)),
        "role": "insurer",
        "valid_from": "2025-01-01",
        "valid_to": None,
        "provenance": _provenance(),
    } | changes


def _identifier(ordinal=20, **changes):
    return {
        "assertion_id": str(UUID(int=ordinal)),
        "identifier_system": "naic_company",
        "identifier_scope": "declared-jurisdiction-a",
        "identifier_value": "12345",
        "valid_from": "2025-01-01",
        "valid_to": "2025-12-31",
        "provenance": _provenance(),
    } | changes


def _command(**changes):
    return {
        "contract": values.CONTRACT,
        "company_id": COMPANY,
        "expected_revision": 0,
        "role_assertions": [_role()],
    } | changes


def _parse(document):
    return values.validated_company_registry_assertions(document)


def test_manual_company_needs_no_external_identifier():
    command = _parse(_command())
    assert command.company_id == UUID(COMPANY)
    assert command.identifier_assertions == ()
    assert command.as_dict()["identifier_assertions"] == []
    assert command.role_assertions[0].role == "insurer"
    assert not {"actor", "authorized", "approved_revision", "client_id"}.intersection(command.as_dict())


def test_roles_and_periods_preserve_existing_identity():
    document = _command(
        role_assertions=[
            _role(3, role="network_operator", valid_to="2025-01-01"),
            _role(2, role="employer"),
            _role(1),
        ],
        identifier_assertions=[_identifier(), _identifier(21, identifier_system="ein", identifier_value="123456789")],
    )
    command = _parse(document)
    assert {entry.role for entry in command.role_assertions} == set(values.COMPANY_ROLES)
    assert command.company_id == UUID(COMPANY)
    assert command.role_assertions[-1].valid_from == command.role_assertions[-1].valid_to
    assert _parse(command.as_dict()) == command
    reversed_document = deepcopy(document)
    reversed_document["role_assertions"].reverse()
    reversed_document["identifier_assertions"].reverse()
    assert _parse(reversed_document).canonical_json() == command.canonical_json()


def test_scope_and_source_provenance_are_independent():
    external = _identifier(
        identifier_system="external_reference",
        identifier_value="opaque-company-reference",
        provenance=_provenance("source_reference"),
    )
    command = _parse(_command(identifier_assertions=[external]))
    retained = command.identifier_assertions[0]
    assert retained.identifier_scope != str(retained.provenance.snapshot_id)
    assert retained.provenance.snapshot_id == UUID(SNAPSHOT)
    assert retained.identifier_value == "opaque-company-reference"
    later = deepcopy(external)
    later["provenance"]["snapshot_id"] = str(UUID(int=300))
    assert (
        _parse(_command(identifier_assertions=[later])).identifier_assertions[0].identifier_scope
        == retained.identifier_scope
    )


def test_same_identifier_in_distinct_declared_scopes():
    command = _parse(
        _command(identifier_assertions=[_identifier(), _identifier(21, identifier_scope="declared-jurisdiction-b")])
    )
    assert len(command.identifier_assertions) == 2
    # Distinct companies may propose the same reference; persistence must resolve conflicts.
    second = _parse(_command(company_id=str(UUID(int=500)), identifier_assertions=[_identifier()]))
    assert second.company_id != command.company_id
    assert second.identifier_assertions[0].identifier_value == command.identifier_assertions[0].identifier_value


def test_values_are_deeply_immutable():
    document = _command(identifier_assertions=[_identifier()])
    command = _parse(document)
    document["role_assertions"][0]["provenance"]["evidence_ref"] = "changed"
    assert command.role_assertions[0].provenance.evidence_ref == "reviewed-reference-a"
    with pytest.raises(FrozenInstanceError):
        command.role_assertions[0].role = "employer"
    exported = command.as_dict()
    exported["identifier_assertions"][0]["identifier_value"] = "99999"
    assert command.identifier_assertions[0].identifier_value == "12345"
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        replace(command, role_assertions=list(command.role_assertions))


@pytest.mark.parametrize("level", ["command", "role", "identifier", "provenance"])
def test_unknown_fields_refuse_closed_documents(level):
    document = _command(identifier_assertions=[_identifier()])
    selected = {
        "command": document,
        "role": document["role_assertions"][0],
        "identifier": document["identifier_assertions"][0],
        "provenance": document["role_assertions"][0]["provenance"],
    }[level]
    selected["authorized"] = True
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        _parse(document)


@pytest.mark.parametrize("field", ["company_id", "assertion_id", "snapshot_id"])
@pytest.mark.parametrize("identity", [str(UUID(int=0)), "1" * 32, "not-a-uuid", "ABCDEFAB-CDEF-4ABC-8DEF-ABCDEFABCDEF"])
def test_invalid_uuid_refuses(field, identity):
    document = _command(role_assertions=[_role(provenance=_provenance("source_reference"))])
    if field == "company_id":
        document[field] = identity
    elif field == "assertion_id":
        document["role_assertions"][0][field] = identity
    else:
        document["role_assertions"][0]["provenance"][field] = identity
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        _parse(document)


@pytest.mark.parametrize(
    "period",
    [
        ("2025-02-30", None),
        ("20250101", None),
        (None, None),
        ("2025-01-02", "2025-01-01"),
        ("2025-01-01T00:00:00", None),
    ],
)
def test_invalid_periods_refuse(period):
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        _parse(_command(role_assertions=[_role(valid_from=period[0], valid_to=period[1])]))
    valid = _parse(_command()).role_assertions[0]
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        replace(valid, valid_from=datetime(2025, 1, 1))


@pytest.mark.parametrize(
    "provenance",
    [
        _provenance() | {"evidence_ref": ""},
        _provenance() | {"evidence_ref": " padded "},
        _provenance() | {"evidence_ref": "x\0y"},
        _provenance() | {"evidence_ref": "é" * 65},
        _provenance() | {"kind": "approved"},
        _provenance() | {"snapshot_id": SNAPSHOT},
        _provenance("source_reference") | {"snapshot_id": None},
        _provenance("source_reference") | {"source_record_key": ""},
    ],
)
def test_blank_or_malformed_provenance_refuses(provenance):
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        _parse(_command(role_assertions=[_role(provenance=provenance)]))


@pytest.mark.parametrize(
    "system,identifier",
    [
        ("hios_issuer", "12345"),
        ("cms_company", "123"),
        ("ein", "000000000"),
        ("ein", "12-3456789"),
        ("naic_company", "00000"),
        ("naic_company", "1234"),
        ("external_reference", ""),
        ("external_reference", "x" * 65),
    ],
)
def test_identifier_formats_do_not_invent_identity(system, identifier):
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        _parse(_command(identifier_assertions=[_identifier(identifier_system=system, identifier_value=identifier)]))


@pytest.mark.parametrize("role", ["payer", "issuer", "INSURER", " insurer", "", None])
def test_unsupported_roles_refuse(role):
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        _parse(_command(role_assertions=[_role(role=role)]))


@pytest.mark.parametrize("duplicate", ["role_key", "identifier_key", "cross_kind_id"])
def test_duplicate_scoped_command_keys_refuse(duplicate):
    document = _command(identifier_assertions=[_identifier()])
    if duplicate == "role_key":
        document["role_assertions"].append(_role(2, valid_to="2025-12-31"))
    elif duplicate == "identifier_key":
        document["identifier_assertions"].append(_identifier(21, valid_from="2026-01-01", valid_to=None))
    else:
        document["identifier_assertions"][0]["assertion_id"] = document["role_assertions"][0]["assertion_id"]
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        _parse(document)


@pytest.mark.parametrize("revision", [True, -1, values.MAX_EXPECTED_REVISION + 1, "0", 0.5])
def test_revision_bounds_refuse(revision):
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        _parse(_command(expected_revision=revision))


def test_command_cardinality_and_bytes_are_bounded():
    assert (
        _parse(_command(expected_revision=values.MAX_EXPECTED_REVISION)).expected_revision
        == values.MAX_EXPECTED_REVISION
    )
    for document in (
        _command(role_assertions=[]),
        _command(role_assertions=[_role()] * (values.MAX_ROLE_ASSERTIONS + 1)),
        _command(identifier_assertions=[_identifier()] * (values.MAX_IDENTIFIER_ASSERTIONS + 1)),
        _command(identifier_assertions=[_identifier(identifier_scope="")]),
    ):
        with pytest.raises(values.CompanyRegistryAssertionValueError):
            _parse(document)
    assert len(_parse(_command()).canonical_json()) <= values.MAX_DOCUMENT_BYTES
    source_provenance = _provenance("source_reference") | {"evidence_ref": '"' * 128, "source_record_key": '"' * 128}
    escaped_identifiers = [
        _identifier(
            ordinal + 20,
            identifier_system="external_reference",
            identifier_scope='"' * 128,
            identifier_value=f"{ordinal:03d}" + '"' * 61,
            provenance=source_provenance,
        )
        for ordinal in range(values.MAX_IDENTIFIER_ASSERTIONS)
    ]
    with pytest.raises(values.CompanyRegistryAssertionValueError):
        _parse(_command(identifier_assertions=escaped_identifiers))


@pytest.mark.parametrize("model", [CompanyRegistryRoleAssertion, CompanyRegistryIdentifierAssertion])
def test_native_metadata_preserves_company_revision(model):
    table = model.__table__
    assert table.schema == CompanyRegistry.__table__.schema
    assert model.__runtime_schema_sync__ is False
    assert set(table.primary_key.columns.keys()) == {"assertion_id", "company_id", "company_revision"}
    assert table.c.company_id.type.as_uuid is True
    assert table.c.valid_from.nullable is False and table.c.valid_to.nullable is True
    assert table.c.evidence_ref.nullable is False and table.c.evidence_ref.type.length == 128
    assert table.c.created_at.type.timezone is True
    assert not table.foreign_keys
    ddl = str(CreateTable(table).compile(dialect=postgresql.dialect()))
    assert "CREATE TABLE" in ddl and "PRIMARY KEY" in ddl and "UNIQUE" in ddl
    assert "valid_to>=valid_from" in ddl and "company_revision>0" in ddl
    assert "source_snapshot_id IS NOT NULL" in ddl and "octet_length(evidence_ref)<=128" in ddl
    assert all(not column.nullable for column in table.primary_key.columns)


def test_scoped_uniqueness_and_source_models_stay_separate():
    table = CompanyRegistryIdentifierAssertion.__table__
    unique = next(constraint for constraint in table.constraints if isinstance(constraint, UniqueConstraint))
    assert tuple(unique.columns.keys()) == (
        "company_id",
        "company_revision",
        "identifier_system",
        "identifier_scope",
        "identifier_value",
    )
    checks = " ".join(
        str(constraint.sqltext) for constraint in table.constraints if isinstance(constraint, CheckConstraint)
    )
    assert "external_reference" in checks and "provenance_kind='source_reference'" in checks
    assert "cms_company" not in checks and "hios" not in checks
    assert RegistryIdentifierBinding.__table__.c.snapshot_id.nullable is False
    assert RegistryIdentifierObservation.__table__.c.resolution_status is not None
    assert HIOSIssuerRegistry.__table__.name == "hios_issuer_registry"


def test_empty_assertion_replacement_requires_existing_revision_and_no_identifier_claims():
    cleared = _parse(_command(expected_revision=2, role_assertions=[], identifier_assertions=[]))
    assert cleared.as_dict()["role_assertions"] == cleared.as_dict()["identifier_assertions"] == []
    for document in (
        _command(role_assertions=[], identifier_assertions=[]),
        _command(expected_revision=2, role_assertions=[], identifier_assertions=[_identifier()]),
    ):
        with pytest.raises(values.CompanyRegistryAssertionValueError):
            _parse(document)
