# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed proposed company assertions; validation supplies no approval authority."""

from __future__ import annotations

import json
import re
from dataclasses import asdict, dataclass, replace
from datetime import date
from uuid import UUID

CONTRACT = "company_registry_assertion_values.v1"
COMPANY_ROLES = ("insurer", "employer", "network_operator")
IDENTIFIER_SYSTEMS = ("ein", "naic_company", "external_reference")
PROVENANCE_KINDS = ("manual_reference", "source_reference")
MAX_ROLE_ASSERTIONS = 32
MAX_IDENTIFIER_ASSERTIONS = 128
MAX_DOCUMENT_BYTES = 131072
MAX_EXPECTED_REVISION = 9223372036854775806


class CompanyRegistryAssertionValueError(ValueError):
    """Reject a complete proposed command without interpreting external evidence."""


def _fail():
    raise CompanyRegistryAssertionValueError("company_registry_assertion_values_invalid")


def _text(value, maximum):
    if (
        type(value) is not str
        or not 1 <= len(value) <= maximum
        or value != value.strip()
        or not value.isprintable()
        or len(value.encode("utf-8")) > maximum
    ):
        _fail()


def _uuid(value):
    if type(value) is not UUID or not value.int:
        _fail()


def _period(valid_from, valid_to):
    if type(valid_from) is not date or valid_to is not None and (type(valid_to) is not date or valid_to < valid_from):
        _fail()


@dataclass(frozen=True, slots=True)
class CompanyAssertionProvenance:
    """A declared reference, never verified source custody or human approval."""

    kind: str
    evidence_ref: str
    snapshot_id: UUID | None = None
    source_record_key: str | None = None

    def __post_init__(self):
        _text(self.kind, 32)
        _text(self.evidence_ref, 128)
        if self.kind not in PROVENANCE_KINDS:
            _fail()
        if self.kind == "manual_reference":
            if self.snapshot_id is not None or self.source_record_key is not None:
                _fail()
        else:
            _uuid(self.snapshot_id)
            _text(self.source_record_key, 128)


def _common(assertion):
    _uuid(assertion.assertion_id)
    _period(assertion.valid_from, assertion.valid_to)
    if type(assertion.provenance) is not CompanyAssertionProvenance:
        _fail()
    replace(assertion.provenance)


@dataclass(frozen=True, slots=True)
class CompanyRoleAssertion:
    """One explicit role period with independent corroborating provenance."""

    assertion_id: UUID
    role: str
    valid_from: date
    valid_to: date | None
    provenance: CompanyAssertionProvenance

    def __post_init__(self):
        _common(self)
        _text(self.role, 32)
        if self.role not in COMPANY_ROLES:
            _fail()


@dataclass(frozen=True, slots=True)
class CompanyIdentifierAssertion:
    """Explicit scoped references do not allocate, merge or identify an issuer."""

    assertion_id: UUID
    identifier_system: str
    identifier_scope: str
    identifier_value: str
    valid_from: date
    valid_to: date | None
    provenance: CompanyAssertionProvenance

    def __post_init__(self):
        _common(self)
        _text(self.identifier_system, 32)
        _text(self.identifier_scope, 128)
        _text(self.identifier_value, 64)
        if self.identifier_system not in IDENTIFIER_SYSTEMS:
            _fail()
        if self.identifier_system == "ein" and (
            re.fullmatch(r"[0-9]{9}", self.identifier_value) is None or self.identifier_value == "000000000"
        ):
            _fail()
        if self.identifier_system == "naic_company" and (
            re.fullmatch(r"[0-9]{5}", self.identifier_value) is None or self.identifier_value == "00000"
        ):
            _fail()
        if self.identifier_system == "external_reference" and self.provenance.kind != "source_reference":
            _fail()


def _entries(entries, kind, minimum, maximum):
    if type(entries) is not tuple or not minimum <= len(entries) <= maximum:
        _fail()
    if any(type(entry) is not kind for entry in entries):
        _fail()
    return tuple(replace(entry, provenance=replace(entry.provenance)) for entry in entries)


def _unique_entries(roles, identifiers):
    assertion_ids = [entry.assertion_id for entry in (*roles, *identifiers)]
    role_keys = [(entry.role, entry.valid_from) for entry in roles]
    identifier_keys = [
        (entry.identifier_system, entry.identifier_scope, entry.identifier_value) for entry in identifiers
    ]
    if (
        len(set(assertion_ids)) != len(assertion_ids)
        or len(set(role_keys)) != len(role_keys)
        or len(set(identifier_keys)) != len(identifier_keys)
    ):
        _fail()


@dataclass(frozen=True, slots=True)
class CompanyRegistryAssertionValues:
    """A whole proposed company revision, without actor, client or approval claims.

    External collisions and overlapping assignments to other companies require
    set-based persisted validation against current reviewed records.
    """

    company_id: UUID
    expected_revision: int
    role_assertions: tuple[CompanyRoleAssertion, ...]
    identifier_assertions: tuple[CompanyIdentifierAssertion, ...] = ()

    def __post_init__(self):
        _uuid(self.company_id)
        if type(self.expected_revision) is not int or not 0 <= self.expected_revision <= MAX_EXPECTED_REVISION:
            _fail()
        minimum_roles = 0 if self.expected_revision > 0 and not self.identifier_assertions else 1
        roles = _entries(self.role_assertions, CompanyRoleAssertion, minimum_roles, MAX_ROLE_ASSERTIONS)
        identifiers = _entries(self.identifier_assertions, CompanyIdentifierAssertion, 0, MAX_IDENTIFIER_ASSERTIONS)
        _unique_entries(roles, identifiers)
        object.__setattr__(self, "role_assertions", tuple(sorted(roles, key=lambda entry: str(entry.assertion_id))))
        object.__setattr__(
            self, "identifier_assertions", tuple(sorted(identifiers, key=lambda entry: str(entry.assertion_id)))
        )
        self.canonical_json()

    def as_dict(self):
        """Return a fresh canonical wire document containing proposed values only."""
        return json.loads(self.canonical_json())

    def canonical_json(self):
        """Encode bounded complete values; these bytes are not authentication."""
        document = asdict(self)
        document["contract"] = CONTRACT
        encoded = json.dumps(
            document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False, default=_wire_scalar
        ).encode("utf-8")
        if len(encoded) > MAX_DOCUMENT_BYTES:
            _fail()
        return encoded


def _wire_scalar(value):
    if type(value) is UUID:
        return str(value)
    if type(value) is date:
        return value.isoformat()
    _fail()


def _closed(document, required, optional=frozenset()):
    if type(document) is not dict or not required <= set(document) or set(document) - required - optional:
        _fail()


def _parse_uuid(value):
    if type(value) is not str or len(value) != 36:
        _fail()
    try:
        parsed = UUID(value)
    except ValueError:
        _fail()
    _uuid(parsed)
    if str(parsed) != value:
        _fail()
    return parsed


def _parse_date(value, *, optional=False):
    if optional and value is None:
        return None
    if type(value) is not str or re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", value) is None:
        _fail()
    try:
        return date.fromisoformat(value)
    except ValueError:
        _fail()


def _parse_provenance(document):
    _closed(document, {"kind", "evidence_ref", "snapshot_id", "source_record_key"})
    return CompanyAssertionProvenance(
        document["kind"],
        document["evidence_ref"],
        None if document["snapshot_id"] is None else _parse_uuid(document["snapshot_id"]),
        document["source_record_key"],
    )


def _parse_assertions(documents, kind, maximum):
    if type(documents) is not list or len(documents) > maximum:
        _fail()
    fields = {"role"} if kind is CompanyRoleAssertion else {"identifier_system", "identifier_scope", "identifier_value"}
    assertions = []
    for document in documents:
        _closed(document, fields | {"assertion_id", "valid_from", "valid_to", "provenance"})
        assertions.append(
            kind(
                assertion_id=_parse_uuid(document["assertion_id"]),
                valid_from=_parse_date(document["valid_from"]),
                valid_to=_parse_date(document["valid_to"], optional=True),
                provenance=_parse_provenance(document["provenance"]),
                **{name: document[name] for name in fields},
            )
        )
    return tuple(assertions)


def validated_company_registry_assertions(document):
    """Parse closed proposals; existing record commands do not accept these fields."""
    _closed(document, {"contract", "company_id", "expected_revision", "role_assertions"}, {"identifier_assertions"})
    if document["contract"] != CONTRACT:
        _fail()
    return CompanyRegistryAssertionValues(
        _parse_uuid(document["company_id"]),
        document["expected_revision"],
        _parse_assertions(document["role_assertions"], CompanyRoleAssertion, MAX_ROLE_ASSERTIONS),
        _parse_assertions(
            document.get("identifier_assertions", []), CompanyIdentifierAssertion, MAX_IDENTIFIER_ASSERTIONS
        ),
    )
