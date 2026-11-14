# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Versioned graph accounting and named temporary workspaces."""

import json
from dataclasses import asdict, dataclass
from datetime import date

from process.network_address_projection import _identifier
from process.network_fhir_source_witness_contract import source_origin

POLICY = "fhir-direct-network-office-v3"
DERIVATION_VERSION = 3
CONTRACT = "registry-fhir-typed-graph-v3"
KINDS = ("Location", "Organization", "OrganizationAffiliation", "Practitioner", "PractitionerRole")
ADDRESS_FIELDS = ("first_line", "second_line", "city_name", "state_name", "postal_code", "country_code")
CELL_COLUMNS = (
    "resource_type",
    "resource_id",
    "payload_sha256",
    "npi",
    "active",
    "subject_ref",
    "network_refs",
    "location_refs",
    "period_start",
    "period_end",
    "location_status",
    *ADDRESS_FIELDS,
    "site_id",
)
PAIR_COLUMNS = (
    "network_source_key",
    "provider_system",
    "provider_id",
    "location_id",
    "address_key",
    "canonical_identity",
    "resource_type",
    "resource_id",
    "location_resource_id",
    "evidence_basis",
    "reference_rows",
)


class TypedFHIRGraphError(ValueError):
    """A Source pass, projected graph or active workspace differs from its declared scope."""


def require(condition, reason="invalid"):
    """Reject a failed typed-graph invariant with its stable reason code."""
    if not condition:
        raise TypedFHIRGraphError("registry_fhir_typed_" + reason)


def policy_date(value):
    """Parse only canonical ISO dates used by the versioned membership policy."""
    require(type(value) is str, "as_of")
    try:
        parsed = date.fromisoformat(value)
    except ValueError:
        raise TypedFHIRGraphError("registry_fhir_typed_as_of") from None
    require(parsed.isoformat() == value, "as_of")
    return parsed


def graph_document(origin, membership_as_of, counts):
    """Describe the logical Source edition and the complete graph accounting."""
    policy_date(membership_as_of)
    require(
        type(counts) is dict
        and set(counts) == {"source_rows", "projected_rows", "pair_rows", "reference_rows", "provider_only_roles"}
    )
    require(all(type(value) is int and 0 <= value < 2**63 for value in counts.values()))
    return {
        "contract": CONTRACT,
        "derivation_version": DERIVATION_VERSION,
        "office_policy": POLICY,
        "canonicalizer_version": 2,
        "membership_as_of": membership_as_of,
        "origin": json.loads(json.dumps(asdict(source_origin(origin)))),
        "counts": dict(counts),
    }


def validate_graph_document(document):
    """Validate graph data and project historical documents to named Source scope."""
    fields = {
        "contract",
        "derivation_version",
        "office_policy",
        "canonicalizer_version",
        "membership_as_of",
        "origin",
        "counts",
    }
    require(type(document) is dict and set(document) in (fields, fields | {"database_identity"}))
    rebuilt = graph_document(document["origin"], document["membership_as_of"], document["counts"])
    require(
        all(document[field] == rebuilt[field] for field in fields - {"origin"})
        and type(document["derivation_version"]) is int
        and type(document["canonicalizer_version"]) is int
    )
    return rebuilt


@dataclass
class TypedFHIRGraphStage:
    """Named graph tables and their data-validation progress."""

    names: dict
    schema_name: str = "pg_temp"
    document: dict | None = None
    active: bool = True
    checked: bool = False

    def relation(self, kind):
        """Render one declared graph table using validated SQL identifiers."""
        require(kind in self.names)
        return _identifier(self.schema_name) + "." + _identifier(self.names[kind])

    def require_ready(self, *, complete=True):
        """Require an active stage and, when requested, complete graph validation."""
        require(self.active, "workspace_closed")
        if complete:
            require(self.checked and self.document is not None, "source_incomplete")
            validate_graph_document(self.document)
