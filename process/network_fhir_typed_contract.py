# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Versioned strict graph inputs; declarations and temporary workspaces grant no custody."""

import asyncio
import json
from dataclasses import asdict, dataclass
from datetime import date

from process.network_address_projection import _identifier
from process.network_fhir_source_epoch_pin import _virtual_xid
from process.network_fhir_source_witness_contract import database_identity, source_origin

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
_BRAND = object()


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


def graph_document(origin, identity, membership_as_of, counts):
    """Bind exact original Source commitments and new derivation semantics without an old capture receipt."""
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
        "database_identity": asdict(database_identity(identity)),
        "counts": dict(counts),
    }


def validate_graph_document(document):
    """Closed metadata is a description; only the live original Source factory can authenticate an input."""
    require(
        type(document) is dict
        and set(document)
        == {
            "contract",
            "derivation_version",
            "office_policy",
            "canonicalizer_version",
            "membership_as_of",
            "origin",
            "database_identity",
            "counts",
        }
    )
    rebuilt = graph_document(
        document["origin"], document["database_identity"], document["membership_as_of"], document["counts"]
    )
    require(
        document == rebuilt
        and type(document["derivation_version"]) is int
        and type(document["canonicalizer_version"]) is int
    )
    return rebuilt


@dataclass
class TypedFHIRGraphStage:
    """Task/transaction-bound temporary output; durable custody is the candidate owner's responsibility."""

    connection: object
    task: object
    virtual_xid: str
    names: dict
    oids: dict
    schema_name: str = "pg_temp"
    document: dict | None = None
    active: bool = True
    checked: bool = False
    brand: object = None

    def relation(self, kind):
        """Resolve only a declared relation in the bound workspace."""
        require(kind in self.names)
        return _identifier(self.schema_name) + "." + _identifier(self.names[kind])

    async def verify(self, connection, *, complete=True):
        """Require the original task, transaction and relation identities before reading rows."""
        require(self.brand is _BRAND and self.active and self.connection is connection, "workspace_closed")
        require(self.task is asyncio.current_task() and connection.is_in_transaction(), "workspace_scope")
        require(await _virtual_xid(connection) == self.virtual_xid, "workspace_scope")
        for kind, oid in self.oids.items():
            require(
                await connection.fetchval(
                    "SELECT to_regclass($1)::oid::bigint", self.schema_name + "." + self.names[kind]
                )
                == oid,
                "workspace_changed",
            )
        if complete:
            require(self.checked and self.document is not None, "source_incomplete")
            validate_graph_document(self.document)
