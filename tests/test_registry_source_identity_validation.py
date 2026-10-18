# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source reviews and legacy aliases retain their exact approved identities."""

import copy
import json
from uuid import UUID

import pytest

from process import registry_network_catalog_read as catalog
from process import registry_ptg_scope_engine as ptg
from tests.test_registry_network_catalog_read import legacy_selector, source_coordinates
from tests.test_registry_ptg_scope_engine import _envelope


def test_reviewed_ptg_approval_retains_complete_valid_source():
    envelope = _envelope("approve")
    before = copy.deepcopy(envelope)
    accepted = ptg.validated_registry_ptg_scope_envelope(envelope, "approve")
    assert accepted == before and envelope == before
    assert accepted["command"]["source"]["snapshot_id"] == "snapshot_example"


def _invalid_ptg_envelope(change):
    envelope = _envelope("approve")
    file_version = envelope["command"]["file_versions"][0]
    patches_by_case = {
        "ordinary_actor": (("actor", "kind"), "client_admin"),
        "duplicate_file_identity": (
            ("command", "file_versions"),
            [file_version, {**file_version, "raw_sha256": "e" * 64}],
        ),
        "foreign_snapshot": (("command", "source", "snapshot_id"), "different_snapshot"),
        "foreign_schema": (("command", "source", "ptg_schema_name"), "different_schema"),
        "non_ptg_source": (("command", "coordinates", "source_system"), "fhir"),
        "boolean_revision": (("command", "approved_revision"), True),
        "empty_file_inventory": (("command", "file_versions"), []),
        "typed_uuid_object": (("command", "scope_id"), UUID(envelope["command"]["scope_id"])),
        "unreviewed_operation": (("command", "operation"), "approve_unreviewed_source"),
        "oversized_owner_node": (("command", "ownership", "assigned_node_id"), "n" * 65),
    }
    path, value = patches_by_case[change]
    parent = envelope
    for key in path[:-1]:
        parent = parent[key]
    parent[path[-1]] = value
    return envelope


@pytest.mark.parametrize(
    "change",
    [
        "ordinary_actor",
        "duplicate_file_identity",
        "foreign_snapshot",
        "foreign_schema",
        "non_ptg_source",
        "boolean_revision",
        "empty_file_inventory",
        "typed_uuid_object",
        "unreviewed_operation",
        "oversized_owner_node",
    ],
)
def test_ptg_source_approval_refuses_ambiguous_or_foreign_review(change):
    envelope = _invalid_ptg_envelope(change)
    before = copy.deepcopy(envelope)
    error = PermissionError if change == "ordinary_actor" else ValueError
    code = "registry_ptg_scope_actor_invalid" if change == "ordinary_actor" else "registry_ptg_scope_request_invalid"
    with pytest.raises(error, match="^" + code + "$"):
        ptg.validated_registry_ptg_scope_envelope(envelope, "approve")
    assert envelope == before


def _fhir_arguments():
    identifier = "a1111111-1111-4111-8111-111111111111"
    scope_by_field = {"organization_id": "Organization.1", "legacy_uuid": identifier, "alias_scope": "reviewed_alias"}
    return dict(
        source=source_coordinates("fhir"),
        namespace="legacy_fhir_uuid",
        value=identifier,
        source_scope_json=json.dumps(scope_by_field).encode(),
    )


def _aca_arguments():
    selected = legacy_selector()
    return dict(
        source=selected.source,
        namespace=selected.namespace,
        value=selected.value,
        source_scope_json=selected.source_scope_json,
    )


@pytest.mark.parametrize("family", ["aca", "fhir"])
def test_valid_legacy_alias_retains_exact_source_and_scope(family):
    arguments = _aca_arguments() if family == "aca" else _fhir_arguments()
    before = copy.deepcopy(arguments)
    selected = catalog.RegistryNetworkCatalogLegacySelector(**arguments)
    assert arguments == before
    assert selected.source == before["source"] and selected.value == before["value"]
    assert selected.source_scope_json == before["source_scope_json"]


def _invalid_legacy_arguments(change):
    arguments = _fhir_arguments() if change.startswith("fhir_") else _aca_arguments()
    scope_by_field = json.loads(arguments["source_scope_json"])
    patches_by_case = {
        "checksum_from_another_network": {"value": "-18"},
        "noncanonical_checksum": {"value": "00"},
        "wrong_source_type": {"source": {"source_system": "aca"}},
        "array_source_scope": {"source_scope_json": b"[]"},
        "foreign_plan_issuer": {
            "source_scope_json": json.dumps({**scope_by_field, "plan_id": "99999CA1234567"}).encode()
        },
        "fhir_foreign_uuid": {"value": "b1111111-1111-4111-8111-111111111111"},
        "fhir_noncanonical_uuid": {"value": arguments["value"].upper()},
        "fhir_malformed_uuid": {"value": "not-a-uuid"},
        "fhir_invalid_organization": {
            "source_scope_json": json.dumps({**scope_by_field, "organization_id": "Organization/foreign"}).encode()
        },
    }
    arguments.update(patches_by_case[change])
    return arguments


@pytest.mark.parametrize(
    "change",
    [
        "checksum_from_another_network",
        "noncanonical_checksum",
        "wrong_source_type",
        "array_source_scope",
        "foreign_plan_issuer",
        "fhir_foreign_uuid",
        "fhir_noncanonical_uuid",
        "fhir_malformed_uuid",
        "fhir_invalid_organization",
    ],
)
def test_legacy_alias_refuses_cross_scope_identity(change):
    arguments = _invalid_legacy_arguments(change)
    before = copy.deepcopy(arguments)
    code = (
        "registry_network_catalog_document_invalid"
        if change in {"array_source_scope", "foreign_plan_issuer", "fhir_invalid_organization"}
        else "registry_network_catalog_request_invalid"
    )
    with pytest.raises(catalog.RegistryNetworkCatalogError, match="^" + code + "$"):
        catalog.RegistryNetworkCatalogLegacySelector(**arguments)
    assert arguments == before
