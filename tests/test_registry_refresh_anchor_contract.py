# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Frozen exact scope grammar shared with the independent service-authority client."""

import copy
from dataclasses import replace

import pytest

from process.registry_refresh_anchor_contract import (
    GENERIC_FHIR_PRODUCER,
    MAX_BYTES,
    ApprovedRefreshBinding,
    RegistryRefreshAnchorUnavailable,
    encoded_document,
    validated_anchor_request,
)

BINDING_ID = "30000000-0000-0000-0000-000000000001"


def source_scope(system="fhir", **changes):
    scope_by_field = {
        "source_system": system,
        "source_id": "source-one",
        "dataset_schema": "original_directory",
        "producer_id": "producer-one",
    }
    if system == "fhir":
        scope_by_field.update(
            endpoint_id="endpoint-one",
            alias_scope="alias-one",
            resource_type="Organization",
            resource_id="network-one",
            legacy_network_id="40000000-0000-0000-0000-000000000001",
        )
    else:
        scope_by_field.update(source_key="network-one", approved_binding_sha256="a" * 64, capture_generation="b" * 64)
    return {**scope_by_field, **changes}


def enrolled_scope(system="fhir", **changes):
    return {
        "binding_id": BINDING_ID,
        "binding_revision": 3,
        "network_id": 7,
        "continuity_policy": {"fhir": "accepted_edition", "aca": "retained_successor", "ptg": "exact_capture"}[system],
        "source_scope": source_scope(system),
        **changes,
    }


def query(**changes):
    return {"approved_revision": 5, "enrolled_scopes": [enrolled_scope()], **changes}


@pytest.mark.parametrize("system", ["fhir", "aca", "ptg"])
def test_exact_source_families_keep_their_frozen_scope_contract(system):
    command = query(enrolled_scopes=[enrolled_scope(system)])
    assert validated_anchor_request(command) == command


def test_generic_fhir_enrollment_binds_original_capture_and_protected_producer():
    scope = enrolled_scope(
        continuity_policy="retained_successor",
        source_scope={
            "source_system": "fhir",
            "source_id": "source-one",
            "dataset_schema": "original_directory",
            "producer_id": GENERIC_FHIR_PRODUCER,
            "source_key": "network-one",
            "approved_binding_sha256": "a" * 64,
            "capture_generation": "b" * 64,
        },
    )
    assert validated_anchor_request(query(enrolled_scopes=[scope]))
    for changes in (
        {"producer_id": "unverified-producer"},
        {"capture_generation": "dataset-alias"},
        {"legacy_network_id": "40000000-0000-0000-0000-000000000001"},
    ):
        malformed = copy.deepcopy(scope)
        malformed["source_scope"].update(changes)
        with pytest.raises(ValueError):
            validated_anchor_request(query(enrolled_scopes=[malformed]))
    with pytest.raises(ValueError):
        validated_anchor_request(query(enrolled_scopes=[{**scope, "continuity_policy": "accepted_edition"}]))


@pytest.mark.parametrize(
    "changes",
    [
        {"actor": {}},
        {"source_reader_sessions": {}},
        {"approved_membership_sha256": "a" * 64},
        {"approved_revision": True},
        {"approved_revision": -1},
        {"approved_revision": 2**63},
        {"enrolled_scopes": []},
        {"enrolled_scopes": [enrolled_scope()] * 1001},
    ],
)
def test_query_refuses_caller_capabilities_and_unbounded_or_untyped_selection(changes):
    with pytest.raises((ValueError, TypeError)):
        validated_anchor_request(query(**changes))


@pytest.mark.parametrize(
    "changes",
    [
        {"binding_revision": True},
        {"network_id": "7"},
        {"binding_id": "00000000-0000-0000-0000-000000000000"},
        {"continuity_policy": "name_match"},
        {"source_scope": {"source_system": "other"}},
        {"extra": True},
    ],
)
def test_no_selector_expansion_or_implicit_grant(changes):
    with pytest.raises((ValueError, TypeError)):
        validated_anchor_request(query(enrolled_scopes=[enrolled_scope(**changes)]))


@pytest.mark.parametrize(
    "field,changed",
    [
        ("resource_type", "PractitionerRole"),
        ("resource_id", "network/name"),
        ("endpoint_id", ""),
        ("dataset_schema", "source;other"),
        ("alias_scope", []),
        ("legacy_network_id", "a" * 64),
    ],
)
def test_fhir_native_coordinates_are_closed_and_typed(field, changed):
    with pytest.raises((ValueError, TypeError)):
        validated_anchor_request(query(enrolled_scopes=[enrolled_scope(source_scope=source_scope(**{field: changed}))]))


def test_sorted_unique_scopes_and_body_bound():
    later = enrolled_scope(binding_id="30000000-0000-0000-0000-000000000002")
    assert validated_anchor_request(query(enrolled_scopes=[enrolled_scope(), later]))
    for scopes in ([later, enrolled_scope()], [enrolled_scope(), enrolled_scope()]):
        with pytest.raises(ValueError):
            validated_anchor_request(query(enrolled_scopes=scopes))
    for system in ("aca", "ptg"):
        with pytest.raises(ValueError):
            validated_anchor_request(
                query(
                    enrolled_scopes=[
                        enrolled_scope(system, source_scope=source_scope(system, capture_generation="run-label"))
                    ]
                )
            )
    with pytest.raises(ValueError):
        validated_anchor_request(
            query(enrolled_scopes=[enrolled_scope(source_scope=source_scope(alias_scope="x" * (1024 * 1024)))])
        )


def native_binding(**changes):
    return ApprovedRefreshBinding(
        "network_binding",
        BINDING_ID,
        3,
        5,
        {
            "binding_id": BINDING_ID,
            "revision": 3,
            "network_id": 7,
            "archived": False,
            "source_system": "fhir",
            "source_id": "source-one",
            "dataset_schema": "original_directory",
            "dataset_id": "dataset-one",
            "producer_id": "producer-one",
            "edition_id": "edition-one",
            "source_key": "network-one",
            "source_scope_json": {"organization_id": "network-one"},
            **changes,
        },
        "a" * 64,
    )


def test_native_record_provenance_keeps_all_original_fingerprint_inputs():
    binding = native_binding()
    assert (binding.binding_id, binding.binding_revision, binding.network_id, binding.custom_revision) == (
        BINDING_ID,
        3,
        7,
        5,
    )
    for changes in (
        {"revision": True},
        {"binding_id": "40000000-0000-0000-0000-000000000001"},
        {"archived": True},
        {"dataset_id": ""},
    ):
        with pytest.raises(RegistryRefreshAnchorUnavailable):
            native_binding(**changes)
    for changes in ({"record_kind": "network"}, {"approved_binding_sha256": "A" * 64}, {"record_revision": 0}):
        with pytest.raises(RegistryRefreshAnchorUnavailable):
            replace(binding, **changes)
    altered = copy.deepcopy(binding.record_json)
    altered["network_id"] = "7"
    with pytest.raises(RegistryRefreshAnchorUnavailable):
        replace(binding, record_json=altered)


@pytest.mark.parametrize("identity", [BINDING_ID.replace("-", ""), "{70000000-0000-0000-0000-000000000001}"])
def test_refresh_identity_is_canonical_without_normalizing_the_reviewed_request(identity):
    with pytest.raises(ValueError, match="invalid refresh identity"):
        validated_anchor_request(query(enrolled_scopes=[enrolled_scope(binding_id=identity)]))


@pytest.mark.parametrize("scope", [None, [], "source-one"])
def test_refresh_source_scope_cannot_be_a_scalar_or_sequence(scope):
    with pytest.raises(ValueError, match="invalid refresh source scope"):
        validated_anchor_request(query(enrolled_scopes=[enrolled_scope(source_scope=scope)]))


def test_canonical_receipt_byte_bound_counts_escaped_unicode_and_is_inclusive():
    prefix = b'{"reason":"'
    suffix = b'"}'
    remaining = MAX_BYTES - len(prefix) - len(suffix)
    reason = "\u03bb" * (remaining // 6) + "x" * (remaining % 6)
    encoded = encoded_document({"reason": reason})
    assert len(encoded) == MAX_BYTES
    assert encoded.startswith(prefix + b"\\u03bb") and encoded.endswith(suffix)
    with pytest.raises(ValueError, match="refresh document exceeds bound"):
        encoded_document({"reason": reason + "x"})
