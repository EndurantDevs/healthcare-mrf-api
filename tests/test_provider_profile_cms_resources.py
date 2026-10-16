# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed retained-resource decoding and independent profile facts."""

import json
from types import SimpleNamespace

import pytest

from api import provider_profile_cms_resources as reader
from api.provider_directory_entities_contract import DirectoryReadError

NPI = "1000000491"


def test_independent_practitioner_and_minimal_medicaid_need_no_network():
    resources = [
        {
            "resource_type": "Practitioner",
            "resource_id": "p",
            "payload_json": {"resource_id": "p", "npi": NPI, "languages": ["en"]},
        },
        {
            "resource_type": "PractitionerRole",
            "resource_id": "r",
            "payload_json": {"resource_id": "r", "practitioner_ref": "Practitioner/p", "accepting_medicaid": False},
        },
    ]
    document_by_field = {"resources": resources, "associations": [], "excluded_network_resource_ids": []}
    assert reader._decode(json.dumps(document_by_field), NPI) == document_by_field


@pytest.mark.parametrize(
    "document",
    [
        None,
        "[]",
        "{}",
        '{"resources":[],"associations":[],"excluded_network_resource_ids":[],"extra":1}',
        '{"resources":[{}],"associations":[],"excluded_network_resource_ids":[]}',
    ],
)
def test_malformed_resources_refuse(document):
    with pytest.raises(DirectoryReadError):
        reader._decode(document, NPI)


def test_missing_provider_reference_refuses_instead_of_certifying_empty():
    role_by_field = {
        "resource_type": "PractitionerRole",
        "resource_id": "r",
        "payload_json": {"resource_id": "r", "practitioner_ref": "Practitioner/missing", "accepting_medicaid": True},
    }
    with pytest.raises(DirectoryReadError):
        reader._decode(
            json.dumps({"resources": [role_by_field], "associations": [], "excluded_network_resource_ids": []}), NPI
        )


def test_transfer_byte_and_row_limits_refuse(monkeypatch):
    monkeypatch.setattr(reader, "MAX_BYTES", 1)
    with pytest.raises(DirectoryReadError):
        reader._decode('{"resources":[],"associations":[],"excluded_network_resource_ids":[]}', NPI)
    monkeypatch.setattr(reader, "MAX_BYTES", 1024)
    monkeypatch.setattr(reader, "MAX_ROWS", 0)
    with pytest.raises(DirectoryReadError):
        reader._decode('{"resources":[{}],"associations":[],"excluded_network_resource_ids":[]}', NPI)


@pytest.mark.parametrize(
    "excluded_resource_ids", [None, {}, True, ["Organization/foreign"], ["same", "same"], ["x" * 65]]
)
def test_excluded_network_resource_ids_require_closed_source_ids(excluded_resource_ids):
    document_by_field = {"resources": [], "associations": [], "excluded_network_resource_ids": excluded_resource_ids}
    with pytest.raises(DirectoryReadError):
        reader._decode(json.dumps(document_by_field), NPI)


def test_denied_networks_use_the_same_approved_source_mapping():
    from api.provider_directory_cms_network_scope import _mapped_networks

    scope = SimpleNamespace(source_schema='"retained"', excluded_network_ids=(84,))
    allowed = _mapped_networks(scope)
    reviewed = _mapped_networks(scope, include_all=True)
    assert "scope_excluded_network_ids" in allowed
    assert "scope_excluded_network_ids" not in reviewed
    differences = [
        (left.strip(), right.strip())
        for left, right in zip(allowed.splitlines(), reviewed.splitlines(), strict=True)
        if left != right
    ]
    assert len(differences) == 1 and differences[0][1] == "AND TRUE"
    for boundary in ("approved_network_bindings", "legacy.source_id='cms-npd'", "source_scope_json", ":scope_alias"):
        assert boundary in allowed and boundary in reviewed
    sql = reader._resource_seeds_sql(scope)
    assert "SELECT resource_id FROM reviewed_networks EXCEPT SELECT resource_id FROM allowed LIMIT 5001" in sql
