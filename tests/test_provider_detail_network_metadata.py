# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed detail metadata retains public facts and exact location identity."""

import json
from contextlib import nullcontext
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from asyncpg.pgproto.pgproto import UUID as DriverUUID

from api import provider_detail_network_metadata as metadata
from api.network_address_scope import network_address_read_scope
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests.test_provider_profile_network_read import address_manifest

_NPI = "1000000491"
_ADDRESS = "22222222-2222-4222-8222-222222222222"
_OTHER = "33333333-3333-4333-8333-333333333333"
_SITE = "44444444-4444-4444-8444-444444444444"


def _fact(**changes):
    return {
        "npi": int(_NPI),
        "address_key": _ADDRESS,
        "network_id": 42,
        "provider_system": "npi",
        "provider_id": _NPI,
        "location_id": _SITE,
        "evidence_id": "a" * 64,
    } | changes


def _session(facts, *, bounded=True):
    summary_by_field = {"bounded": bounded, "facts_json": json.dumps(facts)}
    query_result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: summary_by_field))
    return SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(return_value=query_result))


async def _project(session, detail_by_field, *, selected=None, evidence=False):
    manifest = address_manifest()
    options = {"excluded_network_ids": (84,)} if selected is None else {}
    with network_address_read_scope(manifest, selected, access_scope_sha256="d" * 64, **options):
        return await metadata.project_managed_detail_metadata(
            session, manifest, _NPI, detail_by_field, include_sources=True, include_evidence=evidence
        )


async def _project_collection(session, provider_rows, *, selected=None, evidence=False, sources=False):
    manifest = address_manifest()
    options = {"excluded_network_ids": (84,)} if selected is None else {}
    with network_address_read_scope(manifest, selected, access_scope_sha256="d" * 64, **options):
        return await metadata.project_managed_collection_metadata(
            session, manifest, provider_rows, include_sources=sources, include_evidence=evidence
        )


@pytest.mark.asyncio
async def test_exact_membership_replaces_legacy_arrays_without_changing_public_facts():
    detail_by_field = {
        "npi": int(_NPI),
        "qualifications": {"network_refs": ["unrelated-independent-value"]},
        "address_list": [
            {
                "address_key": _ADDRESS,
                "telephone_number": "555-0101",
                "aca_network_array": [84],
                "provider_directory_sources": [{"network_refs": ["hidden"]}],
                "address_sources": ["public"],
            },
            {"address_key": _OTHER, "telephone_number": "555-0102"},
        ],
    }
    original_by_field = deepcopy(detail_by_field)
    session = _session([_fact(), _fact(evidence_id="b" * 64)])
    projected_by_field = await _project(session, detail_by_field, evidence=True)
    assert (
        detail_by_field == original_by_field
        and projected_by_field["qualifications"] == detail_by_field["qualifications"]
    )
    first, other_address = projected_by_field["address_list"]
    assert first["telephone_number"] == "555-0101" and first["address_sources"] == ["public"]
    assert "aca_network_array" not in first and "provider_directory_sources" not in first
    assert first["network_memberships"] == [
        {
            "network_id": 42,
            "provider_system": "npi",
            "provider_id": _NPI,
            "location_id": _SITE,
            "evidence_ids": ["a" * 64, "b" * 64],
        }
    ]
    assert other_address["telephone_number"] == "555-0102" and other_address["network_memberships"] == []
    statement, parameters = session.execute.await_args.args
    sql = str(statement)
    assert "binding.location_key,binding.entity_type,binding.entity_id" in sql
    assert "member.provider_system,member.provider_id,member.location_id" in sql
    assert "network.approved_revision=:metadata_revision" in sql
    assert parameters["metadata_excluded"] == [84] and parameters["metadata_selected"] is None
    assert parameters["metadata_npis"] == [int(_NPI), int(_NPI)]
    assert parameters["metadata_address_keys"] == sorted([_ADDRESS, _OTHER])
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_premise_members_preserve_exact_location_and_hide_opt_in_evidence():
    session = _session([_fact()])
    detail_by_field = {
        "npi": int(_NPI),
        "address_groups": [{"group_key": "premise", "members": [{"address_key": _ADDRESS}]}],
    }
    projected_by_field = await _project(session, detail_by_field, selected=(42,))
    member = projected_by_field["address_groups"][0]["members"][0]["network_memberships"][0]
    assert member["location_id"] == _SITE and "evidence_ids" not in member
    parameters = session.execute.await_args.args[1]
    assert parameters["metadata_selected"] == [42] and parameters["_canonical_network_ids"] == [42]


@pytest.mark.asyncio
@pytest.mark.parametrize("addresses", [[], [{"telephone_number": "555-0101"}]])
async def test_default_identity_without_canonical_address_needs_no_membership_query(addresses):
    session = _session([])
    projected_by_field = await _project(session, {"npi": int(_NPI), "address_list": addresses})
    assert len(projected_by_field["address_list"]) == len(addresses)
    if addresses:
        assert projected_by_field["address_list"][0]["network_memberships"] == []
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("facts", [[], [_fact(network_id=84)], [_fact(address_key=_OTHER)]])
async def test_selected_address_requires_an_exact_selected_membership(facts):
    with pytest.raises(RegistryNetworkCatalogError):
        await _project(_session(facts), {"npi": int(_NPI), "address_list": [{"address_key": _ADDRESS}]}, selected=(42,))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"network_id": True},
        {"network_id": 0},
        {"network_id": 84},
        {"provider_id": "1000000004"},
        {"provider_system": "unknown"},
        {"location_id": "not-a-location"},
        {"evidence_id": ""},
        {"evidence_id": "x" * 1025},
        {"extra": "unscoped"},
    ],
)
async def test_malformed_or_denied_database_facts_fail_closed(changes):
    with pytest.raises(RegistryNetworkCatalogError):
        await _project(_session([_fact(**changes)]), {"npi": int(_NPI), "address_list": [{"address_key": _ADDRESS}]})


@pytest.mark.asyncio
async def test_overflow_refuses_without_a_partial_document():
    with pytest.raises(RegistryNetworkCatalogError, match="response_bounds"):
        await _project(_session([], bounded=False), {"npi": int(_NPI), "address_list": [{"address_key": _ADDRESS}]})


@pytest.mark.asyncio
async def test_metadata_requires_existing_matching_scope_and_transaction():
    session = _session([])
    with pytest.raises(RegistryNetworkCatalogError):
        await metadata.project_managed_detail_metadata(
            session, address_manifest(), _NPI, {"npi": int(_NPI)}, include_sources=False, include_evidence=False
        )
    session.execute.assert_not_awaited()


def _shared_office_rows(other_npi):
    return [
        {
            "npi": int(_NPI),
            "address_key": _ADDRESS,
            "telephone_number": "555-0101",
            "aca_network_array": [42, 84],
            "provider_directory_sources": [{"network_refs": [84]}],
            "network_memberships": [{"network_id": 84}],
            "qualifications": {"network_refs": ["independent"]},
            "address_list": [{"address_key": _ADDRESS, "ptg_plan_array": [84]}],
        },
        {
            "npi": int(other_npi),
            "address_groups": [{"members": [{"address_key": _ADDRESS, "telephone_number": "555-0102"}]}],
        },
    ]


@pytest.mark.asyncio
async def test_shared_office_collection_preserves_exact_memberships():
    """Keep mixed office shapes private to each provider in one bounded query."""
    other_npi = "1000000004"
    provider_rows = _shared_office_rows(other_npi)
    original = deepcopy(provider_rows)
    session = _session(
        [
            _fact(evidence_id="b" * 64),
            _fact(),
            _fact(),
            _fact(npi=int(other_npi), provider_id=other_npi, location_id=_OTHER),
        ]
    )
    projected_rows = await _project_collection(session, provider_rows, selected=(42,), evidence=True, sources=True)
    assert provider_rows == original and projected_rows is not provider_rows
    first_provider_by_field, second_provider_by_field = projected_rows
    assert first_provider_by_field["qualifications"] == original[0]["qualifications"]
    assert first_provider_by_field["telephone_number"] == "555-0101"
    assert (
        "aca_network_array" not in first_provider_by_field
        and "provider_directory_sources" not in first_provider_by_field
    )
    assert "ptg_plan_array" not in first_provider_by_field["address_list"][0]
    assert (
        first_provider_by_field["network_memberships"]
        == first_provider_by_field["address_list"][0]["network_memberships"]
    )
    assert first_provider_by_field["network_memberships"] == [
        {
            "network_id": 42,
            "provider_system": "npi",
            "provider_id": _NPI,
            "location_id": _SITE,
            "evidence_ids": ["a" * 64, "b" * 64],
        }
    ]
    second_office = second_provider_by_field["address_groups"][0]["members"][0]
    assert second_office["telephone_number"] == "555-0102"
    assert second_office["network_memberships"][0]["provider_id"] == other_npi
    assert second_office["network_memberships"][0]["location_id"] == _OTHER
    statement, parameters_by_name = session.execute.await_args.args
    assert parameters_by_name["metadata_npis"] == [int(other_npi), int(_NPI)]
    assert parameters_by_name["metadata_address_keys"] == [_ADDRESS, _ADDRESS]
    sql = str(statement)
    assert "unnest(CAST(:metadata_npis AS bigint[]),CAST(:metadata_address_keys AS uuid[]))" in sql
    assert "(requested.npi,requested.address_key)" in sql
    assert "SELECT DISTINCT office.npi,office.address_key" in sql
    assert "count(*)<=:metadata_max_facts" in sql and "octet_length(document::text)" in sql
    assert "CASE WHEN bounded THEN" in sql
    assert "network.record_json->'archived'='false'::jsonb" in sql
    session.execute.assert_awaited_once()
    first_provider_by_field["network_memberships"][0]["provider_id"] = "changed"
    assert first_provider_by_field["address_list"][0]["network_memberships"][0]["provider_id"] == _NPI


@pytest.mark.asyncio
async def test_default_collection_keeps_public_and_zero_office_identities_without_borrowing_memberships():
    other_npi = 1000000004
    rows = [
        {"npi": int(_NPI), "address_key": _ADDRESS, "contact": {"phone": "555-0101"}},
        {"npi": other_npi, "address_key": _ADDRESS, "aca_network_array": [84]},
        {"npi": 1000000005, "address_list": []},
        {"npi": 1000000006, "address_key": None, "telephone_number": "555-0103"},
    ]
    session = _session([_fact()])
    projected = await _project_collection(session, rows)
    assert projected[0]["contact"] == rows[0]["contact"]
    assert projected[0]["network_memberships"][0]["network_id"] == 42
    assert projected[1]["network_memberships"] == [] and "aca_network_array" not in projected[1]
    assert projected[2] == rows[2] and "network_memberships" not in projected[2]
    assert projected[3]["telephone_number"] == "555-0103" and projected[3]["network_memberships"] == []
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("uuid_type", [UUID, DriverUUID])
async def test_native_uuid_offices_share_canonical_lookup_and_preserve_public_types(uuid_type):
    address_key = uuid_type(_ADDRESS)
    provider_rows = [
        {
            "npi": int(_NPI),
            "address_key": address_key,
            "address_list": [{"address_key": address_key}],
            "address_groups": [{"members": [{"address_key": address_key}]}],
        }
    ]
    session = _session([_fact()])
    projected_rows = await _project_collection(session, provider_rows, selected=(42,))
    for office_by_field in metadata._addresses(projected_rows[0]):
        assert type(office_by_field["address_key"]) is uuid_type
        assert office_by_field["address_key"] == address_key
        assert office_by_field["network_memberships"][0]["network_id"] == 42
    assert session.execute.await_args.args[1]["metadata_address_keys"] == [_ADDRESS]
    assert "network_memberships" not in provider_rows[0]
    session.execute.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "changes",
    [
        {"npi": 1000000004},
        {"npi": str(_NPI)},
        {"npi": True},
        {"npi": []},
        {"address_key": []},
        {"network_id": 2147483648},
        {"network_id": 43},
        {"network_id": 84},
        {"provider_id": "1000000004"},
    ],
)
async def test_collection_rejects_foreign_or_out_of_selected_scope_facts(changes):
    with pytest.raises(RegistryNetworkCatalogError):
        await _project_collection(
            _session([_fact(**changes)]), [{"npi": int(_NPI), "address_key": _ADDRESS}], selected=(42,)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        None,
        {},
        [None],
        [{"npi": True}],
        [{"npi": "1000000491"}],
        [{"npi": -1}],
        [{"npi": 10000000000}],
        [{"npi": int(_NPI), "address_key": UUID(int=0)}],
        [{"npi": int(_NPI), "address_list": {}}],
        [{"npi": int(_NPI), "address_groups": [{"members": [None]}]}],
    ],
)
async def test_collection_rejects_malformed_input_without_querying(rows):
    session = _session([])
    with pytest.raises(RegistryNetworkCatalogError):
        await _project_collection(session, rows)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("rows", [[], [{"npi": int(_NPI)}]])
async def test_empty_collection_or_zero_office_identity_needs_no_query(rows):
    session = _session([])
    assert await _project_collection(session, rows) == rows
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rows",
    [
        [{"npi": int(_NPI)}] * 3,
        [{"npi": int(_NPI), "address_list": [{"address_key": _ADDRESS}] * 3}],
        [{"npi": int(_NPI), "address_key": _ADDRESS, "address_list": [{"address_key": _ADDRESS}] * 2}],
    ],
)
async def test_combined_input_rows_are_bounded_even_for_duplicate_offices(monkeypatch, rows):
    monkeypatch.setattr(metadata, "MAX_MEMBERSHIP_FACTS", 2)
    session = _session([])
    with pytest.raises(RegistryNetworkCatalogError, match="response_bounds"):
        await _project_collection(session, rows)
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_collection_defends_database_fact_and_byte_bounds(monkeypatch):
    rows = [{"npi": int(_NPI), "address_key": _ADDRESS}]
    monkeypatch.setattr(metadata, "MAX_MEMBERSHIP_FACTS", 1)
    with pytest.raises(RegistryNetworkCatalogError):
        await _project_collection(_session([_fact(), _fact(evidence_id="b" * 64)]), rows)
    monkeypatch.setattr(metadata, "MAX_METADATA_BYTES", 1)
    with pytest.raises(RegistryNetworkCatalogError):
        await _project_collection(_session([_fact()]), rows)


@pytest.mark.asyncio
async def test_repeated_offices_cannot_amplify_metadata_beyond_output_bounds(monkeypatch):
    rows = [{"npi": int(_NPI), "address_list": [{"address_key": _ADDRESS}] * 2}]
    monkeypatch.setattr(metadata, "MAX_MEMBERSHIP_FACTS", 3)
    with pytest.raises(RegistryNetworkCatalogError, match="response_bounds"):
        await _project_collection(_session([_fact(), _fact(location_id=_OTHER)]), rows)
    monkeypatch.setattr(metadata, "MAX_METADATA_BYTES", len(json.dumps([_fact()]).encode("utf-8")))
    with pytest.raises(RegistryNetworkCatalogError, match="response_bounds"):
        await _project_collection(_session([_fact()]), rows, evidence=True)


@pytest.mark.asyncio
async def test_collection_requires_exact_manifest_transaction_and_flags():
    manifest = address_manifest()
    session = _session([])
    with network_address_read_scope(manifest, None, access_scope_sha256="d" * 64, excluded_network_ids=()):
        for supplied_manifest, in_transaction, sources, evidence in [
            (replace(manifest, generation_id=manifest.generation_id + 1), True, False, False),
            (manifest, False, False, False),
            (manifest, True, 1, False),
            (manifest, True, False, "true"),
        ]:
            session.in_transaction = lambda: in_transaction
            with pytest.raises(RegistryNetworkCatalogError):
                await metadata.project_managed_collection_metadata(
                    session, supplied_manifest, [], include_sources=sources, include_evidence=evidence
                )
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_detail_and_singleton_collection_match_for_padded_npi():
    npi = "0000000491"
    detail_by_field = {"npi": int(npi), "address_list": [{"address_key": _ADDRESS}]}
    facts = [_fact(npi=int(npi), provider_id=npi)]
    manifest = address_manifest()
    with network_address_read_scope(manifest, (42,), access_scope_sha256="d" * 64):
        projected_detail = await metadata.project_managed_detail_metadata(
            _session(facts), manifest, npi, detail_by_field, include_sources=False, include_evidence=False
        )
        projected_rows = await metadata.project_managed_collection_metadata(
            _session(facts), manifest, [detail_by_field], include_sources=False, include_evidence=False
        )
    assert projected_detail == projected_rows[0]
    assert projected_detail["address_list"][0]["network_memberships"][0]["provider_id"] == npi


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical", [False, True])
async def test_unmanaged_collection_bridge_preserves_existing_objects_without_queries(canonical):
    provider_rows = [{"npi": int(_NPI), "aca_network_array": [84]}]
    session = _session([])
    scope = network_address_read_scope(address_manifest(), (42,)) if canonical else nullcontext()
    with scope:
        assert await metadata.project_provider_network_metadata(provider_rows, session=session) is provider_rows
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_managed_collection_bridge_maps_projection_refusal_to_private_unavailable():
    from sanic.exceptions import ServiceUnavailable

    manifest = address_manifest()
    with network_address_read_scope(manifest, (42,), access_scope_sha256="d" * 64):
        with pytest.raises(ServiceUnavailable) as raised:
            await metadata.project_provider_network_metadata(
                [{"npi": int(_NPI), "address_key": _ADDRESS}], session=_session([], bounded=False)
            )
    assert raised.value.status_code == 503 and raised.value.headers == {"Cache-Control": "private, no-store"}
