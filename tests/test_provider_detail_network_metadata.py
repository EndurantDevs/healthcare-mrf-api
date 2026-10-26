# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed detail metadata retains public facts and exact location identity."""

import asyncio
import json
from contextlib import asynccontextmanager, nullcontext
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from asyncpg.pgproto.pgproto import UUID as DriverUUID
from sanic import response
from sanic.exceptions import ServiceUnavailable
from sqlalchemy.exc import DBAPIError

from api import network_address_scope as address_scope
from api import provider_detail_network_metadata as metadata
from api import provider_profile_snapshot as snapshot
from api.endpoint import npi as provider
from api.network_address_scope import network_address_read_scope
from db import connection as db_connection
from process.registry_network_catalog_read import RegistryNetworkCatalogError
from tests.test_provider_profile_network_read import address_manifest

_NPI = "1000000491"
_ADDRESS = "22222222-2222-4222-8222-222222222222"
_OTHER = "33333333-3333-4333-8333-333333333333"
_SITE = "44444444-4444-4444-8444-444444444444"
_DIRECT_DETAIL_DOCUMENT = {
    "npi": int(_NPI),
    "aca_network_array": [84],
    "qualifications": {"network_refs": ["independent"]},
    "address_list": [
        {
            "address_key": _ADDRESS,
            "location_key": _SITE,
            "aca_network_array": [84],
            "ptg_plan_array": ["other-plan"],
            "telephone_number": "555-0101",
            "raw": {"network_refs": [84]},
        }
    ],
}


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


def _direct_reader_snapshot(boundary):
    @asynccontextmanager
    async def snapshot(database, schema, *, include_detail):
        assert database is provider.db and schema == provider._runtime_db_schema() and include_detail is True
        boundary.snapshot_calls += 1
        if not boundary.depth:
            boundary.reader_opens += 1
        boundary.depth += 1
        try:
            yield boundary.session
        finally:
            boundary.depth -= 1

    return snapshot


@pytest.fixture
def direct_detail(monkeypatch):
    """Keep ordinary and nested Reader lifetimes distinct without database I/O."""
    manifest = address_manifest()
    session = _session([_fact()])
    ordinary_session = _session([])
    ordinary_session.execute.side_effect = AssertionError("Metadata must use the active Reader session")
    request = SimpleNamespace(args={"network_ids": "42"}, ctx=SimpleNamespace(sa_session=ordinary_session))
    document_by_field = deepcopy(_DIRECT_DETAIL_DOCUMENT)
    boundary = SimpleNamespace(
        request=request,
        session=session,
        document=document_by_field,
        cached_body=None,
        access_scope="d" * 64,
        depth=0,
        reader_opens=0,
        snapshot_calls=0,
    )

    async def read_metadata(*_arguments):
        assert address_scope.current_network_address_scope().manifest is manifest
        assert session.in_transaction() and boundary.depth == 1
        return session.execute.return_value

    session.execute.side_effect = read_metadata

    async def pin(actual_request, selectors):
        assert actual_request is request and selectors["network_ids"] == "42"
        return manifest, (42,), boundary.access_scope, None

    async def native_read(actual_request, npi):
        assert actual_request is request and npi == _NPI
        if "network_ids" in request.args:
            assert address_scope.current_network_address_scope().manifest is manifest
        async with provider.provider_profile_read_snapshot(
            provider.db, provider._runtime_db_schema(), include_detail=True
        ) as native_session:
            assert native_session is session
            response_body = boundary.cached_body or json.dumps(document_by_field).encode()
            return response.raw(
                response_body,
                status=206,
                content_type="application/json",
                headers={"X-Example-Detail": "retained", "Content-Length": str(len(response_body))},
            )

    monkeypatch.setattr(address_scope, "_pin_network_scope", pin)
    monkeypatch.setattr(provider, "provider_profile_read_snapshot", _direct_reader_snapshot(boundary))
    boundary.native_read = AsyncMock(side_effect=native_read)
    monkeypatch.setattr(provider, "read_npi_detail", boundary.native_read)
    return boundary


@pytest.mark.asyncio
@pytest.mark.parametrize("cached", [False, True])
async def test_direct_detail_keeps_reader_active_through_projection(direct_detail, cached):
    boundary = direct_detail
    if cached:
        boundary.cached_body = json.dumps(boundary.document).encode()
    http_response = await provider.get_npi(boundary.request, _NPI)
    assert json.loads(http_response.body)["address_list"][0]["network_memberships"][0]["network_id"] == 42
    assert boundary.snapshot_calls == 2 and boundary.reader_opens == 1 and boundary.depth == 0
    boundary.session.execute.assert_awaited_once()
    boundary.request.ctx.sa_session.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "grouped,options,evidence",
    [(False, {}, False), (True, {"debug": "true"}, True), (False, {"include_evidence": "true"}, True)],
)
async def test_direct_detail_projects_exact_office(direct_detail, grouped, options, evidence):
    boundary = direct_detail
    if grouped:
        boundary.document["address_groups"] = [{"members": boundary.document.pop("address_list")}]
    boundary.request.args.update(options)
    native_document = deepcopy(boundary.document)
    http_response = await provider.get_npi(boundary.request, _NPI)
    projected = json.loads(http_response.body)
    office = metadata._addresses(projected)[0]
    assert "aca_network_array" not in projected
    assert "aca_network_array" not in office and "ptg_plan_array" not in office
    membership_by_field = {key: _fact()[key] for key in ("network_id", "provider_system", "provider_id", "location_id")}
    if evidence:
        membership_by_field["evidence_ids"] = ["a" * 64]
    assert office["network_memberships"] == [membership_by_field]
    assert office["location_key"] == _SITE and office["telephone_number"] == "555-0101"
    assert office["raw"] == metadata._addresses(native_document)[0]["raw"]
    assert projected["qualifications"] == native_document["qualifications"]
    assert http_response.status == 206 and http_response.headers["X-Example-Detail"] == "retained"
    assert http_response.content_type == "application/json"
    assert http_response.headers["Content-Length"] == str(len(http_response.body))
    assert http_response.headers["X-Network-Generation"] == str(address_manifest().generation_id)
    assert http_response.headers["Cache-Control"] == "private, no-store"
    boundary.session.execute.assert_awaited_once()
    assert boundary.session.execute.await_args.args[1]["metadata_selected"] == [42]
    assert boundary.document == native_document


@pytest.mark.asyncio
@pytest.mark.parametrize("facts", [[], [_fact(network_id=84)], [_fact(address_key=_OTHER)]])
async def test_direct_detail_refuses_missing_membership(direct_detail, facts):
    boundary = direct_detail
    boundary.session.execute.return_value = _session(facts).execute.return_value
    with pytest.raises(ServiceUnavailable) as raised:
        await provider.get_npi(boundary.request, _NPI)
    assert raised.value.status_code == 503
    assert raised.value.headers["Cache-Control"] == "private, no-store"
    boundary.session.execute.assert_awaited_once()


@pytest.mark.asyncio
async def test_cached_detail_rechecks_current_membership(direct_detail):
    boundary = direct_detail
    boundary.cached_body = json.dumps(boundary.document).encode()
    first = await provider.get_npi(boundary.request, _NPI)
    assert json.loads(first.body)["address_list"][0]["network_memberships"][0]["network_id"] == 42
    boundary.session.execute.return_value = _session([]).execute.return_value
    with pytest.raises(ServiceUnavailable):
        await provider.get_npi(boundary.request, _NPI)
    assert boundary.session.execute.await_count == 2
    assert boundary.native_read.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("canonical", [False, True])
async def test_unmanaged_detail_preserves_native_response(direct_detail, canonical):
    boundary = direct_detail
    if canonical:
        boundary.access_scope = None
    else:
        boundary.request.args = {}
    http_response = await provider.get_npi(boundary.request, _NPI)
    assert json.loads(http_response.body) == boundary.document
    assert http_response.status == 206 and http_response.headers["X-Example-Detail"] == "retained"
    boundary.session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_detail_preserves_native_error(direct_detail):
    boundary = direct_detail
    native_error = response.json({"error": "unavailable"}, status=503, headers={"Retry-After": "1"})
    boundary.native_read.side_effect = None
    boundary.native_read.return_value = native_error
    http_response = await provider.get_npi(boundary.request, _NPI)
    assert http_response is native_error and http_response.headers["Retry-After"] == "1"
    assert json.loads(http_response.body) == {"error": "unavailable"}
    boundary.session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_detail_preserves_native_response_without_length(direct_detail):
    """Native raw responses leave framing to the HTTP server."""
    boundary = direct_detail
    boundary.native_read.side_effect = None
    boundary.native_read.return_value = response.raw(
        json.dumps(boundary.document).encode(), content_type="application/json"
    )
    http_response = await provider.get_npi(boundary.request, _NPI)
    assert http_response.status == 200 and "Content-Length" not in http_response.headers
    assert json.loads(http_response.body)["address_list"][0]["network_memberships"][0]["network_id"] == 42
    boundary.session.execute.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("native_body", [b"{", b'{"npi":1}', b'{"npi":true}', b'{"npi":1,"npi":1}'])
async def test_direct_detail_refuses_invalid_document(direct_detail, native_body):
    boundary = direct_detail
    boundary.cached_body = native_body
    with pytest.raises(ServiceUnavailable):
        await provider.get_npi(boundary.request, _NPI)
    boundary.session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_detail_bounds_serialized_response(direct_detail, monkeypatch):
    boundary = direct_detail
    boundary.document["qualifications"]["description"] = "x" * 512
    monkeypatch.setattr(metadata, "MAX_METADATA_BYTES", len(json.dumps(boundary.document).encode()))
    with pytest.raises(ServiceUnavailable):
        await provider.get_npi(boundary.request, _NPI)
    boundary.session.execute.assert_awaited_once()


def _geocode_reader_session(outcome):
    """Model PostgreSQL's aborted transaction until its savepoint rolls back."""
    state = SimpleNamespace(aborted=False, depth=0, rollbacks=0, lookups=[], memberships=0)
    session = _session([_fact()])
    session.info = {"api_reader_verified": True}
    owner_task = asyncio.current_task()
    membership_result = session.execute.return_value

    @asynccontextmanager
    async def savepoint():
        state.depth += 1
        try:
            yield
        except BaseException:
            state.aborted = False
            state.rollbacks += 1
            raise
        finally:
            state.depth -= 1

    async def execute(statement, parameters=None):
        assert asyncio.current_task() is owner_task
        assert not state.aborted, "optional lookup left the shared Reader aborted"
        if "metadata_npis" in (parameters or {}):
            state.memberships += 1
            return membership_result
        sql = str(statement)
        if sql.startswith("SET LOCAL"):
            return None
        if "to_regclass" in sql:
            return SimpleNamespace(scalar=lambda: "mrf.openaddresses_geocode")
        lookup = "openaddresses" if "openaddresses" in sql else "archive"
        assert lookup == "openaddresses" or "address_archive" in sql
        state.lookups.append((lookup, state.depth))
        if outcome == lookup + "_error":
            state.aborted = True
            raise DBAPIError(sql, None, RuntimeError("synthetic optional lookup failure"))
        coordinates = SimpleNamespace(
            long=-87.5,
            lat=41.5,
            place_id=None,
            geo_source="openaddresses",
            geocode_source="openaddresses",
            geocode_quality="exact",
        )
        value = coordinates if outcome == lookup or outcome == "archive_error" and lookup == "openaddresses" else None
        return SimpleNamespace(
            scalar=lambda: value, first=lambda: SimpleNamespace(_mapping=vars(value)) if value else None
        )

    session.begin_nested = savepoint
    session.execute.side_effect = execute
    return session, state


def _install_geocode_detail_loaders(monkeypatch):
    office_by_field = deepcopy(_DIRECT_DETAIL_DOCUMENT["address_list"][0]) | {
        "npi": int(_NPI),
        "checksum": 1,
        "type": "practice",
        "first_line": "100 Example Avenue",
        "city_name": "Example",
        "state_name": "IL",
        "postal_code": "60001",
        "lat": None,
        "long": None,
    }
    replacements_by_name = {
        "_build_npi_details": {"npi": int(_NPI)},
        "_fetch_npi_location_candidates": [office_by_field],
        "_hydrate_selected_provider_locations": [office_by_field],
        "_apply_location_statuses": None,
        "_fetch_other_names": [],
        "_fetch_provider_enrichment_summary_detail": None,
        "_is_table_available": True,
    }
    for name, value in replacements_by_name.items():
        monkeypatch.setattr(provider, name, AsyncMock(return_value=value))
    monkeypatch.setattr(provider, "_NPI_DETAIL_RESPONSE_CACHE_TTL_SECONDS", 0)
    monkeypatch.setenv("HLTHPRT_ADDRESS_ARCHIVE_CUTOVER", "false")
    monkeypatch.setattr(snapshot, "_lock_serving_relations", AsyncMock(return_value={}))
    monkeypatch.setattr(snapshot, "_requires_cms_receipt", AsyncMock(return_value=False))
    monkeypatch.setattr(snapshot, "_read_native_authorities", AsyncMock(return_value={}))


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["archive_error", "openaddresses_error", "archive", "openaddresses"])
async def test_optional_geocodes_preserve_reader_membership(monkeypatch, outcome):
    _install_geocode_detail_loaders(monkeypatch)
    database = db_connection.Database()
    session, state = _geocode_reader_session(outcome)
    monkeypatch.setattr(provider, "db", database)
    writer = AsyncMock(side_effect=AssertionError("geocode reads cannot open a Writer"))
    monkeypatch.setattr(provider, "run_as_writer", writer)
    request = SimpleNamespace(
        args={"lookup_stored_geocode": "true", "sync_geocode": "false", "include_profile": "false", "view": "summary"},
        app=SimpleNamespace(config={"NPI_API_UPDATE_GEOCODE": False}),
    )
    binding = db_connection._TransactionBinding(id(database), session, asyncio.current_task())
    token = db_connection._READER.set(binding)
    try:
        with network_address_read_scope(address_manifest(), (42,), access_scope_sha256="d" * 64):
            http_response = await metadata.read_direct_detail_response(request, _NPI)
        assert database._reader_binding() is binding and not state.aborted
    finally:
        db_connection._READER.reset(token)
    assert http_response.status == 200
    office_by_field = json.loads(http_response.body)["address_list"][0]
    assert office_by_field["telephone_number"] == "555-0101"
    assert office_by_field["network_memberships"][0]["network_id"] == 42
    assert office_by_field["lat"] == (None if outcome == "openaddresses_error" else 41.5)
    assert [name for name, _depth in state.lookups] == (
        ["archive"] if outcome == "archive" else ["archive", "openaddresses"]
    )
    assert all(depth > 0 for _name, depth in state.lookups)
    assert state.rollbacks == int(outcome.endswith("_error")) and state.memberships == 1
    assert snapshot._SNAPSHOT.get() is None and state.depth == 0
    writer.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_offices_without_canonical_coverage_keep_facts_without_invented_membership():
    manifest = address_manifest()
    document_by_field = {
        "npi": int(_NPI),
        "address_list": [
            {"address_key": _ADDRESS, "first_line": "Covered office"},
            {"address_key": _OTHER, "first_line": "Legacy sibling office"},
            {"first_line": "Legacy office without canonical location"},
        ],
    }
    with network_address_read_scope(manifest, (42,), access_scope_sha256="d" * 64, legacy_checksums=(84,)):
        result = await metadata.project_managed_collection_metadata(
            _session([_fact()]), manifest, [document_by_field], include_sources=False, include_evidence=False
        )
    offices = result[0]["address_list"]
    assert len(offices) == 3
    assert offices[0]["network_memberships"][0]["network_id"] == 42
    assert offices[1]["network_memberships"] == offices[2]["network_memberships"] == []
    assert [office["first_line"] for office in offices] == [
        office["first_line"] for office in document_by_field["address_list"]
    ]
