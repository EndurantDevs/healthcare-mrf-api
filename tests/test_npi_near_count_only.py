# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact nearby counts share the pinned office predicate without reading a page."""

import json
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from sanic.exceptions import InvalidUsage

from api import provider_list_sql
from api.endpoint import npi
from api.network_address_scope import network_address_read_scope
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from process.network_serving_read import PinnedNetworkServingManifest
from tests.openapi_route_contract_support import load_openapi_document
from tests.test_npi_near_snapshot import near_snapshot
from tests.test_openapi_spec import _collect_code_routes, _collect_spec_routes


@pytest.fixture
def count_reader(monkeypatch):
    state = SimpleNamespace(session=object(), calls=[], total=7)

    async def query(statement, **parameters):
        state.calls.append((str(statement), parameters))
        assert "COUNT(DISTINCT (a.npi, a.address_key))" in str(statement)
        return [SimpleNamespace(_mapping={"total_count": state.total})]

    proxy = Mock(return_value=SimpleNamespace(all=query))
    monkeypatch.setattr(provider_list_sql, "NetworkReadConnectionProxy", proxy)
    monkeypatch.setattr(npi, "db", SimpleNamespace(acquire=Mock(side_effect=AssertionError("unpinned read"))))
    monkeypatch.setattr(npi, "_plan_release_npi_scope", AsyncMock(return_value=(None, {})))
    metadata = AsyncMock(side_effect=AssertionError("count must not hydrate provider metadata"))
    monkeypatch.setattr(npi, "_project_provider_network_metadata", metadata)
    state.proxy = proxy
    return state


def _pinned_manifest(generation):
    candidate_id = UUID(int=generation)
    return PinnedNetworkServingManifest(
        generation, str(candidate_id), "network_candidate_" + candidate_id.hex, generation + 10, {}, 0, "c" * 64, 1
    )


@pytest.mark.parametrize("selected_ids,excluded_ids", (((42, 84), None), (None, (84,))))
@pytest.mark.parametrize("generation,total", ((12, 7), (13, 0)))
@pytest.mark.parametrize("count_only,include_total,limit", (("1", "0", "1"), ("true", "1", "50")))
async def test_near_count_uses_exact_pinned_filter(
    count_reader, selected_ids, excluded_ids, generation, total, count_only, include_total, limit
):
    manifest = _pinned_manifest(generation)
    count_reader.total = total
    query_by_name = {
        "lat": "41",
        "long": "-87",
        "radius": "10",
        "classification": "Internal Medicine",
        "exclude_npi": "1000000012",
        "entity_type_code": "1",
        "provider_sex_code": "F",
        "count_only": count_only,
        "include_total": include_total,
        "limit": limit,
    }
    request = SimpleNamespace(
        args=query_by_name, ctx=SimpleNamespace(sa_session=count_reader.session), app=SimpleNamespace()
    )
    with network_address_read_scope(
        manifest, selected_ids, access_scope_sha256="d" * 64, excluded_network_ids=excluded_ids
    ):
        query_by_name["cursor"] = npi._encode_nearby_cursor(
            npi._nearby_cursor_scope(query_by_name), 100, 1000000004, "00000000-0000-0000-0000-000000000001"
        )
        document = json.loads((await npi.get_near_npi(request)).body)
    assert document == {"rows": total}
    assert len(count_reader.calls) == 1
    assert all(call.args[1] is count_reader.session for call in count_reader.proxy.call_args_list)
    sql, parameters = count_reader.calls[0]
    assert f'FROM ONLY "{manifest.schema_name}".entity_address_unified' in sql
    assert "LIMIT" not in sql and "cursor_" not in sql
    assert "limit" not in parameters and not any(key.startswith("cursor_") for key in parameters)
    assert {"min_lat", "max_lat", "min_long", "max_long"} <= parameters.keys()
    assert parameters["exclude_npi"] == 1000000012 and parameters["provider_sex_code"] == "F"
    assert parameters["classification"] == "Internal Medicine" and parameters["entity_type_code"] == 1
    assert "a.npi <> :exclude_npi" in sql and "d.entity_type_code = :entity_type_code" in sql
    if selected_ids is not None:
        assert "WHERE canonical_network_ids && CAST(:_canonical_network_ids AS INTEGER[])" in sql
        assert parameters["_canonical_network_ids"] == list(selected_ids)
    else:
        assert "row_origin IN ('base','ptg_overlay') OR EXISTS" in sql
        assert "NOT policy_ids.network_id=ANY(CAST(:_network_excluded_ids AS integer[]))" in sql
        assert parameters["_network_excluded_ids"] == [84]
        assert parameters["_network_approved_revision"] == manifest.approved_custom_revision


@pytest.mark.parametrize("selected_ids,excluded_ids", (((42, 84), None), (None, (84,))))
@pytest.mark.parametrize("generation", (12, 13))
async def test_near_compact_count_keeps_generation_arrays(count_reader, selected_ids, excluded_ids, generation):
    manifest = _pinned_manifest(generation)
    artifact_id = UUID(int=generation + 100)
    artifact_schema = "network_candidate_" + artifact_id.hex
    layout = CompactNetworkLayout(
        CompactAddressRelation(manifest.schema_name, "network_slice_ref", 2),
        CompactAddressRelation("retained_addresses", "entity_address_unified", 1),
        (CompactEdgeArtifact(str(artifact_id), artifact_schema, 3),),
    )
    object.__setattr__(manifest, "_compact_layout", layout)
    request = SimpleNamespace(
        args={"lat": "41", "long": "-87", "count_only": "1"},
        ctx=SimpleNamespace(sa_session=count_reader.session),
        app=SimpleNamespace(),
    )
    with network_address_read_scope(
        manifest, selected_ids, access_scope_sha256="d" * 64, excluded_network_ids=excluded_ids
    ):
        document = json.loads((await npi.get_near_npi(request)).body)
    assert document == {"rows": 7} and len(count_reader.calls) == 1
    sql, parameters = count_reader.calls[0]
    assert 'FROM ONLY "retained_addresses"."entity_address_unified" base' in sql
    assert f'FROM ONLY "{manifest.schema_name}"."network_slice_ref" ref' in sql
    assert f'JOIN ONLY "{artifact_schema}".network_office_edge edge' in sql
    assert "ARRAY(SELECT DISTINCT membership.network_id" in sql
    assert parameters["_compact_artifact_0"] == str(artifact_id)
    if selected_ids is not None:
        assert parameters["_canonical_network_ids"] == list(selected_ids)
    else:
        assert parameters["_network_approved_revision"] == manifest.approved_custom_revision
        assert parameters["_network_excluded_ids"] == [84]


@pytest.mark.parametrize("include_total", ("0", "1"))
async def test_near_count_keeps_snapshot_and_skips_page(near_snapshot, include_total):
    near_snapshot.total_count = 37
    reply = await npi.get_near_npi(
        SimpleNamespace(
            args={"lat": "41", "long": "-87", "limit": "1", "count_only": "1", "include_total": include_total},
            app=SimpleNamespace(),
        )
    )
    assert json.loads(reply.body) == {"rows": 37}
    assert near_snapshot.reads == ["address", "plan", "acquire", "count", "encode"]
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active


async def test_near_unresolved_count_is_scalar_zero(near_snapshot, monkeypatch):
    monkeypatch.setattr(npi, "_resolve_internal_filter_codes", AsyncMock(return_value=([], None)))
    reply = await npi.get_near_npi(
        SimpleNamespace(
            args={"procedure_codes": "1001", "count_only": "1", "include_total": "1"},
            app=SimpleNamespace(),
        )
    )
    assert json.loads(reply.body) == {"rows": 0}
    assert near_snapshot.reads == ["year", "encode"]
    assert near_snapshot.entered == near_snapshot.exited == 1 and not near_snapshot.active


@pytest.mark.parametrize("count_only", (None, "0", "false"))
async def test_near_noncount_keeps_page_contract(near_snapshot, count_only):
    first = near_snapshot.provider_rows[0]._mapping
    near_snapshot.provider_rows.append(
        SimpleNamespace(
            _mapping={
                **first,
                "npi": 1000000012,
                "cursor_distance_meters": 200.0,
            }
        )
    )
    near_snapshot.total_count = 2
    query_by_name = {"lat": "41", "long": "-87", "limit": "1", "include_total": "1"}
    if count_only is not None:
        query_by_name["count_only"] = count_only
    first_page = json.loads((await npi.get_near_npi(SimpleNamespace(args=query_by_name, app=SimpleNamespace()))).body)
    assert first_page["total_count"] == 2 and first_page["has_more"] is True
    assert first_page["result_identity"] == ["npi", "address_key"]
    assert len(first_page["items"]) == 1 and first_page["items"][0]["npi"] == 1000000004
    near_snapshot.provider_rows = near_snapshot.provider_rows[1:]
    query_by_name["cursor"] = first_page["next_cursor"]
    next_page = json.loads((await npi.get_near_npi(SimpleNamespace(args=query_by_name, app=SimpleNamespace()))).body)
    assert next_page["total_count"] == 2 and next_page["has_more"] is False
    assert next_page["next_cursor"] is None and next_page["items"][0]["npi"] == 1000000012


async def test_near_count_refuses_invalid_cursor_before_snapshot(near_snapshot):
    with pytest.raises(InvalidUsage):
        await npi.get_near_npi(
            SimpleNamespace(
                args={"lat": "41", "long": "-87", "count_only": "1", "cursor": "invalid"},
                app=SimpleNamespace(),
            )
        )
    assert near_snapshot.entered == near_snapshot.exited == 0 and near_snapshot.reads == []


def test_near_count_openapi_matches_handler():
    route = ("get", "/npi/near/")
    assert _collect_code_routes()[route]["query_params"] == _collect_spec_routes()[route]["query_params"]
    operation = load_openapi_document(Path("doc/openapi.yaml"))["paths"]["/npi/near/"]["get"]
    count_parameter = next(parameter for parameter in operation["parameters"] if parameter["name"] == "count_only")
    assert count_parameter["schema"] == {"type": "boolean", "default": False}
    responses = operation["responses"]["200"]["content"]["application/json"]["schema"]["oneOf"]
    assert {"$ref": "#/components/schemas/NpiCountResponse"} in responses
