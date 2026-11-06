# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded office lookups retain the ordinary selector and request identity."""

import json
from dataclasses import asdict
from types import SimpleNamespace
from uuid import UUID

import pytest
from sanic.request.parameters import RequestParameters
from sqlalchemy import select
from sqlalchemy.dialects.postgresql.asyncpg import PGDialect_asyncpg

from api import network_compact_address as compact
from api import plan_pricing_em_managed as managed
from api import provider_list_sql
from api.network_address_scope import (
    current_network_address_scope,
    is_scoped_address_relation,
    network_address_read_scope,
    scoped_address_cache_key,
    scoped_address_relation_sql,
)
from api.network_read_execution import network_read_relations
from api.provider_geo_sql import nearby_row_tiebreaker
from process import network_bootstrap_address_artifact as bootstrap
from process import network_indexed_address_artifact as indexed
from process.network_compact_layout import CompactAddressRelation, CompactEdgeArtifact, CompactNetworkLayout
from tests import test_custom_import_provider_geo_sql as geo
from tests import test_network_em_provider_inputs as em_inputs
from tests.test_network_compact_address_postgres import _lookup_manifest, _manifest


def _layout():
    artifacts = tuple(
        CompactEdgeArtifact(str(UUID(int=index)), "network_candidate_" + UUID(int=index).hex, index) for index in (1, 2)
    )
    return CompactNetworkLayout(
        CompactAddressRelation(artifacts[-1].schema_name, "network_slice_ref", 3),
        CompactAddressRelation("held_base", "entity_address_unified", 4),
        artifacts,
        CompactAddressRelation("held_custom", "entity_address_unified", 5),
    )


@pytest.mark.parametrize("contract", [None, bootstrap.CONTRACT, "unknown", indexed.CONTRACT])
def test_scoped_lookup_requires_verified_indexed_dependency(contract):
    manifest = _lookup_manifest(_layout(), contract)
    assert manifest.supports_location_lookup is (contract == indexed.CONTRACT)
    assert asdict(manifest) == asdict(_manifest(manifest.compact_layout))
    with network_address_read_scope(manifest, (42,)):
        ordinary = scoped_address_relation_sql("mrf.entity_address_unified")
        lookup = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
        assert (lookup != ordinary) is manifest.supports_location_lookup
        assert ("bucket_members OFFSET 0" in lookup) is manifest.supports_location_lookup


def test_unverified_manifest_keeps_ordinary_lookup():
    manifest = _manifest(_layout())
    assert not manifest.supports_location_lookup
    with network_address_read_scope(manifest, (42,)):
        assert scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True) == (
            scoped_address_relation_sql("mrf.entity_address_unified")
        )


def test_lookup_preserves_ordinary_query():
    layout = _layout()
    ordinary = compact.compact_address_relation_sql(layout, network_ids=(7, 42))
    lookup = compact.compact_address_relation_sql(layout, network_ids=(7, 42), location_lookup=True)
    separator = " WHERE office.location_key IN ("
    assert ordinary.split(separator)[0] == lookup.split(separator)[0]
    assert "office." not in compact._membership_arms(layout, selected=True)
    assert "bucket_members" not in ordinary
    selected = compact._membership_arms(layout, selected=True, location_lookup=True)
    assert selected.endswith(") bucket_members OFFSET 0")
    for index, artifact in enumerate(layout.artifacts):
        arm = selected.split(" UNION ALL ")[index]
        assert f'FROM ONLY "{layout.ref_map.schema_name}"."network_slice_ref" ref' in arm
        assert f'JOIN ONLY "{artifact.schema_name}".network_office_edge edge' in arm
        assert "edge.slice_id=ref.slice_id" in arm
        assert f"ref.artifact_candidate_id=CAST(:_compact_artifact_{index} AS uuid)" in arm
        assert "ref.network_id=ANY(CAST(:_canonical_network_ids AS integer[]))" in arm
        assert "edge.location_key=office.location_key" in arm
        assert "ref.office_bucket=(get_byte(sha256(convert_to(office.location_key,'UTF8')),0)&63)" in arm
    for visibility in (None, "TRUE"):
        assert compact.compact_address_relation_sql(layout, visible_membership_sql=visibility) == (
            compact.compact_address_relation_sql(layout, visible_membership_sql=visibility, location_lookup=True)
        )


@pytest.mark.parametrize("lookup", [None, 0, 1, "true", (), {}])
def test_lookup_rejects_untyped_switch(lookup):
    for builder, options in (
        (compact.compact_address_relation_sql, {}),
        (compact.compact_address_selectable, {}),
        (compact.compact_address_native_sql, {"first_parameter": 1}),
    ):
        with pytest.raises(ValueError, match="lookup"):
            builder(_layout(), network_ids=(42,), location_lookup=lookup, **options)
    with pytest.raises(ValueError, match="lookup"):
        scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=lookup)


def test_lookup_preserves_binds_and_guard():
    layout = _layout()
    with network_address_read_scope(_lookup_manifest(layout), (7, 42)) as scope:
        cache_key = scoped_address_cache_key("same-query")
        default_relation = scope.address_relation
        ordinary = scoped_address_relation_sql("mrf.entity_address_unified")
        lookup = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
        assert ordinary != lookup
        for relation in (ordinary, lookup):
            assert is_scoped_address_relation(relation) and provider_list_sql._is_unified_address_table(relation)
            assert nearby_row_tiebreaker(relation) == "a.location_key ASC"
        assert not is_scoped_address_relation(lookup + " ")
        typed = compact.compact_address_selectable(
            layout, network_ids=(7, 42), location_lookup=True, read_statements=scope.read_statements
        )
        compiled = typed.element.compile(dialect=PGDialect_asyncpg())
        native, arguments = compact.compact_address_native_sql(
            layout, network_ids=(7, 42), location_lookup=True, first_parameter=3
        )
        assert arguments == tuple(compiled.params[name] for name in compiled.positiontup)
        assert "$1" not in native and "$2" not in native and "$3" in native
        assert all(candidate in arguments for candidate in compact.compact_address_parameters(layout).values())
        assert (
            network_read_relations(ordinary) == network_read_relations(lookup) == network_read_relations(select(typed))
        )
        assert scope.address_relation is default_relation and current_network_address_scope() is scope
        assert scoped_address_cache_key("same-query") == cache_key
    assert current_network_address_scope() is None and not is_scoped_address_relation(lookup)


@pytest.mark.parametrize("legacy", [False, True])
def test_lookup_preserves_visibility_modes(legacy):
    options = {"legacy_checksums": (11,)} if legacy else {"excluded_network_ids": (99,)}
    with network_address_read_scope(
        _lookup_manifest(_layout()), (42,) if legacy else None, access_scope_sha256="b" * 64, **options
    ):
        assert scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True) == (
            scoped_address_relation_sql("mrf.entity_address_unified")
        )
        assert scoped_address_relation_sql("mrf.npi_address", location_lookup=True) == "mrf.npi_address"


def test_managed_em_bounds_lookup_source():
    with network_address_read_scope(_lookup_manifest(_layout()), (42,)):
        ordinary = scoped_address_relation_sql("mrf.entity_address_unified")
        lookup = scoped_address_relation_sql("mrf.entity_address_unified", location_lookup=True)
        page = managed._page_sql(em_inputs.family(), '"held_rates"."rates"')
        assert f"FROM {lookup} addr" in page and f"FROM {ordinary} addr" not in page
        assert "public.ST_DWithin(" in page and "LIMIT :candidate_limit" in page
        assert network_read_relations(page) == network_read_relations(ordinary)


async def test_custom_geo_uses_local_lookup(monkeypatch):
    session = SimpleNamespace(calls=[])
    context = geo._context()
    monkeypatch.setattr(provider_list_sql, "NetworkReadConnectionProxy", geo._RecordingGeoProxy)
    monkeypatch.setattr(geo.npi_module, "_plan_release_npi_scope", geo._empty_plan_scope)
    native_args = RequestParameters({"lat": ["0"], "long": ["0"], "include_total": ["true"], "limit": ["2"]})

    async def prepare_cursor(**options):
        assert is_scoped_address_relation(options["address_table_sql"])
        assert "bucket_members OFFSET 0" in options["address_table_sql"]
        assert options["query_parameters"]["in_lat"] == options["query_parameters"]["in_long"] == 0.0
        return None

    async def prepare_scope(native_entity_scope):
        return geo._scoped_import_context(context, native_entity_scope)

    with network_address_read_scope(_lookup_manifest(_layout()), (42,)) as scope:
        reply = await geo.npi_module.get_near_npi(
            SimpleNamespace(args={}, ctx=SimpleNamespace(sa_session=session)),
            native_args=native_args,
            import_context=context,
            prepare_cursor=prepare_cursor,
            native_scope_preparer=prepare_scope,
        )
        assert current_network_address_scope() is scope
        assert "bucket_members" not in scoped_address_relation_sql("mrf.entity_address_unified")
    body = json.loads(reply.body)
    assert reply.status == 200 and body["total_count"] == 3 and len(body["items"]) == 2
    assert body["_custom_import_next_anchor"] == ["1000000003", geo._THIRD_ADDRESS_KEY]
    geo._assert_native_geo_scope(session.calls[0][0])
