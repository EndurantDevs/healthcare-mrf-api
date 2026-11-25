# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded office lookups retain the ordinary selector and request identity."""

import json
import re
from dataclasses import asdict, replace
from types import SimpleNamespace

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
from api.network_read_execution import qualified_relation_sql
from api.provider_geo_sql import nearby_row_tiebreaker
from db.models import EntityAddressUnified
from process import network_address_artifact as named
from process import network_bootstrap_address_artifact as bootstrap
from process import network_indexed_address_artifact as indexed
from tests import test_custom_import_provider_geo_sql as geo
from tests import test_network_em_provider_inputs as em_inputs
from tests.network_compact_address_postgres_support import _layout, _lookup_manifest, _manifest


@pytest.mark.parametrize(
    ("contract", "revision", "expected_lookup"),
    [
        (None, None, False),
        (bootstrap.CONTRACT, None, False),
        ("unknown", None, False),
        (indexed.CONTRACT, None, True),
        (named.CONTRACT, None, False),
        (named.CONTRACT, 2, True),
    ],
)
def test_scoped_lookup_requires_verified_indexed_dependency(contract, revision, expected_lookup):
    manifest = _lookup_manifest(_layout(), contract, index_definition_revision=revision)
    assert manifest.supports_location_lookup is expected_lookup
    # The published fixture row carries a genuine digest in place of the placeholder.
    expected = asdict(_manifest(manifest.compact_layout)) | {"manifest_sha256": manifest.manifest_sha256}
    assert asdict(manifest) == expected
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
    separator = " WHERE (office.location_key IN ("
    assert ordinary.split(" WHERE EXISTS (")[0] == lookup.split(separator)[0]
    assert lookup.endswith("bucket_members OFFSET 0)) IS TRUE)")
    assert "office." not in compact._membership_arms(layout, selected=True)
    assert "bucket_members" not in ordinary
    selected = compact._membership_arms(layout, selected=True, location_lookup=True)
    assert selected.endswith(") bucket_members OFFSET 0")
    for index, artifact in enumerate(layout.artifacts):
        arm = selected.split(" UNION ALL ")[index]
        assert f'FROM ONLY "{layout.ref_map.schema_name}"."network_slice_ref" ref' in arm
        assert f'FROM ONLY "{artifact.schema_name}".network_office_edge edge' in arm
        assert "edge.slice_id=ANY(ARRAY(" in arm and "SELECT ref.slice_id" in arm
        assert f"ref.artifact_candidate_id=CAST(:_compact_artifact_{index} AS uuid)" in arm
        assert "ref.network_id=ANY(CAST(:_canonical_network_ids AS integer[]))" in arm
        assert "edge.location_key=office.location_key" in arm
        assert "ref.office_bucket=(get_byte(sha256(convert_to(office.location_key,'UTF8')),0)&63)" in arm
    for visibility in (None, "TRUE"):
        assert compact.compact_address_relation_sql(layout, visible_membership_sql=visibility) == (
            compact.compact_address_relation_sql(layout, visible_membership_sql=visibility, location_lookup=True)
        )


def test_global_selection_checks_memberships_once_before_scanning_offices():
    layout = _layout()
    selected = compact._membership_arms(layout, selected=True)
    ordinary = compact.compact_address_relation_sql(layout, network_ids=(42,))
    assert f" WHERE EXISTS ({selected}) AND office.location_key IN ({selected})" in ordinary
    lookup = compact.compact_address_relation_sql(layout, network_ids=(42,), location_lookup=True)
    assert f"EXISTS ({selected})" not in lookup
    assert compact.compact_address_relation_sql(replace(layout, artifacts=()), network_ids=(42,)).endswith(
        " WHERE FALSE)"
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


@pytest.mark.parametrize("lookup_mode", [False, True])
def test_lookup_preserves_binds_columns_and_named_query_scope(lookup_mode):
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
        typed = compact.compact_address_selectable(layout, network_ids=(7, 42), location_lookup=lookup_mode)
        compiled = typed.element.compile(dialect=PGDialect_asyncpg())
        native, arguments = compact.compact_address_native_sql(
            layout, network_ids=(7, 42), location_lookup=lookup_mode, first_parameter=3
        )
        assert arguments == tuple(compiled.params[name] for name in compiled.positiontup)
        assert "$1" not in native and "$2" not in native and "$3" in native
        assert all(candidate in arguments for candidate in compact.compact_address_parameters(layout).values())
        assert tuple(typed.c.keys()) == tuple(EntityAddressUnified.__table__.columns.keys())
        assert native == "(" + re.sub(r"\$(\d+)", lambda match: "$" + str(int(match[1]) + 2), str(compiled)) + ")"
        typed_sql = str(select(typed).compile(dialect=PGDialect_asyncpg()))
        expected = compact.compact_address_read_relations(layout)
        assert len(expected) == 5 and len(set(expected)) == 5
        for relation in expected:
            qualified = qualified_relation_sql(relation.schema_name, relation.table_name)
            unquoted_table = '"' + relation.schema_name + '".' + relation.table_name
            assert all(qualified in query or unquoted_table in query for query in (ordinary, lookup, typed_sql, native))
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
        for relation in compact.compact_address_read_relations(current_network_address_scope().manifest.compact_layout):
            qualified = qualified_relation_sql(relation.schema_name, relation.table_name)
            unquoted_table = '"' + relation.schema_name + '".' + relation.table_name
            assert all(qualified in query or unquoted_table in query for query in (page, ordinary))


async def test_custom_geo_uses_local_lookup(monkeypatch):
    session = SimpleNamespace(calls=[])
    context = geo._context()
    monkeypatch.setattr(provider_list_sql, "ConnectionProxy", geo._RecordingGeoProxy)
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


@pytest.mark.parametrize(
    "columns",
    [(), ("location_key", "location_key"), ("missing_column",), ["source_count"], "source_count", (None,), ([],)],
)
def test_native_projection_rejects_invalid_columns(columns):
    with pytest.raises(ValueError, match="projection"):
        compact.compact_address_native_sql(_layout(), first_parameter=1, columns=columns)


@pytest.mark.parametrize("lookup_mode", [False, True])
def test_native_projection_preserves_hidden_columns(lookup_mode):
    layout = _layout()
    query, arguments = compact.compact_address_native_sql(
        layout, first_parameter=3, visible_membership_sql="TRUE", columns=("source_count",)
    )
    assert query.startswith('(SELECT "source_count" FROM (')
    assert 'office."row_origin"' in query and "visible_office WHERE row_origin" in query
    assert "AS canonical_network_ids" not in query and "network_office_edge" not in query
    assert arguments == ()
    selected, arguments = compact.compact_address_native_sql(
        layout, first_parameter=3, network_ids=(7, 42), location_lookup=lookup_mode, columns=("source_count",)
    )
    assert selected.startswith('(SELECT "source_count" FROM (')
    assert 'office."location_key"' in selected
    assert ("bucket_members OFFSET 0" in selected) is lookup_mode
    assert (" WHERE EXISTS (" in selected) is (not lookup_mode)
    assert set(map(int, re.findall(r"\$(\d+)", selected))) == set(range(3, 3 + len(arguments)))
    assert all(candidate in arguments for candidate in compact.compact_address_parameters(layout).values())
    assert [7, 42] in arguments


def test_native_projection_retains_requested_membership_arrays():
    query, arguments = compact.compact_address_native_sql(
        _layout(), first_parameter=1, visible_membership_sql="TRUE", columns=("canonical_network_ids",)
    )
    assert query.startswith('(SELECT "canonical_network_ids" FROM (')
    assert 'office."location_key"' in query and 'office."row_origin"' in query
    assert "ARRAY(SELECT DISTINCT membership.network_id" in query
    assert arguments == tuple(compact.compact_address_parameters(_layout()).values())
