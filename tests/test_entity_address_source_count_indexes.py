# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Build the same plain source-count index across address candidate paths."""

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db.models.entity_address_unified import ENTITY_ADDRESS_UNIFIED_SERVING_STAGE_INDEXES, EntityAddressUnified
from process import network_compact_preparation as compact
from process import network_membership_serving_indexes as network
from process.network_compact_layout import CompactAddressRelation

address = importlib.import_module("process.entity_address_unified")
restore = importlib.import_module("process.entity_address_snapshot_restore")


def _source_count_index():
    definitions = [index for index in EntityAddressUnified.__my_additional_indexes__ if index["name"] == "source_count"]
    assert definitions == [{"index_elements": ("source_count",), "name": "source_count"}]
    return definitions[0]


def test_source_count_index_preserves_stored_column_and_profile_defaults(monkeypatch):
    _source_count_index()
    assert "source_count" in ENTITY_ADDRESS_UNIFIED_SERVING_STAGE_INDEXES
    column = EntityAddressUnified.__table__.c.source_count
    assert not column.nullable and str(column.type) == "INTEGER"
    assert column.server_default.arg == "0"
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE", raising=False)
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_POST_PUBLISH_INDEX_PROFILE", raising=False)
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES", raising=False)
    assert address._stage_index_profile() == "all"
    assert address._post_publish_index_profile() == "none"


@pytest.mark.parametrize("profile,enabled", [("all", True), ("serving", True), ("none", False)])
def test_stage_plan_respects_source_count_index_profiles(monkeypatch, profile, enabled):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE", profile)
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES", raising=False)
    stage = SimpleNamespace(__tablename__="address_candidate", __main_table__=EntityAddressUnified.__main_table__)
    context_map = {}
    statements = address._stage_index_statements(stage, "example", [_source_count_index()], context_map)
    expected = (
        "CREATE INDEX IF NOT EXISTS address_candidate_idx_source_count ON example.address_candidate (source_count);"
    )
    assert statements == ([("source_count", expected)] if enabled else [])
    assert context_map.get("skipped_stage_indexes", []) == ([] if enabled else ["address_candidate.source_count"])


@pytest.mark.parametrize("concurrent", [False, True])
def test_live_and_network_plans_share_the_plain_source_count_index(concurrent):
    statements, skipped = address._post_publish_index_plan("example", "serving", build_concurrently=concurrent)
    modifier = "CONCURRENTLY " if concurrent else ""
    assert dict(statements)["source_count"] == (
        f"CREATE INDEX {modifier}IF NOT EXISTS entity_address_unified_idx_source_count "
        "ON example.entity_address_unified (source_count);"
    )
    assert "entity_address_unified.source_count" not in skipped
    assert dict(network._serving_plan("example"))["entity_address_unified_idx_source_count"] == (
        'CREATE INDEX entity_address_unified_idx_source_count ON "example".entity_address_unified (source_count);'
    )
    disabled, skipped = address._post_publish_index_plan("example", "none", build_concurrently=concurrent)
    assert not disabled and "entity_address_unified.source_count" in skipped


@pytest.mark.asyncio
@pytest.mark.parametrize("profile,enabled", [("all", True), ("serving", True), ("none", False)])
async def test_native_restore_generates_the_same_source_count_index(monkeypatch, profile, enabled):
    monkeypatch.setenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_STAGE_INDEX_PROFILE", profile)
    monkeypatch.delenv("HLTHPRT_ENTITY_ADDRESS_UNIFIED_DEFER_ADDITIONAL_INDEXES", raising=False)
    session = AsyncMock()
    _schema, date, stage_names = restore._stage_plan(db_schema="example", import_date="20990101")
    await restore._create_model_relations(
        session, schema_name="example_restore", stage_names=stage_names, import_date=date
    )
    expected = restore._additional_index_sql(
        schema_name="example_restore",
        table_name=EntityAddressUnified.__tablename__,
        stage_table_name=stage_names[EntityAddressUnified.__tablename__],
        index=_source_count_index(),
    )
    assert expected == (
        'CREATE INDEX "entity_address_unified_20990101_idx_source_count" '
        'ON "example_restore"."entity_address_unified" (source_count)'
    )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert (expected in statements) is enabled


@pytest.mark.asyncio
async def test_compact_custom_offices_build_the_shared_serving_index_plan(monkeypatch):
    build = AsyncMock()
    require = AsyncMock()
    monkeypatch.setattr(network, "_build_serving_indexes", build)
    monkeypatch.setattr(network, "_require_address_search_indexes", require)
    monkeypatch.setattr(compact, "capture_compact_edge_artifacts", AsyncMock(return_value=()))
    geo_search = importlib.import_module("process.network_geo_search")
    monkeypatch.setattr(geo_search, "prepare_network_geo_search", AsyncMock(return_value=None))
    connection = object()
    target = SimpleNamespace(schema_name="example_candidate")
    offices = SimpleNamespace(
        physical_base=CompactAddressRelation("example_source", "entity_address_unified"),
        custom=CompactAddressRelation("example_candidate", "entity_address_unified"),
    )
    await compact._prepare_address_search(connection, target, offices, SimpleNamespace(), "example_control")
    build.assert_awaited_once_with(connection, "example_candidate", network._serving_plan("example_candidate"))
    require.assert_awaited_once_with(connection, offices.custom)
    planned_by_name = dict(build.await_args.args[2])
    assert planned_by_name["entity_address_unified_idx_source_count"] == (
        "CREATE INDEX entity_address_unified_idx_source_count "
        'ON "example_candidate".entity_address_unified (source_count);'
    )
