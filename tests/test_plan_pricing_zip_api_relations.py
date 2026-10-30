# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite SQL renderer coverage; these doubles confer no Source authority."""

import hashlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import plan_pricing_code_build as build
from api import plan_pricing_projection_contract as contract
from api import plan_pricing_projection_read as projection
from api import plan_pricing_projection_source as sources
from api import ptg2_serving as serving
from api import ptg2_tables as tables
from api.ptg2_code_scope import _SOURCE_READ_BRAND
from process.ptg_parts import ptg2_v4_finalizer_map_sql as finalizer_sql
from process.ptg_parts import ptg2_v4_finalizer_maps as finalizers


class _CapturedSQL(Exception):
    """Stop a renderer test before any native authority or payload is simulated."""


class _SourceSQL:
    _source_read_brand = _SOURCE_READ_BRAND

    def __init__(self):
        self.statements = []

    def source_relation(self, schema, name):
        return f'ONLY "{schema}"."{name}"'

    async def execute(self, statement, parameters=None):
        self.statements.append((str(statement), parameters))
        raise _CapturedSQL


def test_producer_query_templates_preserve_original_bytes():
    assert hashlib.sha256(tables._PUBLISHED_SNAPSHOT_SQL.encode()).hexdigest() == (
        "832e1a1beeabed471c7e32f4c78f4cfb3245c137cab671d2b4eff2509cb402f1"
    )
    assert hashlib.sha256(finalizer_sql._ROOT_SELECTION_SQL.encode()).hexdigest() == (
        "2fcee77d3b23a35575960d2abee20e44acdb3abc6648de2c2f6ba362073198d1"
    )


def test_published_read_routes_every_fixed_relation_without_changing_predicates():
    session = _SourceSQL()
    sql = tables._published_source_read_sql(session, "synthetic")
    expected = tables._PUBLISHED_SNAPSHOT_SQL.replace("__PTG2_SCHEMA__", "synthetic")
    for name in tables._PUBLISHED_SOURCE_RELATIONS:
        assert f'ONLY "synthetic"."{name}"' in sql
        sql = sql.replace(f'ONLY "synthetic"."{name}"', "synthetic." + name)
    assert sql == expected


def test_logical_plan_mapping_keeps_original_columns_binds_and_order():
    session = _SourceSQL()
    source = SimpleNamespace(snapshot_id="synthetic")
    options_by_name = {"requested_plan": "plan", "plan_market_type": "group"}
    default = serving._shared_v3_code_scope_sql(source, **options_by_name)
    api = serving._shared_v3_code_scope_sql(source, api_source_session=session, **options_by_name)
    sql = api[0]
    for name in ("ptg2_v3_snapshot_scope", "ptg2_v3_snapshot_plan_scope"):
        sql = sql.replace(f'ONLY "{serving.PTG2_SCHEMA}"."{name}"', serving.PTG2_SCHEMA + "." + name)
    assert (sql, *api[1:]) == default


async def test_binding_market_reader_uses_only_the_verified_plan_heap():
    session = _SourceSQL()
    with pytest.raises(_CapturedSQL):
        await sources._binding_market_type(session, serving, "snapshot", "plan", "group")
    sql, parameters = session.statements[0]
    assert f'FROM ONLY "{serving.PTG2_SCHEMA}"."ptg2_v3_snapshot_plan_scope"' in sql
    assert parameters == {"snapshot_id": "snapshot", "plan_id": "plan", "market_type": "group"}


async def test_graph_and_finalizer_readers_close_all_payload_joins(monkeypatch):
    session = _SourceSQL()
    with pytest.raises(_CapturedSQL):
        await tables._load_v4_provider_graph_root(
            session, snapshot_key=3, physical_binding=SimpleNamespace(schema_name="synthetic")
        )
    sql = session.statements.pop()[0]
    for name in ("ptg2_v4_snapshot_map_root", "ptg2_v3_snapshot_layout", tables.PTG2_V4_GRAPH_DIAGNOSTIC_TABLE):
        assert f'ONLY "synthetic"."{name}"' in sql
    monkeypatch.setattr(finalizers, "_has_finalizer_map_tables", AsyncMock(return_value=True))
    with pytest.raises(_CapturedSQL):
        await finalizers.read_v4_finalizer_map_set(session, schema_name="synthetic", snapshot_keys=[3])
    sql = session.statements.pop()[0]
    for name in ("ptg2_v4_finalizer_map_root", "ptg2_v3_snapshot_layout", "ptg2_v3_snapshot_block"):
        assert f'ONLY "synthetic"."{name}"' in sql


def test_geo_and_provider_state_have_finite_direct_heap_reads():
    center, params = projection._center_query({}, "48104", api_read=True)
    assert "FROM ONLY " in center and params == {"zip5": "48104"}
    assert projection._radius_cells_sql(center, api_read=True).count("FROM ONLY ") == 2
    assert contract._provider_signature_sql(api_read=True).count("FROM ONLY ") == 2
    assert "FROM ONLY " not in contract._provider_signature_sql()


@pytest.mark.parametrize("shape", [(3, "p", False), (3, "r", True)])
def test_renderer_cannot_admit_partition_or_inherited_heap(shape):
    reader = build._CodeAPISetup(SimpleNamespace(info={}), {("synthetic", "ptg2_v3_code"): shape})
    with pytest.raises(contract.PlanPricingProjectionUnavailable, match="routing"):
        reader.source_relation("synthetic", "ptg2_v3_code")


def test_private_physical_cache_alone_is_not_read_authority():
    session = SimpleNamespace(info={"ptg2_local_read_bindings": {"synthetic": object()}})
    with pytest.raises(contract.PlanPricingProjectionUnavailable, match="routing"):
        build._CodeAPISetup(session).source_relation("synthetic", "ptg2_v3_code")
