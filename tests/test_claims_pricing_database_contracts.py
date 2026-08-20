# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.provider_quality_parts import state as provider_quality_state
from tests.claims_pricing_contract_fakes import AsyncTransaction


claims_pricing = importlib.import_module("process.claims_pricing")


def test_finalize_reuses_shared_import_mutex():
    assert (
        claims_pricing._has_global_finalize_lock
        is provider_quality_state._has_global_finalize_lock
    )
    assert (
        claims_pricing._release_global_finalize_lock
        is provider_quality_state._release_global_finalize_lock
    )


@pytest.mark.asyncio
async def test_finalize_waits_for_shared_import_mutex(monkeypatch):
    redis = object()
    finalize_spec = claims_pricing._ClaimsFinalizeSpec("i", "r", False, "mrf", "s", 0)
    acquire_global_lock = AsyncMock(return_value=False)
    release_claims_lock = AsyncMock()
    materialize = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_claims_finalize_spec", lambda *_args: finalize_spec)
    monkeypatch.setattr(claims_pricing, "ensure_database", AsyncMock())
    monkeypatch.setattr(claims_pricing, "_validate_claims_finalize_manifest", Mock())
    monkeypatch.setattr(
        claims_pricing,
        "_wait_for_claims_finalize_turn",
        AsyncMock(return_value=None),
    )
    monkeypatch.setattr(claims_pricing, "_has_global_finalize_lock", acquire_global_lock)
    monkeypatch.setattr(claims_pricing, "_materialize_and_publish_claims", materialize)
    monkeypatch.setattr(
        claims_pricing,
        "_release_claims_finalize_lock_safely",
        release_claims_lock,
    )

    with pytest.raises(claims_pricing.Retry):
        await claims_pricing.claims_pricing_finalize({"redis": redis}, {})

    acquire_global_lock.assert_awaited_once_with(redis, "claims_pricing:r")
    materialize.assert_not_awaited()
    release_claims_lock.assert_awaited_once_with(redis, finalize_spec)


@pytest.mark.asyncio
async def test_ensure_indexes_builds_declared_shapes(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(claims_pricing.db, "status", status)
    indexed_cls = SimpleNamespace(
        __tablename__="provider_stage",
        __main_table__="provider",
        __my_index_elements__=("npi", "year"),
        __my_additional_indexes__=[
            {"index_elements": []},
            {
                "index_elements": ["state"],
                "name": "state_idx",
                "using": "btree",
                "where": "state IS NOT NULL",
            },
            {"index_elements": ["zip5"]},
        ],
    )
    await claims_pricing._ensure_indexes(indexed_cls, "mrf")
    await claims_pricing._ensure_indexes(SimpleNamespace(__tablename__="plain"), "mrf")
    sql_text = "\n".join(status_call.args[0] for status_call in status.await_args_list)
    assert "UNIQUE INDEX" in sql_text
    assert "USING btree" in sql_text
    assert "WHERE state IS NOT NULL" in sql_text
    assert "provider_stage_provider_stage_zip5_idx" in sql_text


@pytest.mark.asyncio
@pytest.mark.parametrize("defer_indexes", [False, True])
async def test_prepare_tables_rebuilds_every_stage(monkeypatch, defer_indexes):
    monkeypatch.setattr(claims_pricing, "CLAIMS_DEFER_STAGE_INDEXES", defer_indexes)
    monkeypatch.setattr(claims_pricing, "get_import_schema", lambda *_args: "mrf_stage")
    monkeypatch.setattr(claims_pricing.db, "status", AsyncMock())
    monkeypatch.setattr(claims_pricing.db, "create_table", AsyncMock())
    monkeypatch.setattr(
        claims_pricing,
        "make_class",
        lambda base_cls, suffix, schema_override: SimpleNamespace(
            __tablename__=f"{base_cls.__name__.lower()}_{suffix}",
            __table__=object(),
        ),
    )
    ensure_indexes = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_ensure_indexes", ensure_indexes)
    classes_by_name, schema = await claims_pricing._prepare_tables("stage", True)
    assert schema == "mrf_stage"
    assert len(classes_by_name) == 8
    assert ensure_indexes.await_count == (0 if defer_indexes else 8)


@pytest.mark.asyncio
async def test_build_staging_indexes_visits_registry(monkeypatch):
    class_names = (
        "PricingProvider",
        "PricingProcedure",
        "PricingProviderProcedure",
        "PricingProviderProcedureLocation",
        "PricingProviderProcedureCostProfile",
        "PricingProcedurePeerStats",
        "PricingProcedureGeoBenchmark",
        "PricingProcedureTaxonomySignal",
    )
    classes_by_name = {class_name: object() for class_name in class_names}
    ensure_indexes = AsyncMock()
    monkeypatch.setattr(claims_pricing, "_ensure_indexes", ensure_indexes)
    await claims_pricing._build_staging_indexes(classes_by_name, "mrf")
    assert ensure_indexes.await_count == 8


@pytest.mark.asyncio
async def test_live_code_tables_create_and_index(monkeypatch):
    create_table = AsyncMock()
    status = AsyncMock()
    ensure_indexes = AsyncMock()
    monkeypatch.setattr(claims_pricing.db, "create_table", create_table)
    monkeypatch.setattr(claims_pricing.db, "status", status)
    monkeypatch.setattr(claims_pricing, "_ensure_indexes", ensure_indexes)
    await claims_pricing._ensure_live_code_tables("mrf")
    assert create_table.await_count == 2
    assert status.await_count == 2
    assert ensure_indexes.await_count == 2


@pytest.mark.asyncio
async def test_cost_materializers_emit_all_staged_queries(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(claims_pricing.db, "status", status)
    classes_by_name = {
        "PricingProvider": SimpleNamespace(__tablename__="provider_stage"),
        "PricingProcedure": SimpleNamespace(__tablename__="procedure_stage"),
        "PricingProviderProcedure": SimpleNamespace(__tablename__="provider_procedure_stage"),
        "PricingProviderProcedureCostProfile": SimpleNamespace(__tablename__="profile_stage"),
        "PricingProcedurePeerStats": SimpleNamespace(__tablename__="peer_stage"),
    }
    await claims_pricing._materialize_code_and_crosswalk_rows(classes_by_name, "mrf")
    assert status.await_count == 4
    await claims_pricing._materialize_cost_level_rows(classes_by_name, "mrf")
    assert status.await_count == 8


@pytest.mark.asyncio
async def test_cost_diagnostics_return_query_rows(monkeypatch):
    query_rows = [
        [{"geography_scope": "national", "rows": 2, "unique_keys": 2}],
        [SimpleNamespace(_mapping={"geography_scope": "national", "rows": 1, "unique_keys": 1})],
        [
            {
                "geography_scope": "national",
                "profile_keys": 2,
                "peer_keys": 1,
                "coverage_pct": 50.0,
            }
        ],
    ]
    monkeypatch.setattr(claims_pricing.db, "all", AsyncMock(side_effect=query_rows))
    diagnostics_by_field = await claims_pricing._collect_cost_level_diagnostics(
        {
            "PricingProviderProcedureCostProfile": SimpleNamespace(__tablename__="profile_stage"),
            "PricingProcedurePeerStats": SimpleNamespace(__tablename__="peer_stage"),
        },
        "mrf",
    )
    assert diagnostics_by_field["profile_scope_rows"][0]["rows"] == 2
    assert diagnostics_by_field["peer_scope_rows"][0]["rows"] == 1
    assert diagnostics_by_field["key_coverage"][0]["coverage_pct"] == 50.0


@pytest.mark.asyncio
async def test_publish_renames_tables_and_owned_indexes(monkeypatch):
    status = AsyncMock()
    monkeypatch.setattr(claims_pricing.db, "status", status)
    monkeypatch.setattr(claims_pricing.db, "transaction", lambda: AsyncTransaction())
    final_classes = (
        claims_pricing.PricingProvider,
        claims_pricing.PricingProcedure,
        claims_pricing.PricingProviderProcedure,
        claims_pricing.PricingProviderProcedureLocation,
        claims_pricing.PricingProviderProcedureCostProfile,
        claims_pricing.PricingProcedurePeerStats,
        claims_pricing.PricingProcedureGeoBenchmark,
        claims_pricing.PricingProcedureTaxonomySignal,
    )
    for final_cls in final_classes:
        monkeypatch.setattr(final_cls, "__my_initial_indexes__", [], raising=False)
        monkeypatch.setattr(final_cls, "__my_additional_indexes__", [], raising=False)
    monkeypatch.setattr(
        final_classes[0],
        "__my_additional_indexes__",
        [{"index_elements": []}, {"index_elements": ["state"], "name": "state_idx"}],
        raising=False,
    )
    monkeypatch.setattr(
        final_classes[2],
        "__my_additional_indexes__",
        [
            {
                "index_elements": (
                    "year",
                    "procedure_code",
                    "total_allowed_amount DESC",
                    "npi",
                ),
                "name": "pricing_provider_proc_amount_page_idx",
                "staging_name": "amt_page",
            }
        ],
        raising=False,
    )
    classes_by_name = {
        final_cls.__name__: SimpleNamespace(__tablename__=f"{final_cls.__main_table__}_stage")
        for final_cls in final_classes
    }
    await claims_pricing._publish_by_table_rename(classes_by_name, "mrf")
    sql_text = "\n".join(status_call.args[0] for status_call in status.await_args_list)
    assert "DROP TABLE IF EXISTS" in sql_text
    assert "ALTER TABLE mrf." in sql_text
    assert "ALTER TABLE IF EXISTS" not in sql_text
    assert "state_idx" in sql_text
    assert (
        "pricing_provider_procedure_stage_amt_page "
        "RENAME TO pricing_provider_proc_amount_page_idx"
    ) in sql_text


@pytest.mark.asyncio
async def test_push_retry_rejects_non_deadlock(monkeypatch):
    monkeypatch.setattr(
        claims_pricing,
        "push_objects",
        AsyncMock(side_effect=RuntimeError("constraint")),
    )
    with pytest.raises(RuntimeError, match="constraint"):
        await claims_pricing._push_objects_with_retry([{"key": 1}], object())


@pytest.mark.asyncio
async def test_push_retry_accepts_empty_batch(monkeypatch):
    push_objects = AsyncMock()
    monkeypatch.setattr(claims_pricing, "push_objects", push_objects)
    await claims_pricing._push_objects_with_retry([], object())
    push_objects.assert_not_awaited()
