# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Ordinary dependency authority is issued and freshly observed, never supplied by names."""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_candidate_preparation as candidate
from process import entity_address_ordinary_dependencies as observed
from process import entity_address_prepared_doctors as prepared
from tests.entity_address_ordinary_dependencies_support import dependency_case


@pytest.fixture(autouse=True)
def configured_node(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")


async def _capture(case):
    return await prepared.capture_dependencies(
        case.database, "mrf", (), dependency_bindings=case.bindings, ordinary_preparation=case.issued
    )


async def test_original_builder_uses_protected_selection(monkeypatch):
    with dependency_case(monkeypatch) as case:
        source_capture = AsyncMock(side_effect=AssertionError("ordinary cannot impersonate Source"))
        monkeypatch.setattr(prepared, "_captured_tiger_dependencies", source_capture)
        result = await _capture(case)
        assert result.ordinary_preparation is case.issued and result.dependency_bindings == case.bindings
        source_capture.assert_not_awaited()
        case.selection.assert_awaited_once_with(
            case.session, "mrf", node_id=case.capture._execution.attestation.node_id
        )
        case.proofs.assert_awaited_once_with(case.session, "mrf", case.selected)
        assert case.protection.await_count == 2
        assert any("LOCK TABLE" in str(call.args[0]) for call in case.session.execute.await_args_list)
        assert case.actor.await_count == 2


@pytest.mark.parametrize(
    "damage", ["forged", "database", "engine", "phase", "hash", "capture", "owner", "database_oid"]
)
async def test_changed_owner_refuses_before_selection(monkeypatch, damage):
    with dependency_case(monkeypatch) as case:
        match damage:
            case "forged":
                case.issued = replace(case.issued)
            case "database":
                case.database = object()
            case "engine":
                case.database.engine = object()
            case "phase" | "hash" | "capture":
                setattr(case.issued, {"hash": "input_hash"}.get(damage, damage), "other")
            case _:
                case.actor_by_field["owner" if damage == "owner" else "database_oid"] += 1
        with pytest.raises((RuntimeError, ValueError, AttributeError)):
            await _capture(case)
        case.selection.assert_not_awaited()


@pytest.mark.parametrize("damage", ["selected_oid", "selected_file", "bindings", "proof", "absent", "backend"])
async def test_fresh_observation_drift_refuses(monkeypatch, damage):
    with dependency_case(monkeypatch) as case:
        selected, bindings = deepcopy((case.selected, case.bindings))
        match damage:
            case "selected_oid" | "selected_file":
                selected["zcta5"]["relation_oid" if damage == "selected_oid" else "relfilenode"] += 1
                case.selection.return_value = selected, bindings
            case "bindings":
                bindings["tiger.zcta5"]["relation_oid"] += 1
                case.selection.return_value = selected, bindings
            case "proof":
                case.proofs.return_value = {}
            case "absent":
                case.selection.return_value = None
                monkeypatch.setattr(
                    observed.bindings, "capture_publication_dependency_bindings", AsyncMock(return_value=bindings)
                )
            case _:
                case.actor.side_effect = [
                    (case.session, object(), dict(case.actor_by_field)),
                    (object(), object(), dict(case.actor_by_field)),
                ]
        with pytest.raises(RuntimeError, match="ordinary_dependencies_changed"):
            await _capture(case)


async def test_detached_map_cannot_replace_original_bindings(monkeypatch):
    with dependency_case(monkeypatch) as case:
        case.bindings = deepcopy(case.bindings)
        case.bindings["tiger.zip_state"]["relation_oid"] += 100
        with pytest.raises(RuntimeError, match="ordinary_dependencies_changed"):
            await _capture(case)
        case.selection.assert_not_awaited()


async def test_child_cannot_refresh_original_selection(monkeypatch):
    with dependency_case(monkeypatch) as case:
        with pytest.raises(RuntimeError, match="ordinary_dependencies_changed"):
            await asyncio.create_task(_capture(case))
        case.selection.assert_not_awaited()


async def test_each_build_backend_checks_locked_heaps(monkeypatch):
    with dependency_case(monkeypatch) as case:
        dependencies = await _capture(case)
        token = candidate._NATIVE_DEPENDENCIES.set(dependencies)
        try:
            assert candidate.has_prepared_doctors() is False
            assert candidate.geo_dependency_options()["dependency_bindings"] == case.bindings
            for _ in range(2):
                await candidate.lock_prepared_doctors(case.database)
            assert case.selection.await_count == 1 and case.protection.await_count == 6
            case.protection.side_effect = None
            case.protection.return_value = {**case.selected["zip_state"], "relation_oid": 999}
            with pytest.raises(RuntimeError, match="ordinary_dependencies_changed"):
                await candidate.lock_prepared_doctors(case.database)
        finally:
            candidate._NATIVE_DEPENDENCIES.reset(token)


async def test_cut_requires_closed_lifecycle_and_fresh_selection(monkeypatch):
    with dependency_case(monkeypatch) as case:
        dependencies = await _capture(case)
        with pytest.raises(RuntimeError, match="ordinary_preparation_invalid"):
            await prepared.assert_applied_dependencies(case.database, dependencies)
        case.issued.phase = "prepared"
        await prepared.assert_prepared_dependencies(case.database, dependencies)
        await prepared.assert_applied_dependencies(case.database, dependencies)
        assert case.selection.await_count == 3
        case.proofs.return_value = {}
        with pytest.raises(RuntimeError, match="ordinary_dependencies_changed"):
            await prepared.assert_applied_dependencies(case.database, dependencies)


async def test_original_candidate_threads_issued_carrier(monkeypatch):
    with dependency_case(monkeypatch) as case:
        scope = SimpleNamespace(source_preparation=case.issued)

        @asynccontextmanager
        async def transaction(database, actual_scope):
            assert database is case.database and actual_scope is scope
            yield case.session

        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
        monkeypatch.setattr(candidate._native(), "db", case.database)
        monkeypatch.setattr(candidate.source_preparation, "preparation_transaction", transaction)
        result = await candidate._capture_dependencies(
            SimpleNamespace(relation_overrides=()), scope, None, case.bindings, None
        )
        assert result.ordinary_preparation is case.issued


@pytest.mark.parametrize("field", ["doctors", "archive", "stage_oids"])
async def test_ordinary_carrier_cannot_borrow_source_apply(monkeypatch, field):
    with dependency_case(monkeypatch) as case:
        dependencies = replace(await _capture(case), **{field: object()})
        with pytest.raises(RuntimeError, match="cannot include prepared Source"):
            await prepared.assert_applied_dependencies(case.database, dependencies)
