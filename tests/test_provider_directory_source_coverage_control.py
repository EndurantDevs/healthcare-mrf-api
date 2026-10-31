# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed coverage custody closes ordinary writes without replacing native controls."""

from contextlib import asynccontextmanager
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_source_coverage_control as control
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy


@pytest.fixture
def catalog(monkeypatch):
    policy = GenericSourcePolicy(
        "source_schema", "source_owner", "source_publisher", ("source_reader", "legacy_builder"), ("legacy_builder",)
    )
    constraints = [{"type": "c", "definition": definition, "valid": True} for definition in control._CHECKS]
    constraints.extend({"type": kind, "definition": kind, "valid": True} for kind in ("p", "f"))
    relation_by_field = dict(
        oid=42,
        nspname=policy.schema,
        relname=control.COVERAGE_ROOT,
        relowner=17,
        relkind="r",
        relpersistence="p",
        relispartition=False,
        parents=[],
        relrowsecurity=False,
        relforcerowsecurity=False,
        native_checks=True,
        columns=[list(column) for column in control.COVERAGE_COLUMNS],
        constraints=constraints,
        direct_acl_closed=True,
        readers_closed=True,
        indexes=[_index(("dataset_id", "release_id", "proof_version"), True), _index(("release_id",), False)],
    )
    guards = [
        dict(tgname=name, function_owner="legacy_builder", proowner=19, provolatile="v") for name in control._GUARDS
    ]
    native = SimpleNamespace(
        fetch=AsyncMock(side_effect=lambda sql, *args: [relation_by_field] if sql == control.CATALOG_SQL else guards)
    )
    native.fetchval = AsyncMock(return_value=True)
    monkeypatch.setattr(control, "storage_roles", AsyncMock(return_value=(17, [18, 19])))
    from process import network_fhir_source_custody as custody

    guard_check = Mock()
    monkeypatch.setattr(custody, "_require_guard", guard_check)
    return SimpleNamespace(
        policy=policy, relation=relation_by_field, native=native, guards=guards, guard_check=guard_check
    )


def _index(keys, primary):
    return dict(
        primary=primary, unique=primary, valid=True, method="btree", predicate=None, expression=None, keys=list(keys)
    )


@pytest.mark.asyncio
async def test_fixed_control_retains_native_fk_and_both_original_guard_contracts(catalog):
    receipt = await control.capture_source_coverage_control(catalog.native, catalog.policy)
    assert receipt["oid"] == 42 and receipt["owner"] == 17
    assert len(receipt["layout"]) == len(receipt["guards"]) == len(receipt["acl"]) == 64
    assert catalog.guard_check.call_count == 2
    assert {call.args[3] for call in catalog.guard_check.call_args_list} == set(control._GUARDS)
    assert any(call.args[0] == control._NATIVE_SQL for call in catalog.native.fetchval.await_args_list)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["relowner", "direct_acl_closed", "readers_closed", "native_checks", "relrowsecurity"]
)
async def test_wrong_owner_or_direct_inherited_column_writes_refuse(catalog, field):
    catalog.relation[field] = 999 if field == "relowner" else field == "relrowsecurity"
    with pytest.raises(GenericCandidateError, match="coverage_.*changed"):
        await control.capture_source_coverage_control(catalog.native, catalog.policy)
    catalog.guard_check.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["column", "check", "key", "index", "native_fk", "hook", "function_owner"])
async def test_unknown_layout_fk_or_hook_cannot_reach_control_rows(catalog, change):
    match change:
        case "column":
            catalog.relation["columns"][0][1] = "text"
        case "check":
            catalog.relation["constraints"][0]["definition"] = "CHECK (true)"
        case "key":
            catalog.relation["indexes"][0]["keys"] = ["dataset_id"]
        case "index":
            catalog.relation["indexes"].append(deepcopy(catalog.relation["indexes"][1]))
        case "native_fk":
            catalog.native.fetchval.return_value = False
        case "hook":
            catalog.guards[0]["tgname"] = "unknown"
        case "function_owner":
            catalog.guards[0]["function_owner"] = "untrusted"
    with pytest.raises(GenericCandidateError, match="coverage_.*changed"):
        await control.capture_source_coverage_control(catalog.native, catalog.policy)


@pytest.mark.asyncio
async def test_legacy_adoption_accepts_only_the_attested_declared_control_owner(catalog):
    catalog.relation["relowner"] = 19
    catalog.relation["direct_acl_closed"] = catalog.relation["readers_closed"] = False
    assert (await control.capture_source_coverage_control(catalog.native, catalog.policy, legacy=True))["owner"] == 19
    catalog.native.fetch.reset_mock()
    catalog.native.fetchval.return_value = False
    with pytest.raises(GenericCandidateError, match="coverage_legacy_owner_changed"):
        await control.capture_source_coverage_control(catalog.native, catalog.policy, legacy=True)
    assert catalog.native.fetch.await_count == 1


@pytest.mark.asyncio
async def test_coverage_preparation_borrows_factory_rr_before_any_target_query(monkeypatch):
    from process import provider_directory_cms_serving_coverage as coverage
    from process import provider_directory_generic_source_policy as policy

    events = []
    admission_query = SimpleNamespace(
        mappings=lambda: SimpleNamespace(
            one_or_none=lambda: {"admission_sha256": "a" * 64},
            one=lambda: {"statement_timeout": "0", "lock_timeout": "0", "plan_cache_mode": "auto"},
        )
    )
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(return_value=admission_query),
        scalar=AsyncMock(return_value=True),
    )

    @asynccontextmanager
    async def savepoint():
        yield

    session.begin_nested = savepoint

    @asynccontextmanager
    async def transaction(database, *, isolation):
        assert isolation == "repeatable read"
        events.append("original_rr")
        yield session

    monkeypatch.setattr(policy, "source_transaction", transaction)
    publisher = SimpleNamespace(connection=object(), policy=object())
    monkeypatch.setattr(policy, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(
        control, "capture_source_coverage_control", AsyncMock(side_effect=lambda *args: events.append("custody"))
    )
    sealed = AsyncMock()
    monkeypatch.setattr(coverage, "assert_sealed_cms_candidate_coverage", sealed)
    fhir = SimpleNamespace(db=SimpleNamespace(session=Mock(side_effect=AssertionError("ordinary session reached"))))
    candidate = SimpleNamespace(dataset_id="dataset", endpoint_id="endpoint")
    assert (await coverage.prepare_cms_candidate_coverage(fhir, candidate, "a" * 64, "b" * 64))[
        "dataset_id"
    ] == "dataset"
    assert events == ["original_rr", "custody"]
    assert sealed.await_args.args[0] is session
