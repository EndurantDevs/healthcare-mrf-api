# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named input routing retains the existing protected preparation and complete recomputation."""

import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import entity_address_candidate_preparation as candidate
from process import entity_address_prepared_doctors as dependencies
from tests.test_entity_address_source_preparation import source_case as source_case

native = importlib.import_module("process.entity_address_unified")


def _bindings(schema):
    return {
        f"{namespace or schema}.{name}": {"schema_name": namespace or schema, "table_name": name}
        for namespace, name in dependencies.projection._PROJECTION_DEPENDENCIES
    }


@pytest.fixture
async def named_source_case(source_case):
    schema = source_case.factory.policy.schema
    inputs = candidate.ProviderDirectoryAddressPreparationInput(
        (), "overlay", 1, semantic_as_of=source_case.admission.plan.desired_profile_as_of
    )
    captured = dependencies.PreparedAddressDependencies(schema, None, (), _bindings(schema))
    inputs_token = candidate._PREPARATION.set(inputs)
    dependencies_token = candidate._NATIVE_DEPENDENCIES.set(captured)
    try:
        yield SimpleNamespace(**vars(source_case), inputs=inputs, captured=captured)
    finally:
        candidate._NATIVE_DEPENDENCIES.reset(dependencies_token)
        candidate._PREPARATION.reset(inputs_token)


async def test_named_geo_inputs_require_original_building_source_scope(named_source_case):
    case = named_source_case
    bindings = _bindings(case.factory.policy.schema)
    assert dependencies._named_prepared_geo_bindings(case.factory.policy.schema, bindings) == bindings
    case.scope.source_preparation.phase = "sealed"
    with pytest.raises(RuntimeError, match="source_preparation_invalid"):
        dependencies._named_prepared_geo_bindings(case.factory.policy.schema, bindings)


@pytest.mark.parametrize("fault", ["schema", "names", "lease", "hash", "inputs"])
async def test_named_geo_inputs_preserve_scope_lease_and_signed_input(named_source_case, fault):
    case = named_source_case
    schema = case.factory.policy.schema
    bindings = _bindings(schema)
    token = None
    if fault == "schema":
        schema = "other_schema"
    elif fault == "names":
        bindings[f"{schema}.npi_address"]["table_name"] = "other_name"
    elif fault == "lease":
        case.admission._assert_lease = Mock(side_effect=RuntimeError("lease expired"))
    elif fault == "hash":
        case.scope.native_input_hash = "f" * 64
    else:
        token = candidate._PREPARATION.set(None)
    try:
        with pytest.raises(RuntimeError):
            dependencies._named_prepared_geo_bindings(schema, bindings)
    finally:
        if token is not None:
            candidate._PREPARATION.reset(token)


def test_named_geo_inputs_cannot_borrow_ordinary_global_freshness():
    assert dependencies._named_prepared_geo_bindings("mrf", _bindings("mrf")) == _bindings("mrf")
    assert dependencies._named_prepared_geo_bindings("mrf", None) is None
    legacy_by_name = {
        key: {**value, "relation_oid": index, "relfilenode": index + 10}
        for index, (key, value) in enumerate(_bindings("mrf").items(), 1)
    }
    assert dependencies._named_prepared_geo_bindings("mrf", legacy_by_name) is None


@pytest.mark.parametrize("fault", [None, "shape", "data", "global_state"])
async def test_named_projection_recomputes_and_validates_without_global_state(named_source_case, monkeypatch, fault):
    case = named_source_case
    schema = case.factory.policy.schema

    @asynccontextmanager
    async def transaction():
        async with case.factory.transaction(case.database) as session:
            yield session

    monkeypatch.setattr(native.preparation_admission, "native_transaction", transaction)
    reads = AsyncMock(side_effect=[fault != "shape", 41])
    monkeypatch.setattr(native.preparation_admission, "read_scalar", reads)
    sql = Mock(return_value="UPDATE prepared_projection SET geo_assurance_version=1")
    monkeypatch.setattr(native, "_materialize_geo_assurance_sql", sql)
    case.database.status.return_value = 9
    validate = AsyncMock(
        side_effect=RuntimeError("incomplete geo assurance") if fault == "data" else None, return_value=0
    )
    monkeypatch.setattr(native, "_validate_geo_assurance_projection", validate)
    global_writer = Mock(side_effect=AssertionError("global candidate state is outside prepared materialization"))
    monkeypatch.setattr(native, "_record_geo_assurance_candidate_sql", global_writer)
    if fault:
        with pytest.raises(
            RuntimeError,
            match={
                "shape": "binding changed",
                "data": "incomplete geo assurance",
                "global_state": "global_state_unsupported",
            }[fault],
        ):
            await native._project_geo_assurance_transaction(
                schema,
                "prepared_stage",
                force=False,
                dependency_bindings=_bindings(schema),
                record_candidate=fault == "global_state",
            )
    else:
        assert await native._project_geo_assurance_transaction(
            schema, "prepared_stage", force=False, dependency_bindings=_bindings(schema), record_candidate=False
        ) == (9, 0, 41, True)
    global_writer.assert_not_called()
    if fault in ("shape", "global_state"):
        sql.assert_not_called()
        validate.assert_not_awaited()
    else:
        sql.assert_called_once_with(schema, "prepared_stage", force=True, dependency_bindings=_bindings(schema))
        validate.assert_awaited_once_with(schema, "prepared_stage")
    if fault != "global_state":
        assert "ACCESS SHARE" in case.database.status.await_args_list[0].args[0]
        assert "pg_relation_filenode" not in reads.await_args_list[0].args[1]


@pytest.mark.parametrize("prepare_only", [False, True])
async def test_real_finalizer_keeps_named_projection_inside_source_preparation(
    named_source_case, monkeypatch, prepare_only
):
    from tests.test_entity_address_unified_no_live_mutation import _mock_shutdown_dependencies, _shutdown_context

    case = named_source_case
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", case.factory.policy.schema)
    _mock_shutdown_dependencies(monkeypatch, [])
    ctx = _shutdown_context(refresh_mode=native.ENTITY_ADDRESS_REFRESH_MODE_FULL)
    monkeypatch.setattr(native.preparation_admission, "read_scalar", AsyncMock(return_value=100))
    geometry = AsyncMock(return_value=100)
    prepared = object()
    monkeypatch.setattr(native, "_materialize_geo_assurance", geometry)
    finish = AsyncMock(return_value=prepared)
    monkeypatch.setattr(candidate, "prepare_finalized_generation", finish)
    if prepare_only:
        assert await native.publish_entity_address_unified_generation(ctx, prepare_only=True) is prepared
        assert geometry.await_args.kwargs["dependency_bindings"] == case.captured.dependency_bindings
        assert geometry.await_args.kwargs["record_candidate"] is False
        finish.assert_awaited_once()
    else:
        with pytest.raises(RuntimeError, match="requires_prepare_only"):
            await native.publish_entity_address_unified_generation(ctx)
        geometry.assert_not_awaited()
        finish.assert_not_awaited()


@pytest.mark.parametrize("projected", [8, 9])
async def test_named_projection_requires_full_accounted_stage_rows(named_source_case, monkeypatch, projected):
    case = named_source_case
    monkeypatch.setattr(native, "_project_geo_assurance_transaction", AsyncMock(return_value=(projected, 0, 41, True)))
    options_by_name = dict(
        force=False,
        context={},
        run_id="",
        stage_rows=9,
        dependency_bindings=case.captured.dependency_bindings,
        record_candidate=False,
    )
    if projected == 9:
        assert (
            await native._materialize_geo_assurance(case.factory.policy.schema, "prepared_stage", **options_by_name)
            == 9
        )
        assert options_by_name["context"]["geo_assurance_forced_full_projection"] is True
        assert options_by_name["context"]["invalid_geo_assurance_rows"] == 0
    else:
        with pytest.raises(RuntimeError, match="named_geo_projected_rows_changed"):
            await native._materialize_geo_assurance(case.factory.policy.schema, "prepared_stage", **options_by_name)
