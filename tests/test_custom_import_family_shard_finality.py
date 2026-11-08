# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Block reconstruction shares the terminal transaction and its live authority."""

import datetime as dt
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import MappingProxyType, SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from sqlalchemy.dialects import postgresql

from process.custom_import import build_output, build_source, family_block_materialization, publication
from process.custom_import import family_shard_finality as finality
from process.custom_import.continuation_runtime import BuildContinuationContext, producer_token_sha256
from process.custom_import.definition import CustomImportDefinition
from process.custom_import.execution import lease_token_sha256
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.runner_types import CandidateRunnerError, LeaseAuthorityLost
from tests.test_custom_import_build_graph import _request
from tests.test_custom_import_build_output import _finality_session, _materialization_case
from tests.test_custom_import_family_block_materialization import _rows
from tests.test_custom_import_publication import _Result


def _case(monkeypatch, tmp_path, *, continuation=False, **bounds):
    now = dt.datetime(2026, 11, 5, tzinfo=dt.UTC)
    clock_values = [100.0]
    request = _request(**bounds)
    if continuation:
        request = replace(
            request,
            continuation=BuildContinuationContext(3, 7, b"p" * 32),
            build_deadline_at=now - dt.timedelta(days=1),
        )
    execution = SimpleNamespace(execution_id=request.execution_id, state="running")
    lease = SimpleNamespace(
        execution_id=request.execution_id,
        fence=request.fence,
        token_sha256=lease_token_sha256(request.lease_token),
        continuation_id=3 if continuation else None,
        continuation_started_at=now if continuation else None,
        expires_at=now + dt.timedelta(seconds=120),
    )
    transaction = SimpleNamespace(is_active=True)
    session = SimpleNamespace(
        info={},
        get=AsyncMock(side_effect=[execution, lease]),
        execute=AsyncMock(side_effect=lambda _statement: _Result(now + dt.timedelta(seconds=600))),
        scalar=AsyncMock(return_value="1s"),
        get_transaction=lambda: transaction,
    )
    bound = family_block_materialization.BlockMaterializationModels(_rows()[1], now)
    canonical = object()
    monkeypatch.setattr(finality.time, "monotonic", lambda: clock_values[0])
    monkeypatch.setattr(build_source, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(publication, "_database_now", AsyncMock(return_value=now))
    monkeypatch.setattr(publication, "_validate_generation_producing_authority", Mock())
    monkeypatch.setattr(publication, "_finality_materialization_models", AsyncMock(return_value=bound))
    monkeypatch.setattr(publication, "_SEAL_LEASE_WINDOW_SECONDS", 600)
    monkeypatch.setattr(finality, "verify_and_bind_continuation", AsyncMock())
    monkeypatch.setattr(finality, "materialization", AsyncMock(return_value=canonical))
    cleanups = []
    work = PlanWork(tmp_path / "finality", SourceFactLimits(1 << 20, 1 << 16), cleanups.append)
    generation = SimpleNamespace(generation_id=11)
    return now, clock_values, request, execution, lease, session, generation, canonical, work


@pytest.mark.parametrize("continuation", [False, True])
async def test_real_lease_renewal_rebinds_page_budget_and_preserves_producer(monkeypatch, tmp_path, continuation):
    now, clock_values, request, _execution, lease, session, generation, canonical, work = _case(
        monkeypatch, tmp_path, continuation=continuation
    )
    async with finality.finality_materialization(session, request, object(), generation, work=work) as actual:
        assert actual is canonical
        assert lease.expires_at == now + dt.timedelta(seconds=600)
        assert session.info[build_source._WINDOW].deadline == 700.0
        plan = family_block_materialization._work_plan(session)
        assert plan.transaction is session.get_transaction()
        clock_values[0] = 250.0  # The original 120-second page window is over.
        plan.check()
        await build_source._prepare_statement(session)
    assert family_block_materialization._WORK_KEY not in session.info
    assert publication._FINALITY_SCAN_WINDOW_KEY not in session.info
    finality.materialization.assert_awaited_once()
    seal_request = publication._validate_generation_producing_authority.call_args.args[-1]
    assert seal_request.token_sha256 == producer_token_sha256(request)
    update = session.execute.await_args_list[0].args[0].compile(dialect=postgresql.dialect())
    assert lease.token_sha256 in update.params.values()
    if continuation:
        assert 3 in update.params.values() and "continuation_id =" in str(update)
        finality.verify_and_bind_continuation.assert_awaited_once()
        assert lease.token_sha256 != seal_request.token_sha256
    else:
        assert "continuation_id IS NULL" in str(update)
        finality.verify_and_bind_continuation.assert_not_awaited()


@pytest.mark.parametrize("bound", ["build", "authorization", "operation"])
async def test_renewal_never_extends_owner_deadlines(monkeypatch, tmp_path, bound):
    case = _case(monkeypatch, tmp_path)
    now, clock_values, request, _execution, _lease, session, generation, _result, work = case
    request = replace(
        request,
        **{
            "build": {"build_deadline_at": now + dt.timedelta(seconds=15)},
            "authorization": {"authorization_expires_at": now + dt.timedelta(seconds=15)},
            "operation": {"operation_deadline": 115.0},
        }[bound],
    )
    with pytest.raises(LeaseAuthorityLost, match="owner deadline"):
        async with finality.finality_materialization(session, request, object(), generation, work=work):
            assert session.info[build_source._WINDOW].deadline == 115.0
            clock_values[0] = 116.0
    assert family_block_materialization._WORK_KEY not in session.info
    assert publication._FINALITY_SCAN_WINDOW_KEY not in session.info


async def test_wrong_continuation_refuses_before_reconstruction(monkeypatch, tmp_path):
    case = _case(monkeypatch, tmp_path, continuation=True)
    _now, _clock, request, _execution, lease, session, generation, _result, work = case
    lease.continuation_id = 4
    with pytest.raises(publication.PublicationConflict, match="current running lease"):
        async with finality.finality_materialization(session, request, object(), generation, work=work):
            pytest.fail("stale continuation reached reconstruction")
    finality.materialization.assert_not_awaited()
    session.execute.assert_not_awaited()


async def test_legacy_binding_cannot_enter_shard_reconstruction(monkeypatch, tmp_path):
    case = _case(monkeypatch, tmp_path)
    _now, _clock, request, _execution, _lease, session, generation, _result, work = case
    publication._finality_materialization_models.return_value = {}
    with pytest.raises(CandidateRunnerError, match="durable block binding"):
        async with finality.finality_materialization(session, request, object(), generation, work=work):
            pytest.fail("legacy binding reached reconstruction")
    finality.materialization.assert_not_awaited()


async def test_terminal_retry_does_not_reconstruct_or_allocate_work(monkeypatch, tmp_path):
    *_prefix, work = _case(monkeypatch, tmp_path)
    receipt = object()
    monkeypatch.setattr(build_output, "_replay", AsyncMock(return_value=receipt))
    monkeypatch.setattr(build_output, "_terminal_seal", AsyncMock())
    assert await finality.seal_shard_build(None, _request(), 7, work=work) is receipt
    build_output._terminal_seal.assert_not_awaited()
    assert not work.root.exists()


async def test_new_shard_seal_cannot_reuse_an_outside_materialization(monkeypatch, tmp_path):
    *_prefix, work = _case(monkeypatch, tmp_path)
    monkeypatch.setattr(build_output, "_replay", AsyncMock(return_value=None))
    monkeypatch.setattr(build_output, "_terminal_seal", AsyncMock(return_value="sealed"))
    request = _request()
    assert await finality.seal_shard_build(None, request, 7, work=work) == "sealed"
    build_output._terminal_seal.assert_awaited_once_with(None, request, 7, None, block_work=work)


def _same_output_base(monkeypatch):
    _rows_case, binding, *_rest = _rows()
    request = replace(
        _request(expected_base_generation_id=9, expected_pointer_version=2), definition=binding.layout.definition
    )
    base = SimpleNamespace(
        generation_id=9,
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
    )
    build = SimpleNamespace(base_generation_id=9, base_pointer_version=2)
    canonical = SimpleNamespace(
        effective_output_sha256=b"d" * 32, materialization_contract="custom-import/materialization/v1"
    )
    seal = SimpleNamespace(
        effective_output_sha256=b"d" * 32, materialization_contract="custom-import/materialization/v1"
    )
    bound = family_block_materialization.BlockMaterializationModels(binding, dt.datetime(2026, 11, 6, tzinfo=dt.UTC))
    monkeypatch.setattr(build_output, "_prepare_statement", AsyncMock())
    monkeypatch.setattr(publication, "_locked_generation", AsyncMock(return_value=base))
    monkeypatch.setattr(publication, "_validated_generation_seal", AsyncMock(return_value=seal))
    monkeypatch.setattr(
        publication, "_locked_pointer", AsyncMock(return_value=SimpleNamespace(generation_id=9, pointer_version=2))
    )
    monkeypatch.setattr(publication, "_finality_materialization_models", AsyncMock(return_value=bound))
    return request, build, base, seal, canonical, bound


@pytest.mark.parametrize("storage", ["canonical", "legacy_snapshot", "compatible_block"])
async def test_identical_output_requires_an_existing_compact_base(monkeypatch, storage):
    request, build, base, seal, canonical, bound = _same_output_base(monkeypatch)
    publication._finality_materialization_models.return_value = {
        "canonical": None,
        "legacy_snapshot": MappingProxyType({}),
        "compatible_block": bound,
    }[storage]
    # The ordinary v1 writer keeps its semantic no-change behavior in every case.
    assert await build_output._unchanged_base(None, request, build, canonical) == (base, seal)
    actual = await finality.unchanged_block_base(None, request, build, canonical)
    assert actual == ((base, seal) if storage == "compatible_block" else None)
    publication._finality_materialization_models.assert_awaited_once_with(None, base)


@pytest.mark.parametrize("failure", ["wrong_scope", "wrong_definition", "unclosed", "unresolved"])
async def test_wrong_base_binding_refuses_instead_of_minting_no_change(monkeypatch, failure):
    request, build, base, _seal, canonical, bound = _same_output_base(monkeypatch)
    if failure == "wrong_scope":
        base.schema_revision_id += 1
    elif failure == "wrong_definition":
        document = json.loads(request.definition.canonical)
        document["revision"]["definition"] += 1
        request = replace(request, definition=CustomImportDefinition.from_mapping(document))
    elif failure == "unclosed":
        bound.closed_at = None
    else:
        publication._finality_materialization_models.return_value = object()
    with pytest.raises(CandidateRunnerError, match="base.*binding"):
        await finality.unchanged_block_base(None, request, build, canonical)


@pytest.mark.parametrize("failure", ["stale relation identity", "retired snapshot"])
async def test_protected_base_resolution_failure_cannot_become_an_upgrade(monkeypatch, failure):
    request, build, _base, _seal, canonical, _bound = _same_output_base(monkeypatch)
    error = publication.PublicationConflict(failure)
    publication._finality_materialization_models.side_effect = error
    with pytest.raises(publication.PublicationConflict) as caught:
        await finality.unchanged_block_base(None, request, build, canonical)
    assert caught.value is error


async def test_changed_output_does_not_require_a_no_change_binding(monkeypatch):
    request, build, _base, _seal, canonical, _bound = _same_output_base(monkeypatch)
    canonical.effective_output_sha256 = b"e" * 32
    assert await finality.unchanged_block_base(None, request, build, canonical) is None
    publication._finality_materialization_models.assert_not_awaited()


@pytest.mark.parametrize("wrong_count", [False, True])
async def test_terminal_writes_remain_inside_actual_reconstruction_context(monkeypatch, tmp_path, wrong_count):
    build, generation, proof, materialization = _materialization_case()
    if wrong_count:
        proof.child_scalar_count += 1
    session = _finality_session(monkeypatch)
    session.get.side_effect = [generation, proof, object(), object()]
    work = PlanWork(tmp_path / "work", SourceFactLimits(1 << 20, 1 << 16), lambda _path: None)
    events = []

    @asynccontextmanager
    async def reconstruct(current_session, request, current_build, current_generation, *, work):
        assert current_session is session and current_build is build and current_generation is generation
        events.append("reconstruct")
        try:
            yield materialization
        finally:
            events.append("close")

    async def finalize(*_args):
        assert events == ["reconstruct"]
        events.append("terminal")

    monkeypatch.setattr(finality, "finality_materialization", reconstruct)
    monkeypatch.setattr(build_output, "_lock_page", AsyncMock(return_value=build))
    monkeypatch.setattr(build_output, "_unchanged_base", AsyncMock(return_value=None))
    monkeypatch.setattr(build_output, "_flush_page", AsyncMock())
    monkeypatch.setattr(build_output, "_finalize_sealed_build", AsyncMock(side_effect=finalize))
    monkeypatch.setattr(publication, "_database_now", AsyncMock())
    monkeypatch.setattr(publication, "_validate_generation_sealing_authority", Mock())
    monkeypatch.setattr(publication, "_new_generation_seal", Mock(return_value=object()))
    monkeypatch.setattr(publication, "_seal_receipt", Mock(return_value="receipt"))
    if wrong_count:
        with pytest.raises(CandidateRunnerError, match="proof counts differ"):
            await build_output._terminal_seal(None, _request(), 7, None, block_work=work)
        assert events == ["reconstruct", "close"]
        session.add.assert_not_called()
        build_output._finalize_sealed_build.assert_not_awaited()
    else:
        output_receipt = await build_output._terminal_seal(None, _request(), 7, None, block_work=work)
        assert output_receipt.seal == "receipt"
        assert events == ["reconstruct", "terminal", "close"]


async def test_ordinary_terminal_seal_preserves_the_existing_no_change_path(monkeypatch):
    build, generation, proof, canonical = _materialization_case()
    session = _finality_session(monkeypatch)
    session.get.side_effect = [generation, proof, object(), object()]
    monkeypatch.setattr(build_output, "_lock_page", AsyncMock(return_value=build))
    monkeypatch.setattr(build_output, "_unchanged_base", AsyncMock(return_value=None))
    monkeypatch.setattr(finality, "unchanged_block_base", AsyncMock(side_effect=AssertionError("block-only guard")))
    monkeypatch.setattr(build_output, "_flush_page", AsyncMock())
    monkeypatch.setattr(build_output, "_finalize_sealed_build", AsyncMock(return_value=None))
    monkeypatch.setattr(publication, "_database_now", AsyncMock())
    monkeypatch.setattr(publication, "_validate_generation_sealing_authority", Mock())
    monkeypatch.setattr(publication, "_new_generation_seal", Mock(return_value=object()))
    monkeypatch.setattr(publication, "_seal_receipt", Mock(return_value="receipt"))
    result = await build_output._terminal_seal(None, _request(), 7, canonical)
    assert result.seal == "receipt" and result.no_change is None
    build_output._unchanged_base.assert_awaited_once_with(session, _request(), build, canonical)
    finality.unchanged_block_base.assert_not_awaited()
