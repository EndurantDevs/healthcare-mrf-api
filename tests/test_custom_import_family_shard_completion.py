# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""One complete original proof precedes durability, acceptance and finality."""

import time
from contextlib import asynccontextmanager, nullcontext
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select

from process.custom_import import family_shard_completion as completion
from process.custom_import.build_source import _WINDOW, _PageWindow
from process.custom_import.family_block_layout import ROLE_SPECS
from process.custom_import.family_shard_closed_verification import ClosedShardVerification
from process.custom_import.family_shard_facts import SourceFactLimits
from process.custom_import.family_shard_plan import PlanWork
from process.custom_import.runner_types import LeaseAuthorityLost
from tests.test_custom_import_build_graph import _request


def _case(monkeypatch, tmp_path, failure=None, *, expired=False):
    calls = []
    request = _request()
    context = SimpleNamespace(common=SimpleNamespace(request=request, layout=object()), build_id=7)
    counts = dict.fromkeys(ROLE_SPECS, 0) | {"root_record": 3, "shard_receipt": 8}
    proof = ClosedShardVerification(object(), tuple(range(8)), b"v" * 32, 8, 17, tuple(sorted(counts.items())))
    work = PlanWork(tmp_path / "complete", SourceFactLimits(1 << 20, 1 << 16), lambda _path: None)
    window = _PageWindow(0.0 if expired else time.monotonic() + 120, request.statement_timeout_ms)
    session = SimpleNamespace(begin=lambda: nullcontext(), execute=AsyncMock(), info={}, no_autoflush=nullcontext())

    @asynccontextmanager
    async def factory():
        calls.append("read-open")
        yield session
        calls.append("read-close")

    @asynccontextmanager
    async def heartbeat(*_args):
        calls.append("heartbeat-start")
        try:
            yield
        finally:
            calls.append("heartbeat-stop")

    @asynccontextmanager
    async def authority(*_args):
        calls.append("authority")
        yield SimpleNamespace(info={_WINDOW: window}), object()

    def operation(name, result=None):
        async def run(*args, **kwargs):
            calls.append(name)
            if name == failure:
                raise RuntimeError(name)
            if name == "verify":
                assert str(session.execute.await_args_list[0].args[0]) == (
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                assert kwargs["work"].root == work.root / "verification"
                assert session.info[_WINDOW] is window
                await kwargs["prepare"](session)
                await completion.store._execute(session, select(1))
            if name == "seal":
                assert calls[-2] in {"heartbeat-stop", "freeze"}
            return result

        return AsyncMock(side_effect=run)

    monkeypatch.setattr(completion, "_renew_while_reading", heartbeat)
    monkeypatch.setattr(completion, "_page_session", authority)
    monkeypatch.setattr(completion.build_output, "_replay", AsyncMock(return_value=None))
    monkeypatch.setattr(completion, "_snapshot", AsyncMock(return_value=SimpleNamespace(phase="verifying")))
    monkeypatch.setattr(completion, "verify_closed_shard_build", operation("verify", proof))
    monkeypatch.setattr(completion.store, "accept_validation", operation("accept"))
    monkeypatch.setattr(completion.store, "make_logged", operation("logged"))
    monkeypatch.setattr(completion, "record_shard_verification", operation("record"))
    monkeypatch.setattr(completion.build_output, "_freeze_output_snapshot", operation("freeze"))
    monkeypatch.setattr(completion, "seal_shard_build", operation("seal", "sealed"))
    return factory, context, proof, work, calls


async def test_complete_proof_is_bound_to_every_exact_transition(monkeypatch, tmp_path):
    factory, context, proof, work, calls = _case(monkeypatch, tmp_path)
    inputs, limits = object(), object()
    assert await completion.complete_closed_shard_build(factory, context, inputs, work=work, limits=limits) == "sealed"
    assert calls == [
        "heartbeat-start",
        "authority",
        "read-open",
        "verify",
        "read-close",
        "accept",
        "logged",
        "record",
        "freeze",
        "heartbeat-stop",
        "seal",
    ]
    for transition in (completion.store.accept_validation, completion.store.make_logged):
        transition.assert_awaited_once_with(
            factory,
            context.common.request,
            proof.identity,
            context.common.layout,
            validation_sha256=proof.validation_sha256,
            expected_completed_shards=proof.completed_shards,
            source_record_count=proof.source_record_count,
            role_counts={name: count for name, count in proof.role_counts if name != "shard_receipt"},
        )
    completion.record_shard_verification.assert_awaited_once_with(
        factory,
        context.common.request,
        context.build_id,
        context.common.layout,
        proof.identity,
        validation_sha256=proof.validation_sha256,
        counts=proof.counts,
    )
    assert completion.seal_shard_build.await_args.kwargs["work"].root == work.root / "finality"


@pytest.mark.parametrize("receipt_count", [None, 0, 9])
async def test_receipt_census_must_match_before_any_durability_transition(monkeypatch, tmp_path, receipt_count):
    factory, context, proof, _work, _calls = _case(monkeypatch, tmp_path)
    roles_by_key = dict(proof.role_counts)
    if receipt_count is None:
        roles_by_key.pop("shard_receipt")
    else:
        roles_by_key["shard_receipt"] = receipt_count
    with pytest.raises(ValueError, match="receipt census"):
        await completion._record_verified_snapshot(
            factory, context, replace(proof, role_counts=tuple(roles_by_key.items()))
        )
    completion.store.accept_validation.assert_not_awaited()
    completion.store.make_logged.assert_not_awaited()
    completion.record_shard_verification.assert_not_awaited()


@pytest.mark.parametrize("failure", ["verify", "accept", "logged", "record", "freeze"])
async def test_failed_boundary_cannot_reach_a_later_acceptance_or_seal(monkeypatch, tmp_path, failure):
    factory, context, _proof, work, calls = _case(monkeypatch, tmp_path, failure)
    with pytest.raises(RuntimeError, match=failure):
        await completion.complete_closed_shard_build(factory, context, object(), work=work, limits=object())
    assert calls[-2:] == [failure, "heartbeat-stop"]
    completion.seal_shard_build.assert_not_awaited()


async def test_expired_owner_budget_refuses_before_original_proof(monkeypatch, tmp_path):
    factory, context, _proof, work, calls = _case(monkeypatch, tmp_path, expired=True)
    with pytest.raises(LeaseAuthorityLost, match="page deadline"):
        await completion.complete_closed_shard_build(factory, context, object(), work=work, limits=object())
    assert calls[-1] == "heartbeat-stop"
    completion.verify_closed_shard_build.assert_not_awaited()
    completion.store.accept_validation.assert_not_awaited()


async def test_terminal_replay_skips_live_candidate_and_original_verification(monkeypatch, tmp_path):
    factory, context, _proof, work, calls = _case(monkeypatch, tmp_path)
    completion.build_output._replay.return_value = "existing receipt"
    assert await completion.complete_closed_shard_build(factory, context, None, work=work, limits=None) == (
        "existing receipt"
    )
    assert calls == []
    completion._snapshot.assert_not_awaited()
    completion.verify_closed_shard_build.assert_not_awaited()


async def test_verified_or_frozen_retry_uses_guarded_freeze_and_real_finality(monkeypatch, tmp_path):
    factory, context, _proof, work, calls = _case(monkeypatch, tmp_path)
    completion._snapshot.return_value = SimpleNamespace(phase="verified")
    assert await completion.complete_closed_shard_build(factory, context, None, work=work, limits=None) == "sealed"
    assert calls == ["freeze", "seal"]
    completion.verify_closed_shard_build.assert_not_awaited()
    completion.store.accept_validation.assert_not_awaited()
    completion.record_shard_verification.assert_not_awaited()
