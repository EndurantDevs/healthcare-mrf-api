# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Production composition keeps original calls, results and failure authority."""

from contextlib import asynccontextmanager, nullcontext
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process.custom_import import execution
from process.custom_import import family_shard_production as production
from process.custom_import import snowflake_segmented_runner as runner
from process.custom_import.build_counts import SourceOutcomeCounts
from process.custom_import.family import CandidateRejected
from process.custom_import.runner_types import CancellationRequested, LeaseAuthorityLost
from tests.test_custom_import_build_continuation import retained as retained
from tests.test_custom_import_segmented_runner import _install_flow, _policy


@pytest.mark.parametrize("grant,status", [(SimpleNamespace(state="running"), "capture_ready"), (None, "lease_lost")])
async def test_capture_profile_stops_after_original_capture_and_same_fence_heartbeat(monkeypatch, grant, status):
    flow = _install_flow(monkeypatch)
    session = object()

    @asynccontextmanager
    async def open_session(*_args, **_kwargs):
        yield session

    heartbeat = AsyncMock(return_value=grant)
    monkeypatch.setattr(runner, "_session", open_session)
    monkeypatch.setattr(runner, "_set_timeout", AsyncMock())
    monkeypatch.setattr(execution, "heartbeat_execution", heartbeat)
    transport = SimpleNamespace(require_candidate_binding=Mock())
    candidate_result = await runner.run_segmented_snowflake_candidate(
        object(),
        flow.connector,
        flow.request,
        processing_policy=_policy(),
        writer_transports=transport,
        capture_only=True,
    )
    assert candidate_result.status == status
    assert (candidate_result.accepted_family_count, candidate_result.rejection_count) == (0, 0)
    flow.calls["acquire_segmented_snowflake_capture"].assert_awaited_once()
    for name, function in flow.calls.items():
        if name != "acquire_segmented_snowflake_capture":
            function.assert_not_awaited()
    assert heartbeat.await_args.kwargs == {
        "execution_id": 9,
        "fence": 7,
        "token": flow.request.lease_token,
        "lease_seconds": _policy().build.lease_seconds,
    }


async def test_capture_only_without_authenticated_stage_never_claims(monkeypatch):
    flow = _install_flow(monkeypatch)
    with pytest.raises(production.CandidateRunnerError):
        await runner.run_segmented_snowflake_candidate(
            object(), flow.connector, flow.request, processing_policy=_policy(), capture_only=True
        )
    flow.calls["acquire_segmented_snowflake_capture"].assert_not_awaited()


def _original():
    return SimpleNamespace(request=object(), captured=SimpleNamespace(execution_id=9), policy=object())


def _job(*, publication=object(), no_change=None, complete=True, rejected=False):
    return SimpleNamespace(
        root=Path("/synthetic/owner"),
        source_counts=SourceOutcomeCounts(3, 1),
        is_rejected=rejected,
        complete=complete,
        no_change=no_change,
        publication=publication,
        output=SimpleNamespace(generation_id=17, seal=object()),
        close_terminal=AsyncMock(),
    )


@pytest.mark.parametrize(
    "case,status", [("activated", "activated"), ("conflict", "sealed_unpublished"), ("no_change", "no_change")]
)
async def test_owner_uses_real_counts_and_existing_publication_without_second_activate(monkeypatch, case, status):
    no_change = object() if case == "no_change" else None
    job = _job(publication=None if case == "conflict" else object(), no_change=no_change)
    advance = AsyncMock()
    original = _original()
    candidate_result = await production.run_original_owner(object(), original, job, advance=advance)
    assert (
        candidate_result.status,
        candidate_result.execution_id,
        candidate_result.generation_id,
        candidate_result.accepted_family_count,
        candidate_result.rejection_count,
    ) == (status, 9, 17, 3, 1)
    advance.assert_awaited_once()
    job.close_terminal.assert_awaited_once()
    if no_change is not None:
        assert candidate_result.publication is no_change


@pytest.mark.parametrize(
    "error,status", [(CancellationRequested("synthetic"), "canceled"), (LeaseAuthorityLost("synthetic"), "lease_lost")]
)
async def test_original_fenced_finish_and_lease_loss_do_not_claim_success(monkeypatch, error, status):
    finish = AsyncMock(return_value="canceled")
    monkeypatch.setattr(production, "_finish", finish)
    job = _job(complete=False)
    candidate_result = await production.run_original_owner(
        object(), _original(), job, advance=AsyncMock(side_effect=error)
    )
    assert candidate_result.status == status
    assert (candidate_result.accepted_family_count, candidate_result.rejection_count) == (3, 1)
    if status == "canceled":
        finish.assert_awaited_once()
    else:
        finish.assert_not_awaited()
    if status == "canceled":
        job.close_terminal.assert_awaited_once()
    else:
        job.close_terminal.assert_not_awaited()


def test_every_native_join_field_is_required():
    native_by_field = {name: 1 for name in production.NATIVE_FIELDS}
    production.require_native(native_by_field, native_by_field)
    for name in production.NATIVE_FIELDS:
        changed = native_by_field | {name: 2}
        with pytest.raises(ValueError):
            production.require_native(changed, native_by_field)


@pytest.mark.parametrize("outcome", ["failed", "canceling", "running"])
async def test_refusal_finish_keeps_files(monkeypatch, outcome):
    factory, session = object(), object()

    @asynccontextmanager
    async def transaction(actual_factory, *, transaction):
        assert actual_factory is factory and transaction is True
        yield session

    transitions = [SimpleNamespace(state=outcome, changed=outcome == "failed")]
    if outcome == "canceling":
        transitions.append(SimpleNamespace(state="canceled", changed=True))
    finish = AsyncMock(side_effect=transitions)
    monkeypatch.setattr(runner, "_session", transaction)
    monkeypatch.setattr(runner, "_set_timeout", AsyncMock())
    monkeypatch.setattr(runner, "finish_execution", finish)
    original = _original()
    original.request = SimpleNamespace(lease_token=b"synthetic-original-bearer")
    original.captured.fence = 7
    original.policy = _policy()
    job = _job(complete=False)
    job.source = SimpleNamespace(source_completed=False)
    job.source_counts, job.output, job.publication = SourceOutcomeCounts(0, 0), None, None
    candidate_result = await production.run_original_owner(
        factory,
        original,
        job,
        advance=AsyncMock(side_effect=CandidateRejected("source candidate has occurrence errors")),
    )
    first = finish.await_args_list[0]
    assert first.args == (session,)
    assert first.kwargs == dict(
        execution_id=9,
        fence=7,
        token=original.request.lease_token,
        terminal_state="failed",
        terminal_reason="candidate_rejected",
    )
    if outcome == "canceling":
        assert finish.await_count == 2
        assert finish.await_args.kwargs == first.kwargs | {
            "terminal_state": "canceled",
            "terminal_reason": "cancellation_requested",
        }
    else:
        finish.assert_awaited_once()
    assert (
        candidate_result.status
        == {"failed": "candidate_rejected", "canceling": "canceled", "running": "lease_lost"}[outcome]
    )
    assert (candidate_result.accepted_family_count, candidate_result.rejection_count) == (0, 0)
    assert candidate_result.generation_id is candidate_result.seal is candidate_result.publication is None
    assert not job.complete and not job.source.source_completed and not job.is_rejected
    job.close_terminal.assert_not_awaited()


async def test_unknown_failure_keeps_files(monkeypatch):
    finish = AsyncMock()
    monkeypatch.setattr(production, "_finish", finish)
    job, failure = _job(complete=False), ValueError("source identity corruption")
    with pytest.raises(ValueError) as caught:
        await production.run_original_owner(object(), _original(), job, advance=AsyncMock(side_effect=failure))
    assert caught.value is failure
    finish.assert_not_awaited()
    job.close_terminal.assert_not_awaited()


async def _continued_factory(retained, monkeypatch):
    from process.custom_import import build_continuation as continuation
    from process.custom_import import family_block_binding
    from process.custom_import.family_block_layout import FAMILY_BLOCK_LAYOUT_CONTRACT

    await continuation.authorize_build_continuation(retained.session, retained.authorization, **retained.pins)
    session = retained.session
    original_get = session.get
    layout = SimpleNamespace(
        layout_contract=FAMILY_BLOCK_LAYOUT_CONTRACT,
        definition_sha256=bytes.fromhex(retained.request.definition.digest),
        collection_slots={},
    )

    async def get(model, identity, **options):
        if model.__tablename__ == "custom_import_family_block_layout":
            return layout
        return await original_get(model, identity, **options)

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def factory():
        yield session

    session.get, session.begin = get, transaction
    loaded = SimpleNamespace(
        dataset_id=retained.execution.dataset_id,
        schema_revision_id=retained.execution.schema_revision_id,
        source_binding_sha256=b"s" * 32,
        definition=retained.request.definition,
        binding=SimpleNamespace(processing_policy=None),
    )
    monkeypatch.setattr(production, "_set_timeout", AsyncMock())
    monkeypatch.setattr(production, "load_snowflake_source_binding", AsyncMock(return_value=loaded))
    monkeypatch.setattr(
        production, "_validated_policy", lambda _value: SimpleNamespace(build=SimpleNamespace(lease_seconds=300))
    )
    binding = AsyncMock(return_value=object())
    monkeypatch.setattr(family_block_binding, "resolve_family_block_binding", binding)
    descriptor_by_field = {
        "dataset_id": retained.execution.dataset_id,
        "definition_revision_id": retained.execution.definition_revision_id,
        "schema_revision_id": retained.execution.schema_revision_id,
        "source_binding_revision_id": retained.execution.source_binding_revision_id,
        "engine_execution_id": retained.execution.execution_id,
        "source_binding_sha256": loaded.source_binding_sha256.hex(),
        "bearer_sha256": bytes(retained.build.producing_token_sha256).hex(),
    }
    return factory, descriptor_by_field, layout, binding


async def test_owner_continuation_proves_block_layout_before_consuming_native_launch(retained, monkeypatch):
    factory, descriptor, layout, binding = await _continued_factory(retained, monkeypatch)
    before = vars(retained.lease).copy()
    inspected = await production.owner_continuation_request(
        factory, descriptor, retained.authorization, retained.nonce, start=True, inspect_only=True
    )
    assert inspected.continuation.continuation_id == retained.session.row.continuation_id
    assert vars(retained.lease) == before
    assert binding.await_args.kwargs == {"persistence": "candidate"}
    request = await production.owner_continuation_request(
        factory, descriptor, retained.authorization, retained.nonce, start=True
    )
    assert (
        request.lease_token == retained.nonce
        and request.continuation.producer_token_sha256 == retained.build.producing_token_sha256
    )
    assert retained.lease.continuation_started_at == retained.now
    started = vars(retained.lease).copy()
    reopened = await production.owner_continuation_request(factory, descriptor, retained.authorization, retained.nonce)
    assert reopened == request and vars(retained.lease) == started


@pytest.mark.parametrize("change", ["legacy", "foreign_definition", "missing_binding", "native_failed"])
async def test_owner_continuation_refuses_legacy_or_changed_native_authority_before_start(
    retained, monkeypatch, change
):
    from process.custom_import.execution import IdempotencyConflict

    factory, descriptor, layout, binding = await _continued_factory(retained, monkeypatch)
    match change:
        case "legacy":
            layout.layout_contract = "custom-import/legacy/v1"
        case "foreign_definition":
            layout.definition_sha256 = b"f" * 32
        case "missing_binding":
            binding.return_value = None
        case _:
            retained.execution.state = "failed"
    with pytest.raises((ValueError, IdempotencyConflict)):
        await production.owner_continuation_request(
            factory, descriptor, retained.authorization, retained.nonce, start=True
        )
    assert retained.lease.continuation_started_at is None


@pytest.mark.parametrize(
    "case,status", [("activated", "activated"), ("conflict", "sealed_unpublished"), ("no_change", "no_change")]
)
async def test_continued_owner_uses_existing_output_counts_and_single_publication(monkeypatch, case, status):
    from process.custom_import import family_shard_owner_job

    job = _job(
        publication=None if case == "conflict" else object(), no_change=object() if case == "no_change" else None
    )
    advance = AsyncMock()
    monkeypatch.setattr(family_shard_owner_job, "advance_owner_shard_job", advance)
    result = await production.run_continued_owner(object(), SimpleNamespace(execution_id=9), job)
    assert (result.status, result.generation_id, result.accepted_family_count, result.rejection_count) == (
        status,
        17,
        3,
        1,
    )
    advance.assert_awaited_once()
    job.close_terminal.assert_awaited_once()


async def test_continued_failure_retains_artifacts_and_does_not_finish_native_execution(monkeypatch):
    from process.custom_import import family_shard_owner_job

    job = _job(complete=False)
    finish = AsyncMock()
    monkeypatch.setattr(execution, "finish_execution", finish)
    monkeypatch.setattr(
        family_shard_owner_job,
        "advance_owner_shard_job",
        AsyncMock(side_effect=production.CandidateRunnerError("synthetic")),
    )
    with pytest.raises(production.CandidateRunnerError):
        await production.run_continued_owner(object(), SimpleNamespace(execution_id=9), job)
    finish.assert_not_awaited()
    job.close_terminal.assert_not_awaited()


async def test_continued_cancellation_uses_current_epoch_and_cleans_only_confirmed_cancel(monkeypatch):
    from process.custom_import import family_shard_owner_job

    @asynccontextmanager
    async def transaction():
        yield

    @asynccontextmanager
    async def factory():
        yield SimpleNamespace(begin=transaction)

    request = SimpleNamespace(
        execution_id=9, fence=3, lease_token=b"n" * 32, continuation=SimpleNamespace(continuation_id=41)
    )
    job = _job(complete=False)
    finish = AsyncMock(return_value=SimpleNamespace(state="canceled"))
    monkeypatch.setattr(production, "_set_timeout", AsyncMock())
    monkeypatch.setattr(execution, "finish_execution", finish)
    monkeypatch.setattr(
        family_shard_owner_job, "advance_owner_shard_job", AsyncMock(side_effect=CancellationRequested("synthetic"))
    )
    result = await production.run_continued_owner(factory, request, job)
    assert result.status == "canceled"
    assert finish.await_args.kwargs["continuation_id"] == 41
    job.close_terminal.assert_awaited_once()


@pytest.mark.parametrize("condition", ["closed", "running", "new_fence", "wrong_token", "live_lease", "continued"])
async def test_terminal_census_native_read_requires_exact_closed_current_owner(monkeypatch, condition):
    """Terminal census reads require the closed exact producer and preserve continuation identity."""
    import datetime as dt

    from process.custom_import import family_shard_retirement_native as native
    from tests.test_control_owner_handoff import _profile

    configuration, parent = _profile()
    descriptor = parent["descriptor"]
    now = dt.datetime.now(dt.UTC)
    execution = SimpleNamespace(state="completed")
    lease = SimpleNamespace(
        fence=descriptor["fence"],
        token_sha256=bytes.fromhex(descriptor["bearer_sha256"]),
        expires_at=now,
        continuation_id=None,
    )
    match condition:
        case "running":
            execution.state = "running"
        case "new_fence":
            lease.fence += 1
        case "wrong_token":
            lease.token_sha256 = b"q" * 32
        case "live_lease":
            lease.expires_at = now + dt.timedelta(seconds=1)
        case "continued":
            lease.continuation_id = 11
            lease.token_sha256 = b"n" * 32
    family, build, capture = SimpleNamespace(family_id=7), object(), object()

    session = SimpleNamespace(begin=nullcontext)

    def factory():
        return nullcontext(session)

    monkeypatch.setattr(native, "_set_timeout", AsyncMock())
    monkeypatch.setattr(native, "require_owner_connection", AsyncMock())
    monkeypatch.setattr(native, "_producer_rows", AsyncMock(return_value=(family, execution, lease, build, capture)))
    monkeypatch.setattr(native, "_database_now", AsyncMock(return_value=now))
    committed = AsyncMock(return_value=((7, b"scope"), SimpleNamespace(revision=1, sha256=b"h" * 32)))
    monkeypatch.setattr(native, "_committed_scope", committed)
    continued = AsyncMock()
    monkeypatch.setattr(production, "_continued_producer", continued)
    if condition not in {"closed", "continued"}:
        with pytest.raises(ValueError):
            await native.read_terminal_scope(factory, parent, configuration)
        committed.assert_not_awaited()
        return
    observed = await native.read_terminal_scope(factory, parent, configuration)
    assert observed.family_id == 7 and not hasattr(observed, "sha256") and not hasattr(observed, "request")
    assert observed.scope == (7, b"scope") and observed.head.revision == 1
    if condition == "continued":
        continued.assert_awaited_once_with(session, descriptor, execution, lease, build)
    else:
        continued.assert_not_awaited()
