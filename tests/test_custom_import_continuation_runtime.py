# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Processing epochs authenticate new writers while retaining original data identity."""

import datetime as dt
import json
import time
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_execution_stop as stop
from process.custom_import import build_graph as graph
from process.custom_import import build_output as output
from process.custom_import import build_source as source
from process.custom_import import continuation_runtime as runtime
from process.custom_import import execution as lifecycle
from process.custom_import import publication
from process.custom_import.runner_types import LeaseAuthorityLost
from tests.test_control_execution_stop import _payload, _Session
from tests.test_control_execution_stop import _request as _stop_request
from tests.test_custom_import_build_source import _request
from tests.test_custom_import_execution import _Result, _submission, _SyntheticSession


def _continued():
    legacy = _request()
    producer = lifecycle.lease_token_sha256(legacy.lease_token)
    context = runtime.BuildContinuationContext(41, 19, producer)
    request = replace(legacy, continuation=context, lease_token=b"synthetic-fresh-nonce")
    now = legacy.build_deadline_at + dt.timedelta(days=1)
    execution = SimpleNamespace(
        dataset_id=request.dataset_id,
        definition_revision_id=request.definition_revision_id,
        schema_revision_id=request.schema_revision_id,
        execution_id=request.execution_id,
        capture_bundle_id=8,
        request_identity_sha256=b"r" * 32,
        state="running",
    )
    lease = SimpleNamespace(
        execution_id=request.execution_id,
        fence=request.fence,
        token_sha256=lifecycle.lease_token_sha256(request.lease_token),
        continuation_id=context.continuation_id,
        continuation_started_at=now - dt.timedelta(hours=1),
        expires_at=now + dt.timedelta(seconds=30),
    )
    authorization = SimpleNamespace(
        **{name: getattr(execution, name) for name in vars(execution) if name != "state"},
        continuation_id=context.continuation_id,
        build_id=context.build_id,
        contract="custom-import/build-continuation/v1",
        processing_deadline_removed=True,
        producer_fence=request.fence,
        producer_token_sha256=producer,
        nonce_sha256=lease.token_sha256,
        original_deadline_at=request.build_deadline_at,
        base_generation_id=request.expected_base_generation_id,
        base_pointer_version=request.expected_pointer_version,
        launch_expires_at=now - dt.timedelta(minutes=50),
    )
    return request, execution, lease, authorization, now


def test_continuation_separates_new_nonce_from_retained_producer_and_deadline():
    request, *_ = _continued()
    assert runtime.producer_token_sha256(request) == lifecycle.lease_token_sha256(_request().lease_token)
    assert runtime.producer_token_sha256(request) != lifecycle.lease_token_sha256(request.lease_token)
    assert runtime.processing_deadline(request) is None
    assert runtime.processing_deadline(_request()) == _request().build_deadline_at
    assert "synthetic-fresh-nonce" not in repr(request)
    with pytest.raises(ValueError):
        runtime.BuildContinuationContext(True, 19, b"p" * 32)
    with pytest.raises(ValueError):
        runtime.BuildContinuationContext(41, 19, bytearray(b"p" * 32))


@pytest.mark.parametrize(
    "target,field,value",
    [
        ("lease", "continuation_id", 40),
        ("lease", "continuation_started_at", None),
        ("lease", "token_sha256", b"x" * 32),
        ("authorization", "nonce_sha256", b"x" * 32),
        ("authorization", "build_id", 20),
        ("authorization", "producer_token_sha256", b"x" * 32),
        ("authorization", "base_pointer_version", 1),
        ("authorization", "dataset_id", 9),
        ("authorization", "processing_deadline_removed", False),
    ],
)
def test_continuation_rejects_replay_wrong_scope_and_unstarted_epochs(target, field, value):
    request, execution, lease, authorization, now = _continued()
    setattr({"lease": lease, "authorization": authorization}[target], field, value)
    with pytest.raises(LeaseAuthorityLost):
        runtime.verify_continuation_scope(request, execution, lease, authorization, now)


async def test_epoch_binding_checks_authority_before_setting_transaction_local_gucs():
    request, execution, lease, authorization, now = _continued()
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(authorization)))
    await runtime.verify_and_bind_continuation(session, request, execution, lease, now)
    assert session.execute.await_count == 2
    statement = session.execute.await_args.args[0]
    parameters = statement.compile().params
    assert "custom_import.continuation_id" in parameters.values()
    assert "41" in parameters.values()
    assert lease.token_sha256.hex() in parameters.values()
    lease.continuation_id = 42
    session.execute.reset_mock()
    with pytest.raises(LeaseAuthorityLost):
        await runtime.verify_and_bind_continuation(session, request, execution, lease, now)
    assert session.execute.await_count == 1
    session.execute.reset_mock()
    with pytest.raises(LeaseAuthorityLost):
        await runtime.verify_and_bind_continuation(session, _request(), execution, lease, now)
    session.execute.assert_not_awaited()


def test_continued_reads_outlive_historical_and_launch_expiry(monkeypatch):
    request, execution, lease, authorization, now = _continued()
    build = SimpleNamespace(capture_bundle_id=execution.capture_bundle_id, build_id=19)
    identity = (build, execution, lease, now, "read committed")
    session = SimpleNamespace(
        info={},
        execute=Mock(side_effect=[None, SimpleNamespace(one=lambda: identity), _Result(authorization), None, None]),
    )
    monkeypatch.setattr(graph, "_verify_request", Mock())
    before = time.monotonic()
    assert graph._read_authority(session, request, 19)[1] > before + 29
    lease.expires_at = now
    session.execute = Mock(side_effect=[None, SimpleNamespace(one=lambda: identity), _Result(authorization)])
    with pytest.raises(LeaseAuthorityLost):
        graph._read_authority(session, request, 19)


async def test_continuation_cannot_reenter_source_or_open_another_build():
    request, *_ = _continued()
    with pytest.raises(source.CandidateRunnerError, match="cannot replay source"):
        await source.stage_segmented_source(object(), request)
    session = SimpleNamespace(in_transaction=lambda: True, new=(), dirty=(), deleted=())
    with pytest.raises(source.CandidateRunnerError, match="retained build"):
        await source._lock_page(session, request, 20)


def test_sealing_retains_producer_identity_and_requires_separate_current_nonce():
    request, execution, lease, authorization, now = _continued()
    producer = runtime.producer_token_sha256(request)
    seal = publication._GenerationSealRequest(request.dataset_id, 9, request.fence, producer)
    generation = SimpleNamespace(
        execution_id=request.execution_id,
        producing_fence=request.fence,
        producing_token_sha256=producer,
        root_count=1,
        family_count=1,
    )
    materialization = SimpleNamespace(root_count=1, family_count=1)
    with pytest.raises(publication.PublicationConflict, match="current running lease"):
        publication._validate_generation_sealing_authority(execution, lease, generation, materialization, seal, now)
    publication._validate_generation_sealing_authority(
        execution,
        lease,
        generation,
        materialization,
        seal,
        now,
        continuation_request=request,
        continuation_authorization=authorization,
    )
    assert seal.token_sha256 == producer != lease.token_sha256
    lease.continuation_id = 42
    with pytest.raises(LeaseAuthorityLost):
        publication._validate_generation_sealing_authority(
            execution,
            lease,
            generation,
            materialization,
            seal,
            now,
            continuation_request=request,
            continuation_authorization=authorization,
        )


@pytest.mark.parametrize("no_change", [False, True])
async def test_terminal_finality_expires_only_current_nonce_and_epoch(no_change):
    request, execution, lease, _, now = _continued()
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(rowcount=1)))
    producer = runtime.producer_token_sha256(request)
    if no_change:
        seal = publication._NoChangeRequest(request.dataset_id, request.execution_id, 2, 1, 9, request.fence, producer)
        await publication._finalize_no_change_execution(session, seal, now, continuation_request=request)
    else:
        seal = publication._GenerationSealRequest(request.dataset_id, 9, request.fence, producer)
        await publication._complete_generation_sealing_execution(
            session, execution, seal, now, continuation_request=request
        )
    where = session.execute.await_args.args[0].compile().params
    assert lease.token_sha256 in where.values()
    assert 41 in where.values()
    assert producer not in where.values()


@pytest.mark.parametrize("legacy,expired", [(False, False), (True, False), (False, True)])
async def test_output_replay_checks_current_epoch_even_before_readonly_return(monkeypatch, legacy, expired):
    request, execution, lease, authorization, now = _continued()
    if legacy:
        request = replace(request, continuation=None)
    if expired:
        lease.expires_at = now
        execution.state = "completed"
    build = SimpleNamespace(build_id=19, generation_id=None)
    session = SimpleNamespace(execute=AsyncMock(return_value=_Result(authorization)), get=AsyncMock(return_value=build))

    @asynccontextmanager
    async def session_context(*_args, **_kwargs):
        yield session

    monkeypatch.setattr(output, "_session", session_context)
    monkeypatch.setattr(output, "_verify_request", Mock())
    for name, value in (
        ("_begin_finality_operation", None),
        ("_locked_dataset", None),
        ("_locked_execution", execution),
        ("_locked_lease", lease),
        ("_database_now", now),
    ):
        monkeypatch.setattr(publication, name, AsyncMock(return_value=value))
    if legacy or expired:
        with pytest.raises(LeaseAuthorityLost):
            await output._replay(None, request, 19)
    else:
        assert await output._replay(None, request, 19) is None


@pytest.mark.parametrize("no_change", [False, True])
async def test_legacy_finality_cannot_replay_a_continued_build(monkeypatch, no_change):
    request, execution, lease, _, _ = _continued()
    replay = AsyncMock(return_value=object())
    monkeypatch.setattr(publication, "_begin_finality_operation", AsyncMock())
    monkeypatch.setattr(publication, "_locked_dataset", AsyncMock())
    if no_change:
        execution.state = "no_change"
        monkeypatch.setattr(publication, "_locked_execution", AsyncMock(return_value=execution))
        monkeypatch.setattr(publication, "_locked_lease", AsyncMock(return_value=lease))
        monkeypatch.setattr(publication, "_replayed_no_change_receipt", replay)
        call = publication.record_no_change(
            object(),
            dataset_id=request.dataset_id,
            execution_id=request.execution_id,
            expected_generation_id=2,
            expected_pointer_version=1,
            candidate_generation_id=9,
            lease_fence=request.fence,
            lease_token=_request().lease_token,
        )
    else:
        monkeypatch.setattr(publication, "_generation_snapshot", AsyncMock(return_value=object()))
        monkeypatch.setattr(
            publication, "_lock_generation_sealing_authority", AsyncMock(return_value=(execution, lease, object()))
        )
        monkeypatch.setattr(publication, "_replayed_generation_seal", replay)
        call = publication.seal_generation(
            object(),
            dataset_id=request.dataset_id,
            generation_id=9,
            lease_fence=request.fence,
            lease_token=_request().lease_token,
        )
    with pytest.raises(publication.PublicationConflict, match="current processing continuation"):
        await call
    replay.assert_not_awaited()


@pytest.mark.parametrize("continuation_id", [None, 41])
async def test_removed_lease_replay_checks_retained_continuation_history(continuation_id):
    session = SimpleNamespace(scalar=AsyncMock(return_value=continuation_id))
    if continuation_id is None:
        await publication._reject_continued_replay(session, 4, None)
    else:
        with pytest.raises(publication.PublicationConflict, match="current processing continuation"):
            await publication._reject_continued_replay(session, 4, None)
    session.scalar.assert_awaited_once()


class _ContinuationSession(_SyntheticSession):
    def _select(self, statement):
        if "set_config" in str(statement):
            return _Result()
        tables = statement.get_final_froms()
        if tables and tables[0].name == "custom_import_build_continuation":
            return _Result(self.authorization)
        return super()._select(statement)


async def _active_lifecycle():
    session = _ContinuationSession()
    submitted = await _submission(session)
    await lifecycle.claim_execution(session, execution_id=submitted.execution_id, token=b"synthetic-old-owner")
    execution = session.executions[submitted.execution_id]
    execution.capture_bundle_id = 8
    execution.request_identity_sha256 = b"r" * 32
    lease = session.leases[submitted.execution_id]
    lease.continuation_id = 41
    lease.continuation_started_at = session.now
    lease.token_sha256 = lifecycle.lease_token_sha256(b"synthetic-fresh-nonce")
    _, _, _, authorization, _ = _continued()
    for name in ("dataset_id", "definition_revision_id", "schema_revision_id", "execution_id"):
        setattr(authorization, name, getattr(execution, name))
    authorization.nonce_sha256 = lease.token_sha256
    session.authorization = authorization
    return session, execution, lease


async def test_legacy_lifecycle_is_fenced_after_opt_in():
    session, execution, lease = await _active_lifecycle()
    for operation in (lifecycle.claim_execution, lifecycle.resume_execution):
        lease.expires_at = session.now - dt.timedelta(seconds=1)
        assert await operation(session, execution_id=execution.execution_id, token=b"synthetic-old-owner") is None
    lease.expires_at = session.now + dt.timedelta(seconds=30)
    lease_arguments_dict = dict(execution_id=execution.execution_id, fence=lease.fence, token=b"synthetic-fresh-nonce")
    assert await lifecycle.heartbeat_execution(session, **lease_arguments_dict) is None
    assert not (await lifecycle.finish_execution(session, **lease_arguments_dict, terminal_state="completed")).changed
    for expected in (None, 40):
        with pytest.raises(lifecycle.IdempotencyConflict):
            await lifecycle.request_cancellation(
                session, execution_id=execution.execution_id, expected_continuation_id=expected
            )
    assert execution.state == "running"
    assert await lifecycle.heartbeat_execution(session, **lease_arguments_dict, continuation_id=41) is not None
    canceled = await lifecycle.request_cancellation(
        session, execution_id=execution.execution_id, expected_continuation_id=41
    )
    assert canceled.state == "canceling"
    assert (
        await lifecycle.finish_execution(session, **lease_arguments_dict, terminal_state="canceled", continuation_id=41)
    ).changed


@pytest.mark.parametrize("finalize", [False, True])
async def test_stop_api_passes_exact_epoch_and_returns_conflict_for_stale_owner(monkeypatch, finalize):
    monkeypatch.setenv(stop._TOKEN_ENV, "synthetic-stop-token")
    monkeypatch.setattr(stop, "_retained_identity", AsyncMock(return_value=(13, b"r" * 32)))

    async def transition(_session, **pins):
        if pins.get("expected_continuation_id") != 41:
            raise lifecycle.IdempotencyConflict("stale processing epoch")
        return SimpleNamespace(execution_id=4, state="canceling")

    monkeypatch.setattr(stop, "request_bound_execution_cancellation", transition)
    monkeypatch.setattr(stop, "finalize_stopped_execution_request", transition)
    for expected, status in ((None, 409), (40, 409), (41, 200), (True, 400), (0, 400)):
        request = _stop_request()
        document = json.loads(request.body)
        if expected is not None:
            document["expected_continuation_id"] = expected
        reply = await stop.serve_execution_stop(_stop_request(document=document), _Session(), finalize=finalize)
        assert reply.status == status
        assert "nonce" not in str(_payload(reply))
