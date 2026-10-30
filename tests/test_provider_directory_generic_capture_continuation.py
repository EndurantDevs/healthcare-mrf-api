# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Post-return publication hook boundaries without opening native connections."""

import asyncio
from dataclasses import FrozenInstanceError
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_generic_source_policy as source
from process import provider_directory_source_local_publication as publication
from process.provider_directory_generic_capture_continuation import GenericSourceCaptureContinuation
from process.registry_generic_capture_continuation import RegistryGenericCaptureTarget

pytestmark = pytest.mark.asyncio
_CAPTURE = GenericSourceCaptureContinuation.capture


@pytest.fixture
async def case(monkeypatch):
    engines = [
        create_async_engine(
            f"postgresql+asyncpg://synthetic_{role}:discard@127.0.0.1/synthetic_capture", pool_size=1, max_overflow=0
        )
        for role in ("reader", "writer")
    ]
    continuation = GenericSourceCaptureContinuation(
        *(async_sessionmaker(engine, expire_on_commit=False) for engine in engines),
        RegistryGenericCaptureTarget("synthetic_owner", ("synthetic_reader",)),
    )
    policy = source.GenericSourcePolicy("synthetic_source", "source_owner", "source_writer", ("source_reader",))
    factory = SimpleNamespace(
        policy=policy,
        producer_identity=("a" * 40, "sha256:" + "b" * 64),
        _require_current_policy=lambda: None,
        generic_fhir_capture_continuation=continuation,
    )
    directory = SimpleNamespace(
        db=SimpleNamespace(_transaction_binding=lambda: None),
        _schema=lambda: policy.schema,
        UHC_RETAINED_SOURCE_ID="special-source",
        is_uhc_flex_profile_source=lambda _: False,
        _uses_alohr_graphql_connector=lambda _: False,
        _uses_uhc_official_file_connector=lambda _: False,
    )
    candidate = SimpleNamespace(
        source_ids=("synthetic-source",),
        endpoint_id="synthetic-endpoint",
        dataset_id="synthetic-dataset",
        acquisition_root_run_id="synthetic-root",
        already_published=False,
    )
    publish, capture = AsyncMock(), AsyncMock()
    monkeypatch.setattr(source, "source_producer_identity", lambda: factory.producer_identity)
    monkeypatch.setattr(publication, "publish_validated_source_local_dataset", publish)
    monkeypatch.setattr(GenericSourceCaptureContinuation, "capture", capture)
    token = source._FACTORY.set(factory)
    try:
        yield SimpleNamespace(
            directory=directory,
            candidate=candidate,
            factory=factory,
            continuation=continuation,
            publish=publish,
            capture=capture,
        )
    finally:
        source._FACTORY.reset(token)
        for engine in engines:
            await engine.dispose()


async def _publish(case):
    with artifacts.source_publication_request(
        case.directory,
        [{"source_id": case.candidate.source_ids[0]}],
        True,
    ):
        await artifacts.publish_source_candidate(case.directory, case.candidate)


async def test_hook_runs_after_publication_returns_and_on_published_replay(case):
    events = []

    async def publish(*args):
        events.append("publication-return")

    async def capture(*args):
        assert events == ["publication-return"]
        events.append("capture")

    case.publish.side_effect, case.capture.side_effect = publish, capture
    await _publish(case)
    assert events == ["publication-return", "capture"]
    case.candidate.already_published = True
    case.capture.side_effect = None
    await _publish(case)
    assert case.publish.await_count == 1 and case.capture.await_count == 2
    assert case.publish.call_args.kwargs == {}


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_capture_error_propagates_and_exact_published_replay_retries(case, failure):
    case.capture.side_effect = failure("capture-failed")
    with pytest.raises(failure, match="capture-failed"):
        await _publish(case)
    case.candidate.already_published = True
    case.capture.side_effect = None
    await _publish(case)
    assert case.publish.await_count == 1 and case.capture.await_count == 2


async def test_failed_source_publication_never_calls_capture(case):
    case.publish.side_effect = RuntimeError("publication-failed")
    with pytest.raises(RuntimeError, match="publication-failed"):
        await _publish(case)
    case.capture.assert_not_awaited()


@pytest.mark.parametrize("binding", ["publisher", "owner", "transaction"])
async def test_outer_source_owner_or_savepoint_cannot_claim_postcommit(case, binding):
    if binding == "transaction":
        case.directory.db._transaction_binding = lambda: SimpleNamespace(session=object(), nested=True)
        with pytest.raises(RuntimeError, match="capture_commit_required"):
            await _publish(case)
    else:
        context = source._PUBLISHER if binding == "publisher" else source._OWNER
        tokens = []

        async def publish(*args):
            tokens.append(context.set(object()))

        case.publish.side_effect = publish
        try:
            with pytest.raises(RuntimeError, match="capture_commit_required"):
                await _publish(case)
        finally:
            for token in reversed(tokens):
                context.reset(token)
    case.capture.assert_not_awaited()


@pytest.mark.parametrize("change", ["policy", "producer", "continuation", "scope"])
async def test_publication_cannot_replace_issued_capture_scope(case, change):
    async def publish(*args):
        if change == "policy":
            policy = case.factory.policy
            case.factory.policy = source.GenericSourcePolicy(
                policy.schema,
                policy.owner_role,
                policy.publisher_role,
                policy.runtime_roles,
            )
        elif change == "producer":
            case.factory.producer_identity = ("c" * 40, "sha256:" + "d" * 64)
        elif change == "continuation":
            case.factory.generic_fhir_capture_continuation = None
        else:
            artifacts._SOURCE_ONLY.get()[5]["active"] = False

    case.publish.side_effect = publish
    with pytest.raises(RuntimeError, match="capture_commit_required"):
        await _publish(case)
    case.capture.assert_not_awaited()


async def test_inherited_worker_task_cannot_run_the_continuation(case):
    with artifacts.source_publication_request(
        case.directory,
        [{"source_id": case.candidate.source_ids[0]}],
        True,
    ):
        with pytest.raises(RuntimeError, match="capture_commit_required"):
            await asyncio.create_task(artifacts.publish_source_candidate(case.directory, case.candidate))
    case.capture.assert_not_awaited()


async def test_none_preserves_ordinary_publication_without_capture(case):
    case.factory.generic_fhir_capture_continuation = None
    await _publish(case)
    case.publish.assert_awaited_once()
    case.capture.assert_not_awaited()


async def test_continuation_is_frozen_and_rejects_same_pool_or_arbitrary_factory(case):
    with pytest.raises(FrozenInstanceError):
        case.continuation.target = None
    with pytest.raises(RuntimeError, match="factories_invalid"):
        GenericSourceCaptureContinuation(
            case.continuation.reader_factory,
            case.continuation.reader_factory,
            case.continuation.target,
        )
    with pytest.raises(ValueError, match="unavailable"):
        GenericSourceCaptureContinuation(lambda: object(), case.continuation.writer_factory, case.continuation.target)


async def test_reconfigured_factory_is_refused_before_native_io(case):
    case.continuation.writer_factory.configure(bind=case.continuation.reader_factory.kw["bind"])
    with artifacts.source_publication_request(
        case.directory,
        [{"source_id": case.candidate.source_ids[0]}],
        True,
    ):
        with pytest.raises(RuntimeError, match="capture_factories_changed"):
            await _CAPTURE(case.continuation, case.directory, case.candidate, artifacts._SOURCE_ONLY.get())
