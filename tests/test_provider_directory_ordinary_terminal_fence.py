# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep Source header pins and the original committing Builder as distinct owners."""

from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_generic_source_artifacts as artifacts
from process import provider_directory_serving_capacity_contract as contract
from process.provider_directory_artifact_bundle_preparation import CapturedServingInputs
from process.provider_directory_cms_nonprofile_capacity import _CapacityProducer
from process.provider_directory_cms_preparation import NonprofileAdmission, NonprofileAdmissionCheck
from tests.provider_directory_serving_capacity_signing_test_support import serving_case, verify_serving_envelope


@pytest.fixture
def ordinary_case(monkeypatch):
    monkeypatch.setenv("HLTHPRT_IMPORT_NODE_ID", "dev-node")
    case = serving_case()
    case.fence.promotion_datasets = ()
    session = SimpleNamespace(in_transaction=lambda: True)
    binding = SimpleNamespace(session=session)
    database = SimpleNamespace(engine=object(), _transaction_binding=lambda: binding, status=AsyncMock())
    directory = SimpleNamespace(
        db=database,
        PROVIDER_DIRECTORY_ARTIFACT_CUTOVER_LOCK_TIMEOUT="500ms",
        PROVIDER_DIRECTORY_ARTIFACT_FENCE_STATEMENT_TIMEOUT="8s",
    )
    factory = SimpleNamespace(database=database, _require_current_policy=Mock())
    monkeypatch.setattr(artifacts.source, "_FACTORY", ContextVar("ordinary_factory", default=factory))
    monkeypatch.setattr(artifacts.source, "is_source_storage_required", lambda: True)
    captured = CapturedServingInputs(
        database,
        database.engine,
        case.execution,
        case.fence,
        contract.canonical({"scope": case.plan.payload["ordinary_scope"]}),
        "{}",
        factory,
    )
    lease = verify_serving_envelope(case.envelope)
    producer = _CapacityProducer(
        directory,
        case.execution,
        case.fence,
        "run_" + "a" * 32,
        lease,
        case.profile,
        case.plan,
        AsyncMock(),
        "0/10",
        captured_inputs=captured,
    )
    producer._cutover_active = True
    producer._cutover_request = NonprofileAdmissionCheck("cutover", lease, case.plan, ())
    producer._cutover_witness, producer._cutover_observation = object(), {}
    admission = NonprofileAdmission(
        lease,
        case.plan,
        producer.check_phase,
        check_cutover=producer.assert_cutover,
        captured_inputs=captured,
    )
    admission._started = admission._cutover_active = True
    monkeypatch.setattr(artifacts.recovery, "begin_overlay", AsyncMock())
    monkeypatch.setattr(artifacts.recovery, "finish_overlay", AsyncMock())
    return SimpleNamespace(
        directory=directory,
        binding=binding,
        factory=factory,
        capture=captured,
        admission=admission,
        producer=producer,
        fence=case.fence,
    )


def _terminal_transport(monkeypatch, case, events, fault):
    executor = SimpleNamespace(status=AsyncMock())

    async def builder_locks(directory, fence, database):
        assert directory is case.directory and fence is case.fence and database is directory.db
        events.append("Builder locked")

    async def header(directory, fence, actual, *, for_update):
        assert directory is case.directory and fence is case.fence and actual is executor and for_update
        assert not artifacts.is_fence_held(directory, fence, executor)
        events.append("Source verified")

    @asynccontextmanager
    async def source_fence(database):
        assert database._transaction_binding() is case.binding
        events.append("Source opened")
        try:
            yield executor
            assert database._transaction_binding() is case.binding
            if fault == "Source exit":
                raise ValueError("Source exit")
        finally:
            events.append("Source closed")

    monkeypatch.setattr(artifacts, "_lock_ordinary_controls", builder_locks)
    monkeypatch.setattr(artifacts, "_verify_header", header)
    case.factory.artifact_fence = source_fence


@pytest.mark.parametrize("fault", [None, "body", "Source exit"])
async def test_issued_common_fence_settles_source_before_builder_commit(ordinary_case, monkeypatch, fault):
    case, events = ordinary_case, []
    _terminal_transport(monkeypatch, case, events, fault)
    assert artifacts.current(case.directory) is None
    try:
        async with artifacts.ordinary_terminal_fence(case.directory, case.admission):
            assert artifacts.current(case.directory) is None
            assert artifacts.is_fence_held(case.directory, case.fence, case.directory.db)
            events.append("Builder writes")
            if fault == "body":
                raise ValueError("body")
        events.append("Builder commits")
    except ValueError as error:
        assert str(error) == fault
        events.append("Builder rolls back")
    assert events == [
        "Builder locked",
        "Source opened",
        "Source verified",
        "Builder writes",
        "Source closed",
        "Builder rolls back" if fault else "Builder commits",
    ]
    assert artifacts._TERMINAL.get() is None
    artifacts.recovery.begin_overlay.assert_not_awaited()
    artifacts.recovery.finish_overlay.assert_not_awaited()


@pytest.mark.parametrize(
    "changed",
    ["factory", "capture", "engine", "binding", "producer", "request", "phase", "execution", "promotion", "fence"],
)
async def test_common_fence_refuses_rebinding_inside_original_owner(ordinary_case, monkeypatch, changed):
    case = ordinary_case
    _terminal_transport(monkeypatch, case, [], None)
    with pytest.raises(RuntimeError, match="changed|capture_owner"):
        async with artifacts.ordinary_terminal_fence(case.directory, case.admission):
            match changed:
                case "factory":
                    artifacts.source._FACTORY.set(object())
                case "capture":
                    case.admission.captured_inputs = replace(case.capture)
                case "engine":
                    case.directory.db.engine = object()
                case "binding":
                    case.directory.db._transaction_binding = lambda: SimpleNamespace(
                        session=SimpleNamespace(in_transaction=lambda: True)
                    )
                case "producer":
                    case.admission.check_cutover = AsyncMock()
                case "request":
                    case.producer._cutover_request = None
                case "phase":
                    case.producer._cutover_active = False
                case "execution":
                    case.producer.execution = replace(case.producer.execution)
                case "promotion":
                    case.fence.promotion_datasets = (object(),)
                case "fence":
                    case.fence.datasets = ()
    assert artifacts._TERMINAL.get() is None


async def test_common_fence_rejects_nested_or_unissued_admission(ordinary_case, monkeypatch):
    case = ordinary_case
    _terminal_transport(monkeypatch, case, [], None)
    async with artifacts.ordinary_terminal_fence(case.directory, case.admission):
        with pytest.raises(RuntimeError, match="terminal_nested"):
            async with artifacts.ordinary_terminal_fence(case.directory, case.admission):
                pytest.fail("nested fence accepted")
    with pytest.raises(RuntimeError, match="terminal_changed"):
        async with artifacts.ordinary_terminal_fence(case.directory, SimpleNamespace()):
            pytest.fail("unissued fence accepted")


async def test_legacy_ordinary_uses_same_builder_without_a_source_factory(ordinary_case, monkeypatch):
    case = ordinary_case
    captured = replace(case.capture, _factory=None)
    case.admission.captured_inputs = case.producer.captured_inputs = captured
    monkeypatch.setattr(artifacts.source, "_FACTORY", ContextVar("ordinary_no_factory", default=None))
    monkeypatch.setattr(artifacts.source, "is_source_storage_required", lambda: False)
    monkeypatch.setattr(artifacts, "_lock_ordinary_controls", AsyncMock())
    monkeypatch.setattr(artifacts, "_verify_header", AsyncMock())
    async with artifacts.ordinary_terminal_fence(case.directory, case.admission):
        assert artifacts.is_fence_held(case.directory, case.fence, None)
    artifacts._verify_header.assert_awaited_once_with(case.directory, case.fence, case.directory.db, for_update=True)
