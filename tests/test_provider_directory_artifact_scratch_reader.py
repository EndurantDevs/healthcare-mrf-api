# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host-only child issuance orchestration; native authentication is substituted."""

import asyncio
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_artifact_scratch_reader as reader
from process import provider_directory_artifact_scratch_scope as scratch
from tests.test_provider_directory_output_snapshot_custody import relation


@contextmanager
def _tokens(case, fence, resource_fence, names, oids):
    assert fence is resource_fence is case.fence
    variables = (
        (case.directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES, names),
        (case.directory._PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS, oids),
        (case.directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE, fence),
        (case.directory._PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS, tuple(fence.source_ids)),
    )
    installed_tokens = [(variable, variable.set(value)) for variable, value in variables]
    try:
        yield
    finally:
        for variable, token in reversed(installed_tokens):
            variable.reset(token)


def _case():
    directory = SimpleNamespace(db=SimpleNamespace(_transaction_binding=lambda: None))
    for name, default in (
        ("RELATION_OVERRIDES", {}),
        ("OWNED_RELATION_OIDS", {}),
        ("DATASET_FENCE", None),
        ("RESOURCE_SCOPE_SOURCE_IDS", ()),
    ):
        setattr(directory, "_PROVIDER_DIRECTORY_ARTIFACT_" + name, ContextVar(name, default=default))
    case = SimpleNamespace(
        directory=directory,
        fence=SimpleNamespace(
            source_ids=("source",),
            datasets=(SimpleNamespace(source_id="source", dataset_id="dataset", artifact_resources=()),),
        ),
        events=[],
        count=0,
    )
    case.factory = SimpleNamespace(transaction=lambda: None)
    case.slots = asyncio.Semaphore(2)
    case.admission = SimpleNamespace(_assert_lease=lambda: case.events.append(("lease", asyncio.current_task())))
    case.projection = SimpleNamespace(projection_hash="a" * 64)
    case.admission.plan = SimpleNamespace(
        desired_fence_hash=scratch.preparation.desired_fence_hash(case.fence),
        artifact_scope_projection_hash=case.projection.projection_hash,
    )
    case.scope = reader.inputs.ScratchInputs(
        directory,
        case.admission,
        None,
        "{}",
        '{"name":"source_copy","copy":{"oid":100}}',
        "{}",
        tuple(
            reader.inputs.stages.OutputStage(
                "host_schema",
                root,
                f"part_{n}",
                n + 101,
                "b" * 64,
                replace(relation(root, n + 201), schema="host_schema", target=root),
            )
            for n, root in enumerate(reader.inputs.TYPED_ROOTS)
        ),
        (),
    )
    directory._artifact_scope_tokens = lambda *args: _tokens(case, *args)
    return case


def _transaction(case):
    @asynccontextmanager
    async def transaction(database, *, isolation):
        assert database is case.directory.db and isolation == "repeatable read"
        assert reader.source._PUBLISHER.get() is reader.source._OWNER.get() is None
        assert reader.inputs._ACTIVE.get() is reader.inputs._VERIFIED.get() is None
        assert reader.database_context._TRANSACTION.get() == ()
        assert reader.database_context._READER.get() is reader.database_context._SESSION.get() is None
        assert case.directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get() == {}
        case.events.append(("open", asyncio.current_task()))
        publisher = reader.source._PUBLISHER.set(object())
        owner = reader.source._OWNER.set(object())
        try:
            yield
        finally:
            reader.source._OWNER.reset(owner)
            reader.source._PUBLISHER.reset(publisher)
            case.events.append(("close", asyncio.current_task()))

    return transaction


def _output(case):
    @asynccontextmanager
    async def output(directory, *, operation, targets):
        assert directory is case.directory and operation == {} and targets == reader.inputs.TYPED_ROOTS
        capability = SimpleNamespace(
            task=asyncio.current_task(),
            policy=SimpleNamespace(schema="host_schema"),
            connection=SimpleNamespace(execute=AsyncMock()),
        )
        token = reader.custody._OUTPUT.set(capability)
        try:
            yield capability
            assert capability.connection.execute.await_count == 1
            assert capability.connection.execute.await_args.args[0].endswith(" IN ACCESS SHARE MODE NOWAIT")
        finally:
            reader.custody._OUTPUT.reset(token)

    return output


@pytest.fixture
def case(monkeypatch):
    state = _case()

    async def capability(value):
        assert value.task is asyncio.current_task() and reader.custody._OUTPUT.get() is value

    async def verify(directory, scope, native, identity):
        assert directory is state.directory and scope is state.scope and identity == "host_identity"
        assert scope.task is not asyncio.current_task() and reader.inputs._ACTIVE.get() is None
        await capability(native)
        state.events.append(("verify", asyncio.current_task()))

    monkeypatch.setattr(reader.source, "source_transaction", _transaction(state))
    monkeypatch.setattr(reader.custody, "output_snapshot_transaction", _output(state))
    monkeypatch.setattr(reader.custody, "_require_capability", capability)
    monkeypatch.setattr(reader.inputs, "_verify_native_membership", verify, raising=False)
    state.issued = lambda: _issued(state)
    return state


@contextmanager
def _issued(state):
    object.__setattr__(state.scope, "task", asyncio.current_task())
    reader.inputs._ISSUED[state.scope] = (
        state.directory,
        state.admission,
        asyncio.current_task(),
        "host_identity",
        reader.inputs._scope_state(state.scope),
    )
    factory = reader.source._FACTORY.set(state.factory)
    projection = scratch._PROJECTION.set((state.scope, state.projection))
    try:
        with reader.inputs.scratch_input_scope(state.scope), _tokens(state, state.fence, state.fence, {}, {}):
            yield
    finally:
        scratch._PROJECTION.reset(projection)
        reader.source._FACTORY.reset(factory)
        reader.inputs._ISSUED.pop(state.scope, None)


async def _body(case, started, release):
    assert reader.inputs.active_scratch_inputs(case.directory) is None
    assert reader._READ.get()[0].scope is case.scope
    assert set(case.directory._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.get()) == {
        reader.inputs.SOURCE,
        *reader.inputs.TYPED_ROOTS,
    }
    async with reader.projection_slot(case.slots):
        started.put_nowait(asyncio.current_task())
        await release.wait()
        return asyncio.current_task()


async def test_two_registered_children_overlap_with_distinct_source_transactions(case):
    with case.issued():
        started, release = asyncio.Queue(), asyncio.Event()
        child_tasks = [
            reader.create_scratch_read_task(case.directory, _body(case, started, release), reader_slots=case.slots)
            for _ in range(2)
        ]
        observed_tasks = {await asyncio.wait_for(started.get(), 1) for _ in child_tasks}
        assert observed_tasks == set(child_tasks)
        assert not any(task.done() for task in child_tasks)
        release.set()
        assert set(await asyncio.gather(*child_tasks)) == observed_tasks
        assert len([event for event in case.events if event[0] == "verify"]) == 4
        assert len([event for event in case.events if event[0] == "close"]) == 2
        assert not reader._TICKETS


async def test_inherited_scope_still_refused_before_native_io(case):
    with case.issued():

        async def inherited():
            return reader.inputs.active_scratch_inputs(case.directory)

        with pytest.raises(RuntimeError, match="scratch_scope_required"):
            await asyncio.create_task(inherited())
        assert case.events == []


@pytest.mark.parametrize("fault", ["expired", "digest", "callback", "fence", "projection"])
async def test_changed_issued_ticket_refused_before_source_io(case, fault):
    with case.issued():
        calls = []

        async def operation():
            calls.append(True)

        task = reader.create_scratch_read_task(case.directory, operation(), reader_slots=case.slots)
        ticket = next(iter(reader._TICKETS))
        match fault:
            case "expired":
                reader.inputs._ISSUED.pop(case.scope)
            case "digest":
                object.__setattr__(case.scope, "source_bindings_json", '{"changed":true}')
            case "fence":
                case.fence.datasets = ()
            case "projection":
                case.projection.projection_hash = "b" * 64
            case "callback":
                object.__setattr__(ticket, "projection", object())
        with pytest.raises(RuntimeError, match="scratch_child_ticket_required"):
            await task
        assert calls == [] and not any(event[0] == "open" for event in case.events)


async def test_issuer_drains_canceled_child_before_family_cleanup(case):
    with case.issued():
        started, release = asyncio.Queue(), asyncio.Event()
        child = reader.create_scratch_read_task(case.directory, _body(case, started, release), reader_slots=case.slots)
        assert await asyncio.wait_for(started.get(), 1) is child
        await reader.drain_scratch_children(case.directory, case.scope)
        assert child.cancelled() and not reader._TICKETS
        assert [event[0] for event in case.events].count("close") == 1


async def test_after_body_verification_failure_is_retained(case, monkeypatch):
    with case.issued():
        original = reader.inputs._verify_native_membership

        async def verify(*args):
            await original(*args)
            case.count += 1
            if case.count == 2:
                raise RuntimeError("host native membership changed")

        monkeypatch.setattr(reader.inputs, "_verify_native_membership", verify)

        async def operation():
            return "completed body"

        with pytest.raises(RuntimeError, match="native membership changed"):
            await reader.create_scratch_read_task(case.directory, operation(), reader_slots=case.slots)
        assert [event[0] for event in case.events].count("close") == 1


async def test_existing_slot_budget_precedes_native_transaction_creation(case):
    with case.issued():
        case.slots = asyncio.Semaphore(1)
        started, release = asyncio.Queue(), asyncio.Event()
        tasks = [
            reader.create_scratch_read_task(
                case.directory,
                _body(case, started, release),
                reader_slots=case.slots,
            )
            for _ in range(3)
        ]
        await asyncio.wait_for(started.get(), 1)
        await asyncio.sleep(0)
        assert len([event for event in case.events if event[0] == "open"]) == 1
        release.set()
        assert len(await asyncio.gather(*tasks)) == 3
        events = [event[0] for event in case.events if event[0] in {"open", "close"}]
        assert events == ["open", "close"] * 3
