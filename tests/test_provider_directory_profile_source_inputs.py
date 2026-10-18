# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host-only transport boundaries for issued original Profile SOURCE inputs."""

import asyncio
import copy
import json
import sys
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_source_inputs as inputs
from process.provider_directory_generic_source_policy import GenericSourcePolicy


class Driver:
    def __init__(self, headers, *, pinned):
        self.headers, self.pinned, self.commands = headers, pinned, []

    async def fetch(self, statement, identifiers):
        headers = [item for item in self.headers if item["dataset_id"] in identifiers]
        if "octet_length" in statement:
            return [dict(dataset_id=item["dataset_id"], bytes=4096) for item in headers]
        return [dict(dataset_id=item["dataset_id"], facts=json.dumps(item)) for item in headers]

    async def fetchval(self, statement, probe):
        assert "pg_try_advisory_xact_lock" in statement and type(probe) is int
        return self.pinned

    async def execute(self, statement):
        assert statement.startswith("LOCK TABLE") and statement.endswith("ACCESS SHARE MODE NOWAIT")
        self.commands.append(statement)


def _dataset(source_id):
    return SimpleNamespace(
        source_id=source_id,
        dataset_id="dataset-" + source_id,
        endpoint_id="endpoint-" + source_id,
        evidence_run_id="evidence-" + source_id,
        dataset_hash="a" * 64,
        status="published",
        is_current=True,
        publication_metadata_hash="b" * 64,
        artifact_resources=("Organization",),
    )


def _header(dataset, index):
    metadata = dict(source_ids=[dataset.source_id], resource_hash_contract="resource-v1")
    binding_by_field = dict(
        contract=inputs.storage.CONTRACT,
        identity=dict(
            dataset_id=dataset.dataset_id,
            endpoint_id=dataset.endpoint_id,
            acquisition_root_run_id=dataset.evidence_run_id,
            metadata=metadata,
        ),
        phase="published",
        ordinal=1,
        resets={},
        auxiliary=None,
        relationship_sha256=None,
        tables={
            root: dict(
                name="pdgs_" + str(index) * 32 + "_" + ("r", "ip", "np", "ao", "w", "rl")[number],
                oid=100 + index * 10 + number,
                owner=99,
                layout_sha256="c" * 64,
            )
            for number, root in enumerate(inputs.storage.RELATIONS)
        },
    )
    return dict(
        dataset_id=dataset.dataset_id,
        endpoint_id=dataset.endpoint_id,
        acquisition_root_run_id=dataset.evidence_run_id,
        dataset_hash=dataset.dataset_hash,
        status="published",
        is_current=True,
        publication_metadata_sha256="b" * 64,
        publication_metadata_json=metadata,
        candidate_storage_json=binding_by_field,
    )


@pytest.fixture
def case(monkeypatch):
    datasets = tuple(_dataset(name) for name in ("changed", "retained"))
    headers = [_header(dataset, index) for index, dataset in enumerate(datasets)]
    pinned, reader = Driver(headers, pinned=True), Driver(headers, pinned=False)
    policy = GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("reader",), ())
    events = []

    @asynccontextmanager
    async def observation(*, isolation):
        events.append(("enter", isolation))
        try:
            yield SimpleNamespace(driver=pinned)
        finally:
            events.append(("exit", isolation))

    factory = SimpleNamespace(policy=policy, observation_transaction=observation)
    directory = SimpleNamespace(
        _schema=lambda: "source_schema",
        db=SimpleNamespace(),
        _verify_provider_directory_artifact_dataset_fence=AsyncMock(),
    )
    monkeypatch.setattr(inputs.source, "source_producer_identity", lambda: ("d" * 40, "sha256:" + "e" * 64))
    monkeypatch.setattr(inputs, "_copy_driver", AsyncMock(side_effect=lambda session: session.driver))
    monkeypatch.setattr(inputs, "verify_generic_source_family", AsyncMock())
    monkeypatch.setattr(inputs.storage, "_verify_tables", AsyncMock())

    @asynccontextmanager
    async def transaction(database, *, isolation):
        assert database is directory.db and isolation == "repeatable read"
        yield

    monkeypatch.setattr(inputs.source, "source_transaction", transaction)
    monkeypatch.setattr(
        inputs.source, "bound_publisher", lambda: SimpleNamespace(factory=factory, policy=policy, connection=reader)
    )
    token = inputs.source._FACTORY.set(factory)
    yield SimpleNamespace(
        directory=directory,
        datasets=datasets,
        headers=headers,
        pinned=pinned,
        reader=reader,
        full=SimpleNamespace(datasets=datasets),
        resource=SimpleNamespace(datasets=datasets[:1]),
        factory=factory,
        events=events,
    )
    inputs.source._FACTORY.reset(token)
    assert inputs._ACTIVE.get() is None


async def test_scope_pins_full_but_selects_changed_resources(case):
    async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
        selected = inputs.resource_relation(case.directory, case.datasets[0])
        assert selected == "pdgs_" + "0" * 32 + "_r"
        captured = inputs.binding(case.directory, case.datasets[0])
        captured["tables"][inputs.storage.RESOURCE]["oid"] = 999
        assert inputs.binding(case.directory, case.datasets[0])["tables"][inputs.storage.RESOURCE]["oid"] == 100
        source_batch = SimpleNamespace(source_id="retained", dataset_id=None)
        assert inputs.binding(case.directory, source_batch)["identity"]["dataset_id"] == "dataset-retained"
        with pytest.raises((ValueError, RuntimeError), match="dataset_changed"):
            inputs.resource_relation(case.directory, case.datasets[1])
        assert len(case.pinned.commands) == 1
        assert all(
            proof["name"] in case.pinned.commands[0]
            for header in case.headers
            for proof in header["candidate_storage_json"]["tables"].values()
        )
    assert case.events == [("enter", "repeatable read"), ("exit", "repeatable read")]
    with pytest.raises((ValueError, RuntimeError), match="scope_required"):
        inputs.binding(case.directory, case.datasets[0])


@pytest.mark.parametrize(
    "field", ["endpoint_id", "acquisition_root_run_id", "dataset_hash", "publication_metadata_sha256"]
)
async def test_changed_header_refuses_before_exposure(case, field):
    case.headers[0][field] = "changed"
    with pytest.raises((ValueError, RuntimeError)):
        async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
            pytest.fail("changed native header exposed")
    assert case.events[-1][0] == "exit"


async def test_wrong_resource_fence_refuses_before_native_setup(case):
    unrelated = SimpleNamespace(datasets=(_dataset("foreign"),))
    with pytest.raises((ValueError, RuntimeError), match="resource_fence_changed"):
        async with inputs.profile_source_input_scope(case.directory, case.full, unrelated):
            pytest.fail("foreign resource fence exposed")
    assert case.events == []


async def test_native_catalog_refusal_propagates(case, monkeypatch):
    failure = RuntimeError("native catalog refusal")
    monkeypatch.setattr(inputs.storage, "_verify_tables", AsyncMock(side_effect=failure))
    with pytest.raises(RuntimeError) as caught:
        async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
            pytest.fail("native refusal ignored")
    assert caught.value is failure
    assert case.events[-1][0] == "exit"


async def test_unregistered_child_refuses_and_scope_cleans(case):
    async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):

        async def foreign():
            return inputs.binding(case.directory, case.datasets[0])

        with pytest.raises((ValueError, RuntimeError), match="scope_required"):
            await asyncio.create_task(foreign())
    assert not inputs._ISSUED


async def test_read_transaction_rechecks_original_factory_and_binding(case, monkeypatch):
    events = []

    @asynccontextmanager
    async def transaction(database, *, isolation):
        assert database is case.directory.db and isolation == "repeatable read"
        events.append("enter")
        yield
        events.append("exit")

    monkeypatch.setattr(inputs.source, "source_transaction", transaction)
    monkeypatch.setattr(
        inputs.source,
        "bound_publisher",
        lambda: SimpleNamespace(factory=case.factory, policy=case.factory.policy, connection=case.reader),
    )
    async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
        async with inputs.read_transaction(case.directory):
            assert inputs.resource_relation(case.directory, case.datasets[0]) == "pdgs_" + "0" * 32 + "_r"
        case.reader.headers = copy.deepcopy(case.headers)
        case.reader.headers[0]["candidate_storage_json"]["tables"][inputs.storage.RESOURCE]["oid"] = 999
        with pytest.raises((ValueError, RuntimeError), match="binding_changed"):
            async with inputs.read_transaction(case.directory):
                pytest.fail("changed child OID admitted")
    assert events[:2] == ["enter", "exit"]


def _worker(case, monkeypatch):
    model = SimpleNamespace(__tablename__="typed_model")
    batch = SimpleNamespace(
        source_id="changed", dataset_id="dataset-changed", evidence_run_id="evidence-changed", batch_number=1
    )
    table = SimpleNamespace(table_name="typed_model", batches=(batch,))
    admission = SimpleNamespace(run_id="run", lease=SimpleNamespace(lease_digest="f" * 64, reservation_id="lease"))
    case.directory.RESOURCE_MODELS = (model,)
    case.directory._provider_directory_profile_capacity_admission = lambda: admission
    case.directory._provider_directory_artifact_scope_table_name = lambda name, run: name + "_" + run
    monkeypatch.setitem(
        sys.modules,
        "process.provider_directory_profile_scope_copy",
        SimpleNamespace(scope_projection=lambda directory: SimpleNamespace(tables=(table,))),
    )
    return model, batch, table, admission


async def test_materialization_ticket_preserves_context_but_clears_physical_owner(case, monkeypatch):
    model, batch, table, _admission = _worker(case, monkeypatch)
    async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):

        async def operation():
            assert inputs.source._PUBLISHER.get() is None and inputs.source._OWNER.get() is None
            assert inputs.owned._ACTIVE_OWNED_TRANSACTION.get() is None
            return inputs.binding(case.directory, batch)

        task = inputs.create_materialization_task(
            case.directory, operation(), model=model, relation="typed_model_run", table_projection=table
        )
        assert (await task)["identity"]["dataset_id"] == "dataset-changed"


async def test_materialization_ticket_refuses_projection_or_relation_change(case, monkeypatch):
    model, _batch, table, _admission = _worker(case, monkeypatch)
    async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):

        async def operation():
            pytest.fail("wrong target executed")

        coroutine = operation()
        with pytest.raises((ValueError, RuntimeError), match="worker_changed"):
            inputs.create_materialization_task(
                case.directory, coroutine, model=model, relation="wrong", table_projection=table
            )
        assert coroutine.cr_frame is None


async def test_scope_drains_owned_worker_on_body_cancellation(case, monkeypatch):
    model, batch, table, _admission = _worker(case, monkeypatch)
    started = asyncio.Event()
    stopped = asyncio.Event()

    async def operation():
        assert inputs.binding(case.directory, batch)
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    failure = asyncio.CancelledError("body cancelled")
    with pytest.raises(asyncio.CancelledError) as caught:
        async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
            task = inputs.create_materialization_task(
                case.directory, operation(), model=model, relation="typed_model_run", table_projection=table
            )
            await started.wait()
            raise failure
    assert caught.value is failure and stopped.is_set() and task.cancelled()
    assert not inputs._ISSUED


async def test_validated_promotion_keeps_actual_selected_dataset(case):
    case.datasets[0].status = "validated"
    case.datasets[0].is_current = False
    case.datasets[0].promote_on_cutover = True
    case.full.should_select_validated_candidates = True
    case.headers[0].update(status="validated", is_current=False)
    async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
        selected = inputs.dataset(case.directory, case.datasets[0])
        assert selected is case.datasets[0] and selected.promote_on_cutover
        assert selected.status == "validated"
        case.directory._verify_provider_directory_artifact_dataset_fence.assert_awaited_once_with(case.full)


@pytest.mark.parametrize("status", ["verification_baseline", "verification_mismatch", "validated"])
async def test_unapproved_origin_refuses_before_observation(case, status):
    case.datasets[0].status = status
    with pytest.raises(RuntimeError):
        async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
            pytest.fail("unapproved origin exposed")
    assert case.events == []


async def test_full_fence_refusal_preserves_original_error(case):
    failure = RuntimeError("actual full fence refused")
    case.directory._verify_provider_directory_artifact_dataset_fence.side_effect = failure
    with pytest.raises(RuntimeError) as caught:
        async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
            pytest.fail("full fence refusal ignored")
    assert caught.value is failure and case.events[-1][0] == "exit"


async def test_stale_metadata_and_lease_refuse(case, monkeypatch):
    model, batch, table, admission = _worker(case, monkeypatch)
    async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
        case.datasets[0].publication_metadata_hash = "z" * 64
        with pytest.raises(RuntimeError, match="fence_changed"):
            inputs.binding(case.directory, batch)
        case.datasets[0].publication_metadata_hash = "b" * 64

        async def operation():
            admission.lease.lease_digest = "changed"
            with pytest.raises(RuntimeError, match="worker_changed"):
                inputs.binding(case.directory, batch)
            return "refused"

        task = inputs.create_materialization_task(
            case.directory, operation(), model=model, relation="typed_model_run", table_projection=table
        )
        assert await task == "refused"


async def test_legacy_scope_preserves_canonical_source_choice(case):
    token = inputs.source._FACTORY.set(None)
    try:
        async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
            assert inputs.resource_relation(case.directory, case.datasets[0]) == inputs.storage.RESOURCE
            assert inputs.binding(case.directory, case.datasets[0]) is None
    finally:
        inputs.source._FACTORY.reset(token)
    assert case.events == []


async def test_worker_projection_must_be_original_object(case, monkeypatch):
    model, _batch, table, _admission = _worker(case, monkeypatch)
    forged = SimpleNamespace(table_name=table.table_name, batches=table.batches)
    async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):

        async def operation():
            pytest.fail("unissued projection executed")

        coroutine = operation()
        with pytest.raises(RuntimeError, match="worker_changed"):
            inputs.create_materialization_task(
                case.directory, coroutine, model=model, relation="typed_model_run", table_projection=forged
            )
        assert coroutine.cr_frame is None


async def test_repeated_cancellation_drains_before_releasing_pins(case, monkeypatch):
    model, batch, table, _admission = _worker(case, monkeypatch)
    started, closing, release = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def operation():
        assert inputs.binding(case.directory, batch)
        started.set()
        try:
            await asyncio.Event().wait()
        finally:
            closing.set()
            await release.wait()

    async def parent():
        async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
            inputs.create_materialization_task(
                case.directory, operation(), model=model, relation="typed_model_run", table_projection=table
            )
            await asyncio.Event().wait()

    pending = asyncio.create_task(parent())
    await started.wait()
    pending.cancel("original cancellation")
    await closing.wait()
    assert case.events == [("enter", "repeatable read")]
    pending.cancel("repeat cancellation")
    release.set()
    with pytest.raises(asyncio.CancelledError):
        await pending
    assert case.events[-1][0] == "exit" and not inputs._ISSUED


async def test_missing_published_binding_refuses_before_exposure(case):
    case.headers[0]["candidate_storage_json"] = None
    with pytest.raises(RuntimeError, match="binding_required"):
        async with inputs.profile_source_input_scope(case.directory, case.full, case.resource):
            pytest.fail("missing native binding exposed")
    assert case.events[-1][0] == "exit"
