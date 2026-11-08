# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host-only transport checks; native catalog_by_root proof remains a Native prerequisite."""

import asyncio
import importlib
from contextlib import asynccontextmanager, contextmanager
from contextvars import ContextVar
from dataclasses import dataclass, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_profile_scope_reads as reads
from process.provider_directory_generic_source_policy import GenericSourcePolicy


@dataclass(frozen=True)
class Projection:
    tables: tuple


class Driver:
    def __init__(self):
        self.in_transaction = True
        self.probe_free = False
        self.commands = []
        self.actor = dict(session="publisher", current="owner", backend=123, xid="456", readonly="off")

    def is_in_transaction(self):
        return self.in_transaction

    async def fetchrow(self, sql):
        assert sql.startswith("SELECT session_user")
        return dict(self.actor)

    async def fetchval(self, sql, probe=None):
        if sql == "SHOW transaction_isolation":
            return "repeatable read"
        assert "pg_try_advisory_xact_lock" in sql and probe == 42
        return self.probe_free

    async def execute(self, sql):
        assert sql.startswith("LOCK TABLE") and sql.endswith("ACCESS SHARE MODE NOWAIT")
        self.commands.append(sql)


def _datasets():
    datasets = tuple(
        SimpleNamespace(
            source_id=name,
            dataset_id="dataset_" + name,
            endpoint_id="endpoint_" + name,
            evidence_run_id="evidence_" + name,
            dataset_hash="a" * 64,
            status="published",
            is_current=True,
            publication_metadata_hash="b" * 64,
            artifact_resources=("Organization",),
        )
        for name in ("retained", "changed")
    )
    return datasets


def _fixture_facts():
    """Construct native-shaped transport facts and the real admission dataclass."""
    models = tuple(type("Model" + str(number), (), {"__tablename__": "root_" + str(number)}) for number in range(9))
    policy = GenericSourcePolicy("synthetic", "owner", "publisher", ("reader",), ())
    factory = SimpleNamespace(policy=policy)
    driver = Driver()
    session = SimpleNamespace(driver=driver)
    binding = ContextVar("profile_reads_host_binding", default=None)
    lease_checks = []
    fhir = importlib.import_module("process.provider_directory_fhir")
    lease = SimpleNamespace(lease_digest="c" * 64, reservation_id="reservation")
    admission = fhir._ProviderDirectoryProfileCapacityAdmission(
        geometry=SimpleNamespace(bounded_admission=True),
        control_wal_projection=None,
        lease=lease,
        database_identity=None,
        build_id="build",
        run_id="run",
        initial_wal_lsn="0/0",
        wal_tracker=None,
    )

    async def acceptance(actual):
        assert actual is lease
        lease_checks.append(True)

    projection = Projection(tuple(SimpleNamespace(table_name=model.__tablename__) for model in models))
    datasets = _datasets()
    full = SimpleNamespace(datasets=datasets, source_ids=("retained", "changed"))
    resource = SimpleNamespace(datasets=datasets[1:], source_ids=("changed",))
    name_by_root = {model.__tablename__: model.__tablename__ + "_run" for model in models}
    oid_by_root = {model.__tablename__: 100 + number for number, model in enumerate(models)}
    return SimpleNamespace(
        models=models,
        policy=policy,
        factory=factory,
        driver=driver,
        session=session,
        binding=binding,
        lease_checks=lease_checks,
        acceptance=acceptance,
        admission=admission,
        projection=projection,
        full=full,
        resource=resource,
        name_by_root=name_by_root,
        oid_by_root=oid_by_root,
    )


def _directory(facts):
    return SimpleNamespace(
        ProviderDirectorySource=facts.models[0],
        RESOURCE_MODELS=facts.models[1:],
        _ProviderDirectoryArtifactScopeExactProjection=Projection,
        _provider_directory_profile_capacity_admission=lambda: facts.admission,
        _provider_directory_profile_capacity_acceptance_time=facts.acceptance,
        _assert_artifact_scope_projection=lambda candidate: candidate is facts.projection or _refuse(),
        _provider_directory_artifact_scope_table_name=lambda root, run: root + "_" + run,
        _schema=lambda: "synthetic",
        db=SimpleNamespace(_transaction_binding=facts.binding.get),
        _PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES=ContextVar("host_names", default=facts.name_by_root),
        _PROVIDER_DIRECTORY_ARTIFACT_OWNED_RELATION_OIDS=ContextVar("host_oids", default=facts.oid_by_root),
        _PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE=ContextVar("host_full", default=facts.full),
        _PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS=ContextVar("host_resource", default=("changed",)),
    )


def _install_transports(monkeypatch, facts):
    # This test installs only the transport/native catalog boundaries, not read guards.
    monkeypatch.setattr(reads.source, "is_source_storage_required", lambda: True)
    monkeypatch.setattr(reads.source, "source_producer_identity", lambda: ("a" * 40, "sha256:" + "b" * 64))
    monkeypatch.setattr(reads, "_copy_driver", AsyncMock(side_effect=lambda value: value.driver))
    catalog_by_root = {
        root: dict(oid=oid, name=facts.name_by_root[root], phase="completed") for root, oid in facts.oid_by_root.items()
    }

    async def targets(actual, publisher, selected, *, phase):
        assert actual is facts.directory and publisher.connection is facts.driver and phase == "completed"
        assert selected == tuple(
            (model, facts.name_by_root[model.__tablename__], facts.oid_by_root[model.__tablename__])
            for model in facts.models
        )
        return {root: dict(catalog) for root, catalog in catalog_by_root.items()}

    monkeypatch.setattr(reads.native_copy, "_target_family_catalog", targets)
    transactions, owner_outcomes = [], []

    @asynccontextmanager
    async def transaction(database, *, isolation):
        assert database is facts.directory.db and isolation == "repeatable read"
        transactions.append("enter")
        publisher = reads.source._Publisher(
            facts.driver, facts.policy, asyncio.current_task(), isolation, facts.factory
        )
        tokens = (
            reads.source._PUBLISHER.set(publisher),
            reads.source._OWNER.set((publisher, facts.session)),
            facts.binding.set(SimpleNamespace(session=facts.session)),
        )
        try:
            yield facts.session
        except BaseException:
            owner_outcomes.append("rollback")
            raise
        else:
            owner_outcomes.append("commit")
        finally:
            facts.binding.reset(tokens[2])
            reads.source._OWNER.reset(tokens[1])
            reads.source._PUBLISHER.reset(tokens[0])
            transactions.append("exit")

    monkeypatch.setattr(reads.source, "source_transaction", transaction)
    facts.catalog_by_root = catalog_by_root
    facts.transactions = transactions
    facts.owner_outcomes = owner_outcomes
    facts.transaction = transaction


@pytest.fixture
async def case(monkeypatch):
    """Issue only host transport boundaries; all Profile read guards execute."""
    facts = _fixture_facts()
    facts.directory = _directory(facts)
    _install_transports(monkeypatch, facts)
    pin = reads.inputs._Inputs(
        facts.directory,
        facts.factory,
        facts.policy,
        object(),
        asyncio.current_task(),
        reads.inputs._fence_json(facts.full),
        reads.inputs._fence_json(facts.resource),
        reads.source.source_producer_identity(),
        42,
    )
    pin_state_by_field = dict(active=True, full=facts.full, resource=facts.resource, tasks={})
    reads.inputs._ISSUED[pin] = pin_state_by_field
    tokens = reads.source._FACTORY.set(facts.factory), reads.inputs._ACTIVE.set(pin)
    facts.pin = pin
    facts.pin_state_by_field = pin_state_by_field
    try:
        yield facts
    finally:
        reads.inputs._ACTIVE.reset(tokens[1])
        reads.source._FACTORY.reset(tokens[0])
        reads.inputs._ISSUED.pop(pin, None)
        assert reads._ACTIVE.get() is None and reads._READ.get() is None


def _refuse():
    raise RuntimeError("projection_changed")


@asynccontextmanager
async def prepared(case):
    pin = replace(case.pin, task=asyncio.current_task())
    reads.inputs._ISSUED[pin] = case.pin_state_by_field
    token = reads.inputs._ACTIVE.set(pin)
    try:
        async with reads.prepared_scope(
            case.directory, case.projection, case.name_by_root, case.oid_by_root, case.full, case.resource
        ):
            yield
    finally:
        reads.inputs._ACTIVE.reset(token)
        reads.inputs._ISSUED.pop(pin, None)


async def test_late_read_uses_fresh_owner_transaction_and_full_vs_changed_fences(case):
    async with prepared(case):
        assert reads.is_active(case.directory)
        assert case.directory.db._transaction_binding() is None
        async with reads.read_transaction(case.directory):
            assert reads.source.source_copy_session() is not None
            assert case.directory._PROVIDER_DIRECTORY_ARTIFACT_DATASET_FENCE.get() is case.full
            assert case.directory._PROVIDER_DIRECTORY_ARTIFACT_RESOURCE_SCOPE_SOURCE_IDS.get() == ("changed",)
        assert case.directory.db._transaction_binding() is None
    assert case.transactions == ["enter", "exit"] * 3
    assert len(case.driver.commands) == 6 and case.lease_checks
    assert not reads.is_active(case.directory)


async def test_existing_genuine_owner_reused_without_second_transaction(case):
    async with prepared(case):
        async with case.transaction(case.directory.db, isolation="repeatable read"):
            before = len(case.transactions)
            async with reads.read_transaction(case.directory) as session:
                assert session is reads.source.source_copy_session()
            assert len(case.transactions) == before


async def test_nested_read_reuses_catalog_but_rechecks_native_origin_and_lease(case, monkeypatch):
    family = AsyncMock(wraps=reads.native_copy._target_family_catalog)
    native_actor = AsyncMock(wraps=reads._native_actor)
    monkeypatch.setattr(reads.native_copy, "_target_family_catalog", family)
    monkeypatch.setattr(reads, "_native_actor", native_actor)
    probes = AsyncMock(wraps=case.driver.fetchval)
    monkeypatch.setattr(case.driver, "fetchval", probes)
    async with prepared(case):
        family.reset_mock()
        native_actor.reset_mock()
        probes.reset_mock()
        lease_checks = len(case.lease_checks)
        transactions = len(case.transactions)
        async with reads.read_transaction(case.directory) as outer:
            active = reads._READ.get()
            async with reads.read_transaction(case.directory) as inner:
                assert inner is outer and reads._READ.get()[2] is active[2]
            assert reads._READ.get() is active
        assert family.await_count == 2 and native_actor.await_count == 4
        assert len(case.lease_checks) == lease_checks + 4
        assert sum("pg_try_advisory_xact_lock" in call.args[0] for call in probes.await_args_list) == 4
        assert len(case.transactions) == transactions + 2
        assert reads._READ.get() is None


@contextmanager
def _changed_nested_origin(case, monkeypatch, change):
    """Restore each refused mutation before the outer read performs its fresh postcheck."""
    with monkeypatch.context() as changed:
        token = _install_nested_read_mutation(case, changed, change)
        try:
            yield
        finally:
            if token is not None:
                reads.source._PUBLISHER.reset(token)


def _install_nested_read_mutation(case, changed, change):
    """Alter one selector or original native actor without changing unrelated facts."""
    if change in {"xid", "backend", "actor"}:
        field = "current" if change == "actor" else change
        changed.setitem(case.driver.actor, field, 456 if change == "backend" else "changed")
        return None
    if change == "driver":
        changed.setattr(reads, "_copy_driver", AsyncMock(return_value=Driver()))
        return None
    if change == "binding":
        changed.setattr(case.directory.db, "_transaction_binding", lambda: SimpleNamespace(session=object()))
        return None
    if change == "publisher":
        return reads.source._PUBLISHER.set(replace(reads.source.bound_publisher()))
    changed.setitem(case.oid_by_root, "root_0", case.oid_by_root["root_0"] + 1)
    return None


@pytest.mark.parametrize("change", ["xid", "backend", "actor", "driver", "binding", "publisher", "selector"])
async def test_nested_read_refuses_changed_origin_without_accepting_a_new_catalog(case, monkeypatch, change):
    family = AsyncMock(wraps=reads.native_copy._target_family_catalog)
    monkeypatch.setattr(reads.native_copy, "_target_family_catalog", family)
    async with prepared(case):
        family.reset_mock()
        async with reads.read_transaction(case.directory):
            with _changed_nested_origin(case, monkeypatch, change), pytest.raises((RuntimeError, ValueError)):
                async with reads.read_transaction(case.directory):
                    pytest.fail("changed nested origin exposed")
            assert family.await_count == 1
        assert family.await_count == 2


async def test_nested_read_keeps_outer_catalog_drift_refusal_before_owner_commit(case):
    with pytest.raises(ExpectedRefusal):
        async with prepared(case):
            transactions = len(case.transactions)
            with pytest.raises((RuntimeError, ValueError), match="catalog_changed"):
                async with reads.read_transaction(case.directory):
                    await _drift_nested_catalog(case)
                    assert len(case.transactions) == transactions + 1
            assert case.transactions[-1] == "exit" and case.owner_outcomes[-1] == "rollback"
            raise ExpectedRefusal


async def _drift_nested_catalog(case):
    """Change physical metadata inside a nested read for the outer fresh postcheck."""
    async with reads.read_transaction(case.directory):
        case.catalog_by_root["root_0"]["oid"] += 1


async def test_nested_read_marker_cannot_be_inherited_by_child_task(case):
    async with prepared(case):
        async with reads.read_transaction(case.directory):

            async def operation():
                with pytest.raises((RuntimeError, ValueError), match="scope_required"):
                    async with reads.read_transaction(case.directory):
                        pytest.fail("inherited nested read exposed")

            await asyncio.create_task(operation())


async def test_nested_cancellation_preserves_primary_and_restores_outer_marker(case, monkeypatch):
    async with prepared(case), reads.read_transaction(case.directory):
        active = reads._READ.get()
        original = asyncio.CancelledError("original nested cancellation")
        with monkeypatch.context() as changed:
            with pytest.raises(asyncio.CancelledError) as caught:
                async with reads.read_transaction(case.directory):
                    changed.setitem(case.driver.actor, "xid", "changed")
                    raise original
            assert caught.value is original
            assert isinstance(caught.value.__cause__, BaseExceptionGroup)
            assert caught.value.__cause__.exceptions[0] is original
        assert reads._READ.get() is active


async def test_nested_cancellation_rolls_back_outer_owner_and_clears_read_scope(case):
    original = asyncio.CancelledError("nested cancellation")
    with pytest.raises(asyncio.CancelledError) as caught:
        async with prepared(case), reads.read_transaction(case.directory):
            async with reads.read_transaction(case.directory):
                raise original
    assert caught.value is original and case.owner_outcomes[-1] == "rollback"
    assert reads._READ.get() is None and reads._ACTIVE.get() is None


def _mutate_case(case, change):
    if change == "name_by_root":
        case.name_by_root["root_0"] = "different"
        return
    if change == "oid_by_root":
        case.oid_by_root["root_0"] += 1
        return
    if change == "full":
        case.full.datasets = case.full.datasets[1:]
        return
    if change == "resource":
        case.resource.source_ids = ()
        return
    if change == "pin":
        case.pin_state_by_field["active"] = False
        return
    if change == "catalog_by_root":
        case.catalog_by_root["root_0"]["oid"] += 1
        return
    if change == "database":
        case.driver.probe_free = True
        return
    case.driver.actor["current"] = "reader"


@pytest.mark.parametrize(
    "change", ["name_by_root", "oid_by_root", "full", "resource", "pin", "catalog_by_root", "database", "actor"]
)
async def test_snapshot_or_native_identity_change_refuses(case, change):
    with pytest.raises(ExpectedRefusal):
        async with prepared(case):
            _mutate_case(case, change)
            with pytest.raises((RuntimeError, ValueError)):
                async with reads.read_transaction(case.directory):
                    pytest.fail("changed identity exposed")
            # Abort scope rather than restore a changed authenticated snapshot.
            raise ExpectedRefusal


class ExpectedRefusal(Exception):
    pass


async def test_unregistered_inherited_child_cannot_read(case):
    async with prepared(case):

        async def operation():
            with pytest.raises((RuntimeError, ValueError), match="scope_required"):
                async with reads.read_transaction(case.directory):
                    pytest.fail("unissued child read")

        await asyncio.create_task(operation())


async def test_child_takes_slot_before_original_transaction_and_reuses_permit(case):
    slots = asyncio.Semaphore(1)
    async with prepared(case):

        async def operation():
            assert case.directory.db._transaction_binding() is None
            assert reads.source._PUBLISHER.get() is None and reads.source._OWNER.get() is None
            assert reads.inputs._ACTIVE.get() is None
            assert reads.is_reader_slot_active(slots)
            async with reads.projection_slot(slots):
                async with case.transaction(case.directory.db, isolation="repeatable read"):
                    async with reads.read_transaction(case.directory):
                        return "original-owner"

        task = reads.create_read_task(case.directory, operation(), reader_slots=slots)
        assert await asyncio.wait_for(task, 1) == "original-owner"
    assert slots._value == 1


async def test_scope_exit_cancels_and_drains_waiting_child_before_closure(case):
    slots = asyncio.Semaphore(0)
    started_calls = []
    async with prepared(case):

        async def operation():
            started_calls.append(True)

        task = reads.create_read_task(case.directory, operation(), reader_slots=slots)
        await asyncio.sleep(0)
    assert task.cancelled() and not started_calls
    with pytest.raises((RuntimeError, ValueError), match="scope_required"):
        async with reads.read_transaction(case.directory):
            pytest.fail("closed read exposed")


async def test_child_permit_cannot_be_inherited_by_unissued_task(case):
    slots = asyncio.Semaphore(1)
    async with prepared(case):

        async def operation():
            async def nested():
                with pytest.raises((RuntimeError, ValueError), match="slot_changed"):
                    async with reads.projection_slot(slots):
                        pytest.fail("inherited permit exposed")

            await asyncio.create_task(nested())

        await reads.create_read_task(case.directory, operation(), reader_slots=slots)


async def test_cancellation_remains_cancellation_when_postcheck_also_refuses(case):
    with pytest.raises(ExpectedRefusal):
        async with prepared(case):
            original = asyncio.CancelledError("original")
            with pytest.raises(asyncio.CancelledError) as caught:
                async with reads.read_transaction(case.directory):
                    case.driver.actor["current"] = "reader"
                    raise original
            assert caught.value is original
            assert isinstance(caught.value.__cause__, BaseExceptionGroup)
            assert caught.value.__cause__.exceptions[0] is original
            raise ExpectedRefusal
