# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Application routing and handoff refusal tests; these are not native actor proofs."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import entity_address_candidate_preparation as candidate
from process import entity_address_preparation_admission as admitted
from process import entity_address_source_preparation as preparation
from process import provider_directory_cms_preparation as cms
from process import provider_directory_generic_source_policy as source
from tests.test_provider_directory_cms_preparation import _admission, _signed_lease


class _Factory:
    """Model per-task original sessions without claiming PostgreSQL/profile qualification."""

    def __init__(self):
        self.policy = source.GenericSourcePolicy(
            "test_schema", "source_owner", "source_publisher", ("test_reader", "test_builder"), ("test_builder",)
        )
        self.binding = ContextVar("unit_source_binding", default=None)
        self.events = []
        self.fail_commit = False
        self.connection = SimpleNamespace(
            fetchrow=AsyncMock(return_value={"oid": 41, "relowner": 11, "owner": 11}),
            fetchval=AsyncMock(return_value=11),
            fetch=AsyncMock(return_value=[]),
            execute=AsyncMock(),
        )

    async def publication_session_factory(self):
        raise AssertionError("final publication pool is outside this unit scope")

    async def bind_existing_transaction(self):
        raise AssertionError("no supplied transaction may substitute for the build factory")

    @asynccontextmanager
    async def transaction(self, database, *, isolation="read committed"):
        assert database._transaction_binding() is None
        session = SimpleNamespace(connection=self.connection)
        binding = SimpleNamespace(session=session, borrowed=True)
        token = self.binding.set(binding)
        self.events.append(("begin", asyncio.current_task(), isolation, session))
        try:
            yield session
            if self.fail_commit:
                raise RuntimeError("unit commit failure")
            self.events.append(("commit", asyncio.current_task(), isolation, session))
        except BaseException:
            self.events.append(("rollback", asyncio.current_task(), isolation, session))
            raise
        finally:
            self.binding.reset(token)


@pytest.fixture
async def source_case(monkeypatch):
    factory = _Factory()
    database = SimpleNamespace(
        _transaction_binding=factory.binding.get,
        scalar=AsyncMock(return_value=41),
        status=AsyncMock(return_value="1"),
        transaction=lambda: pytest.fail("ordinary database transaction was selected"),
    )
    admission = _admission()
    admission._started = True
    admission.register_external_relation = AsyncMock()
    admission.assert_ready = AsyncMock()
    monkeypatch.setattr(source, "source_transaction", factory.transaction)
    monkeypatch.setattr(
        source, "bound_publisher", lambda: SimpleNamespace(policy=factory.policy, connection=factory.connection)
    )
    monkeypatch.setattr(source, "source_copy_session", lambda: factory.binding.get().session)
    monkeypatch.setattr(admitted._native(), "db", database)
    monkeypatch.setattr(admitted, "apply_temp_file_limit", AsyncMock())
    monkeypatch.setattr(admitted._native(), "_apply_entity_address_transaction_settings", AsyncMock())
    monkeypatch.setattr(preparation, "_close_grants", AsyncMock())
    token = cms._ACTIVE.set(admission)
    try:
        async with source.bind_generic_source_transaction_factory(factory):
            scope = admitted._admitted_preparation(admission, admission.plan.native_address_input_hash)
            scope.db_schema = factory.policy.schema
            scope_token = admitted._ADMISSION.set(scope)
            try:
                yield SimpleNamespace(factory=factory, database=database, admission=admission, scope=scope)
            finally:
                admitted._ADMISSION.reset(scope_token)
    finally:
        cms._ACTIVE.reset(token)


async def test_first_ddl_uses_source_original_session_and_closes_before_commit(source_case):
    case = source_case
    observed_sessions = []

    async def create(session):
        observed_sessions.append(session)
        assert case.factory.binding.get().session is session

    await admitted._create_owned_relation("test_schema", "stage_a", create)
    assert observed_sessions == [case.factory.events[0][3]]
    assert [event[0] for event in case.factory.events] == ["begin", "commit"]
    preparation._close_grants.assert_awaited_once_with(case.factory.connection, case.factory.policy, "stage_a")
    assert case.scope.owned_oids == {"stage_a": 41}
    case.admission.register_external_relation.assert_awaited_once()
    assert case.factory.binding.get() is None


async def test_failed_first_ddl_keeps_exact_cleanup_identity_without_registering(source_case):
    case = source_case
    case.factory.fail_commit = True
    with pytest.raises(RuntimeError, match="commit failure"):
        await admitted._create_owned_relation("test_schema", "stage_a", AsyncMock())
    assert case.scope.cleanup_oids == (("stage_a", "stage_a", 41),)
    assert [event[0] for event in case.factory.events] == ["begin", "rollback"]
    case.admission.register_external_relation.assert_not_awaited()
    assert not case.scope.worker_tasks and case.factory.binding.get() is None


async def test_sibling_source_operations_receive_distinct_original_sessions(source_case):
    case = source_case

    async def operation():
        async with admitted._worker_slot(), preparation.preparation_transaction(case.database, case.scope) as session:
            await asyncio.sleep(0)
            assert case.database._transaction_binding().session is session
            return session

    first, second = await admitted.gather(operation(), operation())
    assert first is not second
    assert case.database._transaction_binding() is None and not case.scope.worker_tasks


@pytest.mark.parametrize("failure", ["inactive", "unstarted", "hash"])
async def test_source_issuer_requires_active_exact_signed_admission_before_io(source_case, failure):
    case = source_case
    token = cms._ACTIVE.set(None if failure == "inactive" else case.admission)
    try:
        if failure == "unstarted":
            case.admission._started = False
        with pytest.raises(RuntimeError, match="source_preparation_invalid"):
            preparation.issue_source_preparation(
                case.admission, "wrong" if failure == "hash" else case.scope.native_input_hash
            )
        assert not case.factory.events
    finally:
        cms._ACTIVE.reset(token)


async def test_constructed_or_different_factory_scope_cannot_open_source_io(source_case):
    case = source_case
    forged = preparation._SourcePreparation(
        case.factory, case.admission, case.scope.native_input_hash, asyncio.current_task()
    )
    with pytest.raises(RuntimeError, match="source_preparation_invalid"):
        preparation._issued(forged, phases={"building"})
    token = source._FACTORY.set(_Factory())
    try:
        with pytest.raises(RuntimeError, match="source_preparation_invalid"):
            async with preparation.preparation_transaction(case.database, case.scope):
                pytest.fail("different factory was admitted")
        assert not case.factory.events
    finally:
        source._FACTORY.reset(token)


def _prepared(case):
    case.scope.source_preparation = preparation.issue_source_preparation(case.admission, case.scope.native_input_hash)
    stages = tuple(("logical_" + str(index), "stage_" + str(index), index + 41) for index in range(7))
    case.scope.stage_oids = stages
    case.scope.owned_oids = {name: oid for _, name, oid in stages}
    case.scope.source_preparation.original_relations = {(name, oid) for _, name, oid in stages}
    return candidate.PreparedEntityAddressGeneration("test_schema", [], [], [], [], stages, {}, {"rows": 1}, None, None)


async def test_complete_handoff_is_issued_only_after_original_commit(source_case, monkeypatch):
    case, observed = source_case, []
    prepared = _prepared(case)
    capture = SimpleNamespace(relation_oids=tuple((logical, oid) for logical, _, oid in prepared.stage_oids))

    async def close(database, actual, scope, issued):
        assert database is case.database and actual is prepared and scope is case.scope
        assert issued.phase == "closing" and case.factory.binding.get() is not None
        with pytest.raises(RuntimeError):
            preparation.require_source_generation(prepared, case.admission)
        observed.append("closed")
        return "test_reader", capture

    monkeypatch.setattr(preparation, "_freeze_native", close)
    await preparation.freeze_source_generation(case.database, prepared, case.scope)
    assert observed == ["closed"] and case.factory.events[-1][0] == "commit"
    assert preparation.require_source_generation(prepared, case.admission) is capture
    assert case.factory.binding.get() is None


@pytest.mark.parametrize("failure", ["native", "cancel", "commit"])
async def test_failed_or_cancelled_freeze_never_issues_finished_handoff(source_case, monkeypatch, failure):
    case = source_case
    prepared = _prepared(case)
    if failure == "commit":
        case.factory.fail_commit = True
        close = AsyncMock(return_value=("test_reader", object()))
    else:
        close = AsyncMock(side_effect=asyncio.CancelledError() if failure == "cancel" else ValueError("native refusal"))
    monkeypatch.setattr(preparation, "_freeze_native", close)
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else (ValueError, RuntimeError)):
        await preparation.freeze_source_generation(case.database, prepared, case.scope)
    assert prepared.source_preparation is None and case.scope.source_preparation.phase == "failed"
    assert case.factory.events[-1][0] == "rollback" and case.factory.binding.get() is None
    with pytest.raises(RuntimeError, match="source_preparation_invalid"):
        preparation.require_source_generation(prepared, case.admission)


async def test_closed_candidate_cannot_reenter_write_or_budget_io(source_case, monkeypatch):
    case = source_case
    case.scope.source_preparation.phase = "prepared"
    storage_check = AsyncMock()
    monkeypatch.setattr(admitted, "check_owned_storage", storage_check)
    with pytest.raises(RuntimeError, match="source_preparation_invalid"):
        await admitted.tuned_status("UPDATE test_schema.stage_a SET value=1")
    assert not case.factory.events
    storage_check.assert_not_awaited()


async def test_incomplete_family_refuses_before_reader_grants(source_case):
    case = source_case
    prepared = _prepared(case)
    prepared.stage_oids = prepared.stage_oids[:-1]
    with pytest.raises(ValueError, match="prepared_address_invalid"):
        await preparation.freeze_source_generation(case.database, prepared, case.scope)
    case.factory.connection.execute.assert_not_awaited()
    assert case.scope.source_preparation.phase == "failed"


async def test_source_parameter_refusal_never_executes_payload(source_case, monkeypatch):
    case = source_case
    monkeypatch.setattr(
        admitted, "apply_temp_file_limit", AsyncMock(side_effect=RuntimeError("parameter authority missing"))
    )
    with pytest.raises(RuntimeError, match="parameter authority missing"):
        await admitted.tuned_status("UPDATE test_schema.stage_a SET value=1")
    case.database.status.assert_not_awaited()
    assert case.factory.events[-1][0] == "rollback" and not case.scope.worker_tasks


async def test_other_task_cannot_finalize_the_issued_preparation(source_case):
    case = source_case
    prepared = _prepared(case)
    with pytest.raises(RuntimeError, match="source_preparation_invalid"):
        await asyncio.create_task(preparation.freeze_source_generation(case.database, prepared, case.scope))
    assert not case.factory.events and case.scope.source_preparation.phase == "building"


async def test_cleanup_of_original_candidates_does_not_request_a_new_spend_lease(source_case, monkeypatch):
    case = source_case
    monkeypatch.setattr(case.admission, "_assert_lease", lambda: (_ for _ in ()).throw(RuntimeError("expired lease")))
    with pytest.raises(RuntimeError, match="expired lease"):
        async with preparation.preparation_transaction(case.database, case.scope):
            pytest.fail("expired admission authorized preparation")
    case.scope.source_preparation.original_relations.add(("stage_a", 41))
    async with preparation.cleanup_transaction(
        case.database, case.scope.source_preparation, relation=("test_schema", "stage_a", 41)
    ):
        assert case.factory.binding.get() is not None
    assert case.factory.events[-1][0] == "commit" and case.factory.binding.get() is None


async def test_expired_lease_cleanup_entrypoint_drains_exact_issued_family(source_case, monkeypatch):
    case = source_case
    prepared = _prepared(case)
    capture = SimpleNamespace(relation_oids=tuple((logical, oid) for logical, _, oid in prepared.stage_oids))
    monkeypatch.setattr(preparation, "_freeze_native", AsyncMock(return_value=("test_reader", capture)))
    await preparation.freeze_source_generation(case.database, prepared, case.scope)
    owned_by_name = {f"test_schema.{name}": oid for _, name, oid in prepared.stage_oids}

    async def actual_oid(_statement, *, relation):
        return owned_by_name[relation]

    case.database.scalar.side_effect = actual_oid
    monkeypatch.setattr(admitted._native(), "_acquire_cutover_locks", AsyncMock())
    monkeypatch.setattr(case.admission, "_assert_lease", lambda: (_ for _ in ()).throw(RuntimeError("expired lease")))
    with pytest.raises(RuntimeError, match="expired lease"):
        preparation.require_source_generation(prepared, case.admission)
    await candidate.cleanup_prepared_entity_address_generation(prepared)
    dropped_statements = [
        call.args[0] for call in case.database.status.await_args_list if call.args[0].startswith("DROP")
    ]
    assert dropped_statements == [f"DROP TABLE test_schema.{name} RESTRICT" for _, name, _ in prepared.stage_oids]
    assert case.database._transaction_binding() is None


@pytest.mark.parametrize("failure", ["schema", "name", "oid"])
async def test_cleanup_refuses_unissued_source_identity_before_factory_io(source_case, failure):
    case = source_case
    case.scope.source_preparation.original_relations.add(("stage_a", 41))
    relation = (
        "other_schema" if failure == "schema" else "test_schema",
        "other_stage" if failure == "name" else "stage_a",
        99 if failure == "oid" else 41,
    )
    with pytest.raises(RuntimeError, match="source_preparation_invalid"):
        async with preparation.cleanup_transaction(case.database, case.scope.source_preparation, relation=relation):
            pytest.fail("unissued source relation authorized cleanup")
    assert case.factory.events == []


async def test_late_name_replacement_is_preserved_by_exact_source_cleanup(source_case, monkeypatch):
    case = source_case
    case.scope.source_preparation.original_relations.add(("stage_a", 41))
    case.database.scalar.return_value = 99
    lock = AsyncMock()
    monkeypatch.setattr(admitted._native(), "_acquire_cutover_locks", lock)
    await candidate._drop_owned_stage("test_schema", "stage_a", 41, source_input=case.scope.source_preparation)
    lock.assert_not_awaited()
    assert not any(call.args[0].startswith("DROP") for call in case.database.status.await_args_list)
    assert case.factory.binding.get() is None


def _source_cleanup_case(case, monkeypatch):
    """Keep the real cleanup dispatch and model only PostgreSQL result rows."""
    relations_by_name = {"stage_a": 41, "stage_b": 42}
    case.scope.owned_oids.update(relations_by_name)
    case.scope.source_preparation.original_relations.update(relations_by_name.items())

    async def scalar(statement, **parameters):
        assert case.database._transaction_binding() is not None
        assert admitted._ADMISSION.get() is None
        if "pg_try_advisory_xact_lock" in statement:
            return True
        return relations_by_name.get(parameters["relation"].split(".")[1])

    async def all_rows(_statement, *, db_schema, relation_names):
        assert db_schema == "test_schema" and case.database._transaction_binding() is not None
        return [(name,) for name in relation_names if name in relations_by_name]

    async def status(statement):
        assert case.database._transaction_binding() is not None
        if statement.startswith("DROP TABLE "):
            del relations_by_name[statement.split()[2].split(".")[1]]

    case.database.scalar.side_effect = scalar
    case.database.status.side_effect = status
    case.database.all = AsyncMock(side_effect=all_rows)
    monkeypatch.setattr(case.admission, "_assert_lease", lambda: pytest.fail("cleanup renewed a spend lease"))
    return relations_by_name


@pytest.mark.parametrize("phase", ["building", "failed", "prepared"])
async def test_source_cleanup_clears_only_build_contexts_and_keeps_original_proof(source_case, monkeypatch, phase):
    case = source_case
    relations_by_name = _source_cleanup_case(case, monkeypatch)
    issued = case.scope.source_preparation
    issued.phase = phase
    dependencies = candidate.prepared_doctors.PreparedAddressDependencies(
        "test_schema", None, (), {}, SimpleNamespace(lock_read_backend=AsyncMock())
    )
    token = candidate._NATIVE_DEPENDENCIES.set(dependencies)
    try:
        assert candidate.has_prepared_doctors()
        await candidate._drain_owned_stages("test_schema", case.scope.cleanup_oids, source_input=issued)
        assert not relations_by_name and issued.phase == phase
        assert admitted._ADMISSION.get() is case.scope
        assert candidate._NATIVE_DEPENDENCIES.get() is dependencies
        dependencies.archive.lock_read_backend.assert_not_awaited()
        statements = [call.args[0] for call in case.database.status.await_args_list]
        assert [sql for sql in statements if sql.startswith("LOCK TABLE")] == [
            "LOCK TABLE test_schema.stage_a IN ACCESS EXCLUSIVE MODE NOWAIT;",
            "LOCK TABLE test_schema.stage_b IN ACCESS EXCLUSIVE MODE NOWAIT;",
        ]
        assert sum("pg_try_advisory_xact_lock" in call.args[0] for call in case.database.scalar.await_args_list) == 2
        assert [event[0] for event in case.factory.events] == ["begin", "commit", "begin", "commit"]
        assert case.database._transaction_binding() is None
    finally:
        candidate._NATIVE_DEPENDENCIES.reset(token)


@pytest.mark.parametrize("failure", ["proof", "replacement", "lock"])
async def test_source_cleanup_refusals_preserve_context_and_unowned_relations(source_case, monkeypatch, failure):
    case = source_case
    relations_by_name = _source_cleanup_case(case, monkeypatch)
    issued = case.scope.source_preparation
    dependencies = candidate.prepared_doctors.PreparedAddressDependencies(
        "test_schema", None, (), {}, SimpleNamespace(lock_read_backend=AsyncMock())
    )
    if failure == "proof":
        issued = preparation._SourcePreparation(
            case.factory, case.admission, case.scope.native_input_hash, asyncio.current_task()
        )
    elif failure == "replacement":
        relations_by_name["stage_a"] = 99
    else:
        case.database.all.side_effect = RuntimeError("unit lock failure")
    token = candidate._NATIVE_DEPENDENCIES.set(dependencies)
    try:
        operation = candidate._cleanup_stage_family("test_schema", (("stage_a", "stage_a", 41),), source_input=issued)
        if failure == "replacement":
            await operation
        else:
            with pytest.raises(RuntimeError):
                await operation
        assert set(relations_by_name) == {"stage_a", "stage_b"}
        assert admitted._ADMISSION.get() is case.scope
        assert candidate._NATIVE_DEPENDENCIES.get() is dependencies
        assert not any(call.args[0].startswith("DROP") for call in case.database.status.await_args_list)
        assert case.database._transaction_binding() is None
        if failure == "proof":
            assert not case.factory.events
        elif failure == "lock":
            assert case.factory.events[-1][0] == "rollback"
    finally:
        candidate._NATIVE_DEPENDENCIES.reset(token)


async def test_source_cleanup_drains_original_transaction_before_owner_cancellation(source_case, monkeypatch):
    case = source_case
    relations_by_name = _source_cleanup_case(case, monkeypatch)
    dependencies = candidate.prepared_doctors.PreparedAddressDependencies(
        "test_schema", None, (), {}, SimpleNamespace(lock_read_backend=AsyncMock())
    )
    entered, release = asyncio.Event(), asyncio.Event()
    status = case.database.status.side_effect

    async def wait_while_locked(statement):
        if statement.startswith("LOCK TABLE "):
            entered.set()
            await release.wait()
        await status(statement)

    case.database.status.side_effect = wait_while_locked

    async def owner():
        token = candidate._NATIVE_DEPENDENCIES.set(dependencies)
        try:
            await candidate._drain_owned_stages(
                "test_schema", case.scope.cleanup_oids, source_input=case.scope.source_preparation
            )
        finally:
            assert admitted._ADMISSION.get() is case.scope
            assert candidate._NATIVE_DEPENDENCIES.get() is dependencies
            candidate._NATIVE_DEPENDENCIES.reset(token)

    task = asyncio.create_task(owner())
    try:
        await asyncio.wait_for(entered.wait(), 1)
        task.cancel()
        await asyncio.sleep(0)
        assert not task.done()
    finally:
        release.set()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not relations_by_name and case.database._transaction_binding() is None
    assert case.factory.events[-1][0] == "commit"


@pytest.mark.parametrize("failure", [ValueError("unit build failure"), asyncio.CancelledError()])
async def test_failed_source_build_keeps_original_error_after_exact_cleanup(source_case, monkeypatch, failure):
    case = source_case
    relations_by_name = _source_cleanup_case(case, monkeypatch)
    dependencies = candidate.prepared_doctors.PreparedAddressDependencies(
        "test_schema", None, (), {}, SimpleNamespace(lock_read_backend=AsyncMock())
    )
    monkeypatch.setattr(admitted, "_admitted_preparation", lambda *_: case.scope)
    monkeypatch.setattr(candidate, "_capture_dependencies", AsyncMock(return_value=dependencies))
    monkeypatch.setattr(admitted._native(), "process_entity_address_unified_data", AsyncMock(side_effect=failure))
    inputs = candidate.ProviderDirectoryAddressPreparationInput(
        (), "overlay_stage", 12, 0, semantic_as_of=case.admission.plan.desired_profile_as_of
    )
    with pytest.raises(type(failure)) as caught:
        await candidate.prepare_provider_directory_entity_address(
            {}, {}, preparation_input=inputs, admission=case.admission, native_input_hash=case.scope.native_input_hash
        )
    assert caught.value is failure and not relations_by_name
    assert admitted._ADMISSION.get() is case.scope and candidate._NATIVE_DEPENDENCIES.get() is None
    assert candidate.current() is None and case.database._transaction_binding() is None


async def test_real_task_cancellation_drains_original_freeze_transaction(source_case, monkeypatch):
    case = source_case
    entered, stopped = asyncio.Event(), asyncio.Event()

    async def close(*_args):
        entered.set()
        try:
            await asyncio.Future()
        finally:
            stopped.set()

    monkeypatch.setattr(preparation, "_freeze_native", close)

    async def operation():
        prepared = _prepared(case)
        await preparation.freeze_source_generation(case.database, prepared, case.scope)

    owner = asyncio.create_task(operation())
    await asyncio.wait_for(entered.wait(), 1)
    owner.cancel()
    with pytest.raises(asyncio.CancelledError):
        await owner
    assert stopped.is_set() and case.scope.source_preparation.phase == "failed"
    assert case.factory.events[-1][0] == "rollback" and case.factory.binding.get() is None


@pytest.mark.parametrize("failure", [None, "missing", "bound"])
async def test_dependency_observer_preserves_original_factory_and_session(source_case, monkeypatch, failure):
    from process import provider_directory_source_observation as observation

    case = source_case
    observed_isolations = []
    observer_session = object()
    directory = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(directory, "db", case.database)

    @asynccontextmanager
    async def issued_observer(actual_directory, *, isolation):
        assert actual_directory is directory and actual_directory.db is case.database
        async with case.factory.observation_transaction(isolation=isolation) as session:
            yield session

    monkeypatch.setattr(observation, "observation_transaction", issued_observer)

    @asynccontextmanager
    async def observer(*, isolation):
        assert case.database._transaction_binding() is None
        observed_isolations.append(isolation)
        yield observer_session

    if failure != "missing":
        monkeypatch.setattr(case.factory, "observation_transaction", observer, raising=False)
    token = case.factory.binding.set(object() if failure == "bound" else None)
    try:
        if failure is None:
            async with preparation.source_dependency_session(
                case.database, case.admission, case.scope.native_input_hash
            ) as session:
                assert session is observer_session and case.database._transaction_binding() is None
            assert observed_isolations == ["repeatable read"]
        else:
            with pytest.raises(RuntimeError, match="source_preparation_invalid"):
                async with preparation.source_dependency_session(
                    case.database, case.admission, case.scope.native_input_hash
                ):
                    pytest.fail("missing or already-bound observer authorized IO")
            assert observed_isolations == []
        assert case.factory.events == []
    finally:
        case.factory.binding.reset(token)


async def _native_freeze_case(case, monkeypatch, failure):
    """Model only fixed native observations used by the application handoff tests."""
    modules = SimpleNamespace()
    modules.equivalence = importlib.import_module("process.network_cms_registry_address_equivalence")
    modules.native_layout = importlib.import_module("process.provider_directory_cms_native_layout")
    modules.read_catalog = importlib.import_module("process.mrf_address_publication")
    modules.pins = importlib.import_module("process.network_registry_cms_prepared_address_pin")
    modules.source_tables = importlib.import_module("process.entity_address_snapshot_source")
    modules.directory = importlib.import_module("process.provider_directory_fhir")
    roots = tuple(sorted(relation.table_name for relation in modules.source_tables.entity_address_archive_relations()))
    case.admission.plan = replace(case.admission.plan, native_address_targets=roots)
    case.admission.lease = _signed_lease(case.admission.plan)
    prepared = _prepared(case)
    stages = tuple((root, root + "_cms" + "a" * 20, index + 41) for index, root in enumerate(roots))
    prepared.stage_oids = case.scope.stage_oids = stages
    case.scope.owned_oids = {name: oid for _, name, oid in stages}
    case.scope.source_preparation.original_relations = {(name, oid) for _, name, oid in stages}
    case.scope.source_preparation.sequences = {name: () for _, name, _ in stages}
    measured_relations = [SimpleNamespace(relation=name, oid=oid) for _, name, oid in stages]
    case.admission.measure = AsyncMock(return_value=measured_relations)
    monkeypatch.setattr(modules.directory, "db", case.database)
    case.database._connect_reader = AsyncMock(
        return_value=SimpleNamespace(_reader_login=("test_builder" if failure == "reader" else "test_reader", ""))
    )
    model_check = AsyncMock(side_effect=ValueError("index changed") if failure == "index" else None)
    custody_check = AsyncMock(side_effect=ValueError("custody changed") if failure == "custody" else None)
    monkeypatch.setattr(modules.native_layout, "capture_retained_native_source", model_check)
    monkeypatch.setattr(modules.pins, "_custody", custody_check)
    monkeypatch.setattr(modules.read_catalog, "require_native_read_catalog", AsyncMock())
    captured = SimpleNamespace(relation_oids=tuple((logical, oid) for logical, _, oid in stages))
    monkeypatch.setattr(
        modules.equivalence, "capture_registry_cms_address_copy_source", AsyncMock(return_value=captured)
    )
    if failure == "budget":
        case.admission.assert_ready.side_effect = RuntimeError("budget refused")
    return prepared, captured, model_check


@pytest.mark.parametrize("failure", [None, "index", "reader", "custody", "budget"])
async def test_exact_seven_reader_handoff_follows_full_native_models_and_commit(source_case, monkeypatch, failure):
    """Model native refusals at the real application seam without fabricating native receipts."""
    case = source_case
    prepared, captured, model_check = await _native_freeze_case(case, monkeypatch, failure)
    if failure is None:
        await preparation.freeze_source_generation(case.database, prepared, case.scope)
        assert preparation.require_source_generation(prepared, case.admission) is captured
    else:
        with pytest.raises((ValueError, RuntimeError)):
            await preparation.freeze_source_generation(case.database, prepared, case.scope)
        assert prepared.source_preparation is None
    statements = [call.args[0] for call in case.factory.connection.execute.await_args_list]
    grants = [statement for statement in statements if statement.startswith("GRANT")]
    if failure in {"index", "reader"}:
        assert grants == []
    else:
        assert len(grants) == 7 and all('TO "test_reader"' in statement for statement in grants)
        assert model_check.await_count == 7
    if failure == "budget":
        assert case.factory.events[-1][0] == "commit"
    elif failure is not None:
        assert case.factory.events[-1][0] == "rollback"
    assert case.factory.binding.get() is None


@pytest.mark.parametrize("failure", ["inactive", "unstarted", "hash", "database"])
async def test_dependency_observer_refuses_changed_admission_or_database(source_case, monkeypatch, failure):
    from process import provider_directory_source_observation as observation

    case = source_case

    @asynccontextmanager
    async def forbidden(**_options):
        pytest.fail("unissued observation reached")
        yield

    monkeypatch.setattr(case.factory, "observation_transaction", forbidden, raising=False)
    monkeypatch.setattr(observation, "observation_transaction", forbidden)
    directory = importlib.import_module("process.provider_directory_fhir")
    monkeypatch.setattr(directory, "db", object() if failure == "database" else case.database)
    token = cms._ACTIVE.set(None if failure == "inactive" else case.admission)
    try:
        if failure == "unstarted":
            case.admission._started = False
        with pytest.raises(RuntimeError, match="source_preparation_invalid"):
            async with preparation.source_dependency_session(
                case.database, case.admission, "wrong" if failure == "hash" else case.scope.native_input_hash
            ):
                pytest.fail("changed dependency authority accepted")
        assert not case.factory.events and case.database._transaction_binding() is None
    finally:
        cms._ACTIVE.reset(token)
