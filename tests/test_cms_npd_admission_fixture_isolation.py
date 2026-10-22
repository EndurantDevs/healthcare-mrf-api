# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep control updates on the exact database owned by the admission fixture."""

import asyncio
import importlib
from contextlib import asynccontextmanager, nullcontext
from contextvars import Context
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

support = importlib.import_module("tests.cms_npd_admission_postgres_support")
control = importlib.import_module("process.control_lifecycle")


def test_common_fixture_includes_authentic_retry():
    order, retry = "20260811130000", "20260824143000"
    prefixes = support.MIGRATION_PREFIXES
    assert prefixes.count(order) == prefixes.count(retry) == 1
    assert prefixes.index("20260810060000") < prefixes.index(order) < prefixes.index(retry)
    assert prefixes.index("20260818020000") < prefixes.index(retry) < prefixes.index("20260830100000")
    assert support.LEGACY_MIGRATION_PREFIXES.count(retry) == 1
    assert prefixes[-4:] == ("20260930120000", "20260930130000", "20260930140000", "20261001100000")


def test_source_fixture_inherits_common_retry():
    from tests.provider_directory_source_postgres_support import source_component_migrations

    prefixes = source_component_migrations()
    assert prefixes.count("20260824143000") == 1
    assert prefixes.index("20260811130000") < prefixes.index("20260824143000")


@pytest.mark.asyncio
async def test_retained_legacy_fixture_keeps_its_heap_contract_with_current_retirement(monkeypatch):
    from tests import provider_directory_source_postgres_support as source

    prefixes = support.LEGACY_MIGRATION_PREFIXES
    assert prefixes[-2:] == ("20261015015000", "20261022120000")
    assert prefixes.count("20261022120000") == 1
    assert prefixes[:-2] == support.MIGRATION_PREFIXES[: support.MIGRATION_PREFIXES.index("20260930100000")]
    component = SimpleNamespace(admin_engine=object(), database=object(), factory=SimpleNamespace(transaction=Mock()))
    administrator = SimpleNamespace()
    migration_selections = []

    @asynccontextmanager
    async def source_database(bindings, *, bind_factory, before_provision, migration_prefixes):
        assert not bind_factory and before_provision is None
        migration_selections.append(migration_prefixes)
        yield component

    provision = AsyncMock()
    monkeypatch.setattr(source, "source_component_database", source_database)
    monkeypatch.setattr(support, "_provision_cms_epoch_controls", provision)
    monkeypatch.setattr(support, "Database", Mock(return_value=administrator))
    async with support.intake_admission_database(monkeypatch, migration_prefixes=prefixes) as database:
        assert database is administrator and database._source_component is component
    provision.assert_awaited_once_with(component)
    assert migration_selections == [source.legacy_source_component_migrations()]
    applied = migration_selections[0]
    assert applied[-2:] == prefixes[-2:] and applied.count("20261022120000") == 1
    assert {"20260930100000", "20260930110000", "20260930130000", "20260930140000"}.isdisjoint(applied)


@pytest.mark.asyncio
@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("exit_kind", ["normal", "body_error", "cancellation"])
async def test_intake_fixture_enrolls_original_calls_in_fresh_worker_context(monkeypatch, legacy, exit_kind):
    from process import provider_directory_cms_npd as cms
    from process import provider_directory_generic_source_policy as policy

    factory = SimpleNamespace(transaction=Mock())
    publisher = object()
    administrator = SimpleNamespace(_source_component=SimpleNamespace(factory=factory, database=publisher))
    arguments, marker = (object(), object()), object()
    observed_tasks = []

    async def original(*args, **kwargs):
        assert policy._FACTORY.get() is factory and support.fhir.db is publisher
        assert args == arguments and kwargs == {"marker": marker}
        observed_tasks.append(asyncio.current_task())
        return marker

    @asynccontextmanager
    async def fixture(bindings):
        bindings.setattr(support.fhir, "db", publisher)
        if legacy:
            async with policy.bind_generic_source_transaction_factory(factory):
                yield administrator
        else:
            yield administrator

    monkeypatch.setattr(cms, "_run_acquired", original)
    monkeypatch.setattr(support, "legacy_source_admission_database", fixture)
    monkeypatch.setattr(support, "protected_admission_database", fixture)
    failure = asyncio.CancelledError() if exit_kind == "cancellation" else RuntimeError("synthetic failure")
    expected = nullcontext() if exit_kind == "normal" else pytest.raises(type(failure))
    with expected:
        async with support.intake_admission_database(
            monkeypatch, migration_prefixes=support.LEGACY_MIGRATION_PREFIXES if legacy else None
        ) as actual:
            assert actual is administrator and policy._FACTORY.get() is factory
            assert await cms._run_acquired(*arguments, marker=marker) is marker
            worker = asyncio.create_task(cms._run_acquired(*arguments, marker=marker), context=Context())
            assert await worker is marker
            assert len(observed_tasks) == 2 and observed_tasks[0] is not observed_tasks[1]
            if exit_kind != "normal":
                raise failure
    assert cms._run_acquired is original and not policy.is_source_storage_required()


@pytest.mark.asyncio
@pytest.mark.parametrize("exit_kind", ["normal", "body_error", "cancellation", "disconnect_failure"])
async def test_admission_fixture_binds_control_database_and_restores_alias(monkeypatch, exit_kind):
    previous_control_database = control.db
    failure = asyncio.CancelledError() if exit_kind == "cancellation" else RuntimeError("synthetic failure")
    statement = object()
    update_result = SimpleNamespace(all=Mock(return_value=[("synthetic-run",)]))
    database = SimpleNamespace(
        _database_override="synthetic_fixture",
        _reader_binding=Mock(return_value=None),
        _transaction_binding=Mock(return_value=None),
        connect=AsyncMock(),
        disconnect=AsyncMock(side_effect=failure if exit_kind == "disconnect_failure" else None),
        execute=AsyncMock(return_value=update_result),
    )
    factory = Mock(return_value=database)

    @asynccontextmanager
    async def migrated_database_url(**kwargs):
        assert kwargs.keys() == {"migration_prefixes"}
        yield (
            SimpleNamespace(
                host="synthetic", port=1, username="synthetic", password=None, database="synthetic_fixture"
            ),
            True,
        )

    monkeypatch.setattr(support, "Database", factory)
    monkeypatch.setattr(support, "_admission_database_url", migrated_database_url)
    expected = nullcontext() if exit_kind == "normal" else pytest.raises(type(failure))
    with expected as caught:
        async with support.admission_database(monkeypatch) as yielded:
            assert control.db is yielded is database, "control alias must be the exact fixture database"
            assert support.fhir.db is database
            assert await control._execute_control_run_update(statement) == 1
            if exit_kind in {"body_error", "cancellation"}:
                raise failure
    if exit_kind != "normal":
        assert caught.value is failure
    assert control.db is previous_control_database
    factory.assert_called_once_with()
    database.execute.assert_awaited_once_with(statement)
    update_result.all.assert_called_once_with()
    database.disconnect.assert_awaited_once_with()
    assert database.connect.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_native_producer_progress_inherits_and_restores_claimed_attempt(monkeypatch, failure):
    from process.live_progress import current_live_progress_context
    from tests import cms_registry_native_producer_postgres_support as producer

    session = SimpleNamespace(execute=AsyncMock())

    @asynccontextmanager
    async def session_scope():
        yield session

    session.begin = session_scope
    factory = SimpleNamespace(_require_current_policy=Mock())
    database = SimpleNamespace(_source_component=SimpleNamespace(factory=factory), session_factory=session_scope)
    attempt = SimpleNamespace(
        run_id="synthetic-run",
        attempt_id="synthetic-run:" + "a" * 32,
        attempt_started_at="2026-10-08T10:00:00+00:00",
    )
    job = SimpleNamespace(
        source_attempt=attempt,
        retention_request=SimpleNamespace(policy=Mock(return_value={})),
        capture_id=object(),
        owner_role="synthetic_owner",
        runtime_roles=("synthetic_reader",),
    )
    monkeypatch.setattr(
        producer, "_paired_native_execution", AsyncMock(side_effect=lambda _patch, execution, **kw: execution)
    )
    monkeypatch.setattr(producer, "_issued_invocation", AsyncMock(return_value=object()))
    monkeypatch.setattr(producer.native, "observe_geometry", Mock())
    outer = current_live_progress_context()
    observed_attempts = []

    async def publish(*args, **kwargs):
        observed_attempts.append(
            control._control_attempt_for_run(
                kwargs["run_id"],
                attempt_id=None,
                attempt_started_at=None,
            )
        )
        assert observed_attempts[-1] == (attempt.attempt_id, attempt.attempt_started_at)
        if failure is not None:
            raise failure("synthetic publication")
        return "published"

    monkeypatch.setattr(producer.serving, "publish_current_attested_profile", publish)
    token = producer.source._FACTORY.set(factory)
    try:
        expected = nullcontext() if failure is None else pytest.raises(failure, match="synthetic publication")
        with expected:
            assert await producer.publish_native_execution(database, object(), monkeypatch, job=job) == "published"
        assert observed_attempts == [(attempt.attempt_id, attempt.attempt_started_at)]
        assert current_live_progress_context() == outer
    finally:
        producer.source._FACTORY.reset(token)
