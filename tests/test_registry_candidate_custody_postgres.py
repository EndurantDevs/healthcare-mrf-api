# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Configured fixture grants and the real named preparation handoff controller."""

from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from db.registry_aca_source_notification import INSERT_COLUMNS, TABLE
from process.network_membership_publication import NetworkPreparationPending
from process.registry_cms_source_consumer import RegistryCMSSourceConsumerConfig
from tests import registry_candidate_custody_postgres_support as support


class PermissionConnection:
    """Track actual permission operations without opening a database connection."""

    def __init__(self):
        self.owner_by_relation = {
            "network_serving_control": "migration",
            "network_membership_candidate": "migration",
            "network_candidate_custody": "migration",
            TABLE: "migration",
        }
        self.schema_owner = "migration"
        self.execute = AsyncMock(side_effect=self._execute)

    @asynccontextmanager
    async def transaction(self):
        yield

    async def _execute(self, statement):
        if statement.startswith('ALTER SCHEMA "control" OWNER TO '):
            self.schema_owner = statement.rsplit('"', 2)[1]
        for name in self.owner_by_relation:
            if statement.startswith(f'ALTER TABLE "control"."{name}" OWNER TO '):
                self.owner_by_relation[name] = statement.rsplit('"', 2)[1]

    async def fetchrow(self, statement, *parameters):
        assert parameters == ("control", TABLE)
        return dict(relname=TABLE, relkind="r", relpersistence="p", columns=(*INSERT_COLUMNS, "created_at"))

    async def fetchval(self, statement, *parameters):
        assert "current_database" in statement and not parameters
        return "fixture_database"


@pytest.mark.asyncio
async def test_fixture_preserves_named_controls_and_grants_preparation_without_frozen_owner_membership(monkeypatch):
    connection = PermissionConnection()
    fixture = SimpleNamespace(
        connection=connection,
        control_schema="control",
        roles={"owner": "control_owner", "reader": "reader"},
        custody_roles={"loading": "loading", "owner": "frozen_owner", "execution": "execution", "custodian": "helper"},
    )
    tables = [{"relname": name, "columns": ["id"]} for name in connection.owner_by_relation if name != TABLE]
    catalog = AsyncMock(return_value=(tables, []))
    monkeypatch.setattr(support, "_catalog", catalog)
    grant = AsyncMock()
    monkeypatch.setattr(support, "_grant_worker", grant)
    original_by_field = dict(rolname="migration")

    await support._install_grants(fixture, original_by_field)
    assert set(connection.owner_by_relation.values()) == {"control_owner"}
    catalog.assert_awaited_once_with(connection, "control")
    assert grant.await_args.args[2] == tables
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert 'GRANT SELECT,INSERT,UPDATE ON hp_snapshot_retention.preparation TO "execution"' in statements
    assert not any(statement.startswith('GRANT "frozen_owner" TO') for statement in statements)
    await support._restore_control_owner(fixture, original_by_field)
    assert connection.schema_owner == "migration"
    assert {
        name: connection.owner_by_relation[name]
        for name in (TABLE, "network_candidate_custody", "network_membership_candidate")
    } == {TABLE: "migration", "network_candidate_custody": "migration", "network_membership_candidate": "migration"}
    assert connection.owner_by_relation["network_serving_control"] == "control_owner"
    assert not any("DROP" in call.args[0] for call in connection.execute.await_args_list)


class AdminEngine:
    def __init__(self, events):
        self.events = events
        self.connection = object()

    @asynccontextmanager
    async def connect(self):
        self.events.append("admin_open")
        try:
            yield self.connection
        finally:
            self.events.append("admin_closed")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["mismatched_pending", "missing_ready"])
async def test_named_fixture_refuses_unmatched_pending_or_missing_ready_before_admin(monkeypatch, failure):
    import directory_catalog_runtime as runtime

    identity = str(uuid4())
    callback = AsyncMock(
        side_effect=NetworkPreparationPending(str(uuid4()) if failure == "mismatched_pending" else identity)
    )
    ready = AsyncMock(return_value=None)
    monkeypatch.setattr(support, "load_named_network_preparation", ready)
    administrative_freeze = AsyncMock()
    monkeypatch.setattr(runtime, "freeze_registry_generation", administrative_freeze)
    fixture = SimpleNamespace(connection=object(), control_schema="control")
    consumer = SimpleNamespace(seal_candidate=callback, preparation_roles={})
    controlled = support.administrative_freeze_callback(fixture, consumer)
    with pytest.raises(AssertionError):
        await controlled(SimpleNamespace(candidate_id=identity))
    administrative_freeze.assert_not_awaited()
    assert callback.await_count == 1
    if failure == "mismatched_pending":
        ready.assert_not_awaited()
    else:
        ready.assert_awaited_once_with(fixture.connection, identity, control_schema="control")


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, "admin_failure", "resume_failure"])
async def test_named_fixture_freezes_exact_ready_candidate_and_resumes_original_callback(monkeypatch, failure):
    import directory_catalog_runtime as runtime

    events = []
    identity = str(uuid4())
    copy_target = SimpleNamespace(candidate_id=identity)
    engine = AdminEngine(events)
    fixture = SimpleNamespace(connection=object(), control_schema="control", engine=engine)
    roles_by_field = {
        "namespace_owner_role": "frozen_owner",
        "publisher_role": "execution",
        "application_roles": ["execution", "builder", "reader"],
    }
    calls = []

    async def callback(supplied, **options):
        calls.append((supplied, options))
        events.append("callback")
        if len(calls) == 1:
            raise NetworkPreparationPending(identity)
        if failure == "resume_failure":
            raise RuntimeError("data_validation_failed")
        return {"state": "validated", "candidate_id": identity}

    handoff_by_field = {
        "kind": "network",
        "candidate_id": identity,
        "catalog_schema": "control",
        "storage_schema": "candidate_storage",
    }
    ready = AsyncMock(return_value=handoff_by_field)
    monkeypatch.setattr(support, "load_named_network_preparation", ready)

    async def freeze(connection, config, preparation_id):
        assert connection is engine.connection
        assert config == {**handoff_by_field, "preparation_roles": roles_by_field} and preparation_id == identity
        events.append("freeze")
        if failure == "admin_failure":
            raise RuntimeError("admin_failed")
        return {"preparation_id": identity, "state": "frozen"}

    administrative_freeze = AsyncMock(side_effect=freeze)
    monkeypatch.setattr(runtime, "freeze_registry_generation", administrative_freeze)
    consumer = SimpleNamespace(seal_candidate=callback, preparation_roles=roles_by_field)
    controlled = support.administrative_freeze_callback(fixture, consumer)
    if failure in {"admin_failure", "resume_failure"}:
        with pytest.raises(
            RuntimeError, match="admin_failed" if failure == "admin_failure" else "data_validation_failed"
        ):
            await controlled(copy_target, pre_enrolled_only=True)
        assert len(calls) == (1 if failure == "admin_failure" else 2)
        assert events[-1] == ("admin_closed" if failure == "admin_failure" else "callback")
    else:
        assert await controlled(copy_target, pre_enrolled_only=True) == {"state": "validated", "candidate_id": identity}
        assert events == ["callback", "admin_open", "freeze", "admin_closed", "callback"]
        assert calls == [(copy_target, {"pre_enrolled_only": True})] * 2
    ready.assert_awaited_once_with(fixture.connection, identity, control_schema="control")


@pytest.mark.asyncio
async def test_named_fixture_preserves_application_result_without_admin_on_replay(monkeypatch):
    callback = AsyncMock(return_value={"state": "published"})
    ready = AsyncMock()
    monkeypatch.setattr(support, "load_named_network_preparation", ready)
    consumer = SimpleNamespace(seal_candidate=callback, preparation_roles={})
    target = SimpleNamespace(candidate_id=str(uuid4()))
    controlled = support.administrative_freeze_callback(SimpleNamespace(), consumer)
    assert await controlled(target) == {"state": "published"}
    callback.assert_awaited_once_with(target)
    ready.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_fixture_uses_configured_worker_with_complete_finite_application_roles(monkeypatch):
    from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
    from process.network_membership_publication import named_network_preparation_roles

    coordinates = RegistryNetworkSourceCoordinates("fhir", "cms-npd", "source", "dataset", "producer", "edition")
    fixture = SimpleNamespace(
        connection=SimpleNamespace(execute=AsyncMock()),
        control_schema="control",
        executor_role="execution",
        custody_roles={"owner": "frozen_owner", "loading": "builder", "custodian": "legacy_reader"},
        roles={"owner": "metadata_owner", "reader": "reader", "outsider": "api_reader"},
        execution_url=object(),
        sql_limits=object(),
        custody_stack=object(),
    )
    pair = SimpleNamespace(recipe=SimpleNamespace(binding_coordinates=coordinates), runtime_roles=("source_reader",))
    old = SimpleNamespace(
        recipe=SimpleNamespace(binding_coordinates=replace(coordinates, dataset_id="old", edition_id="old")),
        runtime_roles=("retained_source_reader",),
    )
    store = SimpleNamespace(
        owner_role="notification_owner", publisher_role="source_publisher", reader_roles=("notice_reader",)
    )
    configurations = []

    async def runtime(config, stack):
        assert stack is fixture.custody_stack and type(config.consumer) is RegistryCMSSourceConsumerConfig
        configurations.append(config)
        roles = named_network_preparation_roles(config.preparation_roles)
        return replace(config.consumer, preparation_roles=roles, seal_candidate=AsyncMock())

    monkeypatch.setattr(support, "_configured_worker_runtime", runtime)
    consumer = await support.candidate_custody_consumer(fixture, pair, old, store)
    assert consumer.prepare_address_sources is None and consumer.seal_candidate is fixture.seal_candidate
    assert len(configurations) == 1
    roles = consumer.preparation_roles
    assert roles["namespace_owner_role"] == "frozen_owner" and roles["publisher_role"] == "execution"
    assert set(roles["application_roles"]) == {
        "execution",
        "builder",
        "reader",
        "api_reader",
        "legacy_reader",
        "metadata_owner",
        "source_reader",
        "retained_source_reader",
        "notification_owner",
        "source_publisher",
        "notice_reader",
    }
    statements = [call.args[0] for call in fixture.connection.execute.await_args_list]
    assert len(statements) == 2 and all('TO "execution"' in statement for statement in statements)
