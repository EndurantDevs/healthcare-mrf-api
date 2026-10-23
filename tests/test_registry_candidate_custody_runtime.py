"""Closed worker custody configuration and independent connection boundaries."""

import asyncio
import json
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest
from sqlalchemy.engine import make_url

from process import network_candidate_custody as foundation
from process import registry_cms_source_worker as cms
from process import registry_ptg_source_worker as ptg
from process import registry_publication_worker as publication
from process.network_membership_copy import MembershipCopyTarget
from tests.test_registry_cms_source_worker import configuration as cms_configuration
from tests.test_registry_ptg_source_worker import configuration as ptg_configuration
from tests.test_registry_publication_worker import configuration as publication_configuration

WORKERS = (
    (publication.parse_registry_publication_worker_config, publication_configuration),
    (cms.parse_registry_cms_source_worker_config, cms_configuration),
    (ptg.parse_registry_ptg_source_worker_config, ptg_configuration),
)


def _configured(factory):
    document = factory()
    url = make_url(document["execution_dsn"]).set(username="sealed_operator", password="p@ss")
    document["custody_dsn"] = url.render_as_string(hide_password=False)
    return document, url


@pytest.mark.parametrize("parser,factory", WORKERS)
def test_custody_is_optional_and_uses_a_distinct_private_connection(parser, factory):
    legacy = parser(json.dumps(factory()))
    assert legacy.custody_url is None
    document, url = _configured(factory)
    parsed = parser(json.dumps(document))
    assert parsed.custody_url.username == url.username
    assert "p@ss" not in repr(parsed)
    assert parsed.execution_url.username != parsed.custody_url.username


@pytest.mark.parametrize("parser,factory", WORKERS)
@pytest.mark.parametrize("damage", ("host", "port", "database", "tls", "execution", "owner", "null", "extra"))
def test_custody_refuses_endpoint_role_and_closed_document_drift(parser, factory, damage):
    document, url = _configured(factory)
    url_changes_by_name = {
        "host": {"host": "another.example.test"},
        "port": {"port": 55432},
        "database": {"database": "another_database"},
        "tls": {"query": {"ssl": "verify-full"}},
        "execution": {"username": document["consumer"]["execution_role"]},
        "owner": {"username": document["consumer"].get("owner_role", document["consumer"].get("physical_owner_role"))},
    }
    if damage in url_changes_by_name:
        url = url.set(**url_changes_by_name[damage])
    document["custody_dsn"] = None if damage == "null" else url.render_as_string(hide_password=False)
    if damage == "extra":
        document["custody_authorize"] = True
    with pytest.raises(ValueError, match="invalid"):
        parser(json.dumps(document))


@pytest.mark.parametrize("parser,factory", WORKERS)
def test_direct_config_cannot_supply_an_unparsed_custody_url(parser, factory):
    parsed = parser(json.dumps(factory()))
    with pytest.raises(ValueError, match="invalid"):
        replace(parsed, custody_url="postgresql://synthetic.example.test/not-a-typed-url")


def test_complete_runtime_role_census_keeps_both_maximum_role_lists():
    parsed = publication.parse_registry_publication_worker_config(json.dumps(publication_configuration()))
    consumer = replace(
        parsed.consumer,
        loader_roles=tuple(f"loader_{index}" for index in range(32)),
        reader_roles=tuple(f"reader_{index}" for index in range(32)),
    )
    roles = publication._custody_roles(replace(parsed, consumer=consumer))
    assert len(roles) == 68
    assert set(consumer.loader_roles + consumer.reader_roles) <= set(roles)
    assert consumer.physical_owner_role in roles
    assert consumer.queue_owner_role not in roles


def test_aca_native_owner_store_keeps_publisher_and_readers_in_custody_census():
    parsed = publication.parse_registry_publication_worker_config(json.dumps(publication_configuration()))
    from process.registry_aca_source_notifications import RegistryACASourceNotificationStore

    store = RegistryACASourceNotificationStore("registry_control", "aca_publisher", ("aca_reader",))
    config = SimpleNamespace(consumer=parsed.consumer, notification_store=store, ptg=None)
    roles = publication._custody_roles(config)
    assert {"aca_publisher", "aca_reader", parsed.consumer.physical_owner_role} <= set(roles)
    assert len(roles) == len(set(roles))


def test_bound_source_authority_roles_are_forbidden_without_expanding_loader_roles():
    parsed = publication.parse_registry_publication_worker_config(json.dumps(publication_configuration()))
    source_roles = ("source_owner", "source_publisher")
    config = SimpleNamespace(consumer=parsed.consumer, source_authority_roles=source_roles, ptg=None)
    roles = publication._custody_roles(config)
    assert set(source_roles) <= set(roles)
    assert not set(source_roles).intersection(parsed.consumer.loader_roles)
    assert not set(source_roles).intersection(parsed.consumer.reader_roles)


@pytest.mark.asyncio
@pytest.mark.parametrize("parser,factory", WORKERS)
async def test_absent_custody_runtime_preserves_consumer_without_connections_or_cleanup(monkeypatch, parser, factory):
    from contextlib import AsyncExitStack
    from unittest.mock import AsyncMock

    config = parser(json.dumps(factory()))
    create = AsyncMock(side_effect=AssertionError("unexpected pool"))
    identity = AsyncMock(side_effect=AssertionError("unexpected identity query"))
    monkeypatch.setattr(publication.asyncpg, "create_pool", create)
    monkeypatch.setattr(publication, "_custody_identity", identity)
    async with AsyncExitStack() as stack:
        assert await publication._custody_runtime(config, stack, config.consumer, object()) is config.consumer
        assert not stack._exit_callbacks
    create.assert_not_awaited()
    identity.assert_not_awaited()


class _CustodyConnection:
    def __init__(self, role, config, outcome, events, identity_by_name):
        self.config, self.outcome, self.events, self.identity_by_name = config, outcome, events, identity_by_name
        self.role, self.active, self.identity_reads = role, False, 0

    async def fetchrow(self, sql, *args):
        assert "safe_login" in sql and args == (self.config.consumer.control_schema,) * 2
        self.identity_reads += 1
        identity_record_by_name = dict(
            self.identity_by_name, safe_login=True, current_role=self.role, session_role=self.role
        )
        if self.role == self.config.custody_url.username and self.outcome == "identity":
            identity_record_by_name["database_oid"] += 1
        return identity_record_by_name

    @asynccontextmanager
    async def transaction(self, **options):
        assert not self.active
        assert options == dict(isolation="repeatable_read", readonly=True)
        self.active = True
        self.events.append("verify begin")
        try:
            yield self
        finally:
            self.active = False
            self.events.append("verify end")


class _CustodyPool:
    def __init__(self, name, connection, config, outcome, events):
        self.config, self.outcome, self.events = config, outcome, events
        self.name, self.connection, self.active, self.checkouts = name, connection, False, 0
        self.closes = 0

    @asynccontextmanager
    async def acquire(self, *, timeout):
        assert timeout == self.config.sql_limits.connect_timeout_seconds and not self.active
        self.checkouts += 1
        self.events.append(self.name + " acquire")
        if self.name == "custody" and self.outcome == "entry":
            raise RuntimeError("custody_entry_failed")
        self.active = True
        try:
            yield self.connection
        finally:
            self.active = False
            self.events.append(self.name + " release")

    async def close(self):
        assert not self.active
        self.closes += 1
        self.events.append(self.name + " close")

    def terminate(self):
        raise AssertionError("unexpected termination")


def _custody_pool_creator(config, case):
    async def create(**options):
        assert options == publication._pool_options(config.custody_url, 1, config.sql_limits)
        assert options["user"] != config.execution_url.username
        assert options["min_size"] == options["max_size"] == 1
        case.events.append("custody created")
        return case.custody_pool

    return create


def _custody_finalizer(config, case):
    async def finalize(connection, actual, *, control_schema, runtime_roles, authorize, pre_enrolled_only=False):
        assert connection is case.custodian and actual is case.copy_target and not case.custodian.active
        assert case.execution_pool.active and case.custody_pool.active
        assert pre_enrolled_only is case.pre_enrolled_only
        assert control_schema == config.consumer.control_schema
        assert runtime_roles == tuple(
            role for role in publication._custody_roles(config) if role != case.identity_by_name["custody_owner_role"]
        )
        if case.protected_store_owner:
            assert config.ptg.store.owner_role not in runtime_roles
            assert config.consumer.owner_role in runtime_roles
            assert config.notification_store.approval_role in runtime_roles
        assert await authorize(case.execution, case.copy_target) is False
        assert await authorize(case.custodian, object()) is False
        authorized = await authorize(case.custodian, case.copy_target)
        if case.outcome == "identity":
            assert authorized is False
            raise ValueError("custody_identity_changed")
        assert authorized is True
        case.events.append("finalize")
        if case.outcome == "seal failure":
            raise RuntimeError("custody_seal_failed")
        if case.outcome == "seal cancel":
            raise asyncio.CancelledError
        return case.sealed_by_name

    return finalize


def _custody_verifier(config, case):
    async def verify(connection, actual, *, control_schema, required):
        assert connection is case.custodian and actual is case.copy_target and case.custodian.active
        assert control_schema == config.consumer.control_schema and required is True
        assert case.events[-2:] == ["finalize", "verify begin"]
        case.events.append("verify")
        if case.outcome == "verify failure":
            raise RuntimeError("custody_verify_failed")
        if case.outcome == "verify cancel":
            raise asyncio.CancelledError
        if case.outcome == "mismatch":
            return {**case.sealed_by_name, "receipt": {**case.sealed_by_name["receipt"], "candidate_sha256": "c" * 64}}
        return dict(case.sealed_by_name)

    return verify


def _runtime_custody_case(monkeypatch, config, outcome, protected_store_owner):
    events = []
    identity_by_name = dict(
        database_name="example",
        database_oid=100,
        server_address="127.0.0.1",
        server_port=5432,
        server_started="2026-01-01",
        control_oid=200,
        inbox_oid=200,
        custody_owner_oid=300,
        custody_owner_role=config.ptg.store.owner_role if protected_store_owner else "protected_control_owner",
    )
    copy_target = MembershipCopyTarget(
        *(str(UUID(int=i)) for i in (1, 2, 3, 4)), "network_candidate_" + UUID(int=4).hex
    )

    execution = _CustodyConnection(config.consumer.execution_role, config, outcome, events, identity_by_name)
    custodian = _CustodyConnection(config.custody_url.username, config, outcome, events, identity_by_name)

    execution_pool, custody_pool = (
        _CustodyPool("execution", execution, config, outcome, events),
        _CustodyPool("custody", custodian, config, outcome, events),
    )

    case = SimpleNamespace(
        copy_target=copy_target,
        execution_pool=execution_pool,
        custody_pool=custody_pool,
        custodian=custodian,
        execution=execution,
        outcome=outcome,
        events=events,
        identity_by_name=identity_by_name,
        protected_store_owner=protected_store_owner,
    )
    create_pool = AsyncMock(side_effect=_custody_pool_creator(config, case))
    monkeypatch.setattr(publication.asyncpg, "create_pool", create_pool)
    sealed_by_name = {
        "seal_sha256": "a" * 64,
        "receipt": {"candidate_sha256": "b" * 64, "closure": {"owner_role": "final_owner"}},
    }

    case.sealed_by_name = sealed_by_name
    finalizer, verifier = (
        AsyncMock(side_effect=_custody_finalizer(config, case)),
        AsyncMock(side_effect=_custody_verifier(config, case)),
    )
    monkeypatch.setattr(foundation, "seal_network_candidate_custody", finalizer)
    monkeypatch.setattr(foundation, "verify_network_candidate_custody", verifier)
    completions = []

    return case, create_pool, finalizer, verifier, completions


async def _exercise_runtime_custody_callback(
    monkeypatch, parser, factory, outcome, *, protected_store_owner=False, pre_enrolled_only=False
):
    import asyncio
    from contextlib import AsyncExitStack

    document, _ = _configured(factory)
    config = parser(json.dumps(document))
    case, create_pool, finalizer, verifier, completions = _runtime_custody_case(
        monkeypatch, config, outcome, protected_store_owner
    )
    case.pre_enrolled_only = pre_enrolled_only
    execution_pool, custody_pool = case.execution_pool, case.custody_pool
    execution, copy_target, events = case.execution, case.copy_target, case.events

    async def operation():
        async with AsyncExitStack() as stack:
            async with execution_pool.acquire(timeout=config.sql_limits.connect_timeout_seconds) as original:
                consumer = await publication._custody_runtime(config, stack, config.consumer, original)
                assert consumer is not config.consumer and config.consumer.seal_candidate is None
                assert consumer.writer_roles == config.consumer.writer_roles
                await consumer.seal_candidate(copy_target, pre_enrolled_only=pre_enrolled_only)
                completions.append(True)

    if outcome == "complete":
        await operation()
        assert completions == [True] and finalizer.await_count == verifier.await_count == 1
    else:
        error = (
            asyncio.CancelledError
            if outcome.endswith("cancel")
            else ValueError
            if outcome in {"mismatch", "identity"}
            else RuntimeError
        )
        with pytest.raises(error):
            await operation()
        assert not completions
        assert verifier.await_count == int(outcome in {"verify failure", "verify cancel", "mismatch"})
        assert finalizer.await_count == int(outcome != "entry")
    assert create_pool.await_count == execution_pool.checkouts == custody_pool.checkouts == 1
    assert execution.identity_reads == 1 and custody_pool.closes == 1
    assert events.index("custody created") < events.index("custody acquire") < events.index("custody close")
    assert events[-1] == "custody close" and not execution_pool.active and not custody_pool.active


@pytest.mark.asyncio
@pytest.mark.parametrize("parser,factory", WORKERS)
@pytest.mark.parametrize("pre_enrolled_only", [False, True])
async def test_runtime_callback_uses_one_distinct_connection_and_checks_exact_final_receipt(
    monkeypatch, parser, factory, pre_enrolled_only
):
    await _exercise_runtime_custody_callback(
        monkeypatch, parser, factory, "complete", pre_enrolled_only=pre_enrolled_only
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "outcome", ("entry", "seal failure", "seal cancel", "verify failure", "verify cancel", "mismatch", "identity")
)
async def test_runtime_callback_failures_cancel_completion_and_close_registered_pool(monkeypatch, outcome):
    await _exercise_runtime_custody_callback(monkeypatch, WORKERS[0][0], WORKERS[0][1], outcome)


@pytest.mark.asyncio
async def test_native_protected_control_owner_can_also_own_the_pricing_authority_store(monkeypatch):
    await _exercise_runtime_custody_callback(
        monkeypatch, WORKERS[2][0], WORKERS[2][1], "complete", protected_store_owner=True
    )
