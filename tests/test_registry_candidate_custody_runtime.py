"""Closed worker custody configuration and independent connection boundaries."""

import asyncio
import json
from contextlib import asynccontextmanager, nullcontext
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


@pytest.mark.parametrize(
    "field", ("lifecycle_role", "execution_role", "api_role", "physical_owner_role", "loader_roles", "reader_roles")
)
@pytest.mark.parametrize("alias", ("schema_owner", "ledger_owner", "sealed_operator"))
def test_runtime_actor_aliases_survive_queue_aggregation_and_owner_filtering(field, alias):
    parsed = publication.parse_registry_publication_worker_config(json.dumps(_configured(publication_configuration)[0]))
    value = (alias,) if field.endswith("roles") else alias
    consumer = replace(parsed.consumer, queue_owner_role=alias, **{field: value})
    config = SimpleNamespace(**{**vars(parsed), "consumer": consumer})
    identity_by_name = dict(schema_owner_role="schema_owner", custody_owner_role="ledger_owner")
    assert alias in publication._custody_actor_roles(config)
    assert alias in publication._custody_runtime_roles(config, identity_by_name)
    with pytest.raises(ValueError, match="invalid") if alias == "sealed_operator" else nullcontext():
        publication._verify_custody_config(config)


@pytest.mark.parametrize(
    "field",
    (
        "lifecycle_role",
        "execution_role",
        "api_role",
        "physical_owner_role",
        "loader_roles",
        "reader_roles",
        "owner_role",
    ),
)
@pytest.mark.parametrize("alias", ("schema_owner", "ledger_owner", "sealed_operator"))
def test_original_actor_cannot_disappear_when_effective_consumer_changes(field, alias):
    parser, factory = WORKERS[2 if field == "owner_role" else 0]
    parsed = parser(json.dumps(_configured(factory)[0]))
    value = (alias,) if field.endswith("roles") else alias
    original = replace(parsed.consumer, **{field: value})
    if field != "owner_role":
        original = replace(original, queue_owner_role=alias)
    effective = replace(original, **{field: ("safe_actor",) if field.endswith("roles") else "safe_actor"})
    config = SimpleNamespace(**{**vars(parsed), "consumer": original})
    identity_by_name = dict(schema_owner_role="schema_owner", custody_owner_role="ledger_owner")
    actors = publication._custody_actor_roles(config, effective)
    assert {alias, "safe_actor"} <= set(actors)
    assert {alias, "safe_actor"} <= set(publication._custody_runtime_roles(config, identity_by_name, effective))
    if alias == "sealed_operator":
        with pytest.raises(ValueError, match="invalid"):
            publication._verify_custody_config(config, effective)


def test_source_physical_owner_and_bound_source_census_are_runtime_actors():
    from process.registry_ptg_producer_scope import RegistryPTGProducerScopeStore

    parsed = ptg.parse_registry_ptg_source_worker_config(json.dumps(_configured(ptg_configuration)[0]))
    store = RegistryPTGProducerScopeStore("schema_owner", "scope_approver", parsed.consumer.control_schema)
    source_roles = ("ledger_owner", "schema_owner", "source_publisher")
    consumer = replace(parsed.consumer, owner_role="ledger_owner")
    config = SimpleNamespace(
        **{
            **vars(parsed),
            "consumer": consumer,
            "source_authority_roles": source_roles,
            "ptg": publication._PTGWorkerConfig(store, parsed.ptg.read_budget),
            "notification_store": store,
        }
    )
    actors = publication._custody_actor_roles(config)
    roles = publication._custody_runtime_roles(
        config, dict(schema_owner_role="schema_owner", custody_owner_role="ledger_owner")
    )
    assert set(source_roles) | {consumer.owner_role, "scope_approver", parsed.reader_role} <= set(actors)
    assert set(actors) <= set(roles)


@pytest.mark.parametrize("alias", ("schema_owner", "ledger_owner", "sealed_operator"))
def test_original_and_effective_aca_source_authorities_both_remain_actors(alias):
    from process.registry_aca_source_consumer import RegistryACASourceConsumerConfig

    parsed = publication.parse_registry_publication_worker_config(json.dumps(_configured(publication_configuration)[0]))
    original = RegistryACASourceConsumerConfig(
        parsed.consumer.control_schema,
        parsed.consumer.execution_role,
        parsed.consumer.physical_owner_role,
        source_id="synthetic_source",
        producer_id="synthetic_producer",
        issuer_id=1,
        year=2026,
        source_authority_roles=("original_source_actor",),
    )
    effective = replace(original, source_authority_roles=(alias,))
    config = SimpleNamespace(
        **{**vars(parsed), "consumer": original, "source_authority_roles": original.source_authority_roles}
    )
    identity_by_name = dict(schema_owner_role="schema_owner", custody_owner_role="ledger_owner")
    expected_roles = {"original_source_actor", alias}
    assert expected_roles <= set(publication._custody_actor_roles(config, effective))
    assert expected_roles <= set(publication._custody_runtime_roles(config, identity_by_name, effective))
    if alias == "sealed_operator":
        with pytest.raises(ValueError, match="invalid"):
            publication._verify_custody_config(config, effective)


@pytest.mark.parametrize("owner", ("schema_owner", "ledger_owner", "unmatched_owner"))
def test_only_native_bound_metadata_owners_are_omitted(owner):
    parsed = ptg.parse_registry_ptg_source_worker_config(json.dumps(_configured(ptg_configuration)[0]))
    store = replace(parsed.ptg.store, owner_role=owner)
    config = SimpleNamespace(**{**vars(parsed), "ptg": replace(parsed.ptg, store=store), "notification_store": store})
    identity_by_name = dict(schema_owner_role="schema_owner", custody_owner_role="ledger_owner")
    assert owner not in publication._custody_actor_roles(config)
    assert (owner in publication._custody_runtime_roles(config, identity_by_name)) is (owner == "unmatched_owner")


def test_runtime_role_bound_is_applied_to_the_complete_actor_union():
    parsed = publication.parse_registry_publication_worker_config(json.dumps(publication_configuration()))
    ordinary_roles = set(publication._custody_actor_roles(parsed))
    config = SimpleNamespace(
        **{**vars(parsed), "source_authority_roles": tuple(f"source_{i}" for i in range(128 - len(ordinary_roles)))}
    )
    identity_by_name = dict(schema_owner_role="schema_owner", custody_owner_role="ledger_owner")
    assert len(publication._custody_runtime_roles(config, identity_by_name)) == 128
    config.source_authority_roles += ("one_more_source_actor",)
    with pytest.raises(ValueError, match="invalid"):
        publication._custody_runtime_roles(config, identity_by_name)


@pytest.mark.parametrize(
    "actor_field",
    ("publisher_role", "reader_roles", "approval_role", "reader_role", "ptg_approval", "source_authority_roles"),
)
@pytest.mark.parametrize("alias", ("schema_owner", "ledger_owner", "sealed_operator"))
def test_notification_and_source_actor_aliases_cannot_be_metadata_exempted(actor_field, alias):
    parser, factory = WORKERS[1 if actor_field in {"publisher_role", "reader_roles"} else 2]
    parsed = parser(json.dumps(_configured(factory)[0]))
    config = SimpleNamespace(**vars(parsed))
    if actor_field in {"publisher_role", "reader_roles", "approval_role"}:
        config.notification_store = replace(
            parsed.notification_store, **{actor_field: (alias,) if actor_field == "reader_roles" else alias}
        )
    elif actor_field == "ptg_approval":
        config.ptg = replace(parsed.ptg, store=replace(parsed.ptg.store, approval_role=alias))
    else:
        setattr(config, actor_field, (alias,) if actor_field == "source_authority_roles" else alias)
    identity_by_name = dict(schema_owner_role="schema_owner", custody_owner_role="ledger_owner")
    assert alias in publication._custody_actor_roles(config)
    assert alias in publication._custody_runtime_roles(config, identity_by_name)
    if alias == "sealed_operator":
        with pytest.raises(ValueError, match="invalid"):
            publication._verify_custody_config(config)


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ("schema_owner", "ledger_owner", "sealed_operator"))
async def test_effective_composition_approval_actor_is_not_lost(monkeypatch, alias):
    from contextlib import AsyncExitStack

    config = ptg.parse_registry_ptg_source_worker_config(json.dumps(_configured(ptg_configuration)[0]))
    async with AsyncExitStack() as stack:
        consumer = publication._ptg_runtime(config, stack)
        runtime = replace(consumer.composition_runtime, store=replace(config.ptg.store, approval_role=alias))
        consumer = replace(consumer, composition_runtime=runtime)
        identity_by_name = dict(schema_owner_role="schema_owner", custody_owner_role="ledger_owner")
        assert alias not in publication._custody_roles(config)
        assert alias in publication._custody_actor_roles(config, consumer)
        assert alias in publication._custody_runtime_roles(config, identity_by_name, consumer)
        if alias == "sealed_operator":
            case, create, finalizer, verifier, _ = _runtime_custody_case(monkeypatch, config, "complete", False)
            with pytest.raises(ValueError, match="invalid"):
                await publication._custody_runtime(config, stack, consumer, case.execution)
            create.assert_not_awaited()
            finalizer.assert_not_awaited()
            verifier.assert_not_awaited()


@pytest.mark.asyncio
async def test_effective_composition_metadata_owner_cannot_alias_custodian(monkeypatch):
    from contextlib import AsyncExitStack

    config = ptg.parse_registry_ptg_source_worker_config(json.dumps(_configured(ptg_configuration)[0]))
    async with AsyncExitStack() as stack:
        consumer = publication._ptg_runtime(config, stack)
        runtime = replace(consumer.composition_runtime, store=replace(config.ptg.store, owner_role="sealed_operator"))
        consumer = replace(consumer, composition_runtime=runtime)
        assert "sealed_operator" not in publication._custody_actor_roles(config, consumer)
        case, create, finalizer, verifier, _ = _runtime_custody_case(monkeypatch, config, "complete", False)
        with pytest.raises(ValueError, match="invalid"):
            await publication._custody_runtime(config, stack, consumer, case.execution)
        create.assert_not_awaited()
        finalizer.assert_not_awaited()
        verifier.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("alias", ("schema_owner", "ledger_owner", "sealed_operator"))
async def test_original_composition_approval_actor_survives_replaced_runtime(monkeypatch, alias):
    from contextlib import AsyncExitStack

    parsed = ptg.parse_registry_ptg_source_worker_config(json.dumps(_configured(ptg_configuration)[0]))
    async with AsyncExitStack() as stack:
        effective = publication._ptg_runtime(parsed, stack)
        original_runtime = replace(effective.composition_runtime, store=replace(parsed.ptg.store, approval_role=alias))
        original = replace(parsed.consumer, composition_runtime=original_runtime)
        config = SimpleNamespace(**{**vars(parsed), "consumer": original})
        identity_by_name = dict(schema_owner_role="schema_owner", custody_owner_role="ledger_owner")
        assert alias not in publication._custody_roles(config)
        assert {alias, parsed.ptg.store.approval_role} <= set(publication._custody_actor_roles(config, effective))
        assert alias in publication._custody_runtime_roles(config, identity_by_name, effective)
        if alias == "sealed_operator":
            case, create, finalizer, verifier, _ = _runtime_custody_case(monkeypatch, config, "complete", False)
            with pytest.raises(ValueError, match="invalid"):
                await publication._custody_runtime(config, stack, effective, case.execution)
            create.assert_not_awaited()
            finalizer.assert_not_awaited()
            verifier.assert_not_awaited()


@pytest.mark.asyncio
async def test_original_composition_metadata_owner_remains_custodian_forbidden(monkeypatch):
    from contextlib import AsyncExitStack

    parsed = ptg.parse_registry_ptg_source_worker_config(json.dumps(_configured(ptg_configuration)[0]))
    async with AsyncExitStack() as stack:
        effective = publication._ptg_runtime(parsed, stack)
        original_runtime = replace(
            effective.composition_runtime, store=replace(parsed.ptg.store, owner_role="sealed_operator")
        )
        config = SimpleNamespace(
            **{**vars(parsed), "consumer": replace(parsed.consumer, composition_runtime=original_runtime)}
        )
        assert "sealed_operator" not in publication._custody_roles(config)
        assert "sealed_operator" not in publication._custody_actor_roles(config, effective)
        case, create, finalizer, verifier, _ = _runtime_custody_case(monkeypatch, config, "complete", False)
        with pytest.raises(ValueError, match="invalid"):
            await publication._custody_runtime(config, stack, effective, case.execution)
        create.assert_not_awaited()
        finalizer.assert_not_awaited()
        verifier.assert_not_awaited()


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
        self.catalog_roles = set(publication._custody_roles(config)) | {
            identity_by_name["schema_owner_role"],
            identity_by_name["custody_owner_role"],
            role,
        }
        self.q_reaching_roles = set()

    async def fetchval(self, sql, *args):
        assert sql == foundation._Q_REACH_SQL
        names, owner_oid = args
        assert type(names) is list and owner_oid == self.identity_by_name["schema_owner_oid"]
        self.events.append("q reach")
        return all(
            name in self.catalog_roles
            and name != self.identity_by_name["schema_owner_role"]
            and name not in self.q_reaching_roles
            for name in names
        )

    async def fetchrow(self, sql, *args):
        assert "safe_login" in sql and args == (self.config.consumer.control_schema,) * 2
        self.identity_reads += 1
        identity_record_by_name = dict(
            self.identity_by_name,
            safe_login=True,
            current_role=self.role,
            session_role=self.role,
            can_assume_schema_owner=self.identity_by_name["schema_owner_oid"]
            == self.identity_by_name["custody_owner_oid"],
        )
        if self.role == self.config.custody_url.username and self.outcome == "identity":
            identity_record_by_name["database_oid"] += 1
        if self.role == self.config.custody_url.username and self.outcome in {
            "assume_schema_owner",
            "inherit_schema_owner",
            "member_schema_owner",
        }:
            identity_record_by_name["can_assume_schema_owner"] = True
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
    async def finalize(
        connection, actual, *, control_schema, runtime_roles, authorize, pre_enrolled_only=False, **office
    ):
        if config.ptg is not None:
            assert connection.driver is case.custodian and case.custodian.active
            assert office == {"office_store": config.ptg.store, "office_read_budget": config.ptg.read_budget}
            connection = connection.driver
        else:
            assert not case.custodian.active and not office
        assert connection is case.custodian and actual is case.copy_target
        assert case.execution_pool.active and case.custody_pool.active
        assert pre_enrolled_only is case.pre_enrolled_only
        assert control_schema == config.consumer.control_schema
        assert runtime_roles == publication._custody_runtime_roles(config, case.identity_by_name)
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
        assert case.events[-2:] == ["custody release" if config.ptg is not None else "finalize", "verify begin"]
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
    """Build real runtime callbacks around isolated native custody and preserve exact cleanup."""
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
        schema_owner_oid=300,
        schema_owner_role=config.ptg.store.owner_role if protected_store_owner else "protected_control_owner",
        candidate_table_oid=301,
        candidate_owner_oid=300,
        seal_table_oid=302,
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
    _install_custody_company_fence(monkeypatch, config, case, finalizer)
    monkeypatch.setattr(foundation, "verify_network_candidate_custody", verifier)
    completions = []

    return case, create_pool, finalizer, verifier, completions


async def _exercise_runtime_custody_callback(
    monkeypatch, parser, factory, outcome, *, protected_store_owner=False, pre_enrolled_only=False, split_owner=False
):
    import asyncio
    from contextlib import AsyncExitStack

    document, _ = _configured(factory)
    config = parser(json.dumps(document))
    case, create_pool, finalizer, verifier, completions = _runtime_custody_case(
        monkeypatch, config, outcome, protected_store_owner
    )
    if split_owner:
        schema_owner = config.ptg.store.owner_role if protected_store_owner else "schema_owner"
        case.identity_by_name.update(schema_owner_oid=400, schema_owner_role=schema_owner, candidate_owner_oid=400)
        case.execution.catalog_roles.add(schema_owner)
        case.custodian.catalog_roles.add(schema_owner)
        if protected_store_owner:
            case.identity_by_name["custody_owner_role"] = "ledger_owner"
            case.execution.catalog_roles.add("ledger_owner")
            case.custodian.catalog_roles.add("ledger_owner")
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
            if outcome in {"mismatch", "identity", "assume_schema_owner", "inherit_schema_owner", "member_schema_owner"}
            else RuntimeError
        )
        with pytest.raises(error):
            await operation()
        assert not completions
        assert verifier.await_count == int(outcome in {"verify failure", "verify cancel", "mismatch"})
        assert finalizer.await_count == int(outcome != "entry")
    assert create_pool.await_count == int(config.ptg is None)
    assert execution_pool.checkouts == custody_pool.checkouts == 1
    assert execution.identity_reads == 1 and custody_pool.closes == 1
    assert events.index("custody created") < events.index("custody acquire") < events.index("custody close")
    assert events.index("q reach") < events.index("custody created")
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
@pytest.mark.parametrize("parser,factory", (WORKERS[0], WORKERS[2]))
@pytest.mark.parametrize(
    "outcome", ("entry", "seal failure", "seal cancel", "verify failure", "verify cancel", "mismatch", "identity")
)
async def test_runtime_callback_failures_cancel_completion_and_close_registered_pool(
    monkeypatch, parser, factory, outcome
):
    await _exercise_runtime_custody_callback(monkeypatch, parser, factory, outcome)


@pytest.mark.asyncio
async def test_native_protected_control_owner_can_also_own_the_pricing_authority_store(monkeypatch):
    await _exercise_runtime_custody_callback(
        monkeypatch, WORKERS[2][0], WORKERS[2][1], "complete", protected_store_owner=True, split_owner=True
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("parser,factory", WORKERS)
async def test_split_runtime_binds_both_owners_without_assuming_schema_owner(monkeypatch, parser, factory):
    await _exercise_runtime_custody_callback(monkeypatch, parser, factory, "complete", split_owner=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["assume_schema_owner", "inherit_schema_owner", "member_schema_owner"])
async def test_split_runtime_refuses_custodian_that_can_assume_schema_owner(monkeypatch, outcome):
    await _exercise_runtime_custody_callback(monkeypatch, WORKERS[0][0], WORKERS[0][1], outcome, split_owner=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("reach", ("self", "member", "missing", "original_actor"))
async def test_complete_actor_q_refusal_precedes_custodian_pool_creation(monkeypatch, reach):
    from contextlib import AsyncExitStack

    parsed = publication.parse_registry_publication_worker_config(json.dumps(_configured(publication_configuration)[0]))
    actor = parsed.consumer.queue_owner_role if reach in {"self", "original_actor"} else parsed.consumer.api_role
    consumer = replace(parsed.consumer, api_role=actor)
    config = SimpleNamespace(**{**vars(parsed), "consumer": consumer})
    case, create, finalizer, verifier, completions = _runtime_custody_case(monkeypatch, config, "complete", False)
    case.identity_by_name.update(
        schema_owner_oid=400, schema_owner_role=parsed.consumer.queue_owner_role, candidate_owner_oid=400
    )
    case.execution.catalog_roles.add(parsed.consumer.queue_owner_role)
    if reach == "original_actor":
        consumer = replace(consumer, api_role="safe_api")
        case.execution.catalog_roles.add("safe_api")
    if reach == "member":
        case.execution.q_reaching_roles.add(actor)
    elif reach == "missing":
        case.execution.catalog_roles.remove(actor)
    async with AsyncExitStack() as stack:
        async with case.execution_pool.acquire(timeout=config.sql_limits.connect_timeout_seconds) as original:
            with pytest.raises(foundation.NetworkCandidateCustodyError):
                await publication._custody_runtime(config, stack, consumer, original)
        assert not stack._exit_callbacks
    create.assert_not_awaited()
    finalizer.assert_not_awaited()
    verifier.assert_not_awaited()
    assert not completions and case.custody_pool.closes == case.custody_pool.checkouts == 0
    assert case.execution.identity_reads == 1
    assert case.events == ["execution acquire", "q reach", "execution release"]


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["candidate_owner_oid", "safe_login", "session_role", "current_role"])
async def test_runtime_identity_refuses_changed_native_owner_or_original_login(drift):
    identity_by_name = dict(
        safe_login=True,
        current_role="custodian",
        session_role="custodian",
        control_oid=10,
        schema_owner_oid=11,
        schema_owner_role="schema_owner",
        custody_owner_oid=12,
        custody_owner_role="custody_owner",
        candidate_owner_oid=11,
        candidate_table_oid=13,
        seal_table_oid=14,
        can_assume_schema_owner=False,
    )
    identity_by_name[drift] = (
        99 if drift == "candidate_owner_oid" else False if drift == "safe_login" else "another_role"
    )
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value=identity_by_name))
    with pytest.raises(ValueError, match="connection_invalid"):
        await publication._custody_identity(connection, "control", "custodian")
    sql = connection.fetchrow.await_args.args[0]
    assert "owner.oid=ledger.relowner" in sql
    for privilege in ("MEMBER", "USAGE", "SET"):
        assert f"pg_has_role(current_user,n.nspowner,'{privilege}')" in sql


def _install_custody_company_fence(monkeypatch, config, case, finalizer):
    """Use the same independent custodian pool for the company-fenced test sessions."""
    custodian, custody_pool, events = case.custodian, case.custody_pool, case.events
    if config.ptg is not None:
        from process import registry_company_approval_fence as fence

        class Session:
            driver = custodian

            async def __aenter__(self):
                return self

            async def __aexit__(self, *args):
                return None

            def begin(self):
                return custodian.transaction(isolation="repeatable_read", readonly=True)

            async def connection(self, **options):
                return self

            async def execute(self, statement):
                assert str(statement) == "SET TRANSACTION READ ONLY"

            async def get_raw_connection(self):
                return SimpleNamespace(driver_connection=custodian)

        sessions = Session

        def owned_sessions(actual, stack, url, *, backend_verifier=None):
            assert backend_verifier is None
            assert actual is config and url == config.custody_url and url != config.execution_url
            events.append("custody created")
            stack.push_async_callback(custody_pool.close)
            return sessions

        @asynccontextmanager
        async def transaction(actual, *, control_schema):
            assert actual is sessions and control_schema == config.consumer.control_schema
            async with custody_pool.acquire(timeout=config.sql_limits.connect_timeout_seconds):
                custodian.active = True
                try:
                    yield Session()
                finally:
                    custodian.active = False

        monkeypatch.setattr(publication, "_worker_sessions", owned_sessions)
        monkeypatch.setattr(fence, "registry_company_approval_transaction", transaction)
        monkeypatch.setattr(foundation, "seal_network_candidate_custody_in_session", finalizer)
