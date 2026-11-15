"""Configured named preparation keeps logical roles, validation callbacks and cleanup."""

import asyncio
import json
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest
from sqlalchemy.engine import make_url

from process import network_membership_publication as preparation
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
    url = make_url(document["execution_dsn"]).set(username="preparation_operator", password="p@ss")
    document["custody_dsn"] = url.render_as_string(hide_password=False)
    return document, url


def _profile(config):
    return {
        "namespace_owner_role": "generation_owner",
        "publisher_role": config.consumer.execution_role,
        "application_roles": list(publication._application_role_names(config)),
    }


@pytest.mark.parametrize("parser,factory", WORKERS)
def test_legacy_preparation_url_remains_optional_and_private(parser, factory):
    assert parser(json.dumps(factory())).custody_url is None
    document, url = _configured(factory)
    parsed = parser(json.dumps(document))
    assert parsed.custody_url == url.set(drivername="postgresql+asyncpg") and "p@ss" not in repr(parsed)


@pytest.mark.parametrize("parser,factory", WORKERS)
@pytest.mark.parametrize("damage", ["null", "extra", "invalid_url", "oversized"])
def test_legacy_preparation_url_keeps_closed_input_validation(parser, factory, damage):
    document, _ = _configured(factory)
    if damage == "extra":
        document["custody_authorize"] = True
    else:
        document["custody_dsn"] = {"null": None, "invalid_url": "not-a-database-url", "oversized": "x" * 4097}[damage]
    with pytest.raises(ValueError, match="invalid"):
        parser(json.dumps(document))


@pytest.mark.parametrize("parser,factory", WORKERS)
def test_direct_config_cannot_supply_an_unparsed_preparation_url(parser, factory):
    parsed = parser(json.dumps(factory()))
    with pytest.raises(ValueError, match="invalid"):
        replace(parsed, custody_url="postgresql://synthetic.example.test/not-a-typed-url")


def test_application_grants_keep_both_maximum_role_lists():
    parsed = publication.parse_registry_publication_worker_config(json.dumps(publication_configuration()))
    consumer = replace(
        parsed.consumer,
        loader_roles=tuple(f"loader_{i}" for i in range(32)),
        reader_roles=tuple(f"reader_{i}" for i in range(32)),
    )
    roles = publication._application_role_names(replace(parsed, consumer=consumer))
    assert len(roles) == 69 and set(consumer.loader_roles + consumer.reader_roles) <= set(roles)
    assert {consumer.physical_owner_role, consumer.queue_owner_role} <= set(roles)


@pytest.mark.parametrize(
    "field",
    [
        "lifecycle_role",
        "execution_role",
        "api_role",
        "physical_owner_role",
        "loader_roles",
        "reader_roles",
        "owner_role",
    ],
)
@pytest.mark.parametrize("alias", ["namespace_actor", "metadata_actor", "preparation_operator"])
def test_configured_and_effective_application_groups_are_both_preserved(field, alias):
    parser, factory = WORKERS[2] if field == "owner_role" else WORKERS[0]
    parsed = parser(json.dumps(factory()))
    value = (alias,) if field.endswith("roles") else alias
    original = replace(parsed.consumer, **{field: value})
    effective = replace(original, **{field: ("effective_actor",) if field.endswith("roles") else "effective_actor"})
    config = SimpleNamespace(**{**vars(parsed), "consumer": original})
    assert {alias, "effective_actor"} <= set(publication._application_role_names(config, effective))


@pytest.mark.parametrize("alias", ["namespace_actor", "metadata_actor", "preparation_operator"])
def test_source_authorities_and_approval_groups_are_not_lost(alias):
    parsed = ptg.parse_registry_ptg_source_worker_config(json.dumps(ptg_configuration()))
    from process.registry_aca_source_consumer import RegistryACASourceConsumerConfig

    original = RegistryACASourceConsumerConfig(
        parsed.consumer.control_schema,
        parsed.consumer.execution_role,
        parsed.consumer.owner_role,
        source_id="synthetic_source",
        producer_id="synthetic_producer",
        issuer_id=1,
        year=2026,
        source_authority_roles=("original_source_actor",),
    )
    effective = replace(original, source_authority_roles=("effective_source_actor",))
    config = SimpleNamespace(
        **{
            **vars(parsed),
            "consumer": original,
            "ptg": replace(parsed.ptg, store=replace(parsed.ptg.store, approval_role=alias)),
        }
    )
    assert {alias, "original_source_actor", "effective_source_actor"} <= set(
        publication._application_role_names(config, effective)
    )
    assert alias not in original.loader_roles and alias not in original.reader_roles


@pytest.mark.parametrize("field", ["owner_role", "publisher_role", "reader_roles"])
def test_notification_roles_remain_in_administrator_application_grants(field):
    parsed = cms.parse_registry_cms_source_worker_config(json.dumps(cms_configuration()))
    value = ("notification_actor",) if field == "reader_roles" else "notification_actor"
    config = SimpleNamespace(
        **{**vars(parsed), "notification_store": replace(parsed.notification_store, **{field: value})}
    )
    assert "notification_actor" in publication._application_role_names(config)


def test_application_role_bound_applies_to_complete_union():
    parsed = publication.parse_registry_publication_worker_config(json.dumps(publication_configuration()))
    ordinary_roles = set(publication._application_role_names(parsed))
    config = SimpleNamespace(
        **{**vars(parsed), "source_authority_roles": tuple(f"source_{i}" for i in range(128 - len(ordinary_roles)))}
    )
    assert len(publication._application_role_names(config)) == 128
    config.source_authority_roles += ("one_more_source_actor",)
    with pytest.raises(ValueError, match="invalid"):
        publication._application_role_names(config)


@pytest.mark.parametrize("parser,factory", WORKERS)
@pytest.mark.parametrize("damage", [None, "missing_actor", "publisher", "owner_overlap"])
def test_named_preparation_profile_covers_actual_application_groups(parser, factory, damage):
    parsed = parser(json.dumps(factory()))
    profile = _profile(parsed)
    if damage == "missing_actor":
        profile["application_roles"].remove(parsed.consumer.execution_role)
    elif damage == "publisher":
        profile["publisher_role"] = "different_publisher"
    elif damage == "owner_overlap":
        profile["application_roles"].append(profile["namespace_owner_role"])
    if damage is None:
        configured = replace(parsed, preparation_roles=profile)
        assert configured.preparation_roles == profile
    else:
        with pytest.raises(ValueError):
            replace(parsed, preparation_roles=profile)


def _runtime_pool(config, outcome, events, connection):
    @asynccontextmanager
    async def acquire(*, timeout):
        assert timeout == config.sql_limits.connect_timeout_seconds
        events.append("acquire")
        if outcome == "entry_error":
            raise RuntimeError("preparation_entry_failed")
        try:
            yield connection
        finally:
            events.append("release")

    async def close():
        events.append("close")

    pool = SimpleNamespace(acquire=acquire, close=AsyncMock(side_effect=close), terminate=Mock())
    create = AsyncMock(return_value=pool)
    return pool, create


@pytest.mark.asyncio
@pytest.mark.parametrize("parser,factory", WORKERS)
@pytest.mark.parametrize("outcome", ["validated", "waiting_admin_freeze", "entry_error", "validation_error", "cancel"])
async def test_configured_runtime_uses_named_validation_and_closes_registered_pool(
    monkeypatch, parser, factory, outcome
):
    parsed = parser(json.dumps(factory()))
    config = replace(parsed, preparation_roles=_profile(parsed))
    copy_target = MembershipCopyTarget(
        *(str(UUID(int=i)) for i in (1, 2, 3, 4)), "network_candidate_" + UUID(int=4).hex
    )
    events, connection = [], object()

    pool, create = _runtime_pool(config, outcome, events, connection)
    monkeypatch.setattr(publication.asyncpg, "create_pool", create)
    failure = (
        RuntimeError("validation_failed")
        if outcome == "validation_error"
        else asyncio.CancelledError()
        if outcome == "cancel"
        else None
    )
    validation_by_field = {"candidate_id": copy_target.candidate_id, "state": outcome}
    validate = AsyncMock(side_effect=failure, return_value=validation_by_field)
    monkeypatch.setattr(preparation, "validate_named_network_preparation", validate)
    async with AsyncExitStack() as stack:
        runtime = await publication._configured_worker_runtime(config, stack)
        assert runtime.preparation_roles == config.preparation_roles
        if outcome in {"entry_error", "validation_error", "cancel"}:
            expected = asyncio.CancelledError if outcome == "cancel" else RuntimeError
            with pytest.raises(expected):
                await runtime.seal_candidate(copy_target, pre_enrolled_only=True)
        else:
            assert await runtime.seal_candidate(copy_target) is validation_by_field
        if outcome == "entry_error":
            validate.assert_not_awaited()
        else:
            validate.assert_awaited_once_with(
                connection,
                copy_target,
                control_schema=config.consumer.control_schema,
                roles=config.preparation_roles,
                source_witness_runtime=None,
                validation_snapshot=runtime.composition_runtime.validation_snapshot
                if runtime.composition_runtime
                else None,
            )
        create.assert_awaited_once_with(**publication._pool_options(config.execution_url, 1, config.sql_limits))
        assert pool.close.await_count == 0
    pool.close.assert_awaited_once()
    pool.terminate.assert_not_called()
    assert events == (["acquire", "close"] if outcome == "entry_error" else ["acquire", "release", "close"])
