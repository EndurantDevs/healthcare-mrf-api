# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owned fixture lifecycle checks; native database behavior is verified separately."""

import asyncio
import os
from contextlib import asynccontextmanager, nullcontext
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy.engine import make_url

from tests import cms_doctors_component_postgres_support as doctors
from tests import cms_npd_admission_postgres_support as admission


@pytest.mark.asyncio
@pytest.mark.parametrize("configured", [False, True])
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_component_template_keeps_anchor_until_cleanup(monkeypatch, configured, failure):
    incoming = make_url("postgresql+asyncpg://synthetic@localhost:5432/ptg2_v3_lifecycle_test_synthetic")
    if configured:
        incoming = incoming.set(database="hc_cms_admission_test_" + "a" * 32)
    owned = incoming.set(database="hc_cms_admission_test_" + "b" * 32)
    previous = incoming.render_as_string(hide_password=False) if configured else None
    monkeypatch.delenv(admission._DSN_ENV, raising=False)
    if configured:
        monkeypatch.setenv(admission._DSN_ENV, previous)
    for setting, configured_value in {
        "USER": "synthetic",
        "HOST": "localhost",
        "PORT": "5432",
        "PASSWORD": "",
        "DATABASE": incoming.database,
    }.items():
        monkeypatch.setenv("HLTHPRT_DB_" + setting, configured_value)
    events = []

    @asynccontextmanager
    async def database(actual):
        assert actual == incoming
        events.append("anchor_open")
        try:
            yield owned, object()
        finally:
            assert os.getenv(admission._DSN_ENV) == previous
            events.append("anchor_close")

    @asynccontextmanager
    async def templates():
        events.append("templates_open")
        try:
            yield
        finally:
            assert os.getenv(admission._DSN_ENV) == previous
            events.append("templates_close")

    monkeypatch.setattr(admission, "_owned_database", database)
    monkeypatch.setattr(doctors, "source_component_templates", templates)
    outcome = nullcontext() if failure is None else pytest.raises(failure, match="synthetic interruption")
    with outcome:
        async with doctors.doctors_component_scope():
            assert admission._database_url() == owned
            if failure is not None:
                raise failure("synthetic interruption")
    assert os.getenv(admission._DSN_ENV) == previous
    assert events == ["anchor_open", "templates_open", "templates_close", "anchor_close"]


@pytest.mark.asyncio
async def test_component_template_requires_an_explicit_disposable_database(monkeypatch):
    monkeypatch.delenv(admission._DSN_ENV, raising=False)
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "ordinary_database")

    def forbidden(*args):
        pytest.fail("unselected fixture reached database setup")

    monkeypatch.setattr(admission, "_owned_database", forbidden)
    with pytest.raises(pytest.skip.Exception, match="explicitly selected disposable"):
        async with doctors.doctors_component_scope():
            pytest.fail("unselected fixture yielded")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "host,ci,allowed",
    [
        ("postgres", None, False),
        ("postgres", "false", False),
        ("postgres", "true", True),
        ("remote.example.invalid", "true", False),
    ],
)
async def test_component_template_checks_host_before_database_setup(monkeypatch, host, ci, allowed):
    monkeypatch.delenv(admission._DSN_ENV, raising=False)
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", "ptg2_v3_lifecycle_test_synthetic")
    monkeypatch.setenv("HLTHPRT_DB_HOST", host)
    if ci is None:
        monkeypatch.delenv("CI", raising=False)
    else:
        monkeypatch.setenv("CI", ci)

    def reached_database(source_url):
        assert source_url.host == host
        raise RuntimeError("owned database reached")

    monkeypatch.setattr(admission, "_owned_database", reached_database)
    error = RuntimeError if allowed else pytest.fail.Exception
    message = "owned database reached" if allowed else "local PostgreSQL test host"
    with pytest.raises(error, match=message):
        async with doctors.doctors_component_scope():
            pytest.fail("fixture unexpectedly yielded")


def test_doctors_modules_register_the_shared_module_template():
    from tests import test_cms_doctors_organizations_postgres as groups
    from tests import test_provider_directory_entity_identity_postgres as identity

    for module in (groups, identity):
        assert module.doctors_component_template is doctors.doctors_component_template
        mark = module.pytestmark
        assert mark.name == "usefixtures" and mark.args == ("doctors_component_template",)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "phase,failure",
    [("body", None), ("body", RuntimeError), ("body", asyncio.CancelledError), ("cleanup", RuntimeError)],
)
async def test_staged_groups_uses_the_owning_component_cleanup(monkeypatch, phase, failure):
    from process import provider_directory_generic_source_policy as policy
    from tests import test_cms_doctors_organizations_postgres as groups

    events = []
    factory = SimpleNamespace(transaction=AsyncMock())

    @asynccontextmanager
    async def component(_monkeypatch):
        async with policy.bind_generic_source_transaction_factory(factory):
            events.append("open")
            try:
                yield SimpleNamespace(database=object())
            finally:
                events.extend(("readers_closed", "owned_database_removed"))
                if phase == "cleanup":
                    raise failure("synthetic interruption")

    staged = SimpleNamespace(ctx={}, schema="synthetic", import_date="synthetic")
    discard = AsyncMock(side_effect=AssertionError("producer cleanup preceded reader closure"))
    monkeypatch.setattr(groups, "_component", component)
    monkeypatch.setattr(groups, "_stage_rows", AsyncMock(return_value=staged))
    monkeypatch.setattr(groups.identities, "discard_doctors_identities", discard)
    generator = groups.staged_groups.__wrapped__(monkeypatch)
    assert await anext(generator) is staged
    assert policy._FACTORY.get() is factory
    if failure is None:
        await generator.aclose()
    elif phase == "cleanup":
        with pytest.raises(failure, match="synthetic interruption"):
            await generator.aclose()
    else:
        with pytest.raises(failure, match="synthetic interruption"):
            await generator.athrow(failure("synthetic interruption"))
    assert events == ["open", "readers_closed", "owned_database_removed"]
    assert policy._FACTORY.get() is None
    discard.assert_not_awaited()
