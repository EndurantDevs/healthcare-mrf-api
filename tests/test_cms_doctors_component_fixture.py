# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Owned fixture lifecycle checks; native database behavior is verified separately."""

import asyncio
import os
from contextlib import asynccontextmanager, nullcontext

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


def test_doctors_modules_register_the_shared_module_template():
    from tests import test_cms_doctors_organizations_postgres as groups
    from tests import test_provider_directory_entity_identity_postgres as identity

    for module in (groups, identity):
        assert module.doctors_component_template is doctors.doctors_component_template
        mark = module.pytestmark
        assert mark.name == "usefixtures" and mark.args == ("doctors_component_template",)
