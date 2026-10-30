# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Disposable parent selection preserves the independent UUID child guard."""

import os
from contextlib import asynccontextmanager

import pytest
from sqlalchemy.engine import make_url

from tests import cms_npd_admission_postgres_support as admission
from tests import generic_source_successor_postgres_support as source

pytestmark = pytest.mark.asyncio


@pytest.mark.parametrize(
    "database",
    [
        "hc_network_registry_" + "a" * 32,
        "hc_cms_admission_test_" + "b" * 32,
        "ptg2_v3_lifecycle_test_ci_runner",
    ],
)
@pytest.mark.parametrize("raise_inside", [False, True])
async def test_disposable_parent_uses_uuid_child_and_restores_settings(monkeypatch, database, raise_inside):
    selected = make_url(f"postgresql://tester@127.0.0.1:5440/{database}")
    child = selected.set(drivername="postgresql+asyncpg", database="hc_cms_admission_test_" + "c" * 32)
    original = selected.render_as_string(hide_password=False)
    monkeypatch.setenv(admission._DSN_ENV, original)
    allocation_events = []

    @asynccontextmanager
    async def allocate(parent):
        assert parent == selected.set(drivername="postgresql+asyncpg")
        allocation_events.append("entered")
        try:
            yield child, None
        finally:
            allocation_events.append("closed")

    monkeypatch.setattr(admission, "_owned_database", allocate)
    try:
        async with source._owned_source_database(monkeypatch):
            assert admission._database_url() == child
            assert child.database != selected.database
            if raise_inside:
                raise RuntimeError("synthetic source setup refusal")
    except RuntimeError as error:
        assert raise_inside and str(error) == "synthetic source setup refusal"
    assert os.environ[admission._DSN_ENV] == original
    assert allocation_events == ["entered", "closed"]


@pytest.mark.parametrize(
    "selected",
    [
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 31,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 33,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "A" * 32,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "g" * 32,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 32 + "_suffix",
        "postgresql://tester@127.0.0.1:5440/hc_network_other_" + "a" * 32,
        "postgresql://tester@127.0.0.1:5440/production",
        "postgresql://tester@outside.example:5440/hc_network_registry_" + "a" * 32,
        "postgresql://tester@127.0.0.1/hc_network_registry_" + "a" * 32,
        "postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 32 + "?sslmode=disable",
        "sqlite:///hc_network_registry_" + "a" * 32,
    ],
)
async def test_invalid_parent_refuses_before_database_creation(monkeypatch, selected):
    monkeypatch.setenv(admission._DSN_ENV, selected)

    def forbidden(*args, **kwargs):
        raise AssertionError("invalid parent reached database allocation")

    monkeypatch.setattr(admission, "_owned_database", forbidden)
    with pytest.raises(pytest.fail.Exception, match="disposable local PostgreSQL test database"):
        async with source._owned_source_database(monkeypatch):
            raise AssertionError("invalid parent entered the Source fixture")


async def test_registry_parent_fallback_preserves_missing_cms_setting(monkeypatch):
    selected = make_url("postgresql://tester@127.0.0.1:5440/hc_network_registry_" + "a" * 32)
    child = selected.set(drivername="postgresql+asyncpg", database="hc_cms_admission_test_" + "b" * 32)
    monkeypatch.delenv(admission._DSN_ENV, raising=False)
    monkeypatch.setenv("NETWORK_REGISTRY_TEST_DSN", selected.render_as_string(hide_password=False))

    @asynccontextmanager
    async def allocate(parent):
        assert parent == selected.set(drivername="postgresql+asyncpg")
        yield child, None

    monkeypatch.setattr(admission, "_owned_database", allocate)
    async with source._owned_source_database(monkeypatch):
        assert admission._database_url() == child
    assert admission._DSN_ENV not in os.environ


async def test_missing_selection_skips_without_allocation(monkeypatch):
    monkeypatch.delenv(admission._DSN_ENV, raising=False)
    monkeypatch.delenv("NETWORK_REGISTRY_TEST_DSN", raising=False)

    def forbidden(*args, **kwargs):
        raise AssertionError("unconfigured fixture reached database allocation")

    monkeypatch.setattr(admission, "_owned_database", forbidden)
    with pytest.raises(pytest.skip.Exception, match="explicitly selected disposable PostgreSQL database"):
        async with source._owned_source_database(monkeypatch):
            raise AssertionError("unconfigured fixture entered the Source fixture")
