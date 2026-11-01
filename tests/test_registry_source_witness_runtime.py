# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed server settings and owned membership factory lifetime, without native admission."""

import asyncio
import builtins
import copy
import json
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import ModuleType
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import registry_publication_worker as worker
from process.registry_source_witness_runtime import install_source_witness_runtime, parse_source_witness_config
from tests.test_network_fhir_source_witness_custody import _profile
from tests.test_registry_candidate_custody_runtime import _configured
from tests.test_registry_publication_worker import configuration, resources
from tests.test_registry_source_witness_worker import _ACTORS, _runtime, _sources


def _document():
    document, _ = _configured(configuration)
    document["initial_address_sources"] = {
        "base_source": {"schema_name": "address_source", "table_name": "address_rows", "generation_id": "one"},
        "compact": True,
    }
    document["source_witness"] = {
        "source_profiles": [_profile().document()],
        "capture_owner_role": "capture_owner",
        "runtime_roles": list(_ACTORS),
    }
    return document


def _config():
    return worker.parse_registry_publication_worker_config(json.dumps(_document()))


def _factory(monkeypatch, events, *, exported=None, enter_failure=None, exit_failure=None):
    source_export = Mock() if exported is None else exported

    @asynccontextmanager
    async def lifetime():
        events.append("source open")
        try:
            if enter_failure is not None:
                raise enter_failure
            yield source_export
        finally:
            events.append("source close")
            if exit_failure is not None:
                raise exit_failure

    module = ModuleType("directory_membership_runtime")
    module.membership_source_export_runtime = Mock(side_effect=lifetime)
    monkeypatch.setitem(sys.modules, module.__name__, module)
    return source_export, module.membership_source_export_runtime


def test_server_profile_is_typed_separate_from_recipes_and_redacted():
    config = _config()
    assert [profile.document() for profile in config.source_witness.source_profiles] == [_profile().document()]
    assert config.source_witness.runtime_roles == _ACTORS
    assert config.source_witness.capture_owner_role == "capture_owner"
    assert config.initial_address_sources.source_witness_runtime is None
    assert "source_owner" not in repr(config) and "capture_owner" not in repr(config.source_witness)


@pytest.mark.parametrize(
    "changes_by_field,missing_field",
    (
        ({"factory": "caller.module:factory"}, None),
        ({"source_dsn": "postgresql://caller@example.test/source"}, None),
        ({}, "capture_owner_role"),
        ({"source_profiles": _profile().document()}, None),
        ({"source_profiles": []}, None),
        ({"source_profiles": [_profile().document(), _profile().document()]}, None),
        ({"runtime_roles": "reader"}, None),
        ({"runtime_roles": list(reversed(_ACTORS))}, None),
        ({"runtime_roles": [*_ACTORS, _ACTORS[-1]]}, None),
        ({"capture_owner_role": _ACTORS[0]}, None),
        ({"runtime_roles": ["invalid.role"]}, None),
    ),
)
def test_source_settings_reject_unbounded_or_ambiguous_authority(changes_by_field, missing_field):
    document = _document()["source_witness"]
    document.update(changes_by_field)
    if missing_field is not None:
        del document[missing_field]
    with pytest.raises(ValueError):
        parse_source_witness_config(document)


@pytest.mark.parametrize("missing", ("custody_dsn", "initial_address_sources", "compact"))
def test_worker_requires_independent_custody_and_compact_inputs(missing):
    document = _document()
    if missing == "compact":
        document["initial_address_sources"]["compact"] = False
    else:
        del document[missing]
    with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
        worker.parse_registry_publication_worker_config(json.dumps(document))


def test_direct_config_cannot_mix_installed_capability_with_another_server_profile():
    config = _config()
    with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
        replace(config, initial_address_sources=_sources(_runtime()))
    with pytest.raises(ValueError, match="registry_publication_worker_config_invalid"):
        replace(config, source_witness=copy.deepcopy(_document()["source_witness"]))


@pytest.mark.asyncio
async def test_installer_owns_original_factory_without_opening_a_capture(monkeypatch):
    events, config = [], _config()
    exported, factory = _factory(monkeypatch, events)
    async with AsyncExitStack() as stack:
        installed = await install_source_witness_runtime(config, stack)
        runtime = installed.initial_address_sources.source_witness_runtime
        assert runtime.source_export is exported
        assert runtime.source_profiles == config.source_witness.source_profiles
        assert runtime.runtime_roles == config.source_witness.runtime_roles
        assert runtime.capture_owner_role == config.source_witness.capture_owner_role
        assert installed.source_witness is None and config.initial_address_sources.source_witness_runtime is None
        assert await install_source_witness_runtime(installed, stack) is installed
        assert events == ["source open"]
    assert events == ["source open", "source close"]
    factory.assert_called_once_with()
    exported.assert_not_called()


@pytest.mark.asyncio
async def test_missing_membership_package_never_uses_catalog_or_opens_worker_pools(monkeypatch):
    original_import = builtins.__import__

    def require_membership(name, *arguments, **options):
        assert name != "directory_catalog_runtime"
        if name == "directory_membership_runtime":
            raise ImportError("membership package absent")
        return original_import(name, *arguments, **options)

    monkeypatch.setattr(builtins, "__import__", require_membership)
    pool = AsyncMock(side_effect=AssertionError("worker pool must remain closed"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", pool)
    with pytest.raises(ValueError, match="registry_source_witness_factory_unavailable"):
        await worker.run_registry_publication_worker(_config())
    pool.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("installed", (False, True))
async def test_source_successor_refresh_is_refused_before_factory_or_pool(monkeypatch, installed):
    events = []
    exported, factory = _factory(monkeypatch, events)
    config = _config()
    if installed:
        config = replace(config, source_witness=None, initial_address_sources=_sources(_runtime()))
    pool = AsyncMock(side_effect=AssertionError("refresh must not open a worker pool"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", pool)
    with pytest.raises(ValueError, match="registry_source_witness_refresh_unsupported"):
        await worker.run_registry_publication_worker(config, membership_refresh_request_id=UUID(int=1))
    assert events == []
    factory.assert_not_called()
    exported.assert_not_called()
    pool.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ("success", "failure", "cancel", "close_failure"))
async def test_actual_worker_keeps_membership_factory_until_owned_pools_close(monkeypatch, outcome):
    events, _ = resources(monkeypatch)
    exported, factory = _factory(
        monkeypatch, events, exit_failure=RuntimeError("source close failed") if outcome == "close_failure" else None
    )
    installed_runtimes = []

    async def qualified(config, stack, qualification, connection):
        runtime = config.initial_address_sources.source_witness_runtime
        assert runtime.source_export is exported and config.source_witness is None
        installed_runtimes.append(runtime)
        return config.consumer

    async def consume(**arguments):
        assert arguments["initial_address_sources"].source_witness_runtime is installed_runtimes[0]
        assert "source close" not in events
        if outcome == "failure":
            raise ValueError("publication refused")
        if outcome == "cancel":
            raise asyncio.CancelledError
        return None

    monkeypatch.setattr(worker, "_qualified_worker_runtime", qualified)
    monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
    if outcome == "success":
        assert await worker.run_registry_publication_worker(_config()) == {"state": "no_work"}
    else:
        expected = (
            asyncio.CancelledError
            if outcome == "cancel"
            else RuntimeError
            if outcome == "close_failure"
            else ValueError
        )
        with pytest.raises(expected):
            await worker.run_registry_publication_worker(_config())
    assert len(installed_runtimes) == 1
    assert events == [
        "source open",
        "authority open",
        "lifecycle open",
        "execution open",
        "execution close",
        "lifecycle close",
        "authority close",
        "source close",
    ]
    factory.assert_called_once_with()
    exported.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", (RuntimeError("source startup failed"), asyncio.CancelledError()))
async def test_factory_startup_failure_prevents_worker_startup(monkeypatch, failure):
    events = []
    exported, factory = _factory(monkeypatch, events, enter_failure=failure)
    pools = AsyncMock(side_effect=AssertionError("worker pool must remain closed"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", pools)
    with pytest.raises(type(failure)):
        await worker.run_registry_publication_worker(_config())
    assert events == ["source open", "source close"]
    factory.assert_called_once_with()
    exported.assert_not_called()
    pools.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("omitted", ("registry_queue_owner", "registry_reader", "sealed_operator"))
async def test_actual_worker_refuses_incomplete_census_before_custody_creation(monkeypatch, omitted):
    from process import network_candidate_custody

    events, pool_options = resources(monkeypatch)
    exported, factory = _factory(monkeypatch, events)
    document = _document()
    document["source_witness"]["runtime_roles"].remove(omitted)
    identity = AsyncMock(return_value={"custody_owner_role": "protected_control_owner"})
    reach = AsyncMock(side_effect=AssertionError("census must refuse before custody reach verification"))
    consume = AsyncMock(side_effect=AssertionError("incomplete census must not claim work"))
    monkeypatch.setattr(worker, "_custody_identity", identity)
    monkeypatch.setattr(network_candidate_custody, "_q_reach_closed", reach)
    monkeypatch.setattr(worker, "consume_registry_publication_once", consume)
    with pytest.raises(ValueError, match="registry_source_witness_runtime_census_differs"):
        await worker.run_registry_publication_worker(
            worker.parse_registry_publication_worker_config(json.dumps(document))
        )
    assert [options["user"] for options in pool_options] == ["worker_lifecycle", "worker_execution"]
    assert events[-4:] == ["execution close", "lifecycle close", "authority close", "source close"]
    identity.assert_awaited_once()
    reach.assert_not_awaited()
    consume.assert_not_awaited()
    factory.assert_called_once_with()
    exported.assert_not_called()


@pytest.mark.asyncio
async def test_noncallable_membership_export_closes_its_owned_context(monkeypatch):
    events = []
    _exported, factory = _factory(monkeypatch, events, exported=object())
    pools = AsyncMock(side_effect=AssertionError("invalid factory must not start the worker"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", pools)
    with pytest.raises(ValueError, match="registry_source_witness_runtime_invalid"):
        await worker.run_registry_publication_worker(_config())
    assert events == ["source open", "source close"]
    factory.assert_called_once_with()
    pools.assert_not_awaited()
