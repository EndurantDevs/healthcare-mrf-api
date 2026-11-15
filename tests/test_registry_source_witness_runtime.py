# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed server settings and owned membership factory lifetime, without native admission."""

import asyncio
import builtins
import copy
import json
import sys
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import replace
from types import ModuleType, SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import registry_publication_worker as worker
from process import registry_source_witness_runtime as source_runtime
from process.network_fhir_sampled_validation import SemanticSamplingPolicy
from process.registry_fhir_catalog_sources import FHIRCatalogSourceReadProfile
from process.registry_source_witness_runtime import install_source_witness_runtime, parse_source_witness_config
from tests.test_registry_publication_worker import _runtime_configuration, configuration, resources
from tests.test_registry_source_witness_worker import _ACTORS, _runtime, _sources


def _profile():
    return FHIRCatalogSourceReadProfile("source_payload", "source_catalog")


def _document():
    document = configuration()
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
    return _runtime_configuration(_document())


def _sampling_document():
    document = _document()
    document["source_witness"]["semantic_validation"] = {"contract": "sampled-semantic-v1", "seed": "a" * 64}
    return document


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
    assert config.source_witness.semantic_policy is None
    assert config.initial_address_sources.source_witness_runtime is None
    assert "source_owner" not in repr(config) and "capture_owner" not in repr(config.source_witness)


def test_omitted_policy_preserves_full_mode_and_original_profile_document():
    document = _document()["source_witness"]
    original = copy.deepcopy(document)
    config = parse_source_witness_config(document)
    assert config.semantic_policy is None and document == original
    assert original == {
        "source_profiles": [profile.document() for profile in config.source_profiles],
        "capture_owner_role": config.capture_owner_role,
        "runtime_roles": list(config.runtime_roles),
    }


def test_explicit_sampling_mode_is_typed_and_keeps_original_profiles():
    document = _sampling_document()
    config = worker.parse_registry_publication_worker_config(json.dumps(document))
    assert type(config.source_witness.semantic_policy) is SemanticSamplingPolicy
    assert config.source_witness.semantic_policy.seed == "a" * 64
    assert [profile.document() for profile in config.source_witness.source_profiles] == [_profile().document()]


@pytest.mark.parametrize(
    "semantic_document",
    (
        None,
        False,
        {},
        {"contract": "exhaustive", "seed": "a" * 64},
        {"contract": "sampled-semantic-v1"},
        {"contract": "sampled-semantic-v1", "seed": None},
        {"contract": "sampled-semantic-v1", "seed": False},
        {"contract": "sampled-semantic-v1", "seed": "A" * 64},
        {"contract": "sampled-semantic-v1", "seed": "a" * 63},
        {"contract": "sampled-semantic-v1", "seed": "g" * 64},
        {"contract": "sampled-semantic-v1", "seed": "a" * 64, "series_count": 10},
        {"contract": "sampled-semantic-v1", "seed": "a" * 64, "items_per_series": 100},
    ),
)
def test_sampling_mode_refuses_null_wrong_contract_options_or_seed(semantic_document):
    document = _document()["source_witness"]
    document["semantic_validation"] = semantic_document
    with pytest.raises(ValueError):
        parse_source_witness_config(document)


@pytest.mark.parametrize("policy", (False, object(), {"seed": "a" * 64}))
def test_direct_settings_require_the_exact_typed_policy(policy):
    with pytest.raises(ValueError, match="registry_source_witness_config_invalid"):
        replace(_config().source_witness, semantic_policy=policy)


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


@pytest.mark.parametrize("missing", ("initial_address_sources", "compact"))
@pytest.mark.parametrize("sampled", (False, True))
def test_worker_requires_declared_compact_source_inputs(missing, sampled):
    document = _sampling_document() if sampled else _document()
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
@pytest.mark.parametrize("missing", ("addresses", "compact"))
async def test_installer_rechecks_required_worker_inputs_before_factory(monkeypatch, missing):
    events = []
    exported, factory = _factory(monkeypatch, events)
    configured = _config()
    addresses = SimpleNamespace(compact=missing != "compact", source_witness_runtime=None)
    config = SimpleNamespace(
        source_witness=configured.source_witness,
        initial_address_sources=None if missing == "addresses" else addresses,
    )
    resources = AsyncMock()
    with pytest.raises(ValueError, match="^registry_source_witness_worker_config_invalid$"):
        await install_source_witness_runtime(config, resources)
    assert events == []
    factory.assert_not_called()
    exported.assert_not_called()
    resources.enter_async_context.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("conflict", ("untyped_profile", "installed_runtime"))
async def test_installer_refuses_conflicting_authority_before_factory(monkeypatch, conflict):
    events = []
    exported, factory = _factory(monkeypatch, events)
    configured = _config()
    config = SimpleNamespace(
        source_witness=_document()["source_witness"] if conflict == "untyped_profile" else configured.source_witness,
        initial_address_sources=_sources(_runtime())
        if conflict == "installed_runtime"
        else configured.initial_address_sources,
    )
    resources = AsyncMock()
    with pytest.raises(ValueError, match="^registry_source_witness_worker_config_invalid$"):
        await install_source_witness_runtime(config, resources)
    assert events == []
    factory.assert_not_called()
    exported.assert_not_called()
    resources.enter_async_context.assert_not_awaited()


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
@pytest.mark.parametrize("sampled", (False, True))
async def test_installer_forwards_policy_only_for_explicit_server_mode(monkeypatch, sampled):
    events = []
    exported, factory = _factory(monkeypatch, events)
    document = _sampling_document() if sampled else _document()
    config = worker.parse_registry_publication_worker_config(json.dumps(document))
    constructor = Mock(wraps=source_runtime.SourceWitnessRuntime)
    monkeypatch.setattr(source_runtime, "SourceWitnessRuntime", constructor)
    async with AsyncExitStack() as stack:
        installed = await install_source_witness_runtime(config, stack)
        runtime = installed.initial_address_sources.source_witness_runtime
        assert runtime.semantic_policy is config.source_witness.semantic_policy
        assert runtime.semantic_validation is None
    expected_options = {"semantic_policy": config.source_witness.semantic_policy} if sampled else {}
    constructor.assert_called_once_with(
        exported,
        config.source_witness.source_profiles,
        config.source_witness.capture_owner_role,
        config.source_witness.runtime_roles,
        **expected_options,
    )
    assert events == ["source open", "source close"]
    factory.assert_called_once_with()
    exported.assert_not_called()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ("initial_address_sources", "compact"))
async def test_installer_rechecks_required_inputs_before_opening_source(monkeypatch, missing):
    events, config = [], _config()
    exported, factory = _factory(monkeypatch, events)
    values = vars(config) | {missing: None}
    if missing == "compact":
        values = vars(config) | {"initial_address_sources": replace(config.initial_address_sources, compact=False)}
    async with AsyncExitStack() as stack:
        with pytest.raises(ValueError, match="registry_source_witness_worker_config_invalid"):
            await install_source_witness_runtime(SimpleNamespace(**values), stack)
        assert not stack._exit_callbacks
    factory.assert_not_called()
    exported.assert_not_called()
    assert events == []


@pytest.mark.asyncio
@pytest.mark.parametrize("mixed", (False, True))
async def test_installer_refuses_unparsed_or_conflicting_settings_before_source(monkeypatch, mixed):
    events, config = [], _config()
    exported, factory = _factory(monkeypatch, events)
    changes = (
        {"initial_address_sources": _sources(_runtime())}
        if mixed
        else {"source_witness": _document()["source_witness"]}
    )
    async with AsyncExitStack() as stack:
        with pytest.raises(ValueError, match="registry_source_witness_worker_config_invalid"):
            await install_source_witness_runtime(SimpleNamespace(**(vars(config) | changes)), stack)
        assert not stack._exit_callbacks
    factory.assert_not_called()
    exported.assert_not_called()
    assert events == []


@pytest.mark.asyncio
async def test_installer_refuses_noncallable_factory_attribute_without_opening_resources(monkeypatch):
    module = ModuleType("directory_membership_runtime")
    module.membership_source_export_runtime = object()
    monkeypatch.setitem(sys.modules, module.__name__, module)
    async with AsyncExitStack() as stack:
        with pytest.raises(ValueError, match="registry_source_witness_factory_unavailable"):
            await install_source_witness_runtime(_config(), stack)
        assert not stack._exit_callbacks


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
async def test_noncallable_installed_factory_never_opens_worker_pools(monkeypatch):
    events = []
    exported, factory = _factory(monkeypatch, events)
    monkeypatch.setattr(sys.modules["directory_membership_runtime"], "membership_source_export_runtime", None)
    pool = AsyncMock(side_effect=AssertionError("worker pool must remain closed"))
    monkeypatch.setattr(worker.asyncpg, "create_pool", pool)
    with pytest.raises(ValueError, match="^registry_source_witness_factory_unavailable$"):
        await worker.run_registry_publication_worker(_config())
    assert events == []
    factory.assert_not_called()
    exported.assert_not_called()
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

    original_runtime = worker._configured_worker_runtime

    async def configured_runtime(config, stack):
        runtime = config.initial_address_sources.source_witness_runtime
        assert runtime.source_export is exported and config.source_witness is None
        installed_runtimes.append(runtime)
        return await original_runtime(config, stack)

    async def consume(**arguments):
        assert arguments["initial_address_sources"].source_witness_runtime is installed_runtimes[0]
        assert "source close" not in events
        if outcome == "failure":
            raise ValueError("publication refused")
        if outcome == "cancel":
            raise asyncio.CancelledError
        return None

    monkeypatch.setattr(worker, "_configured_worker_runtime", configured_runtime)
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
        "preparation open",
        "preparation close",
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
