# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Managed composition preserves independent facts and fences continuations."""

import asyncio
from contextlib import asynccontextmanager
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from api import provider_profile_network_read as reader
from api.endpoint import npi as provider
from api.provider_directory_entities_contract import DirectoryReadError
from api.provider_profile_network_contract import parse_managed_profile_query
from process.registry_source_recipe_store import RegistrySourceRecipeError
from tests.test_provider_profile_cms import _state_projection
from tests.test_provider_profile_network_contract import manifest


def address_manifest():
    pinned = manifest()
    return replace(pinned, schema_name="network_candidate_" + UUID(pinned.candidate_id).hex)


@pytest.fixture
def profile_read(monkeypatch):
    events = []
    driver = object()
    session = SimpleNamespace(connection=AsyncMock())
    session.connection.return_value.get_raw_connection = AsyncMock(
        return_value=SimpleNamespace(driver_connection=driver)
    )

    @asynccontextmanager
    async def snapshot(database, schema, *, include_detail):
        assert database is provider.db and include_detail
        events.append("enter")
        try:
            yield session
        finally:
            events.append("exit")

    async def pin(connection, *, api_read):
        assert api_read is True
        assert connection is driver
        events.append("manifest")
        return manifest()

    async def state(npi):
        assert events == ["enter", "manifest"] and npi == 1000000491
        events.append("state")
        return _state_projection()

    async def directory(*args):
        assert args[:3] == (session, driver, manifest()) and events[-1] == "state"
        events.append("directory")
        return None

    monkeypatch.setattr(reader, "provider_profile_read_snapshot", snapshot)
    monkeypatch.setattr(reader, "resolve_network_serving_manifest", pin)
    monkeypatch.setattr(provider, "fetch_provider_profile_projection", state)
    monkeypatch.setattr(reader, "_directory_profile", directory)
    return events


@pytest.mark.asyncio
async def test_managed_profile_preserves_independent_education_and_one_snapshot(profile_read):
    selector = parse_managed_profile_query({"npi": "1000000491", "include_evidence": "true"})
    page = await reader.read_managed_provider_profile(
        selector, excluded_network_ids=(84,), access_scope_sha256="d" * 64
    )
    assert profile_read == ["enter", "manifest", "state", "directory", "exit"]
    assert page["generation"] == "9" and page["approved_custom_revision"] == "4"
    assert (
        page["provider_profile"]["categories"]["education"]["items"][0]["value"]["institution"]
        == "State Medical School"
    )
    assert (
        page["provider_profile_evidence"]["sources"]["state_regulator"]["records"][0]["source_record_id"]
        == "state-school"
    )


@pytest.mark.asyncio
async def test_managed_profile_changed_policy_refuses_old_category_continuation(profile_read):
    selector = parse_managed_profile_query({"npi": "1000000491", "category": "education", "limit": "1"})
    first = await reader.read_managed_provider_profile(selector, excluded_network_ids=(), access_scope_sha256="d" * 64)
    profile_read.clear()
    stale = parse_managed_profile_query(
        {
            "npi": "1000000491",
            "category": "education",
            "limit": "1",
            "generation_id": first["provider_profile"]["generation_id"],
        }
    )
    refusal = await reader.read_managed_provider_profile(stale, excluded_network_ids=(), access_scope_sha256="e" * 64)
    assert refusal.status == 409 and b"provider_profile_generation_changed" in refusal.body
    assert profile_read[-1] == "exit"


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [ValueError("source invalid"), asyncio.CancelledError()])
async def test_managed_profile_failure_releases_reader_without_retry(profile_read, monkeypatch, failure):
    async def directory(*args):
        raise failure

    monkeypatch.setattr(reader, "_directory_profile", directory)
    with pytest.raises(type(failure)):
        await reader.read_managed_provider_profile(
            parse_managed_profile_query({"npi": "1000000491"}), excluded_network_ids=(), access_scope_sha256="d" * 64
        )
    assert profile_read == ["enter", "manifest", "state", "exit"]


@pytest.mark.asyncio
@pytest.mark.parametrize("sources", [[{"source_id": "unretained-directory"}], ["invalid"], None])
async def test_directory_profile_never_silently_drops_observed_unretained_sources(monkeypatch, sources):
    from api.provider_directory_entities_contract import DirectoryReadError

    monkeypatch.setattr(reader, "resolve_retained_registry_source_recipes", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        provider,
        "_fetch_provider_directory_profile_map",
        AsyncMock(return_value={1000000491: {"profile": {"sources": sources}}}),
    )
    with pytest.raises(DirectoryReadError) as refused:
        await reader._directory_profile(
            None, object(), manifest(), "1000000491", parse_managed_profile_query({"npi": "1000000491"})[1], (), "mrf"
        )
    assert refused.value.status == 503


@pytest.mark.asyncio
@pytest.mark.parametrize("selected,excluded", [((42, 84), None), (None, (84,))])
async def test_embedded_profile_reuses_exact_address_manifest_and_selection(monkeypatch, selected, excluded):
    from api.network_address_scope import network_address_read_scope

    native = object()
    session = SimpleNamespace(connection=AsyncMock())
    session.connection.return_value.get_raw_connection = AsyncMock(
        return_value=SimpleNamespace(driver_connection=native)
    )
    directory = AsyncMock(return_value={"profile": {"facts": {}}})
    monkeypatch.setattr(reader, "_directory_profile", directory)
    pinned = address_manifest()
    with network_address_read_scope(pinned, selected, excluded_network_ids=excluded, access_scope_sha256="d" * 64):
        result = await reader.read_address_scoped_directory_profile(session, 1000000491, include_evidence=True)
    arguments, options = directory.await_args
    assert arguments[:4] == (session, native, pinned, "1000000491")
    assert arguments[4].include_evidence and arguments[5] == (excluded or ())
    assert options == {"selected_network_ids": selected}
    assert result == {"profile": {"facts": {}}}


@pytest.mark.asyncio
async def test_embedded_profile_requires_an_existing_address_scope():
    session = SimpleNamespace(connection=AsyncMock())
    with pytest.raises(ValueError, match="network_address_scope_required"):
        await reader.read_address_scoped_directory_profile(session, 1000000491, include_evidence=False)
    session.connection.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("npi", ["0000000000", "9999999999", "1000000005"])
async def test_legacy_native_identity_outside_cms_domain_needs_no_cms_profile(monkeypatch, npi):
    from tests.test_provider_profile_network_facts import scope

    monkeypatch.setattr(
        reader,
        "resolve_retained_registry_source_recipes",
        AsyncMock(return_value=[SimpleNamespace(source_pin=scope().source_pin)]),
    )
    monkeypatch.setattr(provider, "_fetch_provider_directory_profile_map", AsyncMock(return_value={}))
    pin = AsyncMock()
    monkeypatch.setattr(reader, "pin_cms_profile_scope", pin)
    query = provider._provider_profile_query_from_args({})
    assert await reader._directory_profile(None, object(), manifest(), npi, query, (), "mrf") is None
    pin.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure",
    [
        DirectoryReadError(503),
        RegistrySourceRecipeError("registry_source_recipes_unavailable"),
        ValueError("registry_cms_source_pair_unavailable"),
    ],
)
async def test_scoped_embedded_profile_failure_is_terminal(monkeypatch, failure):
    from sanic.exceptions import ServiceUnavailable

    @asynccontextmanager
    async def snapshot(*args, **kwargs):
        yield None

    from api import provider_profile_network_read as profile_reader
    from api.network_address_scope import network_address_read_scope

    monkeypatch.setattr(provider, "provider_profile_read_snapshot", snapshot)
    scoped = AsyncMock(side_effect=failure)
    unscoped = AsyncMock()
    monkeypatch.setattr(profile_reader, "read_address_scoped_directory_profile", scoped)
    monkeypatch.setattr(provider, "_fetch_provider_directory_profile_map", unscoped)
    request = SimpleNamespace(args={"sync_geocode": "false", "lookup_stored_geocode": "false"})
    with network_address_read_scope(address_manifest(), (42,)):
        with pytest.raises(ServiceUnavailable):
            await provider.get_npi(request, "1000000491")
    scoped.assert_awaited_once_with(None, 1000000491, include_evidence=False)
    unscoped.assert_not_awaited()


@pytest.mark.asyncio
async def test_directory_profile_disables_jit_once_before_scoped_aggregation(monkeypatch):
    from api import provider_profile_cms_resources as resources
    from tests.test_provider_profile_network_facts import scope

    pinned = scope()
    monkeypatch.setattr(
        reader,
        "resolve_retained_registry_source_recipes",
        AsyncMock(return_value=[SimpleNamespace(source_pin=pinned.source_pin)]),
    )
    monkeypatch.setattr(provider, "_fetch_provider_directory_profile_map", AsyncMock(return_value={}))
    monkeypatch.setattr(reader, "pin_cms_profile_scope", AsyncMock(return_value=pinned))
    monkeypatch.setattr(
        resources,
        "read_cms_profile_resources",
        AsyncMock(return_value={"resources": [], "associations": [], "excluded_network_resource_ids": []}),
    )
    statements = []

    async def execute(statement, parameters=None):
        sql = str(statement)
        statements.append(sql)
        if sql == "SET LOCAL jit = off":
            assert parameters in (None, {})
        else:
            assert statements[0] == "SET LOCAL jit = off"
            assert sql.startswith("WITH projection AS MATERIALIZED")
            assert parameters["profile_npi"] == 1000000491
            assert parameters["profile_source_id"] == pinned.source_pin.source_id
        return SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: None))

    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    query = parse_managed_profile_query({"npi": "1000000491"})[1]
    for _attempt in range(2):
        assert await reader._directory_profile(session, object(), manifest(), "1000000491", query, (), "mrf") is None
    assert statements.count("SET LOCAL jit = off") == 1
    assert sum(sql.startswith("WITH projection AS MATERIALIZED") for sql in statements) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("optional,retained_capture", [(True, True), (False, True), (True, False)])
async def test_generic_profile_omits_only_optional_authenticated_retained_enrichment(
    monkeypatch, optional, retained_capture
):
    from api.network_address_scope import network_address_read_scope
    from tests.test_network_fhir_retained_capture import pin, retained

    source = pin(retained())
    if not retained_capture:
        source = replace(source, retained_capture=None)
    recipes = AsyncMock(return_value=[SimpleNamespace(source_pin=source)])
    observed = AsyncMock(return_value={})
    monkeypatch.setattr(reader, "resolve_retained_registry_source_recipes", recipes)
    monkeypatch.setattr(provider, "_fetch_provider_directory_profile_map", observed)
    session = SimpleNamespace(connection=AsyncMock())
    session.connection.return_value.get_raw_connection = AsyncMock(
        return_value=SimpleNamespace(driver_connection=object())
    )
    with network_address_read_scope(address_manifest(), (42,)):
        if optional and retained_capture:
            assert await reader.read_detail_directory_profile(session, 1000000491, include_evidence=False) is None
        elif optional:
            with pytest.raises(reader.ServiceUnavailable):
                await reader.read_detail_directory_profile(session, 1000000491, include_evidence=False)
        else:
            with pytest.raises(DirectoryReadError):
                await reader.read_address_scoped_directory_profile(session, 1000000491, include_evidence=False)
    recipes.assert_awaited_once()
    if retained_capture:
        observed.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["mixed", "recipe"])
async def test_optional_profile_preserves_other_proof_failures(monkeypatch, failure):
    from api.network_address_scope import network_address_read_scope
    from tests.test_network_fhir_retained_capture import pin, retained
    from tests.test_provider_profile_network_facts import scope

    resolve = AsyncMock(
        return_value=[SimpleNamespace(source_pin=pin(retained())), SimpleNamespace(source_pin=scope().source_pin)]
    )
    if failure == "recipe":
        resolve.side_effect = RegistrySourceRecipeError("registry_source_recipes_unavailable")
    monkeypatch.setattr(reader, "resolve_retained_registry_source_recipes", resolve)
    observed = AsyncMock(return_value={})
    monkeypatch.setattr(provider, "_fetch_provider_directory_profile_map", observed)
    session = SimpleNamespace(connection=AsyncMock())
    session.connection.return_value.get_raw_connection = AsyncMock(
        return_value=SimpleNamespace(driver_connection=object())
    )
    with network_address_read_scope(address_manifest(), (42,)), pytest.raises(reader.ServiceUnavailable):
        await reader.read_detail_directory_profile(session, 1000000491, include_evidence=False)
    if failure == "recipe":
        observed.assert_not_awaited()
