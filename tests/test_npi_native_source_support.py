# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic source families must not assign one identity to unrelated address keys."""

from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import mrf_address_publication as canonical
from tests import npi_native_source_support as support
from tests import test_npi_result_archive_postgres as native_tests


@pytest.mark.asyncio
async def test_model_extensions_return_the_configured_connection_path():
    """Propagate the actual extension namespaces and inherited path, not just one namespace."""
    path = '"synthetic_extensions","$user",public'
    connection = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(side_effect=['"synthetic_extensions"', path]))
    assert await native_tests._ensure_model_extensions(connection) == path
    assert [str(call.args[0]) for call in connection.execute.await_args_list] == [
        "CREATE EXTENSION IF NOT EXISTS intarray",
        "CREATE EXTENSION IF NOT EXISTS pg_trgm",
    ]
    lookup, configured = connection.scalar.await_args_list
    assert "pg_catalog.pg_extension" in str(lookup.args[0]) and "HAVING count(*) = 2" in str(lookup.args[0])
    assert "pg_catalog.current_setting('search_path')" in str(configured.args[0])
    assert configured.args[1] == {"extension_search_path": '"synthetic_extensions"'}


@pytest.mark.asyncio
@pytest.mark.parametrize("namespace", [None, 1])
async def test_model_extensions_refuse_missing_namespace_before_actor_connections(namespace):
    connection = SimpleNamespace(execute=AsyncMock(), scalar=AsyncMock(return_value=namespace))
    with pytest.raises(pytest.fail.Exception, match="model index extensions are unavailable"):
        await native_tests._ensure_model_extensions(connection)
    assert connection.scalar.await_count == 1


@pytest.mark.parametrize("actor", ["publisher", "builder", "reader"])
def test_each_model_actor_connection_keeps_the_extension_path(monkeypatch, actor):
    """New logins must not lose the path set only in the administrator's transaction."""
    calls = []
    engine = object()

    def create(url, **options):
        calls.append((url, options))
        return engine

    monkeypatch.setattr(support, "create_async_engine", create)
    url = f"postgresql+asyncpg://{actor}@localhost/synthetic"
    path = '"synthetic_extensions","$user",public'
    assert support._model_actor_engine(url, path) is engine
    assert calls == [
        (
            url,
            {
                "poolclass": support.NullPool,
                "hide_parameters": True,
                "connect_args": {"server_settings": {"search_path": path}},
            },
        )
    ]


@pytest.mark.asyncio
async def test_independent_source_seeds_bind_distinct_canonical_identities(monkeypatch):
    keys = iter((UUID(int=1), UUID(int=2)))
    monkeypatch.setattr(support, "uuid4", lambda: next(keys))
    archive = support.archive
    monkeypatch.setattr(archive, "precreate_npi_restore", AsyncMock(return_value=SimpleNamespace(schema_name="stage")))
    monkeypatch.setattr(archive, "complete_npi_restore", AsyncMock())
    monkeypatch.setattr(archive.native_archive, "_create_model_heaps", AsyncMock())
    monkeypatch.setattr(archive.native_archive, "_create_model_indexes", AsyncMock())
    monkeypatch.setattr(canonical, "canonical_spatial_index", AsyncMock())
    monkeypatch.setattr(native_tests, "_run_migration", AsyncMock())
    session = SimpleNamespace(execute=AsyncMock(), connection=AsyncMock())
    for schema in ("synthetic_source", "synthetic_destination"):
        await support._create_source(session, schema, UUID(int=3))
    calls = session.execute.await_args_list
    seed_calls = [call for call in calls if ".address_archive_v2(address_key,identity_key," in str(call.args[0])]
    assert all(set(call.args[0].compile().params) == {"key", "identity"} for call in seed_calls)
    seeds = [call.args[1] for call in seed_calls]
    links = [call.args[1]["key"] for call in calls if ".npi_address SET address_key=:key" in str(call.args[0])]
    assert [seed["key"] for seed in seeds] == links == [UUID(int=1), UUID(int=2)]
    assert len({seed["identity"] for seed in seeds}) == 2
    assert all(seed["identity"] == f"synthetic-native-address-{seed['key'].hex}" for seed in seeds)
