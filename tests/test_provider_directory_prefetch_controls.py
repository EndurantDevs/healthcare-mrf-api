# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio
import contextlib
import importlib
from unittest.mock import AsyncMock

import pytest

from tests.test_process_provider_directory_fhir_unit import (
    _RestPageHarness,
    unit_checkpoint_catalog,
    unit_source_factory,
)

importer = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.usefixtures("unit_checkpoint_catalog")
@pytest.mark.parametrize(
    ("environment_enabled", "override", "expected"),
    [
        (None, None, False),
        (True, None, True),
        (False, None, False),
        (None, True, True),
        (False, True, False),
        ("invalid", True, False),
        (True, False, False),
        (None, False, False),
    ],
)
async def test_prefetch_override_preserves_default_and_explicit_disable(
    monkeypatch, environment_enabled, override, expected
):
    page_scan = _RestPageHarness(monkeypatch, prefetch=bool(environment_enabled))
    if environment_enabled is None:
        monkeypatch.delenv(importer.SOURCE_HTTP_PAGE_PREFETCH_ENV, raising=False)
    elif isinstance(environment_enabled, str):
        monkeypatch.setenv(importer.SOURCE_HTTP_PAGE_PREFETCH_ENV, environment_enabled)
    token = importer._REST_PAGE_PREFETCH_OVERRIDE.set(override)
    try:
        result = await page_scan.run()
    finally:
        importer._REST_PAGE_PREFETCH_OVERRIDE.reset(token)

    assert result.complete
    assert result.rows_written == 2
    assert result.page_prefetch_eligible is expected
    assert result.page_prefetch_started == int(expected)
    assert result.page_prefetch_consumed == int(expected)
    assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is None


async def test_prefetch_request_settings_are_isolated_between_concurrent_imports(monkeypatch):
    both_started = asyncio.Event()
    started_source_ids = []

    @contextlib.asynccontextmanager
    async def session_scope(**_kwargs):
        yield

    async def import_resources(sources, **options):
        assert "rest_page_prefetch" not in options
        started_source_ids.append(sources[0]["source_id"])
        if len(started_source_ids) == 2:
            both_started.set()
        await asyncio.wait_for(both_started.wait(), 1)
        return importer._REST_PAGE_PREFETCH_OVERRIDE.get()

    monkeypatch.setattr(importer, "_source_http_session_scope", session_scope)
    monkeypatch.setattr(importer, "_import_resources", import_resources)
    values = await asyncio.gather(
        importer._import_resources_with_source_http_session([{"source_id": "source_a"}], rest_page_prefetch=True),
        importer._import_resources_with_source_http_session([{"source_id": "source_b"}], rest_page_prefetch=False),
    )

    assert values == [True, False]
    assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is None


@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_prefetch_override_restores_outer_context_on_failure(monkeypatch, failure):
    @contextlib.asynccontextmanager
    async def session_scope(**_kwargs):
        yield

    async def import_resources(_sources, **_kwargs):
        assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is True
        raise failure()

    monkeypatch.setattr(importer, "_source_http_session_scope", session_scope)
    monkeypatch.setattr(importer, "_import_resources", import_resources)
    token = importer._REST_PAGE_PREFETCH_OVERRIDE.set(False)
    try:
        with pytest.raises(failure):
            await importer._import_resources_with_source_http_session(
                [{"source_id": "source_a"}], rest_page_prefetch=True
            )
        assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is False
    finally:
        importer._REST_PAGE_PREFETCH_OVERRIDE.reset(token)


async def test_prefetch_override_does_not_force_disabled_transport(monkeypatch):
    monkeypatch.setenv(importer.SOURCE_HTTP_KEEPALIVE_ENV, "false")

    async def import_resources(_sources, **_kwargs):
        assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is True
        assert importer._SOURCE_HTTP_SESSION.get() is None
        assert (
            importer._rest_page_prefetch_session(
                {"api_base": "https://example.test/fhir"}, "https://example.test/fhir/Practitioner"
            )
            is None
        )
        return {}

    monkeypatch.setattr(importer, "_import_resources", import_resources)
    assert (
        await importer._import_resources_with_source_http_session([{"source_id": "source_a"}], rest_page_prefetch=True)
        == {}
    )
    assert importer._REST_PAGE_PREFETCH_OVERRIDE.get() is None


@pytest.mark.parametrize("setting", ["true", 1, 0, {}, []])
async def test_prefetch_setting_rejects_non_boolean_values_before_database_work(monkeypatch, setting):
    database_setup = AsyncMock()
    monkeypatch.setattr(importer, "ensure_database", database_setup)
    with pytest.raises(ValueError, match="provider_directory_rest_page_prefetch_invalid"):
        await importer.process_data({"context": {}}, {"rest_page_prefetch": setting, "probe": False})
    database_setup.assert_not_awaited()
