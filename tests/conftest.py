# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os
import sys
from pathlib import Path

import pytest

TEST_ENV_DEFAULTS = {
    "HLTHPRT_LOG_CFG": str(Path(__file__).resolve().parents[1] / "logging.yaml"),
    "HLTHPRT_DB_DRIVER": "asyncpg",
    "HLTHPRT_DB_HOST": "127.0.0.1",
    "HLTHPRT_DB_PORT": "5432",
    "HLTHPRT_DB_USER": "postgres",
    "HLTHPRT_DB_PASSWORD": "",
    "HLTHPRT_DB_DATABASE": "healthporta",
    "HLTHPRT_DB_SCHEMA": "mrf",
    "HLTHPRT_DB_POOL_MIN_SIZE": "1",
    "HLTHPRT_DB_POOL_MAX_SIZE": "5",
    "HLTHPRT_DB_ECHO": "False",
    "HLTHPRT_REDIS_ADDRESS": "redis://127.0.0.1:6379",
    "HLTHPRT_PTG2_SOURCE_IMPORT_LOCK_ENABLED": "false",
    "HLTHPRT_CMS_NPD_STAGE_WORKERS": "0",
}

for key, value in TEST_ENV_DEFAULTS.items():
    os.environ.setdefault(key, value)


@pytest.fixture(autouse=True)
def clear_npi_detail_response_cache():
    # Unrelated tests should not load the endpoint just to clear an empty cache.
    npi_module = sys.modules.get("api.endpoint.npi")
    if npi_module is not None:
        npi_module._NPI_DETAIL_RESPONSE_CACHE.clear()
    yield
    if npi_module is not None:
        npi_module._NPI_DETAIL_RESPONSE_CACHE.clear()
    current_module = sys.modules.get("api.endpoint.npi")
    if current_module is not None and current_module is not npi_module:
        current_module._NPI_DETAIL_RESPONSE_CACHE.clear()


@pytest.fixture(autouse=True)
def clear_ptg2_price_hydration_cache():
    from api.ptg2_price_hydration_cache import PRICE_HYDRATION_CACHE

    PRICE_HYDRATION_CACHE.clear()
    yield
    PRICE_HYDRATION_CACHE.clear()


@pytest.fixture(autouse=True)
def read_addresses_without_provider_geo_search(request, monkeypatch):
    # Nearby handlers look up the all-address projection and importers rebuild it after publishing; stubbed
    # sessions would answer those calls. Tests that exercise the projection opt in with @pytest.mark.provider_geo_search.
    if request.node.get_closest_marker("provider_geo_search") is not None:
        return

    from api import provider_geo_search
    from process import provider_geo_search as provider_geo_search_builder

    async def no_projection(*_args, **_kwargs):
        return None

    monkeypatch.setattr(provider_geo_search, "current_provider_geo_search", no_projection)
    monkeypatch.setattr(provider_geo_search_builder, "refresh_after_publication", no_projection)


@pytest.fixture(autouse=True)
def freeze_specialty_resolution_cache():
    # Endpoint handlers lazily refresh the shared specialty cache from the DB;
    # unit tests stub sessions with queued results, so a refresh would consume
    # from the queue and shift every subsequent stubbed query. Mark the cache
    # fresh so only tests that target it (via their own instances or an
    # explicit monkeypatch) exercise the DB tier.
    import time

    from api import provider_specialty_filters as psf_module

    cache = psf_module._SPECIALTY_RESOLUTION_CACHE
    previous_loaded_at = cache._loaded_at
    cache._loaded_at = time.monotonic()
    yield
    cache._loaded_at = previous_loaded_at
