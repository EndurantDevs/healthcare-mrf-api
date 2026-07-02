# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import os

import pytest

TEST_ENV_DEFAULTS = {
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
}

for key, value in TEST_ENV_DEFAULTS.items():
    os.environ.setdefault(key, value)


@pytest.fixture(autouse=True)
def clear_npi_detail_response_cache():
    from api.endpoint import npi as npi_module

    npi_module._NPI_DETAIL_RESPONSE_CACHE.clear()
    yield
    npi_module._NPI_DETAIL_RESPONSE_CACHE.clear()


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
