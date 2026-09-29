# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Keep loader-mocked route tests independent of a running database."""

from contextlib import asynccontextmanager

import pytest

from api.endpoint import npi


@pytest.fixture(autouse=True)
def stub_provider_profile_snapshot(monkeypatch):
    """Stub only the snapshot boundary in tests that already replace database reads."""

    @asynccontextmanager
    async def snapshot(_database, _schema, *, include_detail=False):
        yield None

    monkeypatch.setattr(npi, "provider_profile_read_snapshot", snapshot)
