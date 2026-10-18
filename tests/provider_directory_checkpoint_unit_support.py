# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit transaction boundary for tests whose SQL executor is already mocked.

These tests exercise checkpoint SQL and lease/state decisions. Native role,
physical-session, custody and rollback acceptance use the real source factory.
"""

from contextlib import asynccontextmanager
from importlib import import_module
from unittest.mock import AsyncMock

import pytest


@pytest.fixture(autouse=True)
def checkpoint_unit_scope(monkeypatch):
    directory = import_module("process.provider_directory_fhir")

    @asynccontextmanager
    async def transaction(database, **coordinates):
        yield

    monkeypatch.setattr(directory, "source_checkpoint_transaction", transaction)
    monkeypatch.setattr(directory, "require_checkpoint_executor", AsyncMock())
