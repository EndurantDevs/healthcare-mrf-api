# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind only the fixture's already-enrolled Reader to its own local connection pool."""

from contextlib import asynccontextmanager

from sqlalchemy.engine import make_url

from db.connection import Database
from process import registry_fhir_catalog_registry_reader as readers


@asynccontextmanager
async def configured_catalog_reader(monkeypatch, dsn, role, password):
    database, url = Database(), make_url(dsn)
    with monkeypatch.context() as settings:
        for key, value in {
            "DRIVER": "asyncpg",
            "HOST": url.host,
            "PORT": url.port or 5432,
            "DATABASE_OVERRIDE": url.database,
            "READER_USER": role,
            "READER_PASSWORD": password,
        }.items():
            settings.setenv("HLTHPRT_DB_" + key, str(value))
        settings.setattr(readers, "db", database)
        try:
            yield database
        finally:
            await database.disconnect()
