# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Credential-preserving PostgreSQL test connections without a database."""

from types import SimpleNamespace

import pytest
from sqlalchemy.engine import URL, make_url

from tests.hospital_price_packed_storage_postgres import _driver_dsn


@pytest.mark.parametrize(("password", "host"), [
    ("synthetic-password", "127.0.0.1"), ("synthetic@:/%+", "postgres"),
])
def test_postgres_driver_dsn_preserves_credentials(password, host) -> None:
    """Keep test credentials and URL escaping when connecting with asyncpg."""
    database_url = URL.create(
        "postgresql+asyncpg", username="fixture", password=password,
        host=host, port=5432, database="hospital_price_schema_test_fixture",
    )
    rendered = _driver_dsn(SimpleNamespace(database_url=database_url))
    assert make_url(rendered) == database_url.set(drivername="postgresql")
