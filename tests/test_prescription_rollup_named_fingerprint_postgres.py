# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rollups stored before the upgrade keep serving only after their aggregate is verified."""

import importlib.util
from pathlib import Path

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import text

from tests.provider_directory_profile_delta_test_support import _delta_database

_VERSIONS = Path(__file__).parents[1] / "alembic" / "versions"


def _migration(prefix):
    (path,) = _VERSIONS.glob(prefix + "_*.py")
    spec = importlib.util.spec_from_file_location("rollup_" + prefix, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def _upgrade(database, prefix):
    def apply(sync):
        with Operations.context(MigrationContext.configure(sync)):
            _migration(prefix).upgrade()

    async with database.engine.begin() as connection:
        await connection.run_sync(apply)


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_after_build", [False, True])
async def test_upgrade_rebinds_only_a_verified_legacy_rollup(monkeypatch, changed_after_build):
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    async with _delta_database(monkeypatch) as (database, schema):
        provider = f'"{schema}".pricing_provider_prescription'
        rollup = f'"{schema}".pricing_provider_rx_rollup'
        await database.status(
            f"CREATE TABLE {provider} (year integer,rx_code_system varchar,rx_code varchar,rx_name varchar,"
            "generic_name varchar,brand_name varchar,total_claims double precision,total_drug_cost numeric,"
            "total_benes double precision)"
        )
        await database.status(
            f"INSERT INTO {provider} VALUES (2024,'RXNORM','1','Alpha',NULL,NULL,2,3.5,1),"
            "(2024,'RXNORM','1','Alpha',NULL,NULL,4,1.5,2),(2024,'RXNORM','2','Beta','beta',NULL,1,1,1)"
        )
        await _upgrade(database, "20260820140000")
        # The previous writer stored the provider relation number.
        await database.status(f"UPDATE {rollup} SET source_relation_fingerprint=to_regclass('{provider}')::oid::text")
        if changed_after_build:
            await database.status(f"INSERT INTO {provider} VALUES (2024,'RXNORM','2','Beta','beta',NULL,5,5,5)")
        await _upgrade(database, "20261115100000")
        fingerprints = await database.all(f"SELECT DISTINCT source_relation_fingerprint FROM {rollup}")
        if changed_after_build:
            assert len(fingerprints) == 1 and fingerprints[0][0].isdigit()
        else:
            assert [fingerprint[0] for fingerprint in fingerprints] == [schema + ".pricing_provider_prescription"]
        assert await database.scalar(f"SELECT count(*) FROM {rollup}") == 2
