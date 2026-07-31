# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for the capacity-preflight runtime observation."""

from __future__ import annotations

import os

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError

from db.connection import Database
from process import provider_directory_profile as profile_artifact
from process import provider_directory_profile_capacity_attestation as lease
from process import provider_directory_profile_runtime_observation as runtime
from tests.test_provider_directory_profile_capacity_attestation import (
    _signed_envelope,
    _verify,
)


_POSTGRES_DSN_ENV = "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN"


def _configure_database(monkeypatch: pytest.MonkeyPatch, dsn: str) -> None:
    url = make_url(dsn)
    monkeypatch.setenv("HLTHPRT_DB_DRIVER", "asyncpg")
    monkeypatch.setenv("HLTHPRT_DB_HOST", str(url.host or "127.0.0.1"))
    monkeypatch.setenv("HLTHPRT_DB_PORT", str(url.port or 5432))
    monkeypatch.setenv("HLTHPRT_DB_USER", str(url.username or "postgres"))
    monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(url.password or ""))
    monkeypatch.setenv("HLTHPRT_DB_DATABASE", str(url.database or "postgres"))
    monkeypatch.delenv("HLTHPRT_DB_DATABASE_OVERRIDE", raising=False)
    monkeypatch.setenv("HLTHPRT_DB_POOL_MIN_SIZE", "1")
    monkeypatch.setenv("HLTHPRT_DB_POOL_MAX_SIZE", "2")


def _lease_for_runtime_observation(
    observation: dict[str, object],
):
    def bind_runtime(body):
        runtime_witness = body["runtime_witness"]
        for field_name in (
            runtime.CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS
        ):
            runtime_witness[field_name] = observation[field_name]
        body["runtime_witness_sha256"] = (
            lease.capacity_runtime_witness_sha256(
                runtime_witness,
                body["deployment_witness"],
            )
        )

    return _verify(_signed_envelope(body_mutator=bind_runtime))


def _assert_runtime_bound_lease_replay(observation) -> None:
    verified_lease = _lease_for_runtime_observation(observation)
    runtime.assert_capacity_lease_matches_runtime_observation(
        verified_lease,
        observation,
    )
    foreign_runtime_by_field = {
        **observation,
        "healthcare_source_commit": "e" * 40,
    }
    foreign_lease = _lease_for_runtime_observation(
        foreign_runtime_by_field
    )
    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="capacity_lease_runtime_mismatch",
    ):
        runtime.assert_capacity_lease_matches_runtime_observation(
            foreign_lease,
            observation,
        )


@pytest.mark.asyncio
async def test_runtime_observation_reads_migrated_postgres_snapshot(
    monkeypatch,
):
    """The real query must bind the one migration head in a read-only tx."""

    dsn = os.getenv(_POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"{_POSTGRES_DSN_ENV} is required")
    _configure_database(monkeypatch, dsn)
    monkeypatch.setattr(
        runtime,
        "build_baked_healthcare_source_commit",
        lambda: "d" * 40,
    )
    expected_heads = set(
        ScriptDirectory.from_config(Config("alembic.ini")).get_heads()
    )
    assert len(expected_heads) == 1
    database = Database()
    try:
        await database.connect()
        async with database.transaction():
            await database.status(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;"
            )
            observation = await runtime.observe_profile_runtime(database)
            _assert_runtime_bound_lease_replay(observation)
            transaction_read_only = await database.scalar(
                "SELECT current_setting('transaction_read_only')::boolean;"
            )
    except (OSError, OperationalError):
        pytest.skip("capacity preflight test needs migrated PostgreSQL")
    finally:
        await database.disconnect()

    assert transaction_read_only is True
    assert observation == {
        "contract_id": runtime.PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        "healthcare_source_commit": "d" * 40,
        "profile_migration_revision": next(iter(expected_heads)),
        "profile_schema_version": profile_artifact.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": (
            profile_artifact.PROFILE_BUILD_STRATEGY_VERSION
        ),
        "postgres_server_version_num": observation[
            "postgres_server_version_num"
        ],
    }
    assert observation["postgres_server_version_num"] >= 180000
