# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proofs for Profile capacity preflight state."""

from __future__ import annotations

import asyncio
import datetime
import importlib
import json
import os
from pathlib import Path
import types
import uuid
from unittest.mock import AsyncMock

import pytest
from alembic.config import Config
from alembic.migration import MigrationContext
from alembic.operations import Operations
from alembic.script import ScriptDirectory
from sqlalchemy.engine import make_url
from sqlalchemy.exc import OperationalError
from sqlalchemy.schema import MetaData

from db.connection import Database
from db.models.system import (
    ProviderDirectoryProfileCapacityLeaseConsumption,
)
from process import provider_directory_profile as profile
from process import provider_directory_profile as profile_artifact
from process import provider_directory_profile_capacity_preflight_contract as contract
from process import provider_directory_profile_capacity_attestation as lease
from process import provider_directory_profile_runtime_observation as runtime
from tests.provider_directory_profile_delta_schema_fixtures import (
    _create_delta_contract_tables,
)
from tests.provider_directory_profile_delta_test_support import (
    _delta_database,
)
from tests.provider_directory_profile_capacity_v2_migration_support import (
    capacity_constraint_definition,
)
from tests.test_provider_directory_profile_capacity_preflight import (
    _serving_state,
)
from tests.test_provider_directory_profile_capacity_runtime import (
    _limits_payload,
)
from tests.test_provider_directory_profile_capacity_attestation import (
    _signed_envelope,
    _verify,
)


_POSTGRES_DSN_ENV = "HLTHPRT_PROVIDER_DIRECTORY_PROFILE_POSTGRES_DSN"
UTC = datetime.timezone.utc
importer = importlib.import_module("process.provider_directory_fhir")
_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic/versions/"
    "20260811010000_provider_directory_profile_capacity_preflight_receipt.py"
)
_MIGRATION_SPEC = importlib.util.spec_from_file_location(
    "provider_directory_profile_capacity_preflight_receipt_migration",
    _MIGRATION_PATH,
)
assert _MIGRATION_SPEC is not None and _MIGRATION_SPEC.loader is not None
receipt_migration = importlib.util.module_from_spec(_MIGRATION_SPEC)
_MIGRATION_SPEC.loader.exec_module(receipt_migration)


async def _run_receipt_migration(
    database: Database,
    *,
    upgrade: bool,
) -> None:
    """Run the isolated receipt revision through real Alembic operations."""

    async with database.engine.begin() as connection:

        def migrate(sync_connection) -> None:
            migration_context = MigrationContext.configure(sync_connection)
            operations = Operations(migration_context)
            original_operations = receipt_migration.op
            receipt_migration.op = operations
            try:
                if upgrade:
                    receipt_migration.upgrade()
                else:
                    receipt_migration.downgrade()
            finally:
                receipt_migration.op = original_operations

        await connection.run_sync(migrate)


async def _prepare_receipt_schema(
    monkeypatch: pytest.MonkeyPatch,
    database: Database,
    schema: str,
) -> None:
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    monkeypatch.setattr(importer, "db", database)
    capacity_ref = profile.qualified_table(
        schema,
        ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__,
    )
    await database.status(f"DROP TABLE IF EXISTS {capacity_ref} CASCADE;")
    capacity_table = (
        ProviderDirectoryProfileCapacityLeaseConsumption.__table__.to_metadata(
            MetaData(), schema=schema
        )
    )
    await database.create_table(capacity_table)
    await database.status(
        f"ALTER TABLE {capacity_ref} DROP CONSTRAINT "
        "pd_profile_capacity_consumption_values_check;"
    )
    v2_condition = receipt_migration._capacity_consumption_check(
        receipt_migration._capacity_contract_predicate(
            receipt_migration._V1_CONTRACT,
            receipt_migration._V2_CONTRACT,
        )
    )
    await database.status(
        f"ALTER TABLE {capacity_ref} ADD CONSTRAINT "
        "pd_profile_capacity_consumption_values_check "
        f"CHECK ({v2_condition});"
    )
    await _run_receipt_migration(database, upgrade=True)


def _digest(label: str) -> str:
    return contract.preflight_domain_sha256(
        "healthporta.test.provider-profile-capacity-preflight.v1",
        {"label": label},
    )


def _receipt_values(
    label: str,
    *,
    issued_at: datetime.datetime,
    expires_at: datetime.datetime,
    request_nonce: str | None = None,
) -> dict[str, object]:
    limits_payload = _limits_payload()
    limits_sha256 = contract.capacity_limits_sha256(limits_payload)
    receipt_by_field = {
        "contract_id": contract.CAPACITY_PREFLIGHT_CONTRACT_ID,
        "capacity_limits": limits_payload,
        "capacity_limits_sha256": limits_sha256,
        "test_label": label,
    }
    return {
        "receipt_sha256": _digest(label + ":receipt"),
        "request_nonce": request_nonce or _digest(label + ":nonce"),
        "request_sha256": _digest(label + ":request"),
        "control_plane_receipt_sha256": _digest(label + ":control"),
        "contract_id": contract.CAPACITY_PREFLIGHT_CONTRACT_ID,
        "request_contract_id": contract.CAPACITY_PREFLIGHT_REQUEST_CONTRACT_ID,
        "limits_contract_id": limits_payload["contract_id"],
        "selection_proof_id": _digest(label + ":selection"),
        "profile_input_digest": _digest(label + ":input"),
        "control_generation": 1,
        "profile_schema_version": profile.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": profile.PROFILE_BUILD_STRATEGY_VERSION,
        "materialization_mode": "source_delta",
        "limits_sha256": limits_sha256,
        "capacity_geometry_hash": _digest(label + ":geometry"),
        "serving_preflight_sha256": _digest(label + ":serving"),
        "quiescence_sha256": _digest(label + ":quiescence"),
        "receipt_json": json.dumps(
            receipt_by_field,
            sort_keys=True,
            separators=(",", ":"),
        ),
        "issued_at": issued_at,
        "expires_at": expires_at,
        "created_at": issued_at,
    }


async def _insert_receipt(
    database: Database,
    schema: str,
    values: dict[str, object],
) -> int:
    receipt_ref = profile.qualified_table(
        schema,
        "provider_directory_profile_capacity_preflight_receipt",
    )
    fields = tuple(values)
    value_sql = ", ".join(
        (
            "CAST(:receipt_json AS jsonb)"
            if field_name == "receipt_json"
            else ":" + field_name
        )
        for field_name in fields
    )
    return int(
        await database.status(
            f"INSERT INTO {receipt_ref} "
            f"({', '.join(fields)}) VALUES ({value_sql});",
            **values,
        )
        or 0
    )


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
        for field_name in runtime.CAPACITY_LEASE_LOCALLY_VERIFIED_RUNTIME_FIELDS:
            runtime_witness[field_name] = observation[field_name]
        body["runtime_witness_sha256"] = lease.capacity_runtime_witness_sha256(
            runtime_witness,
            body["deployment_witness"],
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
    foreign_lease = _lease_for_runtime_observation(foreign_runtime_by_field)
    with pytest.raises(
        runtime.ProviderDirectoryProfileRuntimeObservationError,
        match="capacity_lease_runtime_mismatch",
    ):
        runtime.assert_capacity_lease_matches_runtime_observation(
            foreign_lease,
            observation,
        )


@pytest.mark.asyncio
async def test_receipt_migration_lifecycle_and_trigger_drift(
    monkeypatch,
):
    """Install, fingerprint, downgrade, and reinstall the empty ledger."""

    async with _delta_database(monkeypatch) as (database, schema):
        await _prepare_receipt_schema(monkeypatch, database, schema)
        receipt_ref = profile.qualified_table(
            schema,
            "provider_directory_profile_capacity_preflight_receipt",
        )
        layout = await importer._profile_capacity_preflight_receipt_layout(schema)
        assert "capacity-lease-v3" in await capacity_constraint_definition(
            database, schema
        )
        assert layout["relation_oid"] > 0
        assert len(layout["main_index_pages"]) == 4
        assert layout["toastable_column_count"] >= 1

        await database.status(
            f"ALTER TABLE {receipt_ref} DISABLE TRIGGER "
            "pd_profile_capacity_preflight_update_guard;"
        )
        with pytest.raises(
            importer.ProviderDirectoryArtifactBuildStale,
            match="preflight_trigger_shape_changed",
        ):
            await importer._profile_capacity_preflight_receipt_layout(schema)
        await database.status(
            f"ALTER TABLE {receipt_ref} ENABLE ALWAYS TRIGGER "
            "pd_profile_capacity_preflight_update_guard;"
        )

        await _run_receipt_migration(database, upgrade=False)
        downgraded_capacity_constraint = await capacity_constraint_definition(
            database, schema
        )
        assert "capacity-lease-v2" in downgraded_capacity_constraint
        assert "capacity-lease-v3" not in downgraded_capacity_constraint
        assert (
            await database.scalar(
                "SELECT to_regclass(:relation_ref) IS NULL;",
                relation_ref=receipt_ref,
            )
            is True
        )
        await _run_receipt_migration(database, upgrade=True)
        reinstalled = await importer._profile_capacity_preflight_receipt_layout(schema)
        assert reinstalled["relation_oid"] != layout["relation_oid"]
        assert "capacity-lease-v3" in await capacity_constraint_definition(
            database, schema
        )


async def _consume_receipt_once(
    database: Database,
    receipt_ref: str,
    receipt_sha256: str,
    now: datetime.datetime,
    suffix: str,
) -> int:
    async with database.transaction():
        return int(
            await database.status(
                f"UPDATE {receipt_ref} "
                "SET consumed_at = :consumed_at, consumed_run_id = :run_id, "
                "consumed_attestation_id = :attestation_id "
                "WHERE receipt_sha256 = :receipt_sha256 "
                "AND consumed_at IS NULL AND expires_at > :consumed_at;",
                consumed_at=now + datetime.timedelta(seconds=1),
                run_id="run_" + suffix * 32,
                attestation_id=suffix * 64,
                receipt_sha256=receipt_sha256,
            )
            or 0
        )


@pytest.mark.asyncio
async def test_receipt_single_use_concurrency_and_limits_immutability(monkeypatch):
    """Only one concurrent admission may consume the limits-bound row."""

    async with _delta_database(monkeypatch) as (database, schema):
        await _prepare_receipt_schema(monkeypatch, database, schema)
        now = datetime.datetime.now(tz=UTC).replace(microsecond=0)
        receipt_values_by_field = _receipt_values(
            uuid.uuid4().hex,
            issued_at=now,
            expires_at=now + datetime.timedelta(hours=1),
        )
        assert await _insert_receipt(database, schema, receipt_values_by_field) == 1
        receipt_ref = profile.qualified_table(
            schema,
            "provider_directory_profile_capacity_preflight_receipt",
        )
        stored_limits_sha256 = await database.scalar(
            f"SELECT limits_sha256 FROM {receipt_ref} "
            "WHERE receipt_sha256 = :receipt_sha256;",
            receipt_sha256=receipt_values_by_field["receipt_sha256"],
        )
        assert stored_limits_sha256 == receipt_values_by_field["limits_sha256"]
        with pytest.raises(Exception, match="preflight_update_invalid"):
            await database.status(
                f"UPDATE {receipt_ref} SET limits_sha256 = :changed "
                "WHERE receipt_sha256 = :receipt_sha256;",
                changed=_digest("changed-limits"),
                receipt_sha256=receipt_values_by_field["receipt_sha256"],
            )

        receipt_sha256 = receipt_values_by_field["receipt_sha256"]
        consumed_counts = await asyncio.gather(
            _consume_receipt_once(database, receipt_ref, receipt_sha256, now, "a"),
            _consume_receipt_once(database, receipt_ref, receipt_sha256, now, "b"),
        )
        assert sorted(consumed_counts) == [0, 1]
        stored = await database.first(
            f"SELECT consumed_run_id, consumed_attestation_id "
            f"FROM {receipt_ref} "
            "WHERE receipt_sha256 = :receipt_sha256;",
            receipt_sha256=receipt_values_by_field["receipt_sha256"],
        )
        assert stored.consumed_run_id in {
            "run_" + "a" * 32,
            "run_" + "b" * 32,
        }
        with pytest.raises(Exception, match="history_immutable"):
            await database.status(
                f"DELETE FROM {receipt_ref} " "WHERE receipt_sha256 = :receipt_sha256;",
                receipt_sha256=receipt_values_by_field["receipt_sha256"],
            )


@pytest.mark.asyncio
async def test_preflight_state_lock_serializes_concurrent_issuance(
    monkeypatch,
):
    """The real advisory/table fence admits only one issuer transaction."""

    async with _delta_database(monkeypatch) as (database, schema):
        await _create_delta_contract_tables(
            database,
            schema,
            evidence_stage="preflight_evidence_stage",
            profile_stage="preflight_profile_stage",
            affected_stage="preflight_affected_stage",
        )
        await _prepare_receipt_schema(monkeypatch, database, schema)
        locked = asyncio.Event()
        release = asyncio.Event()

        async def holder() -> None:
            async with database.transaction():
                await importer._lock_profile_capacity_preflight_state(schema)
                locked.set()
                await release.wait()

        holder_task = asyncio.create_task(holder())
        await locked.wait()
        try:
            assert (
                await database.scalar(
                    "SELECT pg_try_advisory_xact_lock("
                    "hashtextextended(:lock_key, 0));",
                    lock_key=importer._PROFILE_CAPACITY_PREFLIGHT_LOCK_KEY,
                )
                is False
            )
        finally:
            release.set()
            await holder_task
