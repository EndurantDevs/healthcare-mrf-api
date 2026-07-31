# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Schema, identity, and lease fixtures for committed replay tests."""

from __future__ import annotations

import dataclasses
import datetime
import hashlib
import importlib
import json
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import null
from sqlalchemy.exc import DBAPIError
from sqlalchemy.schema import MetaData

from db.models.system import (
    ProviderDirectoryProfileBuildCheckpoint,
    ProviderDirectoryProfileCapacityLeaseConsumption,
    ProviderDirectoryProfileDeltaReceipt,
    ProviderDirectoryProfileServingGeneration,
)
from process import provider_directory_profile as profile
from process import provider_directory_profile_capacity as capacity
from process import provider_directory_profile_capacity_attestation as lease
from process.provider_directory_profile_capacity_attestation_contract import (
    CapacityLeaseConsumptionBinding,
)
from process.provider_directory_profile_selection_contract import (
    ProviderDirectoryProfileExecution,
    ProviderDirectoryProfileSelectionAttestation,
)
from tests.test_provider_directory_profile_capacity import _geometry_payload
from tests.test_provider_directory_profile_capacity_attestation import (
    _signed_envelope,
    _trust,
)
from tests.provider_directory_profile_delta_test_support import _delta_database


importer = importlib.import_module("process.provider_directory_fhir")
UTC = datetime.timezone.utc


async def _create_replay_import_run_table(database, schema) -> None:
    """Create the minimal import-run relation used by replay lookup."""
    await database.status(
        f"""
        CREATE TABLE {profile.qualified_table(schema, "import_run")} (
            run_id varchar(64) PRIMARY KEY,
            importer varchar(64) NOT NULL,
            status varchar(32) NOT NULL
        );
        """
    )


async def _create_replay_model_tables(database, schema):
    """Create control models and return the three seeded table objects."""
    checkpoint_table = (
        ProviderDirectoryProfileBuildCheckpoint.__table__.to_metadata(
            MetaData(),
            schema=schema,
        )
    )
    await database.create_table(checkpoint_table)
    tables = []
    for model in (
        ProviderDirectoryProfileServingGeneration,
        ProviderDirectoryProfileDeltaReceipt,
        ProviderDirectoryProfileCapacityLeaseConsumption,
    ):
        table = model.__table__.to_metadata(MetaData(), schema=schema)
        await database.create_table(table)
        tables.append(table)
    return tuple(tables)


def _receipt_guard_statements(schema: str) -> tuple[str, ...]:
    """Return immutable delta-receipt guard DDL."""
    receipt_ref = profile.qualified_table(
        schema,
        ProviderDirectoryProfileDeltaReceipt.__tablename__,
    )
    guard_function_ref = profile.qualified_table(
        schema,
        "provider_directory_profile_delta_receipt_immutable_test",
    )
    return (
        f"""
        CREATE FUNCTION {guard_function_ref}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'provider_directory_profile_delta_receipt_immutable';
        END;
        $$;
        """,
        f"""
        CREATE TRIGGER provider_directory_profile_delta_receipt_write_guard
        BEFORE UPDATE OR DELETE ON {receipt_ref}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {guard_function_ref}();
        """,
        f"""
        CREATE TRIGGER provider_directory_profile_delta_receipt_truncate_guard
        BEFORE TRUNCATE ON {receipt_ref}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {guard_function_ref}();
        """,
        f"""
        ALTER TABLE {receipt_ref}
        ENABLE ALWAYS TRIGGER
            provider_directory_profile_delta_receipt_write_guard;
        """,
        f"""
        ALTER TABLE {receipt_ref}
        ENABLE ALWAYS TRIGGER
            provider_directory_profile_delta_receipt_truncate_guard;
        """,
    )


def _capacity_guard_statements(schema: str) -> tuple[str, ...]:
    """Return immutable capacity-consumption guard DDL."""
    capacity_consumption_ref = profile.qualified_table(
        schema,
        ProviderDirectoryProfileCapacityLeaseConsumption.__tablename__,
    )
    capacity_guard_function_ref = profile.qualified_table(
        schema,
        "provider_directory_profile_capacity_immutable_test",
    )
    return (
        f"""
        CREATE FUNCTION {capacity_guard_function_ref}()
        RETURNS trigger
        LANGUAGE plpgsql
        AS $$
        BEGIN
            RAISE EXCEPTION
                'provider_directory_profile_capacity_consumption_immutable';
        END;
        $$;
        """,
        f"""
        CREATE TRIGGER provider_directory_profile_capacity_write_guard
        BEFORE UPDATE OR DELETE ON {capacity_consumption_ref}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {capacity_guard_function_ref}();
        """,
        f"""
        CREATE TRIGGER provider_directory_profile_capacity_truncate_guard
        BEFORE TRUNCATE ON {capacity_consumption_ref}
        FOR EACH STATEMENT
        EXECUTE FUNCTION {capacity_guard_function_ref}();
        """,
        f"""
        ALTER TABLE {capacity_consumption_ref}
        ENABLE ALWAYS TRIGGER
            provider_directory_profile_capacity_write_guard;
        """,
        f"""
        ALTER TABLE {capacity_consumption_ref}
        ENABLE ALWAYS TRIGGER
            provider_directory_profile_capacity_truncate_guard;
        """,
    )


def _replay_immutable_guard_statements(schema: str) -> tuple[str, ...]:
    """Return all immutable replay-control guard DDL."""
    return _receipt_guard_statements(schema) + _capacity_guard_statements(
        schema
    )


async def _create_replay_target_tables(database, schema) -> None:
    """Create the serving evidence and profile target relations."""
    for statement in (
        profile.profile_evidence_table_sql(
            schema,
            profile.PROFILE_EVIDENCE_TABLE,
            logged=True,
        ),
        profile.profile_table_sql(
            schema,
            profile.PROFILE_TABLE,
            logged=True,
        ),
    ):
        await database.status(statement)


async def _create_replay_tables(database, schema):
    """Create the complete disposable replay schema."""
    await _create_replay_import_run_table(database, schema)
    tables = await _create_replay_model_tables(database, schema)
    for statement in _replay_immutable_guard_statements(schema):
        await database.status(statement)
    await _create_replay_target_tables(database, schema)
    return tables


def _serving_state_for_identity(
    evidence_target_oid: int,
    profile_target_oid: int,
):
    return importer._ProviderDirectoryProfileServingState(
        status="published",
        operation="publish",
        control_generation=1,
        generation_id="pdprofile_" + "1" * 32,
        selection_proof_id="1" * 64,
        authority_revision=1,
        profile_schema_version=profile.PROFILE_SCHEMA_VERSION,
        profile_strategy_version=profile.PROFILE_BUILD_STRATEGY_VERSION,
        source_vector=(),
        source_vector_hash=(
            importer._provider_directory_profile_source_vector_hash(())
        ),
        source_context_vector=(),
        source_context_vector_hash=(
            importer._provider_directory_profile_source_context_vector_hash(
                ()
            )
        ),
        executable_plan_hash="1" * 64,
        evidence_target_oid=evidence_target_oid,
        profile_target_oid=profile_target_oid,
        evidence_rows=0,
        profile_rows=0,
        profile_as_of="2026-07-30",
        published_at="2026-07-30T00:00:00+00:00",
    )


def _capacity_envelope(
    geometry_hash: str,
    database_identity,
    accepted_at: datetime.datetime,
):
    observed_at = accepted_at - datetime.timedelta(seconds=2)
    issued_at = accepted_at - datetime.timedelta(seconds=1)
    deadline = accepted_at + datetime.timedelta(hours=7)
    expires_at = accepted_at + datetime.timedelta(hours=8)
    data_digest = "31" * 32
    temp_digest = (
        data_digest
        if database_identity.temp_tablespace_oid
        == database_identity.tablespace_oid
        else "32" * 32
    )
    wal_digest = "33" * 32

    def mutate(body):
        body.update(
            {
                "capacity_geometry_hash": geometry_hash,
                "database_name": database_identity.database_name,
                "database_oid": database_identity.database_oid,
                "database_system_identifier": (
                    database_identity.database_system_identifier
                ),
                "observed_at": _utc_text(observed_at),
                "issued_at": _utc_text(issued_at),
                "max_build_deadline": _utc_text(deadline),
                "expires_at": _utc_text(expires_at),
                "reservation_id": "pd-capacity-replay-postgres",
            }
        )
        body["tablespaces"] = [
            {
                "tablespace_name": database_identity.tablespace_name,
                "tablespace_oid": database_identity.tablespace_oid,
                "usage": "data",
                "volume_digest": data_digest,
            },
            {
                "tablespace_name": (
                    database_identity.temp_tablespace_name
                ),
                "tablespace_oid": database_identity.temp_tablespace_oid,
                "usage": "temp",
                "volume_digest": temp_digest,
            },
        ]
        body["volumes"] = [
            _volume("data", data_digest, 200_000_000_000),
            _volume("temp", temp_digest, 20_000_000_000),
            _volume("wal", wal_digest, 150_000_000_000),
        ]

    return _signed_envelope(body_mutator=mutate)


def _volume(volume_class: str, digest: str, reserved_bytes: int):
    return {
        "available_after_all_reservations_bytes": 700_000_000_000,
        "available_bytes": 1_000_000_000_000,
        "reserved_bytes": reserved_bytes,
        "volume_class": volume_class,
        "volume_digest": digest,
    }


def _utc_text(value: datetime.datetime) -> str:
    return value.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _execution(
    *,
    proof_id: str,
    profile_input_digest: str,
    source_id: str,
    dataset_id: str,
    capacity_attestation,
) -> ProviderDirectoryProfileExecution:
    attestation = ProviderDirectoryProfileSelectionAttestation(
        proof_id=proof_id,
        node_id="node-test",
        catalog_digest="8" * 64,
        selection_fingerprint="9" * 64,
        authority_revision=7,
        profile_schema_version=profile.PROFILE_SCHEMA_VERSION,
        profile_strategy_version=profile.PROFILE_BUILD_STRATEGY_VERSION,
        source_context_digest="a" * 64,
        profile_input_digest=profile_input_digest,
        operation="publish",
        pairs=(
            {
                "source_id": source_id,
                "dataset_id": dataset_id,
            },
        ),
        payload={},
    )
    return ProviderDirectoryProfileExecution(
        attestation=attestation,
        generation=7,
        capacity_attestation=capacity_attestation,
    )
