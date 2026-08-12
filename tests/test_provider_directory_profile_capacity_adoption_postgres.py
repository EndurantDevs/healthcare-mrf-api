# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proofs for legacy serving-state adoption."""

from __future__ import annotations

import asyncio
import datetime
import os
from pathlib import Path
import sys
import types
from unittest.mock import AsyncMock

import pytest
from alembic.config import Config
from alembic.script import ScriptDirectory

from process import provider_directory_profile as profile
from process import provider_directory_profile as profile_artifact
from process import provider_directory_profile_capacity_attestation as lease
from process import provider_directory_profile_runtime_observation as runtime
from tests.provider_directory_profile_capacity_runtime_test_support import (
    PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION,
)
from tests.provider_directory_profile_delta_schema_fixtures import (
    _create_delta_contract_tables,
)
from tests.provider_directory_profile_delta_test_support import _delta_database
from tests.test_provider_directory_profile_capacity_preflight import _serving_state
from tests.test_provider_directory_profile_capacity_preflight_postgres import (
    UTC,
    _POSTGRES_DSN_ENV,
    _assert_runtime_bound_lease_replay,
    _configure_database,
    importer,
)


_ALEMBIC_ADOPTION_BASE_REVISION = "20260610143000_address_checksums_bigint"


def _install_invalid_lease_admission_stubs(monkeypatch, virtual_state):
    monkeypatch.setattr(
        importer, "_assert_profile_capacity_run_unconsumed", AsyncMock()
    )
    monkeypatch.setattr(
        importer,
        "_profile_capacity_preflight_serving",
        AsyncMock(
            return_value=importer._ProfileCapacityPreflightServing(
                state=virtual_state,
                payload={"resolution": "legacy_adoption"},
                payload_sha256="1" * 64,
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_identity",
        AsyncMock(return_value=types.SimpleNamespace()),
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_workload",
        AsyncMock(
            return_value=types.SimpleNamespace(
                database_identity=types.SimpleNamespace()
            )
        ),
    )
    monkeypatch.setattr(
        importer, "_profile_admission_inputs", lambda *_args: types.SimpleNamespace()
    )
    monkeypatch.setattr(
        importer,
        "_profile_admission_geometry",
        lambda *_args: types.SimpleNamespace(geometry=types.SimpleNamespace()),
    )
    monkeypatch.setattr(
        importer, "_validated_admission_run_id", lambda *_args: "run_" + "a" * 32
    )

    def reject_invalid_lease(*_args):
        raise lease.ProviderDirectoryCapacityLeaseError(
            "invalid_signature", "signature"
        )

    monkeypatch.setattr(importer, "_verified_admission_lease", reject_invalid_lease)


@pytest.mark.asyncio
async def test_invalid_lease_does_not_adopt_missing_serving_generation(monkeypatch):
    """Lease verification precedes the serialized legacy adoption insert."""

    async with _delta_database(monkeypatch) as (database, schema):
        await _create_delta_contract_tables(
            database,
            schema,
            evidence_stage="invalid_lease_evidence_stage",
            profile_stage="invalid_lease_profile_stage",
            affected_stage="invalid_lease_affected_stage",
        )
        monkeypatch.setattr(importer, "db", database)
        _install_invalid_lease_admission_stubs(monkeypatch, _serving_state())
        with pytest.raises(
            lease.ProviderDirectoryCapacityLeaseError, match="invalid_signature"
        ):
            await importer._admit_provider_directory_profile_capacity(
                run_id="run_" + "a" * 32,
                control_run_id="run_" + "a" * 32,
                execution=types.SimpleNamespace(),
                fence=types.SimpleNamespace(),
                resource_fence=types.SimpleNamespace(),
                artifact_resource_types=frozenset({"Practitioner"}),
            )
        serving_ref = profile.qualified_table(
            schema, "provider_directory_profile_serving_generation"
        )
        assert (
            await database.scalar(f"SELECT count(*)::bigint FROM {serving_ref};") == 0
        )


def _adoption_candidate(template):
    return importer._ProfileAdoptionCandidate(
        generation_id=template.generation_id,
        selection_result={
            "operation": "publish",
            "generation": template.control_generation,
            "proof_id": template.selection_proof_id,
            "authority_revision": template.authority_revision,
            "profile_schema_version": template.profile_schema_version,
            "profile_strategy_version": template.profile_strategy_version,
        },
        source_vector=template.source_vector,
        source_context_vector=template.source_context_vector,
        profile_as_of=template.profile_as_of,
        profile_target_oid=template.profile_target_oid,
        evidence_target_oid=template.evidence_target_oid,
        profile_rows=template.profile_rows,
        evidence_rows=template.evidence_rows,
        published_at=template.published_at,
        executable_plan_hash=template.executable_plan_hash,
    )


def _alembic_head_environment(dsn: str, schema: str) -> dict[str, str]:
    """Return an explicit, schema-isolated Alembic subprocess environment."""

    from sqlalchemy.engine import make_url

    url = make_url(dsn)
    environment = os.environ.copy()
    values_by_name = {
        "DB_HOST": str(url.host or "127.0.0.1"),
        "DB_PORT": str(url.port or 5432),
        "DB_USER": str(url.username or "postgres"),
        "DB_PASSWORD": str(url.password or ""),
        "DB_DATABASE": str(url.database or "postgres"),
        "DB_SCHEMA": schema,
    }
    environment.update(values_by_name)
    environment.update(
        {f"HLTHPRT_{name}": value for name, value in values_by_name.items()}
    )
    return environment


async def _upgrade_disposable_schema_to_head(dsn: str, schema: str) -> None:
    """Apply the CI-supported migration baseline through repository head."""

    environment = _alembic_head_environment(dsn, schema)
    for alembic_arguments in (
        ("stamp", _ALEMBIC_ADOPTION_BASE_REVISION),
        ("upgrade", "head"),
    ):
        process = await asyncio.create_subprocess_exec(
            sys.executable,
            "-m",
            "alembic",
            *alembic_arguments,
            cwd=str(Path(__file__).resolve().parents[1]),
            env=environment,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
        )
        try:
            return_code = await asyncio.wait_for(process.wait(), timeout=180)
        except TimeoutError:
            process.kill()
            await process.wait()
            raise AssertionError("disposable_alembic_head_upgrade_timed_out") from None
        assert return_code == 0, "disposable_alembic_head_upgrade_failed"


@pytest.mark.asyncio
async def test_missing_state_adoption_is_exact_and_concurrency_safe(monkeypatch):
    """Project a missing legacy row, then let two adopters converge once."""

    async with _delta_database(monkeypatch) as (database, schema):
        await _create_delta_contract_tables(
            database,
            schema,
            evidence_stage="adoption_evidence_stage",
            profile_stage="adoption_profile_stage",
            affected_stage="adoption_affected_stage",
        )
        monkeypatch.setattr(importer, "db", database)
        template = _serving_state(
            published_at=datetime.datetime(2026, 8, 9, 8, 0, tzinfo=UTC)
        )
        candidate_loader = AsyncMock(return_value=_adoption_candidate(template))
        monkeypatch.setattr(
            importer, "_profile_adoption_candidate_from_legacy", candidate_loader
        )
        projected = await importer._profile_capacity_preflight_serving(schema)
        serving_ref = profile.qualified_table(
            schema, "provider_directory_profile_serving_generation"
        )
        assert projected.payload["resolution"] == "legacy_adoption"
        assert await database.scalar(f"SELECT count(*) FROM {serving_ref};") == 0
        adopted_states = await asyncio.gather(
            importer._adopt_provider_directory_profile_serving_generation(schema),
            importer._adopt_provider_directory_profile_serving_generation(schema),
        )
        assert adopted_states[0] == adopted_states[1]
        assert await database.scalar(f"SELECT count(*) FROM {serving_ref};") == 1
        incumbent = await importer._profile_capacity_preflight_serving(schema)
        assert incumbent.payload["resolution"] == "existing"
        assert incumbent.state == adopted_states[0]


@pytest.mark.asyncio
async def test_adoption_target_oid_drift_fails_before_insert(monkeypatch):
    """A drop/recreate between target reads must trip the exact OID fence."""

    async with _delta_database(monkeypatch) as (database, schema):
        await _create_delta_contract_tables(
            database,
            schema,
            evidence_stage="oid_evidence_stage",
            profile_stage="oid_profile_stage",
            affected_stage="oid_affected_stage",
        )
        monkeypatch.setattr(importer, "db", database)
        original_relation_oid = importer._provider_directory_relation_oid
        relation_read_counts = [0]

        async def relation_oid_with_drift(schema_name, relation_name):
            relation_oid = await original_relation_oid(schema_name, relation_name)
            relation_read_counts[0] += 1
            if relation_read_counts[0] == 2:
                profile_ref = profile.qualified_table(schema, profile.PROFILE_TABLE)
                await database.status(f"DROP TABLE {profile_ref};")
                await database.status(
                    profile.profile_table_sql(
                        schema, profile.PROFILE_TABLE, logged=True
                    )
                )
                for statement in profile.profile_index_statements(
                    schema, profile.PROFILE_TABLE, evidence=False
                ):
                    await database.status(statement)
            return relation_oid

        monkeypatch.setattr(
            importer, "_provider_directory_relation_oid", relation_oid_with_drift
        )
        with pytest.raises(RuntimeError, match="adoption_target_changed"):
            await importer._locked_profile_adoption_target_oids(
                schema,
                profile.qualified_table(schema, profile.PROFILE_TABLE),
                profile.qualified_table(schema, profile.PROFILE_EVIDENCE_TABLE),
            )


@pytest.mark.asyncio
async def test_runtime_observation_reads_migrated_postgres_snapshot(monkeypatch):
    """A freshly migrated disposable schema must witness the sole current head."""

    dsn = os.getenv(_POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"{_POSTGRES_DSN_ENV} is required")
    _configure_database(monkeypatch, dsn)
    monkeypatch.setattr(
        runtime, "build_baked_healthcare_source_commit", lambda: "d" * 40
    )
    expected_heads = set(ScriptDirectory.from_config(Config("alembic.ini")).get_heads())
    assert expected_heads == {
        "20260812010000_provider_directory_artifact_selection_receipt"
    }
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setenv("DB_SCHEMA", schema)
        await _upgrade_disposable_schema_to_head(dsn, schema)
        async with database.transaction():
            await database.status(
                "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY;"
            )
            observation = await runtime.observe_profile_runtime(database)
            _assert_runtime_bound_lease_replay(observation)
            transaction_read_only = await database.scalar(
                "SELECT current_setting('transaction_read_only')::boolean;"
            )
    assert transaction_read_only is True
    assert observation == {
        "contract_id": runtime.PROFILE_RUNTIME_OBSERVATION_CONTRACT_ID,
        "healthcare_source_commit": "d" * 40,
        "profile_migration_revision": PROFILE_RUNTIME_WITNESS_MIGRATION_REVISION,
        "profile_schema_version": profile_artifact.PROFILE_SCHEMA_VERSION,
        "profile_strategy_version": profile_artifact.PROFILE_BUILD_STRATEGY_VERSION,
        "postgres_server_version_num": observation["postgres_server_version_num"],
    }
    assert observation["postgres_server_version_num"] >= 180000
