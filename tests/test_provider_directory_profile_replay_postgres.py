# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""PostgreSQL proof for restart-safe committed Profile replay."""

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
)
from tests.provider_directory_profile_capacity_trust_fixtures import (
    capacity_trust_from_envelope,
)
from tests.provider_directory_profile_delta_test_support import _delta_database


importer = importlib.import_module("process.provider_directory_fhir")
UTC = datetime.timezone.utc
from tests.provider_directory_profile_replay_seed import (
    _ReplaySeed,
    _insert_replay_rows,
)
from tests.provider_directory_profile_replay_test_support import (
    _capacity_envelope,
    _create_replay_tables,
    _execution,
    _serving_state_for_identity,
)


@dataclasses.dataclass(frozen=True)
class _ReplayScenario:
    """Stable identity inputs for same-run and cross-run replay."""

    is_cross_run: bool
    receipt_run_id: str
    requested_run_id: str
    build_id: str
    source_id: str
    dataset_id: str
    source_vector: tuple[tuple[str, str], ...]
    source_context_vector: tuple[tuple[str, str], ...]
    accepted_at: datetime.datetime
    committed_at: datetime.datetime
    proof_id: str
    profile_input_digest: str
    plan_hash: str


def _replay_scenario(is_cross_run: bool) -> _ReplayScenario:
    """Return deterministic replay identities for one test variant."""
    receipt_run_id = "run_" + "6" * 32
    source_id = "pdfhir_payer"
    dataset_id = "dataset-replay"
    accepted_at = datetime.datetime(2026, 7, 29, 0, 0, 2, tzinfo=UTC)
    requested_run_id = "run_" + "8" * 32 if is_cross_run else receipt_run_id
    return _ReplayScenario(
        is_cross_run=is_cross_run,
        receipt_run_id=receipt_run_id,
        requested_run_id=requested_run_id,
        build_id="pdpb_" + "7" * 32,
        source_id=source_id,
        dataset_id=dataset_id,
        source_vector=((source_id, dataset_id),),
        source_context_vector=((source_id, "5" * 64),),
        accepted_at=accepted_at,
        committed_at=accepted_at + datetime.timedelta(hours=1),
        proof_id="3" * 64,
        profile_input_digest="4" * 64,
        plan_hash="2" * 64,
    )


async def _insert_replay_import_runs(
    database,
    schema: str,
    scenario: _ReplayScenario,
) -> None:
    """Insert the receipt owner and optional new requesting run."""
    import_run_ref = profile.qualified_table(schema, "import_run")
    await database.status(
        f"INSERT INTO {import_run_ref} "
        "VALUES (:run_id, 'provider-directory-fhir', :status);",
        run_id=scenario.receipt_run_id,
        status="succeeded" if scenario.is_cross_run else "running",
    )
    if scenario.is_cross_run:
        await database.status(
            f"INSERT INTO {import_run_ref} "
            "VALUES (:run_id, 'provider-directory-fhir', 'running');",
            run_id=scenario.requested_run_id,
        )


async def _replay_target_oids(database, schema: str) -> tuple[int, int]:
    """Resolve exact evidence and profile serving relation OIDs."""
    relation_names = (profile.PROFILE_EVIDENCE_TABLE, profile.PROFILE_TABLE)
    relation_oids = []
    for relation_name in relation_names:
        relation_oids.append(
            int(
                await database.scalar(
                    "SELECT to_regclass(:relation)::oid::bigint;",
                    relation=profile.qualified_table(schema, relation_name),
                )
            )
        )
    return tuple(relation_oids)


def _database_geometry_by_field(database_identity) -> dict[str, object]:
    """Project runtime PostgreSQL identity into capacity geometry fields."""
    identity_fields = (
        "database_system_identifier",
        "database_oid",
        "database_name",
        "tablespace_oid",
        "tablespace_name",
        "postgres_server_version_num",
        "postgres_block_size_bytes",
        "postgres_wal_block_size_bytes",
        "postgres_wal_segment_size_bytes",
        "postgres_full_page_writes",
        "postgres_wal_compression",
        "postgres_wal_level",
        "postgres_wal_log_hints",
        "postgres_data_checksums",
        "postgres_default_toast_compression",
        "postgres_checkpoint_timeout_seconds",
        "postgres_max_wal_size_bytes",
        "evidence_target_storage_fingerprint",
        "profile_target_storage_fingerprint",
        "build_checkpoint_oid",
        "serving_generation_oid",
        "delta_receipt_oid",
        "build_checkpoint_storage_fingerprint",
        "serving_generation_storage_fingerprint",
        "delta_receipt_storage_fingerprint",
    )
    return {
        field_name: getattr(database_identity, field_name)
        for field_name in identity_fields
    }


def _replay_geometry(
    scenario: _ReplayScenario,
    database_identity,
    evidence_target_oid: int,
    profile_target_oid: int,
):
    """Bind selection lineage and exact PostgreSQL identity into geometry."""
    source_hash = importer._provider_directory_profile_source_vector_hash(
        scenario.source_vector
    )
    context_hash = (
        importer._provider_directory_profile_source_context_vector_hash(
            scenario.source_context_vector
        )
    )
    return capacity.validated_capacity_geometry(
        _geometry_payload(
            selection_proof_id=scenario.proof_id,
            profile_input_digest=scenario.profile_input_digest,
            profile_schema_version=profile.PROFILE_SCHEMA_VERSION,
            profile_strategy_version=profile.PROFILE_BUILD_STRATEGY_VERSION,
            executable_plan_hash=scenario.plan_hash,
            current_source_vector_hash="0" * 64,
            desired_source_vector_hash=source_hash,
            current_context_vector_hash="1" * 64,
            desired_context_vector_hash=context_hash,
            sql_contract_digest=(
                importer._provider_directory_profile_sql_contract_digest()
            ),
            evidence_target_oid=evidence_target_oid,
            profile_target_oid=profile_target_oid,
            **_database_geometry_by_field(database_identity),
        )
    )


async def _seed_committed_replay(database, schema, scenario):
    """Seed the committed receipt and return its execution dependencies."""
    tables = await _create_replay_tables(database, schema)
    await _insert_replay_import_runs(database, schema, scenario)
    evidence_target_oid, profile_target_oid = await _replay_target_oids(
        database, schema
    )
    identity_state = _serving_state_for_identity(
        evidence_target_oid, profile_target_oid
    )
    database_identity = (
        await importer._provider_directory_profile_capacity_database_identity(
            schema, identity_state
        )
    )
    geometry = _replay_geometry(
        scenario, database_identity, evidence_target_oid, profile_target_oid
    )
    envelope = _capacity_envelope(
        capacity.capacity_geometry_hash(geometry),
        database_identity,
        scenario.accepted_at,
    )
    seed = _ReplaySeed(
        database=database,
        tables=tables,
        run_id=scenario.receipt_run_id,
        build_id=scenario.build_id,
        geometry=geometry,
        envelope=envelope,
        accepted_at=scenario.accepted_at,
        committed_at=scenario.committed_at,
        source_vector=scenario.source_vector,
        source_context_vector=scenario.source_context_vector,
    )
    await _insert_replay_rows(seed)
    return envelope


def _replay_execution(
    scenario: _ReplayScenario,
    envelope: dict[str, object],
    *,
    proof_id: str | None = None,
) -> ProviderDirectoryProfileExecution:
    """Bind a replay request to the seeded selection and lease."""
    return _execution(
        proof_id=proof_id or scenario.proof_id,
        profile_input_digest=scenario.profile_input_digest,
        source_id=scenario.source_id,
        dataset_id=scenario.dataset_id,
        capacity_attestation=envelope,
    )


def _configure_replay_dependencies(monkeypatch, scenario, envelope) -> None:
    """Replace external replay dependencies with exact committed evidence."""
    monkeypatch.setattr(
        importer.profile_capacity_runtime,
        "configured_capacity_lease_trust",
        lambda: capacity_trust_from_envelope(envelope),
    )
    monkeypatch.setattr(
        importer,
        "_provider_directory_profile_replay_source_context",
        AsyncMock(
            return_value=(
                scenario.source_vector,
                scenario.source_context_vector,
            )
        ),
    )
    monkeypatch.setattr(
        importer,
        "_is_provider_directory_dataset_cutover_committed",
        AsyncMock(return_value=True),
    )


async def _committed_replay(
    scenario: _ReplayScenario,
    execution: ProviderDirectoryProfileExecution,
):
    """Run committed replay for the scenario's requesting control run."""
    return await importer._provider_directory_profile_committed_run_replay(
        run_id=scenario.requested_run_id,
        control_run_id=scenario.requested_run_id,
        execution=execution,
        fence=importer.ProviderDirectoryArtifactDatasetFence(()),
    )


def _assert_replay(
    replay: dict[str, object],
    scenario: _ReplayScenario,
    envelope: dict[str, object],
) -> None:
    """Assert replay attribution, counts, and capacity attestation."""
    assert replay["evidence_rows"] == 0
    assert replay["profile_rows"] == 0
    assert replay["committed_replay"]["build_id"] == scenario.build_id
    assert (
        replay["committed_replay"]["run_id"] == scenario.receipt_run_id
    )
    if scenario.is_cross_run:
        assert replay["committed_replay"]["replayed_by_run_id"] == (
            scenario.requested_run_id
        )
    else:
        assert "replayed_by_run_id" not in replay["committed_replay"]
    assert replay["capacity"]["attestation_id"] == (
        envelope["lease"]["attestation_id"]
    )


async def _assert_replay_identity_conflict(
    scenario: _ReplayScenario,
    envelope: dict[str, object],
) -> None:
    """Refuse a cross-run replay with a conflicting proof identity."""
    conflicting_execution = _replay_execution(
        scenario, envelope, proof_id="9" * 64
    )
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="provider_directory_profile_replay_receipt_identity_conflict",
    ):
        await _committed_replay(scenario, conflicting_execution)


def _duplicate_receipt_sql(receipt_ref: str) -> str:
    """Return exact SQL for cloning the seeded receipt by build identity."""
    return f"""
            INSERT INTO {receipt_ref}
            SELECT (
                jsonb_populate_record(
                    NULL::{receipt_ref},
                    to_jsonb(receipt)
                    || jsonb_build_object(
                        'build_id',
                        CAST(:build_id AS text)
                    )
                )
            ).*
              FROM {receipt_ref} AS receipt
             WHERE build_id = :source_build_id;
        """


async def _assert_ambiguous_receipt_refused(
    database,
    schema: str,
    scenario: _ReplayScenario,
    execution: ProviderDirectoryProfileExecution,
) -> None:
    """Prove database uniqueness and replay ambiguity both fail closed."""
    receipt_ref = profile.qualified_table(
        schema,
        ProviderDirectoryProfileDeltaReceipt.__tablename__,
    )
    duplicate_receipt_sql = _duplicate_receipt_sql(receipt_ref)
    duplicate_build_id = "pdpb_" + "a" * 32
    clone_params_by_name = {
        "build_id": duplicate_build_id,
        "source_build_id": scenario.build_id,
    }
    with pytest.raises(DBAPIError):
        await database.status(duplicate_receipt_sql, **clone_params_by_name)
    await database.status(
        f"ALTER TABLE {receipt_ref} DROP CONSTRAINT "
        '"pd_profile_delta_receipt_control_proof_key";'
    )
    await database.status(duplicate_receipt_sql, **clone_params_by_name)
    with pytest.raises(
        importer.ProviderDirectoryArtifactBuildStale,
        match="provider_directory_profile_replay_receipt_ambiguous",
    ):
        await _committed_replay(scenario, execution)


@pytest.mark.parametrize(
    "is_cross_run",
    (False, True),
    ids=("same-run", "new-run"),
)
@pytest.mark.asyncio
async def test_expired_receipt_replays_without_new_admission_or_scratch(
    monkeypatch,
    is_cross_run,
):
    """Replay an expired committed receipt without allocating new scratch."""
    scenario = _replay_scenario(is_cross_run)
    async with _delta_database(monkeypatch) as (database, schema):
        monkeypatch.setattr(importer, "db", database)
        envelope = await _seed_committed_replay(database, schema, scenario)
        execution = _replay_execution(scenario, envelope)
        _configure_replay_dependencies(monkeypatch, scenario, envelope)

        replay = await _committed_replay(scenario, execution)

        assert replay is not None
        _assert_replay(replay, scenario, envelope)
        if scenario.is_cross_run:
            await _assert_replay_identity_conflict(scenario, envelope)
            await _assert_ambiguous_receipt_refused(
                database, schema, scenario, execution
            )
