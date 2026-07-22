# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proofs for native projection materialization."""

from __future__ import annotations

from dataclasses import dataclass
import json
import os
from pathlib import Path

import pytest

import process.provider_directory_projection_native as native
from process.provider_directory_projection_copy_summary import (
    NATIVE_DECODER_CONTRACT_ID,
    NATIVE_TRANSFORM_CONTRACT_ID,
)
from process.provider_directory_physical_projection import claim_projection_shard
from process.provider_directory_projection_child_read import (
    claim_projection_child_read_lease,
    release_projection_child_read_lease,
    verify_projection_child_read_lease,
)
from process.provider_directory_projection_contract import projection_proof_shard
from process.provider_directory_projection_stage import (
    copy_projection_stage_records,
    ensure_projection_stage,
    prepare_projection_stage_partition,
)
from process.provider_directory_projection_workset import complete_projection_shard
from process.provider_directory_projection_types import (
    ProjectionStage,
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_foundation_postgres_support import (
    projection_foundation_postgres,
)
from tests.provider_directory_projection_materializer_context import (
    synthetic_projection_context,
)
from tests.provider_directory_projection_native_copy_support import (
    golden_projection_rows,
)
from tests.test_provider_directory_physical_projection import _rows
from tests.test_provider_directory_projection_foundation_postgres import (
    _registered_projection_context,
)


@dataclass(frozen=True)
class _MaterializationCase:
    lease: object
    stage: object
    claim: object
    child: object
    normalized_row: dict
    proof: object


def _database_arguments(postgres):
    return {"database": postgres.database, "schema": postgres.schema}


def _stage_record(lease, claim, normalized_row):
    """Map one normalized proof row to the exact stage COPY contract."""

    return (
        lease.recipe.recipe_id,
        normalized_row["resource_type"],
        normalized_row["resource_id"],
        claim.shard.partition_id,
        normalized_row["payload_hash"],
        json.dumps(normalized_row["payload_json"]),
        normalized_row["source_rank"],
        normalized_row["summary_npi"],
        normalized_row["summary_address_count"],
        normalized_row["summary_addressed_location"],
        normalized_row["summary_geocoded_location"],
        normalized_row["summary_network_link_count"],
        normalized_row["summary_affiliation_link_count"],
        True,
        None,
        None,
        None,
        None,
    )


async def _clear_stage_partition(transaction, stage, partition_id):
    """Prime the shared transaction and replace one retry partition."""

    await transaction.status(
        f'DELETE FROM "{stage.schema}"."{stage.relation}" '
        "WHERE proof_partition_id = :partition_id;",
        partition_id=partition_id,
    )


async def _serving_state(postgres):
    """Snapshot only sealed/serving state that PR1 must never publish."""

    database = postgres.database
    schema = postgres.schema
    physical_projection_count = await database.scalar(
        f'SELECT count(*) FROM "{schema}".provider_directory_physical_projection;'
    )
    serving_resource_count = await database.scalar(
        f'SELECT count(*) FROM "{schema}".'
        "provider_directory_physical_projection_resource;"
    )
    live_reference_count = await database.scalar(
        f'SELECT count(*) FROM "{schema}".'
        "provider_directory_physical_projection_reference "
        "WHERE released_at IS NULL;"
    )
    return (
        int(physical_projection_count or 0),
        int(serving_resource_count or 0),
        int(live_reference_count or 0),
    )


async def _materialization_case(postgres, label: str) -> _MaterializationCase:
    await postgres.upgrade()
    await postgres.upgrade_child_read()
    lease, retained, _block, shard, admission_id = (
        await _registered_projection_context(
            postgres,
            label,
            decoder_contract_id=NATIVE_DECODER_CONTRACT_ID,
            transform_contract_id=NATIVE_TRANSFORM_CONTRACT_ID,
        )
    )
    stage = await ensure_projection_stage(
        lease,
        **_database_arguments(postgres),
    )
    claim = await claim_projection_shard(
        lease,
        admission_id=admission_id,
        **_database_arguments(postgres),
    )
    assert claim is not None and claim.shard == shard
    child = await claim_projection_child_read_lease(
        claim,
        **_database_arguments(postgres),
    )
    (normalized_row,) = _rows(retained.family, label)
    proof = projection_proof_shard(
        (normalized_row,),
        recipe=lease.recipe,
        attempt=lease.attempt,
        partition_ordinal=claim.shard.partition_ordinal,
        resource_type=claim.shard.resource_type,
        input_sha256=claim.shard.input_sha256,
        partition_id=claim.shard.partition_id,
        partition_attempt=claim.partition_attempt,
    )
    return _MaterializationCase(
        lease,
        stage,
        claim,
        child,
        normalized_row,
        proof,
    )


async def _commit_case(postgres, case: _MaterializationCase) -> None:
    async with postgres.database.acquire() as transaction:
        atomic_database_options_map = {
            "database": transaction,
            "schema": postgres.schema,
        }
        await _clear_stage_partition(
            transaction,
            case.stage,
            case.claim.shard.partition_id,
        )
        copied_count = await copy_projection_stage_records(
            case.stage,
            (_stage_record(case.lease, case.claim, case.normalized_row),),
            transaction=transaction,
        )
        assert copied_count == 1
        await verify_projection_child_read_lease(
            case.child,
            byte_count=case.child.expected_byte_count,
            record_count=case.child.expected_record_count,
            input_sha256=case.child.input_sha256,
            payload_sha256=case.child.expected_payload_sha256,
            **atomic_database_options_map,
        )
        await complete_projection_shard(
            case.claim,
            case.proof,
            child_lease=case.child,
            **atomic_database_options_map,
        )
    await release_projection_child_read_lease(
        case.child,
        **_database_arguments(postgres),
    )


async def _assert_committed_case(postgres, case: _MaterializationCase) -> None:
    stage_catalog = await postgres.database.first(
        """
        SELECT class_record.relpersistence,
               (SELECT count(*) FROM pg_index
                 WHERE indrelid = class_record.oid) AS index_count
          FROM pg_class AS class_record
         WHERE class_record.oid = CAST(:relation_oid AS oid);
        """,
        relation_oid=case.stage.relation_oid,
    )
    stage_catalog_values = tuple(stage_catalog)
    stage_persistence = stage_catalog_values[0]
    if isinstance(stage_persistence, bytes):
        stage_persistence = stage_persistence.decode("ascii")
    assert (stage_persistence, stage_catalog_values[1]) == ("p", 0)
    stage_count = await postgres.database.scalar(
        f'SELECT count(*) FROM "{case.stage.schema}"."{case.stage.relation}" '
        "WHERE proof_partition_id = :partition_id;",
        partition_id=case.claim.shard.partition_id,
    )
    checkpoint = await postgres.database.first(
        f"""
        SELECT status, partition_attempt, canonical_row_sha256, resource_count
          FROM "{postgres.schema}".provider_directory_projection_proof_shard
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND partition_id = :partition_id;
        """,
        recipe_id=case.lease.recipe.recipe_id,
        attempt=case.lease.attempt,
        partition_id=case.claim.shard.partition_id,
    )
    assert stage_count == 1
    assert tuple(checkpoint) == (
        "complete",
        case.claim.partition_attempt,
        case.proof.canonical_row_sha256,
        1,
    )


@pytest.mark.asyncio
async def test_materializer_checkpoint_and_logged_rows_commit_atomically(
    monkeypatch,
) -> None:
    """A complete checkpoint is durable, index-free, and not publication."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        case = await _materialization_case(
            postgres,
            "native-materializer-atomic",
        )
        serving_before = await _serving_state(postgres)
        await _commit_case(postgres, case)
        await _assert_committed_case(postgres, case)
        assert await _serving_state(postgres) == serving_before


async def _rollback_case(postgres, case: _MaterializationCase) -> None:
    with pytest.raises(RuntimeError, match="synthetic-process-crash"):
        async with postgres.database.acquire() as transaction:
            atomic_database_options_map = {
                "database": transaction,
                "schema": postgres.schema,
            }
            await _clear_stage_partition(
                transaction,
                case.stage,
                case.claim.shard.partition_id,
            )
            await copy_projection_stage_records(
                case.stage,
                (_stage_record(case.lease, case.claim, case.normalized_row),),
                transaction=transaction,
            )
            await verify_projection_child_read_lease(
                case.child,
                byte_count=case.child.expected_byte_count,
                record_count=case.child.expected_record_count,
                input_sha256=case.child.input_sha256,
                payload_sha256=case.child.expected_payload_sha256,
                **atomic_database_options_map,
            )
            raise RuntimeError("synthetic-process-crash")


async def _assert_rolled_back_case(postgres, case: _MaterializationCase) -> None:
    stage_count = await postgres.database.scalar(
        f'SELECT count(*) FROM "{case.stage.schema}"."{case.stage.relation}";'
    )
    checkpoint_is_building = await postgres.database.scalar(
        f"""
        SELECT status = 'building' AND canonical_row_sha256 IS NULL
          FROM "{postgres.schema}".provider_directory_projection_proof_shard
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND partition_id = :partition_id;
        """,
        recipe_id=case.lease.recipe.recipe_id,
        attempt=case.lease.attempt,
        partition_id=case.claim.shard.partition_id,
    )
    child_is_unverified = await postgres.database.scalar(
        f"""
        SELECT status = 'active' AND verified_at IS NULL
          FROM "{postgres.schema}".provider_directory_projection_child_read_lease
         WHERE recipe_id = :recipe_id AND partition_id = :partition_id;
        """,
        recipe_id=case.lease.recipe.recipe_id,
        partition_id=case.claim.shard.partition_id,
    )
    assert stage_count == 0
    assert checkpoint_is_building is True
    assert child_is_unverified is True


@pytest.mark.asyncio
async def test_materializer_copy_and_checkpoint_roll_back_together(monkeypatch) -> None:
    """A crash-shaped exception cannot leave rows ahead of its checkpoint."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        case = await _materialization_case(
            postgres,
            "native-materializer-rollback",
        )
        await _rollback_case(postgres, case)
        await _assert_rolled_back_case(postgres, case)
        await release_projection_child_read_lease(
            case.child,
            **_database_arguments(postgres),
        )


async def _mutate_stage_catalog(postgres, case, mutation_kind: str) -> None:
    relation_ref = f'"{case.stage.schema}"."{case.stage.relation}"'
    if mutation_kind == "trigger":
        function_ref = f'"{case.stage.schema}".projection_stage_test_trigger'
        await postgres.retained_connection.execute(
            f"CREATE FUNCTION {function_ref}() RETURNS trigger "
            "LANGUAGE plpgsql AS 'BEGIN RETURN NEW; END'"
        )
        await postgres.retained_connection.execute(
            f"CREATE TRIGGER projection_stage_test_trigger BEFORE INSERT "
            f"ON {relation_ref} FOR EACH ROW EXECUTE FUNCTION {function_ref}()"
        )
        return
    await postgres.retained_connection.execute(
        f"CREATE RULE projection_stage_test_rule AS ON INSERT TO {relation_ref} "
        "DO ALSO NOTHING"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation_kind", ("trigger", "rule"))
async def test_materializer_rejects_stage_transform_hooks(
    monkeypatch,
    mutation_kind,
) -> None:
    """The locked stage must remain a plain, untransformed COPY target."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        case = await _materialization_case(
            postgres,
            f"native-materializer-{mutation_kind}",
        )
        await _mutate_stage_catalog(postgres, case, mutation_kind)
        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="provider_directory_projection_stage_invalid",
        ):
            async with postgres.database.acquire() as transaction:
                await prepare_projection_stage_partition(
                    case.stage,
                    case.claim,
                    database=transaction,
                )


def _native_binary_path() -> str:
    binary_path = os.getenv("HLTHPRT_PROVIDER_DIRECTORY_PROJECTION_NATIVE_BIN")
    if not binary_path:
        pytest.skip("set the native Provider Directory binary path")
    resolved_path = Path(binary_path).resolve()
    assert resolved_path.is_file()
    return str(resolved_path)


def _real_copy_context():
    resource_map = {
        "resourceType": "Organization",
        "id": "organization-real-copy",
        "active": True,
        "identifier": [
            {
                "system": "http://hl7.org/fhir/sid/us-npi",
                "value": "1234567893",
            }
        ],
        "name": "Real COPY organization",
        "address": {"city": "Des Moines", "state": "IA"},
        "period": {"start": "2025-01-01", "end": "2027-01-01"},
        "meta": {"lastUpdated": "2026-07-22T00:00:00Z"},
    }
    return synthetic_projection_context(
        "ndjson",
        resources=(resource_map,),
        label="real-native-rust-copy",
    )


async def _create_real_copy_stage(postgres) -> ProjectionStage:
    relation = "pd_projection_real_copy"
    await postgres.retained_connection.execute(
        f'CREATE TABLE "{postgres.schema}"."{relation}" '
        f'(LIKE "{postgres.schema}".'
        '"provider_directory_physical_projection_resource" '
        "INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING STORAGE)"
    )
    relation_oid = await postgres.retained_connection.fetchval(
        "SELECT to_regclass($1)::oid::bigint",
        f"{postgres.schema}.{relation}",
    )
    return ProjectionStage(postgres.schema, relation, int(relation_oid))


async def _copy_real_native_context(postgres, context, stage):
    async def retained_bytes():
        yield context.fixture.encoded

    async with postgres.retained_connection.transaction():
        return await native.run_native_projection_command(
            context.claim,
            context.child_lease,
            stage,
            retained_bytes(),
            framing="ndjson",
            transaction=postgres.retained_connection,
            materializer_workers=1,
            native_threads=2,
            executable=_native_binary_path(),
        )


async def _real_copy_evidence(postgres, stage):
    stored_record = await postgres.retained_connection.fetchrow(
        f'SELECT *, payload_json::text AS payload_text, '
        f'profile_evidence_json::text AS evidence_text FROM '
        f'"{stage.schema}"."{stage.relation}"'
    )
    catalog_record = await postgres.retained_connection.fetchrow(
        "SELECT relpersistence, (SELECT count(*) FROM pg_index "
        "WHERE indrelid = $1::oid) FROM pg_class WHERE oid = $1::oid",
        stage.relation_oid,
    )
    return stored_record, catalog_record


@pytest.mark.asyncio
async def test_native_copy_reaches_logged_index_free_postgres_stage(
    monkeypatch,
) -> None:
    """Cross the real Rust and asyncpg COPY boundaries into the exact stage shape."""

    context = _real_copy_context()
    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        stage = await _create_real_copy_stage(postgres)
        outcome = await _copy_real_native_context(postgres, context, stage)
        stored_record, catalog_record = await _real_copy_evidence(postgres, stage)

    (oracle_row_map,) = golden_projection_rows(context)
    stored_row_map = {
        field_name: stored_record[field_name]
        for field_name in oracle_row_map
        if field_name not in {"payload_json", "profile_evidence_json"}
    }
    stored_row_map["payload_json"] = json.loads(stored_record["payload_text"])
    stored_row_map["profile_evidence_json"] = json.loads(
        stored_record["evidence_text"]
    )
    assert stored_row_map == oracle_row_map
    assert outcome.copied_record_count == outcome.record_count == 1
    persistence = catalog_record[0]
    if isinstance(persistence, bytes):
        persistence = persistence.decode("ascii")
    assert (persistence, catalog_record[1]) == ("p", 0)
