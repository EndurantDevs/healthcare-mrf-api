# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proofs for atomic projection finalization."""

from __future__ import annotations

from dataclasses import replace
import hashlib

import pytest

import process.provider_directory_projection_finalizer as finalizer
from process.provider_directory_projection_child_read import (
    release_projection_child_read_lease,
    verify_projection_child_read_lease,
)
from process.provider_directory_physical_projection import finalize_projection
from process.provider_directory_projection_contract import (
    PROJECTION_WINNER_POLICY_CONTRACT_ID,
    projection_proof_shard,
)
from process.provider_directory_projection_stage import copy_projection_stage_records
from process.provider_directory_projection_types import stable_json
from process.provider_directory_projection_workset import complete_projection_shard
from tests.provider_directory_projection_foundation_postgres_support import (
    projection_foundation_postgres,
)
from tests.test_provider_directory_projection_materializer_postgres import (
    _clear_stage_partition,
    _commit_case,
    _materialization_case,
    _stage_record,
)


def _arguments(postgres):
    return {"database": postgres.database, "schema": postgres.schema}


async def _finalizer_case(postgres, label):
    case = await _materialization_case(postgres, label)
    normalized_row_map = dict(case.normalized_row)
    normalized_row_map["payload_hash"] = hashlib.sha256(
        stable_json(normalized_row_map["payload_json"]).encode()
    ).hexdigest()
    normalized_row_map["source_rank"] = (
        f"{case.claim.shard.partition_ordinal:020d}:"
        f"{normalized_row_map['payload_hash']}:"
        "00000000000000000000"
    )
    normalized_row_map["summary_npi"] = 1234567890
    proof = projection_proof_shard(
        (normalized_row_map,),
        recipe=case.lease.recipe,
        attempt=case.lease.attempt,
        partition_ordinal=case.claim.shard.partition_ordinal,
        resource_type=case.claim.shard.resource_type,
        input_sha256=case.claim.shard.input_sha256,
        partition_id=case.claim.shard.partition_id,
        partition_attempt=case.claim.partition_attempt,
    )
    return replace(case, normalized_row=normalized_row_map, proof=proof)


async def _catalog_state(postgres, case):
    database, schema = postgres.database, postgres.schema
    recipe = await database.first(
        f"""
        SELECT status, physical_projection_id, lease_token,
               prepared_proof_json IS NOT NULL AS has_proof
          FROM "{schema}".provider_directory_projection_recipe
         WHERE recipe_id = :recipe_id;
        """,
        recipe_id=case.lease.recipe.recipe_id,
    )
    physical_count = await database.scalar(
        f'SELECT count(*) FROM "{schema}".provider_directory_physical_projection;'
    )
    partition_count = await database.scalar(
        f'SELECT count(*) FROM "{schema}".'
        "provider_directory_physical_projection_partition;"
    )
    source_summary_count = await database.scalar(
        f'SELECT count(*) FROM "{schema}".'
        "provider_directory_physical_projection_source_summary;"
    )
    serving_count = await database.scalar(
        f'SELECT count(*) FROM "{schema}".'
        "provider_directory_physical_projection_resource;"
    )
    stage_catalog = await database.first(
        f"""
        SELECT (SELECT count(*) FROM pg_index WHERE indrelid = class_record.oid),
               (SELECT count(*) FROM pg_trigger
                 WHERE tgrelid = class_record.oid AND NOT tgisinternal),
               EXISTS (SELECT 1 FROM pg_inherits
                        WHERE inhrelid = class_record.oid),
               (SELECT count(*) FROM "{case.stage.schema}"."{case.stage.relation}")
          FROM pg_class AS class_record
         WHERE class_record.oid = CAST(:relation_oid AS oid);
        """,
        relation_oid=case.stage.relation_oid,
    )
    return {
        "recipe": tuple(recipe),
        "catalog_counts": (
            physical_count,
            partition_count,
            source_summary_count,
            serving_count,
        ),
        "stage": tuple(stage_catalog),
    }


async def _competing_case(postgres, label):
    case = await _finalizer_case(postgres, label)
    first_map = {
        **case.normalized_row,
        "payload_json": {
            "resourceType": case.normalized_row["resource_type"],
            "id": label,
            "active": True,
        },
        "summary_npi": 1234567890,
    }
    second_map = {
        **case.normalized_row,
        "payload_json": {
            "resourceType": case.normalized_row["resource_type"],
            "id": label,
            "active": False,
        },
        "summary_npi": 1987654321,
    }
    for input_ordinal, resource_map in enumerate((first_map, second_map)):
        resource_map["payload_hash"] = hashlib.sha256(
            stable_json(resource_map["payload_json"]).encode()
        ).hexdigest()
        resource_map["source_rank"] = (
            f"{case.claim.shard.partition_ordinal:020d}:"
            f"{resource_map['payload_hash']}:"
            f"{input_ordinal:020d}"
        )
    staged_resources = tuple(
        sorted(
            (first_map, second_map),
            key=lambda resource_map: (
                resource_map["source_rank"],
                resource_map["payload_hash"],
            ),
        )
    )
    proof = projection_proof_shard(
        staged_resources,
        recipe=case.lease.recipe,
        attempt=case.lease.attempt,
        partition_ordinal=case.claim.shard.partition_ordinal,
        resource_type=case.claim.shard.resource_type,
        input_sha256=case.claim.shard.input_sha256,
        partition_id=case.claim.shard.partition_id,
        partition_attempt=case.claim.partition_attempt,
    )
    return replace(case, normalized_row=staged_resources[0], proof=proof), staged_resources


async def _commit_competing_case(postgres, case, staged_resources):
    async with postgres.database.acquire() as transaction:
        database_options_map = {"database": transaction, "schema": postgres.schema}
        await _clear_stage_partition(
            transaction,
            case.stage,
            case.claim.shard.partition_id,
        )
        assert await copy_projection_stage_records(
            case.stage,
            tuple(
                _stage_record(case.lease, case.claim, staged_resource)
                for staged_resource in staged_resources
            ),
            transaction=transaction,
        ) == len(staged_resources)
        await verify_projection_child_read_lease(
            case.child,
            byte_count=case.child.expected_byte_count,
            record_count=case.child.expected_record_count,
            input_sha256=case.child.input_sha256,
            payload_sha256=case.child.expected_payload_sha256,
            **database_options_map,
        )
        await complete_projection_shard(
            case.claim,
            case.proof,
            child_lease=case.child,
            **database_options_map,
        )
    await release_projection_child_read_lease(
        case.child,
        database=postgres.database,
        schema=postgres.schema,
    )


@pytest.mark.asyncio
async def test_finalizer_seals_atomically_and_replays_exact_proof(monkeypatch):
    async with projection_foundation_postgres(monkeypatch) as postgres:
        case = await _finalizer_case(postgres, "native-finalizer-seal")
        await _commit_case(postgres, case)
        await postgres.upgrade_finalizer()
        original_prepared_projection = finalizer._prepared_projection
        seal_barrier_by_name = {}

        async def prepared_projection_after_barrier(*args, **kwargs):
            prepared = await original_prepared_projection(*args, **kwargs)
            seal_barrier_by_name["timestamp"] = await args[0].scalar(
                "SELECT clock_timestamp();"
            )
            return prepared

        monkeypatch.setattr(
            finalizer,
            "_prepared_projection",
            prepared_projection_after_barrier,
        )

        proof = await finalize_projection(
            case.lease,
            retain_seconds=60,
            **_arguments(postgres),
        )
        assert proof.physical_projection_id == case.lease.recipe.recipe_id
        assert proof.resource_count == 1
        assert proof.proof["raw_shards"] == [case.proof.descriptor]
        assert await _catalog_state(postgres, case) == {
            "recipe": (
                "sealed",
                case.lease.recipe.recipe_id,
                None,
                True,
            ),
            "catalog_counts": (1, 1, 1, 1),
            "stage": (3, 2, True, 1),
        }
        physical_times = await postgres.database.first(
            f'SELECT sealed_at, retain_until FROM "{postgres.schema}".'
            "provider_directory_physical_projection;"
        )
        recipe_sealed_at = await postgres.database.scalar(
            f'SELECT sealed_at FROM "{postgres.schema}".'
            "provider_directory_projection_recipe;"
        )
        assert physical_times[0] >= seal_barrier_by_name["timestamp"]
        assert recipe_sealed_at >= seal_barrier_by_name["timestamp"]
        assert physical_times[1] > physical_times[0]

        replay = await finalize_projection(
            case.lease,
            retain_seconds=0,
            **_arguments(postgres),
        )
        assert replay == proof
        await postgres.downgrade_finalizer()
        await postgres.upgrade_finalizer()


@pytest.mark.asyncio
async def test_finalizer_materializes_minimum_ranked_competing_winner(monkeypatch):
    async with projection_foundation_postgres(monkeypatch) as postgres:
        case, staged_resources = await _competing_case(
            postgres, "native-finalizer-winner"
        )
        await _commit_competing_case(postgres, case, staged_resources)
        await postgres.upgrade_finalizer()

        proof = await finalize_projection(
            case.lease,
            retain_seconds=60,
            **_arguments(postgres),
        )
        stored_winner = await postgres.database.first(
            f'SELECT source_rank, payload_hash, summary_npi FROM "{postgres.schema}".'
            "provider_directory_physical_projection_resource;"
        )
        assert tuple(stored_winner) == (
            staged_resources[0]["source_rank"],
            staged_resources[0]["payload_hash"],
            staged_resources[0]["summary_npi"],
        )
        assert proof.resource_count == 1
        assert (
            proof.proof["reducer"]["winner_policy_contract_id"]
            == PROJECTION_WINNER_POLICY_CONTRACT_ID
        )
        assert proof.proof["source_summary"]["semantic_summary"]["outcome_counts"][
            "distinct_npis"
        ] == 1
        assert await finalize_projection(
            case.lease,
            retain_seconds=0,
            **_arguments(postgres),
        ) == proof


@pytest.mark.asyncio
async def test_finalizer_fault_rolls_back_indexes_catalog_and_attach(monkeypatch):
    async with projection_foundation_postgres(monkeypatch) as postgres:
        case, staged_resources = await _competing_case(
            postgres, "native-finalizer-rollback"
        )
        await _commit_competing_case(postgres, case, staged_resources)
        await postgres.upgrade_finalizer()
        original_insert = finalizer._insert_projection_catalog

        async def fail_after_catalog(*args, **kwargs):
            await original_insert(*args, **kwargs)
            raise RuntimeError("synthetic-finalizer-crash")

        monkeypatch.setattr(finalizer, "_insert_projection_catalog", fail_after_catalog)
        with pytest.raises(RuntimeError, match="synthetic-finalizer-crash"):
            await finalize_projection(
                case.lease,
                retain_seconds=60,
                **_arguments(postgres),
            )
        assert await _catalog_state(postgres, case) == {
            "recipe": ("building", None, case.lease.lease_token, False),
            "catalog_counts": (0, 0, 0, 0),
            "stage": (0, 0, False, 2),
        }

        monkeypatch.setattr(
            finalizer,
            "_insert_projection_catalog",
            original_insert,
        )
        proof = await finalize_projection(
            case.lease,
            retain_seconds=60,
            **_arguments(postgres),
        )
        assert proof.resource_count == 1
        assert (await _catalog_state(postgres, case))["stage"] == (3, 2, True, 1)
