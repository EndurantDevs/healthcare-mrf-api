# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Disposable PostgreSQL proofs for same-transaction staged census fences."""

from __future__ import annotations

from contextlib import asynccontextmanager

import pytest

import process.provider_directory_projection_materializer as materializer
import process.provider_directory_projection_stage as projection_stage
from process.provider_directory_physical_projection import claim_projection_shard
from process.provider_directory_projection_copy_summary import (
    NATIVE_DECODER_CONTRACT_ID,
    NATIVE_TRANSFORM_CONTRACT_ID,
)
from process.provider_directory_projection_contract import projection_proof_shard
from process.provider_directory_projection_native import NativeProjectionResult
from process.provider_directory_projection_stage import (
    copy_projection_stage_records,
    ensure_projection_stage,
)
from process.provider_directory_projection_types import (
    ProviderDirectoryProjectionError,
)
from tests.provider_directory_projection_foundation_postgres_support import (
    projection_foundation_postgres,
)
from tests.test_provider_directory_physical_projection import _rows
from tests.test_provider_directory_projection_foundation_postgres import (
    _registered_projection_context,
)
from tests.test_provider_directory_projection_materializer_postgres import (
    _database_arguments,
    _stage_record,
)


@asynccontextmanager
async def _unused_retained_stream(_child):
    async def byte_chunks():
        yield b"synthetic-offline-retained-byte"

    yield byte_chunks()


async def _ndjson_framing(_claim, **_options) -> str:
    return "ndjson"


def _native_result(child, proof) -> NativeProjectionResult:
    return NativeProjectionResult(
        byte_count=child.expected_byte_count,
        record_count=child.expected_record_count,
        input_sha256=child.input_sha256,
        payload_sha256=child.expected_payload_sha256,
        shard_proof=proof,
        native_thread_count=1,
        materializer_worker_count=1,
        copied_record_count=child.expected_record_count,
    )


def _projection_proof(lease, shard, normalized_row):
    return projection_proof_shard(
        (normalized_row,),
        recipe=lease.recipe,
        attempt=lease.attempt,
        partition_ordinal=shard.partition_ordinal,
        resource_type=shard.resource_type,
        input_sha256=shard.input_sha256,
        partition_id=shard.partition_id,
        partition_attempt=1,
    )


async def _registered_census_case(postgres, label: str):
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
    (normalized_row,) = _rows(retained.family, label)
    proof = _projection_proof(lease, shard, normalized_row)
    return lease, shard, admission_id, stage, normalized_row, proof


async def _assert_uncommitted_census(postgres, lease, shard, stage) -> None:
    stage_count = await postgres.database.scalar(
        f'SELECT count(*) FROM "{stage.schema}"."{stage.relation}";'
    )
    checkpoint = await postgres.database.first(
        f"""
        SELECT status, canonical_row_sha256
          FROM "{postgres.schema}".provider_directory_projection_proof_shard
         WHERE recipe_id = :recipe_id AND attempt = :attempt
           AND partition_id = :partition_id;
        """,
        recipe_id=lease.recipe.recipe_id,
        attempt=lease.attempt,
        partition_id=shard.partition_id,
    )
    child_state = await postgres.database.first(
        f"""
        SELECT status, verified_at
          FROM "{postgres.schema}".provider_directory_projection_child_read_lease
         WHERE recipe_id = :recipe_id AND partition_id = :partition_id
         ORDER BY child_generation DESC LIMIT 1;
        """,
        recipe_id=lease.recipe.recipe_id,
        partition_id=shard.partition_id,
    )
    assert stage_count == 0
    assert tuple(checkpoint) == ("building", None)
    assert tuple(child_state) == ("released", None)


async def _run_materializer(postgres, lease, admission_id, native_runner) -> None:
    await materializer.materialize_projection_shards(
        lease,
        admission_id,
        child_stream_factory=_unused_retained_stream,
        native_runner=native_runner,
        framing_resolver=_ndjson_framing,
        database=postgres.database,
        schema=postgres.schema,
        enabled=True,
        max_workers=1,
        native_threads=1,
        heartbeat_seconds=10,
    )


@pytest.mark.asyncio
async def test_materializer_rejects_consistent_result_without_stage_copy(
    monkeypatch,
) -> None:
    """A forged success object cannot substitute for same-transaction rows."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        lease, shard, admission_id, stage, _normalized_row, proof = (
            await _registered_census_case(postgres, "native-census-no-copy")
        )

        async def no_copy_runner(_claim, child, _stage, _stream, **_options):
            return _native_result(child, proof)

        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="provider_directory_projection_stage_census_mismatch",
        ):
            await _run_materializer(
                postgres,
                lease,
                admission_id,
                no_copy_runner,
            )
        await _assert_uncommitted_census(postgres, lease, shard, stage)


@pytest.mark.asyncio
async def test_materializer_rejects_wrong_per_resource_stage_census(
    monkeypatch,
) -> None:
    """An equal total with the wrong resource family cannot verify a shard."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        lease, shard, admission_id, stage, _normalized_row, proof = (
            await _registered_census_case(postgres, "native-census-wrong-family")
        )
        (wrong_row,) = _rows("Organization", "native-census-wrong-family")

        async def wrong_family_runner(claim, child, stage, _stream, **options):
            copied_count = await copy_projection_stage_records(
                stage,
                (_stage_record(lease, claim, wrong_row),),
                transaction=options["transaction"],
            )
            assert copied_count == 1
            return _native_result(child, proof)

        with pytest.raises(
            ProviderDirectoryProjectionError,
            match="provider_directory_projection_stage_census_mismatch",
        ):
            await _run_materializer(
                postgres,
                lease,
                admission_id,
                wrong_family_runner,
            )
        await _assert_uncommitted_census(postgres, lease, shard, stage)


@pytest.mark.asyncio
async def test_stage_census_extrema_match_c_byte_order_in_postgres(
    monkeypatch,
) -> None:
    """Mixed-case and punctuation IDs retain Rust-compatible C ordering."""

    async with projection_foundation_postgres(monkeypatch) as postgres:
        await postgres.upgrade()
        await postgres.upgrade_child_read()
        lease, _retained, _block, _shard, admission_id = (
            await _registered_projection_context(
                postgres,
                "native-census-collation",
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
        assert claim is not None
        normalized_rows = (
            *_rows("Organization", "a-mixed", "Z-punct!"),
            *_rows("Practitioner", "!leading", "~trailing"),
        )
        expected_identities = sorted(
            (
                normalized_row["resource_type"].encode("utf-8"),
                normalized_row["resource_id"].encode("utf-8"),
            )
            for normalized_row in normalized_rows
        )
        async with postgres.database.acquire() as transaction:
            await copy_projection_stage_records(
                stage,
                tuple(
                    _stage_record(lease, claim, normalized_row)
                    for normalized_row in normalized_rows
                ),
                transaction=transaction,
            )
            census = await projection_stage._stage_partition_census(
                stage,
                claim,
                transaction,
            )

        assert census.first_identity == tuple(
            identity_part.decode("utf-8")
            for identity_part in expected_identities[0]
        )
        assert census.last_identity == tuple(
            identity_part.decode("utf-8")
            for identity_part in expected_identities[-1]
        )
