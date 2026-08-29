# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""PostgreSQL integrity proof for factorized pricing projections."""

from __future__ import annotations

import importlib.util
import hashlib
import os
import random
import uuid
import zlib
from decimal import Decimal
from pathlib import Path

import pytest

from api.plan_pricing_aggregate_pack import (
    AggregateCodeIdentity,
    AggregatePack,
    AggregatePackKey,
    AggregateZipRecord,
    aggregate_logical_digest,
    aggregate_pack_raw_byte_count,
    encode_aggregate_pack,
)
from api import plan_pricing_aggregate_pack as aggregate_pack

from .test_plan_pricing_projection_postgres import (
    POSTGRES_DSN_ENV,
    TEST_DATABASE_PATTERN,
    _create_import_run_stub,
    _migration_statements,
)


asyncpg = pytest.importorskip("asyncpg")
MIGRATION_PATH = (
    Path(__file__).resolve().parents[1]
    / "alembic/versions/20260828120000_plan_pricing_factorized_projection.py"
)


def _factorized_migration_statements(monkeypatch, schema: str) -> list[str]:
    module_spec = importlib.util.spec_from_file_location(
        f"factorized_projection_{schema}", MIGRATION_PATH
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    statements: list[str] = []
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", schema)
    monkeypatch.setenv("DB_SCHEMA", schema)
    monkeypatch.setattr(migration.op, "execute", statements.append)
    migration.upgrade()
    return statements


def _pack_receipt(projection_id: str):
    code_identity = AggregateCodeIdentity("CPT", "27447")
    aggregate_record = AggregateZipRecord(
        "10001", 1, 2, Decimal("10"), Decimal("15"), Decimal("20")
    )
    encoded_payload = encode_aggregate_pack(
        AggregatePack(
            AggregatePackKey(projection_id, code_identity, "10"),
            (aggregate_record,),
        )
    )
    return code_identity, aggregate_record, encoded_payload


async def _insert_candidate(admin, schema: str, projection_id: str, digest: str):
    await admin.execute(
        f"""INSERT INTO {schema}.plan_pricing_projection_candidate (
            projection_id, contract_version, binding_manifest_digest,
            binding_manifest, provider_signature, state
        ) VALUES ($1, 'plan_pricing_factorized_v3', $2, '[]'::jsonb, $2, 'building')""",
        projection_id,
        digest,
    )


async def _insert_complete_pack_receipt(
    admin,
    schema: str,
    projection_id: str,
    encoded_payload: bytes,
    logical_digest: str,
) -> int:
    raw_byte_count = aggregate_pack_raw_byte_count(encoded_payload)
    await admin.execute(
        f"""INSERT INTO {schema}.plan_pricing_provider_membership
            (projection_id, binding_ordinal, provider_set_key, npi)
            VALUES ($1, 0, 7, 1234567890)""",
        projection_id,
    )
    await admin.execute(
        f"""INSERT INTO {schema}.plan_pricing_provider_cell (
            projection_id, geo_cell, npi, entity_type_code,
            taxonomy_codes, fragment
        ) VALUES ($1, '10001', 1234567890, 1, ARRAY['207X00000X'], $2)""",
        projection_id,
        b"{}",
    )
    await admin.execute(
        f"""INSERT INTO {schema}.plan_pricing_prewarm_shape (
            projection_id, shape_rank, code_system, code,
            geo_cell, provider_count
        ) VALUES ($1, 1, 'CPT', '27447', '10001', 1)""",
        projection_id,
    )
    await admin.execute(
        f"""INSERT INTO {schema}.plan_pricing_rate_profile (
            projection_id, code_system, code, binding_ordinal,
            provider_set_key, membership_count, minimum_negotiated_rate,
            maximum_negotiated_rate, rate_count, negotiated_rates,
            rate_multiplicities
        ) VALUES (
            $1, 'CPT', '27447', 0, 7, 1, 10, 20, 2,
            ARRAY[10, 20]::numeric[], ARRAY[1, 1]::bigint[]
        )""",
        projection_id,
    )
    await admin.execute(
        f"""INSERT INTO {schema}.plan_pricing_aggregate_pack (
            projection_id, code_system, code, zip_prefix_2,
            entry_count, raw_byte_count, stored_byte_count,
            logical_digest, payload_sha256, payload
        ) VALUES ($1, 'CPT', '27447', '10', 1, $2, $3, $4,
                  pg_catalog.sha256($5), $5)""",
        projection_id,
        raw_byte_count,
        len(encoded_payload),
        logical_digest,
        encoded_payload,
    )
    return raw_byte_count


async def _seal_candidate(
    admin,
    schema: str,
    projection_id: str,
    digest: str,
    raw_byte_count: int,
    stored_byte_count: int,
    rate_profile_count: int = 1,
) -> None:
    await admin.execute(
        f"""UPDATE {schema}.plan_pricing_projection_candidate
           SET state = 'ready', content_digest = $2,
               provider_membership_count = 1, provider_cell_count = 1,
               provider_fragment_byte_count = 2, rate_profile_count = $5,
               aggregate_entry_count = 1,
               aggregate_pack_count = 1, aggregate_raw_byte_count = $3,
               aggregate_stored_byte_count = $4, prewarm_shape_count = 1,
               build_seconds = 0, completed_at = transaction_timestamp()
         WHERE projection_id = $1""",
        projection_id,
        digest,
        raw_byte_count,
        stored_byte_count,
        rate_profile_count,
    )


async def _assert_ready_receipt_rejects_nulls(admin, schema: str) -> None:
    statement = f"""INSERT INTO {schema}.plan_pricing_projection_candidate (
        projection_id, contract_version, binding_manifest_digest,
        binding_manifest, provider_signature, state, content_digest,
        provider_membership_count, provider_cell_count,
        provider_fragment_byte_count, rate_profile_count,
        aggregate_entry_count, aggregate_pack_count,
        aggregate_raw_byte_count, aggregate_stored_byte_count,
        prewarm_shape_count, build_seconds, completed_at
    ) VALUES (
        $1, 'plan_pricing_factorized_v3', $2, '[]'::jsonb, $2, 'ready',
        $3, 0, 0, 0, 0, 0, 0, 0, 0, 0, $4, transaction_timestamp()
    )"""
    for candidate_id, content_digest, build_seconds in (
        ("7" * 64, None, 0),
        ("8" * 64, "8" * 64, None),
    ):
        with pytest.raises(asyncpg.CheckViolationError):
            await admin.execute(
                statement,
                candidate_id,
                "9" * 64,
                content_digest,
                build_seconds,
            )


async def _assert_bad_raw_count_rejected(
    admin,
    schema: str,
    encoded_payload: bytes,
    raw_byte_count: int,
    logical_digest: str,
) -> None:
    corrupt_id = "5" * 64
    await _insert_candidate(admin, schema, corrupt_id, "e" * 64)
    with pytest.raises(asyncpg.CheckViolationError):
        await admin.execute(
            f"""INSERT INTO {schema}.plan_pricing_aggregate_pack (
                projection_id, code_system, code, zip_prefix_2,
                entry_count, raw_byte_count, stored_byte_count,
                logical_digest, payload_sha256, payload
            ) VALUES ($1, 'CPT', '27447', '10', 1, $2, $3, $4,
                      pg_catalog.sha256($5), $5)""",
            corrupt_id,
            raw_byte_count + 1,
            len(encoded_payload),
            logical_digest,
            encoded_payload,
        )


async def _assert_stored_size_boundary(admin, schema: str) -> None:
    raw_payload = random.Random(99213).randbytes(
        aggregate_pack.MAX_AGGREGATE_PACK_DECODED_BYTES
    )
    encoded_payload = aggregate_pack._HEADER.pack(
        aggregate_pack._MAGIC,
        len(raw_payload),
        hashlib.sha256(raw_payload).digest(),
    ) + zlib.compress(raw_payload)
    assert len(encoded_payload) <= aggregate_pack._MAX_ENCODED_BYTES
    boundary_id = "6" * 64
    await _insert_candidate(admin, schema, boundary_id, "f" * 64)
    insert_sql = f"""INSERT INTO {schema}.plan_pricing_aggregate_pack (
        projection_id, code_system, code, zip_prefix_2, entry_count,
        raw_byte_count, stored_byte_count, logical_digest,
        payload_sha256, payload
    ) VALUES ($1, 'HCPCS', 'G0439', '10', 1, $2, $3, $4,
              pg_catalog.sha256($5), $5)"""
    await admin.execute(
        insert_sql,
        boundary_id,
        len(raw_payload),
        len(encoded_payload),
        "1" * 64,
        encoded_payload,
    )
    overflow_payload = encoded_payload + bytes(
        aggregate_pack._MAX_ENCODED_BYTES - len(encoded_payload) + 1
    )
    with pytest.raises(asyncpg.CheckViolationError):
        await admin.execute(
            insert_sql.replace("'10'", "'11'"),
            boundary_id,
            len(raw_payload),
            len(overflow_payload),
            "2" * 64,
            overflow_payload,
        )


@pytest.mark.asyncio
async def test_factorized_pack_receipt_is_sql_bound_and_immutable(monkeypatch):
    dsn = os.getenv(POSTGRES_DSN_ENV)
    if not dsn:
        pytest.skip(f"set {POSTGRES_DSN_ENV} for the PostgreSQL proof")
    admin = await asyncpg.connect(dsn)
    database_name = await admin.fetchval("SELECT current_database()")
    if TEST_DATABASE_PATTERN.search(str(database_name)) is None:
        await admin.close()
        pytest.fail(f"{POSTGRES_DSN_ENV} must target an explicit test database")

    schema = f"plan_pricing_v3_{uuid.uuid4().hex[:12]}"
    projection_id = "4" * 64
    digest = "d" * 64
    code_identity, aggregate_record, encoded_payload = _pack_receipt(projection_id)
    logical_digest = aggregate_logical_digest(code_identity, (aggregate_record,))
    try:
        await admin.execute(f"CREATE SCHEMA {schema}")
        await _create_import_run_stub(admin, schema)
        for statement in _migration_statements(monkeypatch, schema):
            await admin.execute(statement)
        for statement in _factorized_migration_statements(monkeypatch, schema):
            await admin.execute(statement)
        await _insert_candidate(admin, schema, projection_id, digest)
        raw_byte_count = await _insert_complete_pack_receipt(
            admin, schema, projection_id, encoded_payload, logical_digest
        )
        assert await admin.fetchval(
            f"""SELECT payload_sha256 = pg_catalog.sha256(payload)
                  FROM {schema}.plan_pricing_aggregate_pack
                 WHERE projection_id = $1""",
            projection_id,
        ) is True
        with pytest.raises(asyncpg.RaiseError, match="receipt counts"):
            await _seal_candidate(
                admin,
                schema,
                projection_id,
                digest,
                raw_byte_count,
                len(encoded_payload),
                rate_profile_count=0,
            )
        await _seal_candidate(
            admin, schema, projection_id, digest, raw_byte_count, len(encoded_payload)
        )
        with pytest.raises(asyncpg.RaiseError, match="immutable"):
            await admin.execute(
                f"DELETE FROM {schema}.plan_pricing_aggregate_pack "
                "WHERE projection_id = $1",
                projection_id,
            )
        await _assert_bad_raw_count_rejected(
            admin, schema, encoded_payload, raw_byte_count, logical_digest
        )
        await _assert_stored_size_boundary(admin, schema)
        await _assert_ready_receipt_rejects_nulls(admin, schema)
    finally:
        await admin.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await admin.close()
