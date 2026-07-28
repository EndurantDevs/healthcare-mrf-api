# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import struct
from typing import AsyncIterator
import zlib

from alembic.migration import MigrationContext
from alembic.operations import Operations
import asyncpg
import pytest
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from process.uhc_semantic_build_store import (
    UHC_SEMANTIC_CONTRACT_ID,
    UHC_SEMANTIC_COPY_COLUMNS,
    UHC_SEMANTIC_COPY_FORMAT_ID,
    UhcSemanticBuildIdentity,
    claim_uhc_semantic_build,
    copy_uhc_semantic_stage,
    load_sealed_uhc_semantic_build,
    prepare_uhc_semantic_stage_indexes,
    seal_uhc_semantic_build,
)
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID
from process.uhc_semantic_stage_verifier import (
    _evidence_identity,
    _fact_identity,
    verify_sealed_uhc_semantic_build,
    verify_uhc_semantic_stage,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260728120000_uhc_semantic_build_registry.py"
)
DSN_ENV = "HLTHPRT_UHC_SEMANTIC_POSTGRES_DSN"
DATABASE_PATTERN = re.compile(
    r"^uhc_semantic_test_[a-z0-9][a-z0-9_]{7,}$"
)
SCHEMA = "mrf_uhc_semantic_proof"


def _digest(label: str) -> str:
    return hashlib.sha256(label.encode()).hexdigest()


def _database_url():
    dsn = os.getenv(DSN_ENV)
    if not dsn:
        pytest.skip(f"set {DSN_ENV} to run UHC semantic PostgreSQL proofs")
    database_url = make_url(dsn)
    database_name = str(database_url.database or "")
    if (
        not database_url.drivername.startswith("postgresql")
        or DATABASE_PATTERN.fullmatch(database_name) is None
        or not database_url.host
        or not database_url.username
    ):
        pytest.fail(f"refusing non-disposable PostgreSQL database {database_name!r}")
    return database_url


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "uhc_semantic_build_registry_postgres_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _upgrade(sync_connection, migration) -> None:
    migration_context = MigrationContext.configure(sync_connection)
    migration.op = Operations(migration_context)
    migration.upgrade()


def _downgrade(sync_connection, migration) -> None:
    migration_context = MigrationContext.configure(sync_connection)
    migration.op = Operations(migration_context)
    migration.downgrade()


async def _install_schema(engine, migration) -> None:
    async with engine.begin() as connection:
        await connection.exec_driver_sql(f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE')
        await connection.exec_driver_sql(f'CREATE SCHEMA "{SCHEMA}"')
        await connection.exec_driver_sql(
            f"""
            CREATE TABLE "{SCHEMA}".provider_directory_uhc_source_binding (
                catalog_set_sha256 varchar(64) NOT NULL,
                source_file_id varchar(64) NOT NULL,
                artifact_sha256 varchar(64) NOT NULL,
                collection_kind varchar(32) NOT NULL,
                released_at timestamptz,
                PRIMARY KEY (catalog_set_sha256, source_file_id)
            )
            """
        )
        await connection.exec_driver_sql(
            f"""
            CREATE TABLE "{SCHEMA}".provider_directory_uhc_raw_layout (
                artifact_sha256 varchar(64) NOT NULL,
                contract_version integer NOT NULL,
                range_count integer NOT NULL,
                status varchar(16) NOT NULL,
                PRIMARY KEY (artifact_sha256, contract_version, range_count)
            )
            """
        )
        await connection.run_sync(
            lambda sync_connection: _upgrade(sync_connection, migration)
        )


def _json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def _line_hash(values: list[bytes]) -> str:
    return hashlib.sha256(b"\n".join(values)).hexdigest()


def _signature_pack(values: list[str]) -> bytes:
    assert len(values) == 9
    return b"".join(hashlib.sha256(value.encode()).digest() for value in values)


def _fixture_fact(
    identity: UhcSemanticBuildIdentity,
    ordinal: int,
) -> tuple[tuple[object, ...], dict[str, object], bytes]:
    fact_payload = _json_bytes({"npi": "1003821380", "ordinal": ordinal})
    payload_hash = hashlib.sha256(fact_payload).hexdigest()
    fact_identity = _fact_identity(
        identity.source_file_id,
        "ProviderMembershipRecord",
        ordinal,
        payload_hash,
    )
    semantic_hash = hashlib.sha256(fact_identity).hexdigest()
    compressed = zlib.compress(fact_payload + b"\n", level=1)
    compressed_hash = hashlib.sha256(compressed).hexdigest()
    fact_block_by_field = {
        "range_ordinal": ordinal,
        "record_start": ordinal,
        "record_count": 1,
        "fact_count": 1,
        "compressed_bytes": len(compressed),
        "compressed_payload_sha256": compressed_hash,
        "semantic_block_sha256": semantic_hash,
    }
    stage_record = (
        1, ordinal, None, None, ordinal, 1, None, None,
        compressed_hash, semantic_hash, compressed,
    )
    return stage_record, fact_block_by_field, fact_identity


def _fixture_evidence(
    ordinal: int,
) -> tuple[tuple[object, ...], dict[str, object], bytes]:
    signature_pack = _signature_pack(
        [
            '"accepting"', '[{\\"address\\":\\"1 Main St\\"}]', '"2026-07-01"',
            "null", "null", '"F"', '"Ada"' if ordinal < 2 else '"Augusta"',
            "INDIVIDUAL", '["Family Medicine"]',
        ]
    )
    evidence_by_field = {
        "occurrence_ordinal": ordinal,
        "npi": "1003821380",
        "conflict_signature_pack": signature_pack,
    }
    evidence_identity = _evidence_identity(evidence_by_field)
    stage_record = (
        2, ordinal, 0, ordinal, None, None, "1003821380",
        signature_pack, None, None, None,
    )
    layout_hash = hashlib.sha256(
        _json_bytes(
            [ordinal, 0, 1, hashlib.sha256(evidence_identity).hexdigest()]
        )
    ).hexdigest()
    evidence_range_by_field = {
        "range_ordinal": ordinal,
        "evidence_count": 1,
        "run_count": 1,
        "layout_sha256": layout_hash,
    }
    return stage_record, evidence_range_by_field, evidence_identity


def _fixture_counters() -> dict[str, int]:
    return {
        "raw_provider_records": 4, "raw_plan_records": 0,
        "raw_individual_records": 4, "raw_facility_records": 0,
        "raw_address_rows": 4, "raw_provider_plan_rows": 4,
        "raw_formulary_entries": 0, "named_facility_records": 0,
        "facility_type_values": 0, "dated_records": 4,
        "accepting_newpt_records": 4, "accepting_nopt_records": 0,
        "accepting_null_records": 0, "invalid_phone_count": 0,
        "valid_phone_count": 4, "multi_address_provider_records": 0,
        "plan_year_rows": 4, "invalid_npi_count": 0,
    }


def _fixture_proof_hash(
    proof_records: list[dict[str, object]],
    fields: tuple[str, ...],
    *,
    contract_prefix: bool,
) -> str:
    return _line_hash(
        [
            _json_bytes(
                [
                    *([UHC_SEMANTIC_CONTRACT_ID] if contract_prefix else []),
                    *(proof_record[field] for field in fields),
                ]
            )
            for proof_record in proof_records
        ]
    )


def _fixture_native_report(
    identity: UhcSemanticBuildIdentity,
    fact_blocks: list[dict[str, object]],
    evidence_ranges: list[dict[str, object]],
    fact_identities: list[bytes],
    evidence_identities: list[bytes],
) -> dict[str, object]:
    fact_fields = (
        "range_ordinal", "record_start", "record_count", "fact_count",
        "compressed_payload_sha256", "semantic_block_sha256",
    )
    evidence_fields = (
        "range_ordinal", "evidence_count", "run_count", "layout_sha256",
    )
    return {
        "contract_id": UHC_SEMANTIC_CONTRACT_ID,
        "contract_version": 2,
        "copy_format_id": UHC_SEMANTIC_COPY_FORMAT_ID,
        "source_id": UHC_PROVIDER_FILE_SOURCE_ID,
        "encoder_sha256": identity.encoder_sha256,
        "lineage": {
            "artifact_sha256": identity.artifact_sha256,
            "manifest_sha256": _digest("manifest"),
            "range_set_sha256": _digest("ranges"),
            "source_file_id": identity.source_file_id,
            "source_binding_id": "postgres-proof",
            "collection_kind": identity.collection_kind,
        },
        "counters": _fixture_counters(),
        "fact_count": 4,
        "evidence_count": 4,
        "fact_set_sha256": _fixture_proof_hash(
            fact_blocks, fact_fields, contract_prefix=True
        ),
        "record_identity_set_sha256": _line_hash(fact_identities),
        "evidence_identity_set_sha256": _line_hash(evidence_identities),
        "evidence_layout_set_sha256": _fixture_proof_hash(
            evidence_ranges, evidence_fields, contract_prefix=False
        ),
        "fact_blocks": fact_blocks,
        "evidence_ranges": evidence_ranges,
        "max_record_bytes": 1024 * 1024,
    }


def _semantic_fixture(
    identity: UhcSemanticBuildIdentity,
) -> tuple[list[tuple[object, ...]], dict[str, object]]:
    """Build deterministic fact/evidence rows and their native proof."""

    stage_records: list[tuple[object, ...]] = []
    fact_blocks: list[dict[str, object]] = []
    evidence_ranges: list[dict[str, object]] = []
    fact_identities: list[bytes] = []
    evidence_identities: list[bytes] = []
    for ordinal in range(4):
        fact_record, fact_block, fact_identity = _fixture_fact(identity, ordinal)
        evidence_record, evidence_range, evidence_identity = _fixture_evidence(ordinal)
        stage_records.extend((fact_record, evidence_record))
        fact_blocks.append(fact_block)
        evidence_ranges.append(evidence_range)
        fact_identities.append(fact_identity)
        evidence_identities.append(evidence_identity)
    native_report_by_field = _fixture_native_report(
        identity, fact_blocks, evidence_ranges, fact_identities, evidence_identities
    )
    assert all(
        len(stage_record) == len(UHC_SEMANTIC_COPY_COLUMNS)
        for stage_record in stage_records
    )
    return stage_records, native_report_by_field


def _binary_copy_field(index: int, field_value: object) -> bytes:
    if index == 0:
        return struct.pack(">h", int(field_value))
    if index in {1, 2, 3, 4, 5}:
        return struct.pack(">q", int(field_value))
    if index in {7, 10}:
        return bytes(field_value)
    return str(field_value).encode()


def _binary_copy(stage_records: list[tuple[object, ...]]) -> bytes:
    encoded = bytearray(b"PGCOPY\n\xff\r\n\0")
    encoded.extend(struct.pack(">ii", 0, 0))
    for stage_record in stage_records:
        encoded.extend(struct.pack(">h", len(stage_record)))
        for index, field_value in enumerate(stage_record):
            if field_value is None:
                encoded.extend(struct.pack(">i", -1))
                continue
            field_bytes = _binary_copy_field(index, field_value)
            encoded.extend(struct.pack(">i", len(field_bytes)))
            encoded.extend(field_bytes)
    encoded.extend(struct.pack(">h", -1))
    return bytes(encoded)


async def _chunks(payload: bytes) -> AsyncIterator[bytes]:
    for offset in range(0, len(payload), 4096):
        yield payload[offset : offset + 4096]


async def _broken_chunks(payload: bytes) -> AsyncIterator[bytes]:
    yield payload[: max(20, len(payload) // 3)]
    raise RuntimeError("injected semantic COPY crash")


async def _install_semantic_identity(
    connection: asyncpg.Connection,
    identity: UhcSemanticBuildIdentity,
) -> None:
    await connection.execute(
        f"""
        INSERT INTO "{SCHEMA}".provider_directory_uhc_source_binding (
            catalog_set_sha256, source_file_id, artifact_sha256,
            collection_kind, released_at
        ) VALUES ($1, $2, $3, $4, NULL)
        """,
        identity.catalog_set_sha256,
        identity.source_file_id,
        identity.artifact_sha256,
        identity.collection_kind,
    )
    await connection.execute(
        f"""
        INSERT INTO "{SCHEMA}".provider_directory_uhc_raw_layout (
            artifact_sha256, contract_version, range_count, status
        ) VALUES ($1, $2, $3, 'verified')
        """,
        identity.artifact_sha256,
        identity.raw_contract_version,
        identity.raw_range_count,
    )


async def _crash_and_recover_semantic_build(
    connection: asyncpg.Connection,
    identity: UhcSemanticBuildIdentity,
    binary_copy_payload: bytes,
):
    first_claim = await claim_uhc_semantic_build(connection, identity)
    with pytest.raises(RuntimeError, match="injected semantic COPY crash"):
        await copy_uhc_semantic_stage(
            connection,
            first_claim,
            _broken_chunks(binary_copy_payload),
        )
    assert await connection.fetchval(f"SELECT count(*) FROM {first_claim.stage_ref}") == 0
    await copy_uhc_semantic_stage(
        connection,
        first_claim,
        _chunks(binary_copy_payload),
    )
    assert await connection.fetchval(f"SELECT count(*) FROM {first_claim.stage_ref}") == 8
    await connection.execute(
        f"""
        UPDATE "{SCHEMA}".provider_directory_uhc_semantic_build
           SET lease_expires_at=now() - interval '1 second'
         WHERE semantic_build_id=$1
        """,
        first_claim.semantic_build_id,
    )
    recovered_claim = await claim_uhc_semantic_build(connection, identity)
    assert recovered_claim.attempt_count == 2
    assert await connection.fetchval(
        f"SELECT count(*) FROM {recovered_claim.stage_ref}"
    ) == 0
    return recovered_claim


async def _seal_and_reuse_semantic_build(
    connection: asyncpg.Connection,
    identity: UhcSemanticBuildIdentity,
    recovered_claim,
    binary_copy_payload: bytes,
    native_report: dict[str, object],
) -> None:
    copied_row_count = await copy_uhc_semantic_stage(
        connection,
        recovered_claim,
        _chunks(binary_copy_payload),
    )
    copy_observation_by_field = {
        "output_bytes": len(binary_copy_payload),
        "output_sha256": hashlib.sha256(binary_copy_payload).hexdigest(),
        "copy_row_count": copied_row_count,
    }
    native_report.update(copy_observation_by_field)
    await prepare_uhc_semantic_stage_indexes(connection, recovered_claim)
    verifier_report = await verify_uhc_semantic_stage(
        connection,
        recovered_claim,
        identity,
        native_report,
        copy_observation=copy_observation_by_field,
    )
    sealed = await seal_uhc_semantic_build(
        connection, recovered_claim, identity, native_report, verifier_report
    )
    assert sealed.attempt_count == 2
    assert sealed.fact_count == sealed.evidence_count == 4
    assert sealed.source_summary["distinct_npis"] == 1
    assert sealed.source_summary["duplicate_npi_groups"] == 1
    assert sealed.source_summary["conflicting_npi_groups"] == 1
    assert sealed.source_summary["conflict_counts"]["names"] == 1
    sealed_row = await load_sealed_uhc_semantic_build(connection, identity)
    assert sealed_row
    sealed_verifier_report = await verify_sealed_uhc_semantic_build(
        connection, identity, sealed_row
    )
    assert sealed_verifier_report["fact_count"] == 4
    assert sealed_verifier_report["evidence_count"] == 4
    reused_claim = await claim_uhc_semantic_build(connection, identity)
    assert reused_claim.sealed_reuse
    assert reused_claim.attempt_count == 2


@pytest.mark.asyncio
async def test_postgres_crash_reclaim_verify_seal_and_reuse(monkeypatch) -> None:
    """A crashed COPY is reclaimed, verified, sealed, and reused exactly."""

    database_url = _database_url()
    engine = create_async_engine(database_url.set(drivername="postgresql+asyncpg"))
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", SCHEMA)
    identity = UhcSemanticBuildIdentity(
        catalog_set_sha256=_digest("catalog"),
        source_file_id=_digest("source"),
        artifact_sha256=_digest("artifact"),
        raw_contract_version=2,
        raw_range_count=4,
        collection_kind="provider_membership",
        encoder_sha256=_digest("encoder"),
    )
    try:
        await _install_schema(engine, migration)
        connection = await asyncpg.connect(
            host=str(database_url.host),
            port=int(database_url.port or 5432),
            user=str(database_url.username),
            password=str(database_url.password or ""),
            database=str(database_url.database),
        )
        try:
            await _install_semantic_identity(connection, identity)
            stage_records, native_report = _semantic_fixture(identity)
            binary_copy_payload = _binary_copy(stage_records)
            recovered_claim = await _crash_and_recover_semantic_build(
                connection, identity, binary_copy_payload
            )
            await _seal_and_reuse_semantic_build(
                connection,
                identity,
                recovered_claim,
                binary_copy_payload,
                native_report,
            )
        finally:
            await connection.close()
        async with engine.begin() as connection:
            await connection.run_sync(
                lambda sync_connection: _downgrade(sync_connection, migration)
            )
            assert (
                await connection.exec_driver_sql(
                    f"SELECT to_regclass('{SCHEMA}.provider_directory_uhc_semantic_build')"
                )
            ).scalar_one_or_none() is None
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(
                f'DROP SCHEMA IF EXISTS "{SCHEMA}" CASCADE'
            )
        await engine.dispose()
