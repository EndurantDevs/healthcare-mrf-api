# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof for bounded sealed-layout archive selection."""

from __future__ import annotations

from contextlib import asynccontextmanager
import os
import uuid

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from db.models._legacy import Base
from process.ptg_parts.ptg2_shared_blocks import SharedBlock, shared_block_hash
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import encode_v4_snapshot_map_pack
from process.ptg_parts import result_archive_closure as archive_closure
from process.ptg_parts.result_archive_closure import (
    ResultArchiveClosureError,
    select_result_archive_closure,
)


_RELATION_SUPPORT_COLUMNS = {
    "ptg2_import_run": "import_run_id text",
    "ptg2_import_job": "import_run_id text",
    "ptg2_source_catalog": "import_run_id text",
    "ptg2_v3_snapshot_scope": "snapshot_id text",
    "ptg2_v3_snapshot_plan_scope": "snapshot_id text",
    "ptg2_v3_snapshot_source": "snapshot_id text, source_trace_set_hash text",
    "ptg2_v3_candidate_audit_attestation": "snapshot_id text",
    "ptg2_v3_audit_occurrence": "snapshot_key bigint",
    "ptg2_artifact_manifest": "artifact_id text, snapshot_id text",
    "ptg2_artifact_blob_chunk": "artifact_id text",
    "ptg2_source_trace_set": "source_trace_set_hash text, source_trace_hashes text[]",
    "ptg2_source_trace": "source_trace_hash text, source_file_version_id text",
    "ptg2_source_file_version": "source_file_version_id text, source_identity_hash text, content_hash text",
    "ptg2_source_identity": "source_identity_hash text",
    "ptg2_content_identity": "content_hash text",
    "ptg2_v4_npi_scope": "snapshot_key bigint",
    "ptg2_v4_provider_component": "snapshot_key bigint",
    "ptg2_v4_pattern": "snapshot_key bigint",
    "ptg2_v4_relation_manifest": "snapshot_key bigint",
    "ptg2_v4_heavy_owner": "snapshot_key bigint",
    "ptg2_v4_provider_set_npi_prefix": "snapshot_key bigint",
    "ptg2_v4_provider_graph_diagnostic": "snapshot_key bigint",
    "ptg2_v4_inferred_taxonomy_candidate": "snapshot_key bigint",
    "ptg2_v3_source_audit_witness": "snapshot_key bigint",
    "ptg2_v3_source_audit_witness_part": "snapshot_key bigint",
}
_NATIVE_DDL_ONLY_TABLES = {
    "ptg2_v4_provider_set_npi_prefix",
    "ptg2_v4_provider_graph_diagnostic",
    "ptg2_v4_inferred_taxonomy_candidate",
}


class _RecordedRows:
    """Expose a result iterator while retaining only evidence of rows delivered."""

    def __init__(self, result, delivered_rows: list[object]) -> None:
        self._result = result
        self._delivered_rows = delivered_rows

    def __iter__(self):
        for row in self._result:
            self._delivered_rows.append(row)
            yield row


class _BoundedReadRecordingSession:
    """Record bounded selector reads while delegating to an async session."""

    def __init__(self, session) -> None:
        self._session = session
        self.payload_parameters: list[dict[str, object]] = []
        self.map_payload_rows: list[object] = []
        self.finalizer_anchor_limits: list[int] = []

    async def execute(self, statement, parameters=None):
        result = await self._session.execute(statement, parameters)
        if "WITH requested AS" in str(statement):
            self.payload_parameters.append(dict(parameters or {}))
        if parameters and "anchor_limit" in parameters:
            self.finalizer_anchor_limits.append(int(parameters["anchor_limit"]))
        if parameters and "max_map_payload_bytes" in parameters:
            return _RecordedRows(result, self.map_payload_rows)
        return result


def _dsn() -> str:
    """Use the opt-in native PG18 test database, never a configured runtime DB."""

    raw = os.getenv("HLTHPRT_PTG2_V4_MIGRATION_POSTGRES_DSN")
    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1" or not raw:
        pytest.skip("set guarded PTG V4 PostgreSQL test variables for native proof")
    return raw.replace("postgresql://", "postgresql+asyncpg://", 1)


@asynccontextmanager
async def _database():
    """Create and remove one synthetic schema in the caller-owned test database."""

    engine = create_async_engine(_dsn())
    schema_name = "archive_closure_" + uuid.uuid4().hex
    schema = '"' + schema_name + '"'
    try:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f"CREATE SCHEMA {schema}")
            await _create_schema_tables(connection, schema)
        yield engine, schema_name, schema
    finally:
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        await engine.dispose()


async def _create_schema_tables(connection, schema: str) -> None:
    """Create only the model tables exercised by the synthetic selector proof."""

    ddl = f"""
                CREATE TABLE {schema}.ptg2_snapshot (
                    snapshot_id text PRIMARY KEY, import_run_id text,
                    status text NOT NULL, manifest jsonb NOT NULL
                );
                CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
                    snapshot_id text PRIMARY KEY, snapshot_key bigint NOT NULL
                );
                CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
                    snapshot_key bigint PRIMARY KEY, generation text NOT NULL, state text NOT NULL,
                    mapping_digest bytea NOT NULL, layout_manifest jsonb NOT NULL
                );
                CREATE TABLE {schema}.ptg2_v4_snapshot_map_root (
                    snapshot_key bigint PRIMARY KEY, state text NOT NULL, map_format text NOT NULL,
                    map_digest bytea NOT NULL, object_kind_count integer NOT NULL,
                    map_pack_count bigint NOT NULL, coordinate_count bigint NOT NULL,
                    entry_count bigint NOT NULL, logical_byte_count bigint NOT NULL,
                    stored_map_byte_count bigint NOT NULL
                );
                CREATE TABLE {schema}.ptg2_v4_finalizer_map_root (
                    snapshot_key bigint PRIMARY KEY, state text NOT NULL, contract text NOT NULL,
                    map_format text NOT NULL, map_digest bytea NOT NULL,
                    object_kind_count integer NOT NULL, map_pack_count bigint NOT NULL,
                    coordinate_count bigint NOT NULL, entry_count bigint NOT NULL,
                    logical_byte_count bigint NOT NULL, stored_map_byte_count bigint NOT NULL,
                    target_block_count bigint NOT NULL
                );
                CREATE TABLE {schema}.ptg2_v3_block (
                    block_hash bytea PRIMARY KEY, format_version smallint NOT NULL,
                    object_kind text NOT NULL, codec text NOT NULL, entry_count bigint NOT NULL,
                    raw_byte_count bigint NOT NULL, stored_byte_count bigint NOT NULL, payload bytea NOT NULL
                );
                CREATE TABLE {schema}.ptg2_v4_snapshot_map_pack (
                    snapshot_key bigint NOT NULL, object_kind text NOT NULL, pack_no integer NOT NULL,
                    coordinate_count integer NOT NULL, entry_count bigint NOT NULL,
                    logical_byte_count bigint NOT NULL, map_block_hash bytea NOT NULL
                );
                CREATE TABLE {schema}.ptg2_v4_finalizer_map_pack (
                    snapshot_key bigint NOT NULL, object_kind text NOT NULL, pack_no integer NOT NULL,
                    coordinate_count integer NOT NULL, entry_count bigint NOT NULL,
                    logical_byte_count bigint NOT NULL, map_block_hash bytea NOT NULL
                );
                CREATE TABLE {schema}.ptg2_v4_finalizer_map_target (
                    snapshot_key bigint NOT NULL, block_hash bytea NOT NULL
                );
    """
    for statement in ddl.split(";"):
        if statement.strip():
            await connection.exec_driver_sql(statement)


async def _create_relation_predicate_support(connection, schema: str) -> None:
    """Create remaining model-named tables referenced by archive predicates."""

    for table_name, columns in _RELATION_SUPPORT_COLUMNS.items():
        await connection.exec_driver_sql(f"CREATE TABLE {schema}.{table_name} ({columns})")


def _model_columns_by_table() -> dict[str, set[str]]:
    """Expose current model column names keyed by physical table name."""

    return {table.name: set(table.columns.keys()) for table in Base.metadata.tables.values()}


async def _seed(engine, schema: str) -> tuple[str, set[bytes]]:
    """Persist one complete synthetic V4 layout and its isolated map closure."""

    snapshot_id = "synthetic-result-closure"
    snapshot_key = 71
    digest = b"d" * 32
    hashes: set[bytes] = set()
    async with engine.begin() as connection:
        await _seed_roots(connection, schema, snapshot_id, digest)
        map_payload_bytes, finalizer_payload_bytes = await _seed_map_blocks(
            connection,
            schema,
            hashes,
        )
        await _seed_root_receipts(
            connection,
            schema,
            map_payload_bytes,
            finalizer_payload_bytes,
        )
    return snapshot_id, hashes


async def _seed_roots(connection, schema: str, snapshot_id: str, digest: bytes) -> None:
    """Insert logical snapshot, binding, layout, and initial complete roots."""

    await connection.exec_driver_sql(
        f"INSERT INTO {schema}.ptg2_snapshot VALUES ('{snapshot_id}', 'synthetic-import', 'validated', '{{\"serving_index\": {{\"storage_generation\": \"shared_blocks_v4\", \"shared_snapshot_key\": 71}}}}')"
    )
    await connection.exec_driver_sql(f"INSERT INTO {schema}.ptg2_v3_snapshot_binding VALUES ('{snapshot_id}', 71)")
    await connection.execute(
        text(
            f'INSERT INTO {schema}.ptg2_v3_snapshot_layout VALUES (71, \'shared_blocks_v4\', \'sealed\', :digest, \'{{"serving_index": {{"storage_generation": "shared_blocks_v4", "shared_snapshot_key": 71}}}}\')'
        ),
        {"digest": digest},
    )
    await connection.execute(
        text(
            f"INSERT INTO {schema}.ptg2_v4_snapshot_map_root VALUES (71, 'complete', 'packed_coordinate_hash_v1', :digest, 0, 0, 0, 0, 0, 0)"
        ),
        {"digest": digest},
    )
    await connection.execute(
        text(
            f"INSERT INTO {schema}.ptg2_v4_finalizer_map_root VALUES (71, 'complete', 'packed_finalizer_map_v2', 'packed_coordinate_hash_v1', :digest, 0, 0, 0, 0, 0, 0, 0)"
        ),
        {"digest": digest},
    )


async def _seed_map_blocks(connection, schema: str, hashes: set[bytes]) -> tuple[int, int]:
    """Insert one decoded target and map block for every required map kind."""

    map_payload_bytes = 0
    finalizer_payload_bytes = 0
    kinds = ("graph_locator_v1", *PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS)
    for index, kind in enumerate(kinds, start=1):
        target_hash = SharedBlock(kind, 0, 0, 1, "none", 1, b"x").block_hash
        map_payload = encode_v4_snapshot_map_pack(
            kind,
            (SharedBlock(kind, 0, 0, 1, "none", 1, b"x").reference(),),
        )
        # Substitute the synthetic target identity into the encoded coordinate.
        map_payload = map_payload[:-32] + target_hash
        map_hash = shared_block_hash(
            format_version=2,
            object_kind="snapshot_coordinate_map_v1",
            codec="none",
            payload=map_payload,
        )
        hashes.update((target_hash, map_hash))
        if index == 1:
            map_payload_bytes += len(map_payload)
        else:
            finalizer_payload_bytes += len(map_payload)
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_v3_block VALUES (:hash, 2, :kind, 'none', :count, :size, :size, :payload)"
            ),
            {"hash": target_hash, "kind": kind, "count": 1, "size": 1, "payload": b"x"},
        )
        await connection.execute(
            text(
                f"INSERT INTO {schema}.ptg2_v3_block VALUES (:hash, 2, 'snapshot_coordinate_map_v1', 'none', 1, :size, :size, :payload)"
            ),
            {"hash": map_hash, "size": len(map_payload), "payload": map_payload},
        )
        table = "ptg2_v4_snapshot_map_pack" if index == 1 else "ptg2_v4_finalizer_map_pack"
        await connection.execute(
            text(f"INSERT INTO {schema}.{table} VALUES (71, :kind, 0, 1, 1, 1, :map_hash)"),
            {"kind": kind, "map_hash": map_hash},
        )
        if index != 1:
            await connection.execute(
                text(f"INSERT INTO {schema}.ptg2_v4_finalizer_map_target VALUES (71, :target_hash)"),
                {"target_hash": target_hash},
            )
    await connection.execute(
        text(f"INSERT INTO {schema}.ptg2_v3_block VALUES (:hash, 2, 'unrelated_v1', 'none', 1, 1, 1, :payload)"),
        {"hash": b"z" * 32, "payload": b"z"},
    )
    return map_payload_bytes, finalizer_payload_bytes


async def _seed_root_receipts(
    connection,
    schema: str,
    map_payload_bytes: int,
    finalizer_payload_bytes: int,
) -> None:
    """Match root aggregate receipts to the synthetic persisted map packs."""

    await connection.execute(
        text(
            f"""
            UPDATE {schema}.ptg2_v4_snapshot_map_root
               SET object_kind_count = 1, map_pack_count = 1,
                   coordinate_count = 1, entry_count = 1, logical_byte_count = 1,
                   stored_map_byte_count = :stored_bytes
            """
        ),
        {"stored_bytes": map_payload_bytes},
    )
    await connection.execute(
        text(
            f"""
            UPDATE {schema}.ptg2_v4_finalizer_map_root
               SET object_kind_count = 6, map_pack_count = 6,
                   coordinate_count = 6, entry_count = 6,
                   logical_byte_count = 6, stored_map_byte_count = :stored_bytes,
                   target_block_count = 6
            """
        ),
        {"stored_bytes": finalizer_payload_bytes},
    )


@pytest.mark.asyncio
async def test_native_archive_closure_selects_only_decoded_blocks() -> None:
    async with _database() as (engine, schema_name, schema):
        snapshot_id, expected_hashes = await _seed(engine, schema)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session, session.begin():
            closure = await select_result_archive_closure(
                session,
                schema_name=schema_name,
                snapshot_id=snapshot_id,
                retention_pin={"pin_id": "synthetic-pin", "repeatable_read_token": "synthetic-read"},
            )
    assert set(closure.block_hashes) == expected_hashes
    assert b"z" * 32 not in closure.block_hashes
    assert any(item.table_name == "ptg2_v3_block" for item in closure.relations)


@pytest.mark.asyncio
async def test_native_archive_closure_groups_small_target_payloads() -> None:
    """All small closure blocks use one bounded native payload query."""

    async with _database() as (engine, schema_name, schema):
        snapshot_id, expected_hashes = await _seed(engine, schema)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session, session.begin():
            recorded_session = _BoundedReadRecordingSession(session)
            await select_result_archive_closure(
                recorded_session,
                schema_name=schema_name,
                snapshot_id=snapshot_id,
                retention_pin={"pin_id": "synthetic-pin", "repeatable_read_token": "synthetic-read"},
            )
    assert len(recorded_session.payload_parameters) == 1
    assert set(recorded_session.payload_parameters[0]["block_hashes"]) == expected_hashes


@pytest.mark.asyncio
async def test_native_archive_closure_rejects_oversized_metadata_before_payload_read() -> None:
    """A native size violation is rejected before the selector requests payload bytes."""

    async with _database() as (engine, schema_name, schema):
        snapshot_id, _ = await _seed(engine, schema)
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.ptg2_v3_block SET raw_byte_count = :oversized, "
                    "stored_byte_count = :oversized WHERE object_kind = 'graph_locator_v1'"
                ),
                {"oversized": archive_closure._MAX_TARGET_BLOCK_PAYLOAD_BYTES + 1},
            )
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session, session.begin():
            recorded_session = _BoundedReadRecordingSession(session)
            with pytest.raises(ResultArchiveClosureError, match="target block is invalid"):
                await select_result_archive_closure(
                    recorded_session,
                    schema_name=schema_name,
                    snapshot_id=snapshot_id,
                    retention_pin={"pin_id": "synthetic-pin", "repeatable_read_token": "synthetic-read"},
                )
    assert not recorded_session.payload_parameters


@pytest.mark.asyncio
async def test_native_archive_closure_rejects_map_payload_length_mismatch_before_delivery() -> None:
    """Map rows with a false small size never expose their payload to the selector."""

    async with _database() as (engine, schema_name, schema):
        snapshot_id, _ = await _seed(engine, schema)
        async with engine.begin() as connection:
            await connection.execute(
                text(
                    f"UPDATE {schema}.ptg2_v3_block SET raw_byte_count = 1, "
                    "stored_byte_count = 1 WHERE object_kind = 'snapshot_coordinate_map_v1'"
                )
            )
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session, session.begin():
            recorded_session = _BoundedReadRecordingSession(session)
            with pytest.raises(ResultArchiveClosureError, match="no map packs"):
                await select_result_archive_closure(
                    recorded_session,
                    schema_name=schema_name,
                    snapshot_id=snapshot_id,
                    retention_pin={"pin_id": "synthetic-pin", "repeatable_read_token": "synthetic-read"},
                )
    assert not recorded_session.map_payload_rows


@pytest.mark.asyncio
async def test_native_archive_closure_rejects_extra_finalizer_anchor_with_bounded_read() -> None:
    """A rogue anchor is detected without reading beyond expected targets plus one."""

    async with _database() as (engine, schema_name, schema):
        snapshot_id, _ = await _seed(engine, schema)
        async with engine.begin() as connection:
            await connection.execute(
                text(f"INSERT INTO {schema}.ptg2_v4_finalizer_map_target VALUES (71, :block_hash)"),
                {"block_hash": b"q" * 32},
            )
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session, session.begin():
            recorded_session = _BoundedReadRecordingSession(session)
            with pytest.raises(ResultArchiveClosureError, match="finalizer targets are incomplete"):
                await select_result_archive_closure(
                    recorded_session,
                    schema_name=schema_name,
                    snapshot_id=snapshot_id,
                    retention_pin={"pin_id": "synthetic-pin", "repeatable_read_token": "synthetic-read"},
                )
    assert recorded_session.finalizer_anchor_limits == [7]


@pytest.mark.asyncio
async def test_native_archive_closure_rejects_an_unsealed_map_root() -> None:
    async with _database() as (engine, schema_name, schema):
        snapshot_id, _ = await _seed(engine, schema)
        async with engine.begin() as connection:
            await connection.exec_driver_sql(f"UPDATE {schema}.ptg2_v4_snapshot_map_root SET state = 'building'")
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session, session.begin():
            with pytest.raises(ResultArchiveClosureError, match="completed map root"):
                await select_result_archive_closure(
                    session,
                    schema_name=schema_name,
                    snapshot_id=snapshot_id,
                    retention_pin={"pin_id": "synthetic-pin", "repeatable_read_token": "synthetic-read"},
                )


@pytest.mark.asyncio
async def test_native_archive_closure_enforces_bound_while_decoding() -> None:
    """Reject reachability at the first decoded map pack before later packs load."""

    async with _database() as (engine, schema_name, schema):
        snapshot_id, _ = await _seed(engine, schema)
        session_factory = async_sessionmaker(engine, expire_on_commit=False)
        async with session_factory() as session, session.begin():
            with pytest.raises(ResultArchiveClosureError, match="reachability exceeds"):
                await select_result_archive_closure(
                    session,
                    schema_name=schema_name,
                    snapshot_id=snapshot_id,
                    retention_pin={"pin_id": "synthetic-pin", "repeatable_read_token": "synthetic-read"},
                    max_block_hashes=1,
                )


@pytest.mark.asyncio
async def test_every_archive_relation_uses_current_model_columns_and_parses() -> None:
    """Execute every returned predicate against synthetic tables named by current models."""

    required_columns_by_table = {
        "ptg2_snapshot": {"snapshot_id", "import_run_id"},
        "ptg2_v3_snapshot_binding": {"snapshot_id", "snapshot_key"},
        "ptg2_v3_snapshot_layout": {"snapshot_key"},
        "ptg2_v4_snapshot_map_root": {"snapshot_key"},
        "ptg2_v4_snapshot_map_pack": {"snapshot_key"},
        "ptg2_v4_finalizer_map_root": {"snapshot_key"},
        "ptg2_v4_finalizer_map_pack": {"snapshot_key"},
        "ptg2_v4_finalizer_map_target": {"snapshot_key"},
        "ptg2_v3_block": {"block_hash"},
    }
    required_columns_by_table.update(
        {
            table_name: {column.split()[0] for column in columns.split(", ")}
            for table_name, columns in _RELATION_SUPPORT_COLUMNS.items()
        }
    )
    model_columns = _model_columns_by_table()
    relations = archive_closure._relations("synthetic_schema")
    assert relations
    for relation in relations:
        if relation.table_name in _NATIVE_DDL_ONLY_TABLES:
            assert required_columns_by_table[relation.table_name] == {"snapshot_key"}
        else:
            assert required_columns_by_table[relation.table_name] <= model_columns[relation.table_name]
    async with _database() as (engine, schema_name, schema):
        async with engine.begin() as connection:
            await _create_relation_predicate_support(connection, schema)
            for relation in archive_closure._relations(schema_name):
                await connection.execute(
                    text(f"EXPLAIN SELECT 1 FROM {schema}.{relation.table_name} WHERE {relation.predicate_sql}"),
                    {"snapshot_id": "synthetic-result-closure", "snapshot_key": 71, "block_hashes": (b"x" * 32,)},
                )
