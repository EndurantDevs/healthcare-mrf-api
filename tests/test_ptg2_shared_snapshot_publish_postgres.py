# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import os
import struct
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from api import ptg2_serving, ptg2_tables
from api.ptg2_db_sidecars import (
    lookup_serving_binary_by_code_from_db,
    lookup_shared_code_page_from_db,
    lookup_shared_price_atom_memberships_from_db,
    lookup_shared_price_atoms_from_db,
    lookup_shared_provider_code_keys_from_db,
)
from api.ptg2_shared_blocks import fetch_shared_blocks, fetch_shared_graph_members
from db.connection import Database, db
from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts import ptg2_shared_gc
from process.ptg_parts import ptg2_shared_snapshot_publish
from process.ptg_parts import ptg2_v4_snapshot_maps
from process.ptg_parts.ptg2_manifest_artifacts import write_global_membership_sidecar
from process.ptg_parts.ptg2_manifest_publish import (
    PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    _copy_price_atom_member_file,
    _copy_price_set_summary_file,
    _copy_price_atom_file,
    _create_serving_stage_table,
    _ptg2_manifest_support_stage_table,
)
from process.ptg_parts.ptg2_candidate_attestation import (
    PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
)
from process.ptg_parts.ptg2_shared_blocks import (
    SharedBlock,
    SharedBlockReference,
    bind_snapshot_to_shared_layout,
    insert_shared_blocks,
    reserve_shared_layout,
    seal_shared_layout,
    shared_block_hash,
    shared_semantic_fingerprint,
    summarize_shared_snapshot_mappings,
)
from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
)
from process.ptg_parts.ptg2_shared_graph import (
    PTG2_V3_GRAPH_GROUP_TO_NPI,
    PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
    PTG2_V3_GRAPH_NPI_TO_GROUP,
    PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
)
from process.ptg_parts.ptg2_shared_reuse import (
    SharedLogicalPlanScope,
    SharedPhysicalArtifactIdentity,
    SharedSnapshotSourceAssignment,
    shared_layout_support_digest,
    shared_source_set_metadata,
)
from process.ptg_parts.ptg2_shared_price import _create_v3_price_key_stage
from process.ptg_parts.ptg2_shared_publish import (
    _upsert_shared_block_mappings,
    copy_shared_block_binary_file,
    create_shared_block_stage,
    publish_shared_block_stage,
)
from process.ptg_parts.ptg2_shared_snapshot_publish import (
    publish_shared_v3_snapshot_sources,
    publish_strict_shared_v3_layout,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATHS = (
    ROOT / "alembic" / "versions" / "20260712120000_ptg2_v3_shared_schema.py",
    ROOT
    / "alembic"
    / "versions"
    / "20260715120000_ptg2_v3_source_audit_witness.py",
    ROOT
    / "alembic"
    / "versions"
    / "20260716130000_ptg2_v3_multi_plan_scope.py",
    ROOT
    / "alembic"
    / "versions"
    / "20260721150000_ptg2_source_witness_parts.py",
    ROOT
    / "alembic"
    / "versions"
    / "20260724100000_ptg2_v4_attempt_fence.py",
    ROOT
    / "alembic"
    / "versions"
    / "20260810120000_ptg2_layout_build_candidates.py",
    ROOT
    / "alembic"
    / "versions"
    / "20260810130000_ptg2_block_build_pins.py",
)
SCANNER_TEST_PATH = Path(__file__).with_name("test_ptg2_scanner_v3_runs.py")
SERVING_RECORD = struct.Struct(">16s16s16sI")
SHARED_BLOCK_COPY_HEADER = b"PGCOPY\n\xff\r\n\x00" + struct.pack(">ii", 0, 0)

_SELECTIVE_V4_LAYOUT_DDL = """
CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
    snapshot_key bigint PRIMARY KEY,
    generation varchar(32) NOT NULL,
    state varchar(16) NOT NULL,
    build_token varchar(96) NOT NULL
)
"""
_SELECTIVE_V4_MAP_ROOT_DDL = """
CREATE TABLE {schema}.ptg2_v4_snapshot_map_root (
    snapshot_key bigint PRIMARY KEY,
    state varchar(16) NOT NULL,
    format_version smallint NOT NULL,
    map_format varchar(32) NOT NULL,
    representation varchar(32) NOT NULL,
    projection_id_scope varchar(32) NOT NULL,
    map_digest bytea,
    object_kind_count integer NOT NULL DEFAULT 0,
    map_pack_count bigint NOT NULL DEFAULT 0,
    coordinate_count bigint NOT NULL DEFAULT 0,
    entry_count bigint NOT NULL DEFAULT 0,
    logical_byte_count bigint NOT NULL DEFAULT 0,
    stored_map_byte_count bigint NOT NULL DEFAULT 0,
    npi_count bigint NOT NULL DEFAULT 0,
    component_count bigint NOT NULL DEFAULT 0,
    pattern_count bigint NOT NULL DEFAULT 0,
    relation_count integer NOT NULL DEFAULT 0,
    heavy_owner_count bigint NOT NULL DEFAULT 0,
    created_at timestamptz NOT NULL DEFAULT now(),
    completed_at timestamptz
)
"""
_SELECTIVE_V4_MAP_PACK_DDL = """
CREATE TABLE {schema}.ptg2_v4_snapshot_map_pack (
    snapshot_key bigint NOT NULL,
    object_kind varchar(64) NOT NULL,
    pack_no integer NOT NULL,
    first_block_key bigint NOT NULL,
    first_fragment_no integer NOT NULL,
    last_block_key bigint NOT NULL,
    last_fragment_no integer NOT NULL,
    coordinate_count integer NOT NULL,
    entry_count bigint NOT NULL,
    logical_byte_count bigint NOT NULL,
    map_block_hash bytea NOT NULL,
    PRIMARY KEY (snapshot_key, object_kind, pack_no)
)
"""


def _shared_block_copy_field(value: bytes) -> bytes:
    return struct.pack(">i", len(value)) + value


def _shared_block_copy_row(
    block_key: int,
    payload: bytes,
    entry_count: int = 1,
) -> bytes:
    block_hash = shared_block_hash(
        format_version=2,
        object_kind="serving",
        codec="none",
        payload=payload,
    )
    fields = (
        block_hash,
        struct.pack(">h", 2),
        b"serving",
        struct.pack(">q", block_key),
        struct.pack(">i", 0),
        struct.pack(">q", entry_count),
        b"none",
        struct.pack(">q", len(payload)),
        struct.pack(">q", len(payload)),
        payload,
    )
    return struct.pack(">h", len(fields)) + b"".join(
        _shared_block_copy_field(field) for field in fields
    )


def _shared_block_copy_payload(*rows: tuple[int, bytes, int]) -> bytes:
    return (
        SHARED_BLOCK_COPY_HEADER
        + b"".join(
            _shared_block_copy_row(block_key, payload, entry_count)
            for block_key, payload, entry_count in rows
        )
        + struct.pack(">h", -1)
    )


class _OpRecorder:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, statement) -> None:
        self.executed.append(str(statement))


def _load_module(path: Path, module_name: str):
    spec = importlib.util.spec_from_file_location(module_name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _graph_artifacts(
    directory: Path,
    *,
    provider_set_id: bytes,
    provider_group_id: bytes,
    npi: int,
) -> list[dict[str, object]]:
    npi_id = b"\0" * 8 + int(npi).to_bytes(8, "big", signed=False)
    graph_members_by_artifact = {
        "provider_group_npi": {provider_group_id: (npi_id,)},
        "provider_npi_group": {npi_id: (provider_group_id,)},
        "provider_inverted": {provider_group_id: (provider_set_id,)},
        "provider_forward": {provider_set_id: (provider_group_id,)},
    }
    entries: list[dict[str, object]] = []
    directory.mkdir(parents=True)
    for name, mapping in graph_members_by_artifact.items():
        manifest = write_global_membership_sidecar(directory, name, mapping)
        sidecar_metadata_map = dict(manifest["sidecars"][0])
        sidecar_metadata_map.update(
            {
                "name": name,
                "path": str(
                    (directory / str(sidecar_metadata_map["path"])).resolve()
                ),
                "source_shard_id": "shared-smoke-shard",
            }
        )
        entries.append(sidecar_metadata_map)
    return entries


async def _create_shared_schema(schema_name: str) -> None:
    recorder = _OpRecorder()
    with patch.dict(os.environ, {"HLTHPRT_DB_SCHEMA": schema_name}):
        for migration_index, migration_path in enumerate(MIGRATION_PATHS):
            migration = _load_module(
                migration_path,
                f"ptg2_shared_schema_{migration_index}_{schema_name}",
            )
            migration.op = recorder
            migration._schema = lambda: schema_name
            migration.upgrade()
    quoted_schema = '"' + schema_name.replace('"', '""') + '"'
    await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
    for statement in recorder.executed:
        await db.execute_ddl(statement)
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.code_catalog
            (code_system, code, display_name, short_description)
        VALUES ('CPT', '99213', 'Office visit', 'Office visit')
        """
    )


async def _create_selective_block_schema(schema_name: str) -> None:
    """Create the selective shared-block PostgreSQL fixture schema."""
    quoted_schema = '"' + schema_name.replace('"', '""') + '"'
    await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
    await _create_selective_block_and_layout_tables(quoted_schema)
    await _create_selective_pin_and_mapping_tables(quoted_schema)
    fixture_tables = {
        "ptg2_v3_snapshot_layout",
        "ptg2_v3_block",
        "ptg2_v3_snapshot_block",
        "ptg2_v3_gc_candidate",
        "ptg2_block_build_pin",
    }
    for table_name in (
        set(ptg2_shared_gc.PTG2_V3_MIGRATION_OWNED_TABLE_NAMES)
        - fixture_tables
    ):
        await db.execute_ddl(
            f'CREATE TABLE {quoted_schema}."{table_name}" (snapshot_key bigint)'
        )


async def _create_selective_block_and_layout_tables(quoted_schema: str) -> None:
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_v3_block (
            block_hash bytea PRIMARY KEY,
            format_version smallint NOT NULL,
            object_kind varchar(64) NOT NULL,
            codec varchar(16) NOT NULL,
            entry_count bigint NOT NULL,
            raw_byte_count bigint NOT NULL,
            stored_byte_count bigint NOT NULL,
            payload bytea NOT NULL,
            created_at timestamp with time zone NOT NULL
        )
        """
    )
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_layout (
            snapshot_key bigint PRIMARY KEY,
            generation varchar(32) NOT NULL,
            state varchar(16) NOT NULL,
            build_token varchar(96) NOT NULL
        )
        """
    )


async def _create_selective_pin_and_mapping_tables(quoted_schema: str) -> None:
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_block_build_pin (
            snapshot_key bigint NOT NULL REFERENCES
                {quoted_schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            build_token varchar(96) NOT NULL,
            pin_token varchar(96) NOT NULL,
            block_hash bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            heartbeat_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            lease_until timestamptz NOT NULL,
            PRIMARY KEY (snapshot_key, pin_token, block_hash)
        )
        """
    )
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_block (
            snapshot_key bigint NOT NULL,
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL,
            fragment_no integer NOT NULL,
            entry_count bigint NOT NULL,
            block_hash bytea NOT NULL,
            PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no),
            FOREIGN KEY (block_hash)
                REFERENCES {quoted_schema}.ptg2_v3_block (block_hash)
        )
        """
    )
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_v3_gc_candidate (
            block_hash bytea PRIMARY KEY,
            eligible_at timestamp with time zone NOT NULL,
            queued_at timestamp with time zone NOT NULL DEFAULT now(),
            FOREIGN KEY (block_hash)
                REFERENCES {quoted_schema}.ptg2_v3_block (block_hash)
                ON DELETE CASCADE
        )
        """
    )


async def _insert_selective_durable_block(
    schema_name: str,
    payload: bytes,
    *,
    entry_count: int = 1,
) -> bytes:
    quoted_schema = '"' + schema_name.replace('"', '""') + '"'
    block_hash = shared_block_hash(
        format_version=2,
        object_kind="serving",
        codec="none",
        payload=payload,
    )
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.ptg2_v3_block
            (block_hash, format_version, object_kind, codec, entry_count,
             raw_byte_count, stored_byte_count, payload, created_at)
        VALUES (:block_hash, 2, 'serving', 'none', :entry_count,
                :payload_bytes, :payload_bytes, :payload, now())
        """,
        block_hash=block_hash,
        entry_count=entry_count,
        payload_bytes=len(payload),
        payload=payload,
    )
    return block_hash


async def _queue_selective_gc_candidate(
    schema_name: str,
    block_hash: bytes,
) -> None:
    quoted_schema = '"' + schema_name.replace('"', '""') + '"'
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.ptg2_v3_gc_candidate
            (block_hash, eligible_at, queued_at)
        VALUES (:block_hash, now() - INTERVAL '1 minute', now())
        """,
        block_hash=block_hash,
    )


async def _stage_selective_block_copy(
    tmp_path: Path,
    schema_name: str,
    *block_rows: tuple[int, bytes, int],
    stage_table: str = "block_stage",
):
    quoted_schema = '"' + schema_name.replace('"', '""') + '"'
    copy_payload = _shared_block_copy_payload(*block_rows)
    copy_path = tmp_path / f"{schema_name}-{stage_table}.copy"
    copy_path.write_bytes(copy_payload)
    await create_shared_block_stage(
        schema_name=schema_name,
        stage_table=stage_table,
    )
    metrics = await copy_shared_block_binary_file(
        copy_path,
        schema_name=schema_name,
        stage_table=stage_table,
        expected_copy_bytes=len(copy_payload),
        expected_copy_sha256=hashlib.sha256(copy_payload).hexdigest(),
        reuse_existing=True,
    )
    assert metrics is not None
    stage_counts = await db.first(
        f"""
        SELECT COUNT(*)::bigint,
               COUNT(payload)::bigint,
               COALESCE(SUM(octet_length(payload)), 0)::bigint
          FROM {quoted_schema}.{stage_table}
        """
    )
    return metrics, tuple(map(int, stage_counts))


async def _stage_null_reuses_before_payload(schema_name: str) -> bytes:
    """Stage one payload after enough null reuses to cross a publish batch."""

    quoted_schema = '"' + schema_name.replace('"', '""') + '"'
    payload_bytes = b"payload-after-null-reuses"
    block_hash = shared_block_hash(
        format_version=2,
        object_kind="serving",
        codec="none",
        payload=payload_bytes,
    )
    await create_shared_block_stage(
        schema_name=schema_name,
        stage_table="block_stage",
    )
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.block_stage
            (block_hash, format_version, object_kind, block_key, fragment_no,
             entry_count, codec, raw_byte_count, stored_byte_count, payload)
        SELECT :block_hash, 2, 'serving', block_key, 0,
               1, 'none', :byte_count, :byte_count, NULL
          FROM generate_series(1, 4096) AS block_key
        """,
        block_hash=block_hash,
        byte_count=len(payload_bytes),
    )
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.block_stage
            (block_hash, format_version, object_kind, block_key, fragment_no,
             entry_count, codec, raw_byte_count, stored_byte_count, payload)
        VALUES (:block_hash, 2, 'serving', 4097, 0,
                1, 'none', :byte_count, :byte_count, :payload)
        """,
        block_hash=block_hash,
        byte_count=len(payload_bytes),
        payload=payload_bytes,
    )
    return block_hash


@asynccontextmanager
async def _selective_block_database(monkeypatch):
    """Build the disposable database used by selective block tests."""
    schema_name = f"ptg2_selective_{uuid.uuid4().hex[:16]}"
    quoted_schema = '"' + schema_name + '"'
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_shared_layout_for_dense_write",
        AsyncMock(),
    )
    prepare_v4_cas = ptg2_shared_publish.prepare_v4_cas_block_stage
    prepare_shared_cas = ptg2_shared_publish.prepare_shared_cas_block_stage
    prepare_owned_v4_cas, prepare_owned_shared_cas = _owned_cas_preparers(
        prepare_v4_cas,
        prepare_shared_cas,
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "prepare_v4_cas_block_stage",
        prepare_owned_v4_cas,
    )
    monkeypatch.setattr(
        ptg2_shared_publish,
        "prepare_shared_cas_block_stage",
        prepare_owned_shared_cas,
    )
    await db.disconnect()
    await db.connect()
    try:
        await _create_selective_block_schema(schema_name)
        yield schema_name, quoted_schema
    finally:
        await db.status(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await db.disconnect()


def _owned_cas_preparers(prepare_v4_cas, prepare_shared_cas):
    """Wrap CAS preparation with deterministic layout ownership rows."""

    async def prepare_owned_shared_cas(**kwargs):
        schema = '"' + kwargs["schema_name"].replace('"', '""') + '"'
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_layout
                (snapshot_key, generation, state, build_token)
            VALUES (:snapshot_key, :generation, 'building', :build_token)
            ON CONFLICT (snapshot_key) DO UPDATE
                SET generation = EXCLUDED.generation,
                    state = EXCLUDED.state,
                    build_token = EXCLUDED.build_token
            """,
            snapshot_key=int(kwargs["snapshot_key"]),
            generation=str(kwargs["expected_generation"]),
            build_token=str(kwargs["build_token"]),
        )
        return await prepare_shared_cas(**kwargs)

    async def prepare_owned_v4_cas(**kwargs):
        schema = '"' + kwargs["schema_name"].replace('"', '""') + '"'
        await db.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_layout
                (snapshot_key, generation, state, build_token)
            VALUES (:snapshot_key, 'shared_blocks_v4', 'building', :build_token)
            ON CONFLICT (snapshot_key) DO UPDATE
                SET generation = EXCLUDED.generation,
                    state = EXCLUDED.state,
                    build_token = EXCLUDED.build_token
            """,
            snapshot_key=int(kwargs["snapshot_key"]),
            build_token=str(kwargs["build_token"]),
        )
        return await prepare_v4_cas(**kwargs)

    return prepare_owned_v4_cas, prepare_owned_shared_cas


async def _publish_rebuild_scope_proof_layout(
    *,
    schema_name: str,
    build_token: str,
    full_rebuild_scope_digest: str | None,
):
    identity_by_field = {"fixture": "full-rebuild-scope-proof-v1"}
    if full_rebuild_scope_digest is not None:
        identity_by_field["full_rebuild_scope_digest"] = (
            full_rebuild_scope_digest
        )
    semantic_fingerprint = shared_semantic_fingerprint(identity_by_field)
    async with db.transaction() as session:
        reservation = await reserve_shared_layout(
            session,
            schema_name=schema_name,
            semantic_fingerprint=semantic_fingerprint,
            build_token=build_token,
        )
        if reservation.reused:
            return reservation, None
        block = SharedBlock(
            object_kind="scope_proof",
            block_key=1,
            fragment_no=0,
            entry_count=1,
            codec="none",
            raw_byte_count=15,
            payload=b"identical-block",
        )
        await insert_shared_blocks(
            session,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
            blocks=(block,),
        )
        summary = await summarize_shared_snapshot_mappings(
            session,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
        )
        sealed = await seal_shared_layout(
            session,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
            build_token=build_token,
            expected_summary=summary,
            support_digest=shared_layout_support_digest(
                core_support={"fixture": "full-rebuild-scope-proof-v1"},
                audit_sample={},
                source_witness={},
                full_rebuild_scope_digest=full_rebuild_scope_digest,
            ),
            layout_manifest={"fixture": "full-rebuild-scope-proof-v1"},
        )
    return reservation, sealed


async def _publish_scope_proof_layout_set(schema_name: str):
    legacy_reservation, legacy_seal = await _publish_rebuild_scope_proof_layout(
        schema_name=schema_name,
        build_token="legacy-build",
        full_rebuild_scope_digest=None,
    )
    legacy_retry, legacy_retry_seal = await _publish_rebuild_scope_proof_layout(
        schema_name=schema_name,
        build_token="legacy-retry",
        full_rebuild_scope_digest=None,
    )
    scoped_layout_pairs = tuple(
        [
            await _publish_rebuild_scope_proof_layout(
                schema_name=schema_name,
                build_token=f"scoped-build-{scope_number}",
                full_rebuild_scope_digest=str(scope_number) * 64,
            )
            for scope_number in (1, 2)
        ]
    )
    return (
        legacy_reservation,
        legacy_seal,
        legacy_retry,
        legacy_retry_seal,
        scoped_layout_pairs,
    )


async def _assert_scope_proof_layout_set(schema_name: str, layout_set) -> None:
    (
        legacy_reservation,
        legacy_seal,
        legacy_retry,
        legacy_retry_seal,
        scoped_layout_pairs,
    ) = layout_set
    scoped_snapshot_keys = [
        reservation.snapshot_key
        for reservation, _sealed_layout in scoped_layout_pairs
    ]
    assert legacy_seal is not None and legacy_seal.reused is False
    assert legacy_retry.reused is True and legacy_retry_seal is None
    assert all(
        reservation.reused is False
        for reservation, _sealed_layout in scoped_layout_pairs
    )
    assert all(
        sealed_layout is not None and sealed_layout.reused is False
        for _reservation, sealed_layout in scoped_layout_pairs
    )
    assert len({legacy_reservation.snapshot_key, *scoped_snapshot_keys}) == 3
    quoted_schema = f'"{schema_name}"'
    stored_block_count = await db.scalar(
        f"SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_block"
    )
    assert int(stored_block_count or 0) == 1
    scoped_mapping_count = await db.scalar(
        f"""
        SELECT COUNT(*)
          FROM {quoted_schema}.ptg2_v3_snapshot_block
         WHERE snapshot_key = ANY(CAST(:snapshot_keys AS bigint[]))
        """,
        snapshot_keys=scoped_snapshot_keys,
    )
    assert int(scoped_mapping_count or 0) == 2


@pytest.mark.asyncio
async def test_real_postgres_full_rebuild_scopes_keep_global_block_dedup():
    """Isolate rebuild layouts while retaining content-addressed block storage."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1 for PostgreSQL proof"
        )
    schema_name = f"ptg2_rebuild_scope_{uuid.uuid4().hex[:16]}"
    quoted_schema = f'"{schema_name}"'
    await db.disconnect()
    await db.connect()
    try:
        await _create_shared_schema(schema_name)
        layout_set = await _publish_scope_proof_layout_set(schema_name)
        await _assert_scope_proof_layout_set(schema_name, layout_set)
    finally:
        await db.status(f"DROP SCHEMA IF EXISTS {quoted_schema} CASCADE")
        await db.disconnect()


async def _create_cross_file_price_summary_fixture(schema_name, summary_table):
    quoted_schema = f'"{schema_name}"'
    await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
    await db.execute_ddl(
        f"""
        CREATE UNLOGGED TABLE {quoted_schema}.{summary_table} (
            price_set_global_id_128 bytea NOT NULL,
            minimum_negotiated_rate numeric NOT NULL
        )
        """
    )
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.{summary_table}
            (price_set_global_id_128, minimum_negotiated_rate)
        VALUES
            (decode(repeat('11', 16), 'hex'), 10.00),
            (decode(repeat('11', 16), 'hex'), 10.00),
            (decode(repeat('22', 16), 'hex'), -1.25),
            (decode(repeat('22', 16), 'hex'), -1.25),
            (decode(repeat('33', 16), 'hex'), 10.00),
            (decode(repeat('33', 16), 'hex'), 10.00)
        """
    )


async def _assert_cross_file_price_ranking(schema_name, summary_table):
    dense_stats = await _create_v3_price_key_stage(
        schema_name=schema_name,
        price_set_summary_table=summary_table,
        price_set_summary_source_count=2,
        stage_table="price_keys",
    )
    assert dense_stats == {
        "row_count": 3,
        "distinct_id_count": 3,
        "distinct_key_count": 3,
        "minimum_key": 0,
        "maximum_key": 2,
    }
    ranked_rows = await db.all(
        f"""
        SELECT encode(price_set_global_id_128, 'hex'),
               price_key,
               minimum_negotiated_rate
          FROM "{schema_name}".price_keys
         ORDER BY price_key
        """
    )
    assert [
        (str(price_row[0]), int(price_row[1]), Decimal(price_row[2]))
        for price_row in ranked_rows
    ] == [
        ("22" * 16, 0, Decimal("-1.25")),
        ("11" * 16, 1, Decimal("10.00")),
        ("33" * 16, 2, Decimal("10.00")),
    ]


async def _assert_cross_file_price_conflict(schema_name, summary_table):
    quoted_schema = f'"{schema_name}"'
    await db.status(f"TRUNCATE TABLE {quoted_schema}.{summary_table}")
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.{summary_table}
            (price_set_global_id_128, minimum_negotiated_rate)
        VALUES
            (decode(repeat('44', 16), 'hex'), 2.50),
            (decode(repeat('44', 16), 'hex'), 2.51)
        """
    )
    with pytest.raises(RuntimeError, match="conflicting minimum rates"):
        await _create_v3_price_key_stage(
            schema_name=schema_name,
            price_set_summary_table=summary_table,
            price_set_summary_source_count=2,
            stage_table="price_keys_conflict",
        )
    assert not bool(
        await db.scalar(
            """
            SELECT EXISTS (
                SELECT 1 FROM pg_catalog.pg_tables
                 WHERE schemaname = :schema_name
                   AND tablename = 'price_keys_conflict'
            )
            """,
            schema_name=schema_name,
        )
    )


@pytest.mark.asyncio
async def test_real_postgres_cross_file_price_set_summary_ranking_and_conflict():
    """Rank matching cross-file summaries and reject conflicting minima."""
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")
    schema_name = f"ptg2_price_summary_{uuid.uuid4().hex[:16]}"
    summary_table = "price_set_summary"
    await db.disconnect()
    await db.connect()
    try:
        await _create_cross_file_price_summary_fixture(schema_name, summary_table)
        await _assert_cross_file_price_ranking(schema_name, summary_table)
        await _assert_cross_file_price_conflict(schema_name, summary_table)
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await db.disconnect()


async def _create_mapping_upsert_fixture(schema_name):
    quoted_schema = f'"{schema_name}"'
    await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
    await db.execute_ddl(
        f"""
        CREATE UNLOGGED TABLE {quoted_schema}.block_stage (
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL,
            fragment_no integer NOT NULL,
            entry_count bigint NOT NULL,
            block_hash bytea NOT NULL
        )
        """
    )
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_block (
            snapshot_key bigint NOT NULL,
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL,
            fragment_no integer NOT NULL,
            entry_count bigint NOT NULL,
            block_hash bytea NOT NULL,
            PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
        )
        """
    )


async def _replace_mapping_stage(schema_name, values_sql):
    quoted_schema = f'"{schema_name}"'
    await db.status(f"TRUNCATE TABLE {quoted_schema}.block_stage")
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.block_stage
            (object_kind, block_key, fragment_no, entry_count, block_hash)
        VALUES {values_sql}
        """
    )


async def _upsert_mapping_stage(schema_name, snapshot_key):
    async with db.transaction() as session:
        await _upsert_shared_block_mappings(
            session,
            schema_name=schema_name,
            stage_table="block_stage",
            snapshot_key=snapshot_key,
        )


async def _snapshot_mapping_count(schema_name, snapshot_key=None):
    where_sql = "" if snapshot_key is None else " WHERE snapshot_key = :snapshot_key"
    query_params = {} if snapshot_key is None else {"snapshot_key": snapshot_key}
    return int(
        await db.scalar(
            f'SELECT COUNT(*) FROM "{schema_name}".ptg2_v3_snapshot_block'
            + where_sql,
            **query_params,
        )
        or 0
    )


async def _assert_idempotent_mapping_upsert(schema_name):
    await _replace_mapping_stage(
        schema_name,
        """
        ('serving', 1, 0, 3, decode(repeat('11', 32), 'hex')),
        ('serving', 2, 0, 4, decode(repeat('22', 32), 'hex'))
        """,
    )
    await _upsert_mapping_stage(schema_name, 7)
    await _upsert_mapping_stage(schema_name, 7)
    assert await _snapshot_mapping_count(schema_name) == 2


async def _assert_duplicate_mapping_rejection(schema_name):
    await db.status(
        f"""
        INSERT INTO "{schema_name}".block_stage
            (object_kind, block_key, fragment_no, entry_count, block_hash)
        VALUES ('serving', 1, 0, 3, decode(repeat('11', 32), 'hex'))
        """
    )
    for snapshot_key in (7, 8):
        with pytest.raises(RuntimeError, match="mapping conflicts"):
            await _upsert_mapping_stage(schema_name, snapshot_key)
    assert await _snapshot_mapping_count(schema_name, 8) == 0
    await _replace_mapping_stage(
        schema_name,
        """
        ('serving', 1, 0, 3, decode(repeat('11', 32), 'hex')),
        ('serving', 1, 0, 4, decode(repeat('ff', 32), 'hex'))
        """,
    )
    with pytest.raises(RuntimeError, match="mapping conflicts"):
        await _upsert_mapping_stage(schema_name, 9)
    assert await _snapshot_mapping_count(schema_name, 9) == 0


async def _assert_conflicting_mapping_rollback(schema_name):
    await _replace_mapping_stage(
        schema_name,
        """
        ('serving', 1, 0, 3, decode(repeat('11', 32), 'hex')),
        ('serving', 2, 0, 4, decode(repeat('ff', 32), 'hex')),
        ('serving', 3, 0, 5, decode(repeat('33', 32), 'hex'))
        """,
    )
    with pytest.raises(RuntimeError, match="mapping conflicts"):
        await _upsert_mapping_stage(schema_name, 7)
    assert await _snapshot_mapping_count(schema_name, 7) == 2
    assert not bool(
        await db.scalar(
            f"""
            SELECT EXISTS (
                SELECT 1 FROM "{schema_name}".ptg2_v3_snapshot_block
                 WHERE snapshot_key = 7 AND block_key = 3
            )
            """
        )
    )
    stored_hash = await db.scalar(
        f"""
        SELECT encode(block_hash, 'hex')
          FROM "{schema_name}".ptg2_v3_snapshot_block
         WHERE snapshot_key = 7 AND block_key = 2
        """
    )
    assert stored_hash == "22" * 32


@pytest.mark.asyncio
async def test_real_postgres_shared_block_mapping_upsert_is_idempotent_and_fail_closed():
    """Accept unique retries while rejecting every duplicate mapping key."""
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")
    schema_name = f"ptg2_mapping_upsert_{uuid.uuid4().hex[:16]}"
    await db.disconnect()
    await db.connect()
    try:
        await _create_mapping_upsert_fixture(schema_name)
        await _assert_idempotent_mapping_upsert(schema_name)
        await _assert_duplicate_mapping_rejection(schema_name)
        await _assert_conflicting_mapping_rollback(schema_name)
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await db.disconnect()


@pytest.mark.asyncio
async def test_real_postgres_selective_copy_stages_each_new_hash_once(
    tmp_path,
    monkeypatch,
):
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        metrics, stage_counts = await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"alpha", 1),
            (2, b"alpha", 1),
            (3, b"beta", 1),
        )
        assert stage_counts == (3, 2, len(b"alpha") + len(b"beta"))
        assert metrics.unique_block_count == 2
        assert metrics.new_block_count == 2
        assert metrics.duplicate_block_row_count == 1
        assert metrics.same_copy_reused_row_count == 1
        publication = await publish_shared_block_stage(
            schema_name=schema_name,
            stage_table="block_stage",
            snapshot_key=1,
            build_token="build-1",
        )
        assert publication.mapping_count == 3
        assert publication.unique_block_count == 2
        assert int(
            await db.scalar(f"SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_block")
            or 0
        ) == 2


@pytest.mark.asyncio
async def test_real_postgres_batched_shared_publish_orders_payload_before_reuses(
    monkeypatch,
):
    """A later payload row protects null reuses split into an earlier batch."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        block_hash = await _stage_null_reuses_before_payload(schema_name)
        progress_events = []
        publication = await publish_shared_block_stage(
            schema_name=schema_name,
            stage_table="block_stage",
            snapshot_key=11,
            build_token="payload-first-shared",
            progress_callback=lambda metric, amount: progress_events.append(
                (metric, amount)
            ),
        )
        assert publication.mapping_count == 4097
        assert publication.unique_block_count == 1
        assert progress_events == [
            ("block_pin_batches", 1),
            ("durable_cas_batches", 1),
            ("sql_stage_rows", 1),
            ("publish_batches", 1),
            ("sql_stage_rows", 4096),
            ("publish_batches", 1),
        ]
        assert (
            await db.scalar(
                f"SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_block "
                "WHERE block_hash = :block_hash",
                block_hash=block_hash,
            )
            == 1
        )


@pytest.mark.asyncio
async def test_real_postgres_batched_v4_cas_orders_payload_before_reuses(
    monkeypatch,
):
    """V4 pins and commits CAS before bounded lock-free validation."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_v4_shared_layout_for_map_write",
        AsyncMock(),
    )
    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        block_hash = await _stage_null_reuses_before_payload(schema_name)
        progress_events = []
        publication = await (
            ptg2_shared_publish._publish_v4_cas_block_stage_compatibility(
                schema_name=schema_name,
                stage_table="block_stage",
                snapshot_key=12,
                build_token="payload-first-v4",
                progress_callback=lambda metric, amount: progress_events.append(
                    (metric, amount)
                ),
            )
        )
        assert publication.staged_row_count == 4097
        assert publication.unique_block_count == 1
        assert progress_events == [
            ("block_pin_batches", 1),
            ("durable_cas_batches", 1),
            ("sql_stage_rows", 1),
            ("publish_batches", 1),
            ("sql_stage_rows", 4096),
            ("publish_batches", 1),
        ]
        assert (
            await db.scalar(
                f"SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_block "
                "WHERE block_hash = :block_hash",
                block_hash=block_hash,
            )
            == 1
        )


@pytest.mark.asyncio
async def test_real_postgres_shared_block_existence_lateral_batches_are_exact(
    monkeypatch,
):
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        stored_count = ptg2_shared_publish._SHARED_BLOCK_EXISTENCE_BATCH_ROWS
        requested_hashes = {
            index.to_bytes(32, "big") for index in range(stored_count + 1)
        }
        await db.status(
            f"""
            INSERT INTO {quoted_schema}.ptg2_v3_block
                (block_hash, format_version, object_kind, codec, entry_count,
                 raw_byte_count, stored_byte_count, payload, created_at)
            SELECT decode(lpad(to_hex(value), 64, '0'), 'hex'),
                   2, 'serving', 'none', 0, 0, 0, ''::bytea, now()
              FROM generate_series(0, :maximum_value) AS value
            """,
            maximum_value=stored_count - 1,
        )

        existing_hashes = await ptg2_shared_publish._existing_shared_block_hashes(
            schema_name=schema_name,
            requested_hashes=requested_hashes,
        )

        assert existing_hashes == {
            index.to_bytes(32, "big") for index in range(stored_count)
        }


@pytest.mark.asyncio
async def test_real_postgres_selective_copy_rejects_same_copy_metadata_conflict(
    tmp_path,
    monkeypatch,
):
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        metrics, stage_counts = await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"same-hash", 1),
            (2, b"same-hash", 9),
        )
        assert stage_counts == (2, 1, len(b"same-hash"))
        assert metrics.same_copy_reused_row_count == 1
        with pytest.raises(RuntimeError, match="conflicts with stored content metadata"):
            await publish_shared_block_stage(
                schema_name=schema_name,
                stage_table="block_stage",
                snapshot_key=6,
                build_token="build-6",
            )
        assert int(
            await db.scalar(f"SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_block")
            or 0
        ) == 0
        assert int(
            await db.scalar(
                f"SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_snapshot_block"
            )
            or 0
        ) == 0


@pytest.mark.asyncio
async def test_real_postgres_selective_copy_full_reuse(
    tmp_path,
    monkeypatch,
):
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (schema_name, _quoted):
        await _insert_selective_durable_block(schema_name, b"alpha")
        await _insert_selective_durable_block(schema_name, b"beta")
        metrics, stage_counts = await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"alpha", 1),
            (2, b"alpha", 1),
            (3, b"beta", 1),
        )
        assert stage_counts == (3, 0, 0)
        assert metrics.existing_block_count == 2
        assert metrics.new_block_count == 0
        assert metrics.durable_reused_row_count == 3
        assert metrics.same_copy_reused_row_count == 0
        publication = await publish_shared_block_stage(
            schema_name=schema_name,
            stage_table="block_stage",
            snapshot_key=2,
            build_token="build-2",
        )
        assert publication.mapping_count == 3
        assert publication.unique_block_count == 2


@pytest.mark.asyncio
async def test_real_postgres_selective_copy_mixed_reuse(
    tmp_path,
    monkeypatch,
):
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (schema_name, _quoted):
        await _insert_selective_durable_block(schema_name, b"alpha")
        metrics, stage_counts = await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"alpha", 1),
            (2, b"beta", 1),
        )
        assert stage_counts == (2, 1, len(b"beta"))
        assert metrics.existing_block_count == 1
        assert metrics.new_block_count == 1
        assert metrics.durable_reused_row_count == 1
        publication = await publish_shared_block_stage(
            schema_name=schema_name,
            stage_table="block_stage",
            snapshot_key=3,
            build_token="build-3",
        )
        assert publication.mapping_count == 2
        assert publication.unique_block_count == 2


@pytest.mark.asyncio
async def test_real_postgres_selective_copy_fails_if_reused_block_disappears(
    tmp_path,
    monkeypatch,
):
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        block_hash = await _insert_selective_durable_block(schema_name, b"gone")
        metrics, stage_counts = await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"gone", 1),
        )
        assert stage_counts == (1, 0, 0)
        assert metrics.existing_block_count == 1
        await db.status(
            f"DELETE FROM {quoted_schema}.ptg2_v3_block WHERE block_hash = :block_hash",
            block_hash=block_hash,
        )
        with pytest.raises(
            RuntimeError,
            match="has no payload or durable CAS row",
        ):
            await publish_shared_block_stage(
                schema_name=schema_name,
                stage_table="block_stage",
                snapshot_key=4,
                build_token="build-4",
            )
        assert int(
            await db.scalar(
                f"SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_snapshot_block"
            )
            or 0
        ) == 0


def _hold_shared_mapping_attach(monkeypatch):
    """Pause snapshot-local mapping after durable pins and CAS are ready."""

    mapping_started = asyncio.Event()
    allow_mapping = asyncio.Event()
    original_mapping_attach = (
        ptg2_shared_publish._publish_shared_block_stage_batched
    )

    async def held_mapping_attach(*args, **kwargs):
        mapping_started.set()
        await allow_mapping.wait()
        return await original_mapping_attach(*args, **kwargs)

    monkeypatch.setattr(
        ptg2_shared_publish,
        "_publish_shared_block_stage_batched",
        held_mapping_attach,
    )
    return mapping_started, allow_mapping


def _hold_v4_after_durable_pins(monkeypatch):
    """Pause V4 CAS work after every stage hash has a durable GC pin."""

    pins_ready = asyncio.Event()
    allow_cas = asyncio.Event()
    original_publish = ptg2_shared_publish._publish_v4_durable_cas_batch

    async def held_publish(**kwargs):
        pins_ready.set()
        await allow_cas.wait()
        return await original_publish(**kwargs)

    monkeypatch.setattr(
        ptg2_shared_publish,
        "_publish_v4_durable_cas_batch",
        held_publish,
    )
    return pins_ready, allow_cas


async def _stage_reverse_order_reuse_publishers(tmp_path, schema_name: str):
    """Create two durable reuse stages with opposite physical row order."""

    first_payload = b"concurrent-reuse-first"
    second_payload = b"concurrent-reuse-second"
    first_hash = await _insert_selective_durable_block(
        schema_name,
        first_payload,
    )
    second_hash = await _insert_selective_durable_block(
        schema_name,
        second_payload,
    )
    await _queue_selective_gc_candidate(schema_name, first_hash)
    await _queue_selective_gc_candidate(schema_name, second_hash)
    await _stage_selective_block_copy(
        tmp_path,
        schema_name,
        (1, first_payload, 1),
        (2, second_payload, 1),
        stage_table="block_stage_forward",
    )
    await _stage_selective_block_copy(
        tmp_path,
        schema_name,
        (1, second_payload, 1),
        (2, first_payload, 1),
        stage_table="block_stage_reverse",
    )


async def _assert_concurrent_reuse_publication(
    quoted_schema: str,
    forward,
    reverse,
) -> None:
    """Verify both overlapping publishers committed without GC residue."""

    assert forward.mapping_count == reverse.mapping_count == 2
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_block",
    ) == 2
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_snapshot_block",
    ) == 4
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_gc_candidate",
    ) == 0


async def _sweep_selective_blocks_once(schema_name: str):
    """Run one independent shared-block sweep transaction."""

    gc_database = Database()
    await gc_database.connect()
    try:
        async with gc_database.acquire() as connection:
            sweep = await ptg2_shared_gc.sweep_ptg2_shared_blocks(
                schema_name=schema_name,
                executor=connection,
                max_bytes=1024,
                max_rows=10,
                require_shared=True,
            )
            assert sweep.tables_available is True
            return sweep
    finally:
        await gc_database.disconnect()


async def _selective_relation_count(
    quoted_schema: str,
    relation_name: str,
) -> int:
    """Return one exact selective fixture relation count."""

    return int(
        await db.scalar(
            f'SELECT COUNT(*) FROM {quoted_schema}."{relation_name}"'
        )
        or 0
    )


async def _install_selective_v4_map_schema(
    schema_name: str,
    snapshot_key: int,
    build_token: str,
) -> None:
    """Add the minimum real packed-map relations to the selective fixture."""

    schema = f'"{schema_name}"'
    await db.execute_ddl(f"DROP TABLE {schema}.ptg2_block_build_pin")
    await db.execute_ddl(f"DROP TABLE {schema}.ptg2_v3_snapshot_layout")
    for ddl_template in (
        _SELECTIVE_V4_LAYOUT_DDL,
        _SELECTIVE_V4_MAP_ROOT_DDL,
        _SELECTIVE_V4_MAP_PACK_DDL,
    ):
        await db.execute_ddl(ddl_template.format(schema=schema))
    await db.execute_ddl(
        f"""
        CREATE TABLE {schema}.ptg2_block_build_pin (
            snapshot_key bigint NOT NULL REFERENCES
                {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            build_token varchar(96) NOT NULL,
            pin_token varchar(96) NOT NULL,
            block_hash bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            heartbeat_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            lease_until timestamptz NOT NULL,
            PRIMARY KEY (snapshot_key, pin_token, block_hash)
        )
        """
    )
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_snapshot_layout
            (snapshot_key, generation, state, build_token)
        VALUES (:snapshot_key, 'shared_blocks_v4', 'building', :build_token)
        """,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
    )


async def _stage_selective_v4_reuse(
    schema_name: str,
    *,
    block_payload: bytes,
    object_kind: str = "v4_atomic_test",
) -> bytes:
    """Create one durable V4 target and a null-payload reuse stage row."""

    schema = f'"{schema_name}"'
    block_hash = shared_block_hash(
        format_version=2,
        object_kind=object_kind,
        codec="none",
        payload=block_payload,
    )
    await db.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_block
            (block_hash, format_version, object_kind, codec, entry_count,
             raw_byte_count, stored_byte_count, payload, created_at)
        VALUES (:block_hash, 2, :object_kind, 'none', 1,
                :byte_count, :byte_count, :payload, now())
        """,
        block_hash=block_hash,
        object_kind=object_kind,
        byte_count=len(block_payload),
        payload=block_payload,
    )
    await create_shared_block_stage(
        schema_name=schema_name,
        stage_table="block_stage",
    )
    await db.status(
        f"""
        INSERT INTO {schema}.block_stage
            (block_hash, format_version, object_kind, block_key, fragment_no,
             entry_count, codec, raw_byte_count, stored_byte_count, payload)
        VALUES (:block_hash, 2, :object_kind, 1, 0,
                1, 'none', :byte_count, :byte_count, NULL)
        """,
        block_hash=block_hash,
        object_kind=object_kind,
        byte_count=len(block_payload),
    )
    return block_hash


async def _append_selective_v4_payload(
    schema_name: str,
    *,
    payload: bytes,
    object_kind: str = "v4_atomic_test",
) -> bytes:
    """Add a new payload-bearing coordinate to the active V4 stage."""

    schema = f'"{schema_name}"'
    block_hash = shared_block_hash(
        format_version=2,
        object_kind=object_kind,
        codec="none",
        payload=payload,
    )
    await db.status(
        f"""
        INSERT INTO {schema}.block_stage
            (block_hash, format_version, object_kind, block_key, fragment_no,
             entry_count, codec, raw_byte_count, stored_byte_count, payload)
        VALUES (:block_hash, 2, :object_kind, 2, 0,
                1, 'none', :byte_count, :byte_count, :payload)
        """,
        block_hash=block_hash,
        object_kind=object_kind,
        byte_count=len(payload),
        payload=payload,
    )
    return block_hash


def _selective_v4_reference(
    block_hash: bytes,
    *,
    raw_byte_count: int,
    object_kind: str = "v4_atomic_test",
    block_key: int = 1,
) -> SharedBlockReference:
    """Describe the single target in the packed-map publication stream."""

    return SharedBlockReference(
        object_kind=object_kind,
        block_key=int(block_key),
        fragment_no=0,
        entry_count=1,
        block_hash=block_hash,
        raw_byte_count=int(raw_byte_count),
    )


@dataclass(frozen=True)
class _ReferenceManifestProof:
    """Expected authentication contract for one reference artifact."""

    path: Path
    byte_count: int
    sha256: str
    row_count: int


@dataclass(frozen=True)
class _V4FixtureRequest:
    schema_name: str
    snapshot_key: int
    build_token: str
    reference: SharedBlockReference
    cas_ready: asyncio.Event | None = None
    allow_map: asyncio.Event | None = None
    manifest_proof: _ReferenceManifestProof | None = None
    expected_block_count: int | None = None


async def _publish_v4_fixture(
    *,
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    reference: SharedBlockReference,
    cas_ready: asyncio.Event | None = None,
    allow_map: asyncio.Event | None = None,
    manifest_proof: _ReferenceManifestProof | None = None,
    expected_block_count: int | None = None,
):
    """Run durable CAS preparation and the production packed-map transaction."""
    request = _V4FixtureRequest(
        schema_name=schema_name,
        snapshot_key=int(snapshot_key),
        build_token=build_token,
        reference=reference,
        cas_ready=cas_ready,
        allow_map=allow_map,
        manifest_proof=manifest_proof,
        expected_block_count=expected_block_count,
    )
    await ptg2_shared_publish.prepare_v4_cas_block_stage(
        schema_name=request.schema_name,
        stage_table="block_stage",
        snapshot_key=request.snapshot_key,
        build_token=request.build_token,
        progress_callback=lambda _metric, _amount: None,
    )
    async with db.transaction() as session:
        return await _publish_v4_fixture_in_session(session, request)


async def _publish_v4_fixture_in_session(session, request: _V4FixtureRequest):
    """Publish CAS rows, the authenticated map, and exact pin release."""
    await ptg2_v4_snapshot_maps.lock_v4_shared_layout_for_map_write(
        session,
        schema_name=request.schema_name,
        snapshot_key=request.snapshot_key,
        build_token=request.build_token,
    )
    cas_publication = (
        await ptg2_shared_publish._publish_v4_cas_block_stage_in_session(
            session,
            schema_name=request.schema_name,
            stage_table="block_stage",
            progress_callback=lambda _metric, _amount: None,
        )
    )
    if request.cas_ready is not None:
        request.cas_ready.set()
    if request.allow_map is not None:
        await request.allow_map.wait()
    references = (request.reference,)
    if request.manifest_proof is not None:
        references = ptg2_shared_snapshot_publish._iter_v4_block_references(
            request.manifest_proof.path,
            expected_byte_count=request.manifest_proof.byte_count,
            expected_sha256=request.manifest_proof.sha256,
            expected_row_count=request.manifest_proof.row_count,
        )
    map_summary = await ptg2_v4_snapshot_maps.publish_v4_snapshot_maps(
        session,
        schema_name=request.schema_name,
        snapshot_key=request.snapshot_key,
        build_token=request.build_token,
        representation="direct_v1",
        references=references,
    )
    if request.expected_block_count is not None:
        ptg2_shared_snapshot_publish._require_v4_atomic_coordinate_counts(
            request.expected_block_count,
            cas_publication,
            map_summary,
        )
    deleted_pin_count = await ptg2_shared_publish.delete_shared_block_build_pins(
        session,
        schema_name=request.schema_name,
        snapshot_key=request.snapshot_key,
        build_token=request.build_token,
        pin_token="block_stage",
    )
    assert deleted_pin_count == cas_publication.unique_block_count
    return cas_publication, map_summary


def _encode_selective_v4_reference(
    reference: SharedBlockReference,
) -> bytes:
    """Encode one canonical failed-build cleanup reference record."""

    return (
        json.dumps(
            {
                "object_kind": reference.object_kind,
                "block_key": int(reference.block_key),
                "fragment_no": int(reference.fragment_no),
                "entry_count": int(reference.entry_count),
                "raw_byte_count": int(reference.raw_byte_count),
                "stored_byte_count": int(reference.raw_byte_count),
                "hash": bytes(reference.block_hash).hex(),
                "codec": "none",
            },
            sort_keys=True,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")


def _write_selective_v4_reference_manifest(
    path: Path,
    *references: SharedBlockReference,
) -> bytes:
    """Write canonical failed-build cleanup reference records."""

    encoded_manifest = b"".join(
        _encode_selective_v4_reference(reference) for reference in references
    )
    path.write_bytes(encoded_manifest)
    return encoded_manifest


def _reorder_selective_v4_reference_bytes(encoded_reference: bytes) -> bytes:
    """Reorder one JSON object without changing its values or byte count."""

    reference_record = json.loads(encoded_reference)
    reordered_reference_map = dict(
        reversed(tuple(reference_record.items()))
    )
    reordered_bytes = (
        json.dumps(
            reordered_reference_map,
            sort_keys=False,
            separators=(",", ":"),
        )
        + "\n"
    ).encode("utf-8")
    assert len(reordered_bytes) == len(encoded_reference)
    assert json.loads(reordered_bytes) == reference_record
    return reordered_bytes


def _observe_v4_map_pack_publication(monkeypatch) -> list[bytes]:
    """Record map blocks written before a later transactional failure."""

    published_map_blocks: list[bytes] = []
    original_publish = ptg2_v4_snapshot_maps._publish_v4_map_pack

    async def publish_and_record(session, **kwargs):
        await original_publish(session, **kwargs)
        published_map_blocks.append(bytes(kwargs["pack"].map_block.block_hash))

    monkeypatch.setattr(
        ptg2_v4_snapshot_maps,
        "_publish_v4_map_pack",
        publish_and_record,
    )
    return published_map_blocks


def _observe_candidate_cancellation_before_map_validation(
    monkeypatch,
    quoted_schema: str,
) -> asyncio.Event:
    """Prove map validation runs after CAS candidate cancellation."""

    cancellation_observed = asyncio.Event()
    original_verify = ptg2_v4_snapshot_maps._verify_target_blocks

    async def verify_after_cancellation(session, **kwargs):
        candidate_count = await session.scalar(
            db.text(
                f"SELECT COUNT(*) FROM "
                f"{quoted_schema}.ptg2_v3_gc_candidate"
            )
        )
        assert int(candidate_count or 0) == 0
        cancellation_observed.set()
        return await original_verify(session, **kwargs)

    monkeypatch.setattr(
        ptg2_v4_snapshot_maps,
        "_verify_target_blocks",
        verify_after_cancellation,
    )
    return cancellation_observed


async def _settle_atomic_test_tasks(
    allow_map: asyncio.Event,
    *tasks: asyncio.Task | None,
) -> None:
    """Release the map gate and consume every task on success or failure."""

    allow_map.set()
    active_tasks = tuple(task for task in tasks if task is not None)
    for task in active_tasks:
        if not task.done():
            task.cancel()
    if active_tasks:
        await asyncio.gather(*active_tasks, return_exceptions=True)


async def _selective_block_count(
    quoted_schema: str,
    block_hash: bytes,
) -> int:
    """Return the exact durable count for one content hash."""

    return int(
        await db.scalar(
            f"SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_block "
            "WHERE block_hash = :block_hash",
            block_hash=block_hash,
        )
        or 0
    )


async def _assert_v4_failed_map_pins(
    quoted_schema: str,
    *,
    new_block_hash: bytes,
    map_block_hashes: tuple[bytes, ...] = (),
) -> None:
    """Prove failed map work rolls back maps but retains pinned durable CAS."""

    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_block",
    ) == 2
    assert await _selective_block_count(
        quoted_schema,
        new_block_hash,
    ) == 1
    for map_block_hash in map_block_hashes:
        assert await _selective_block_count(
            quoted_schema,
            map_block_hash,
        ) == 0
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_gc_candidate",
    ) == 0
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_block_build_pin",
    ) == 2
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v4_snapshot_map_root",
    ) == 0
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v4_snapshot_map_pack",
    ) == 0


def _write_drifted_v4_manifest(
    path: Path,
    *,
    manifest_case: str,
    reused_reference: SharedBlockReference,
    new_reference: SharedBlockReference,
) -> _ReferenceManifestProof:
    """Write one authenticated reference manifest failure fixture."""

    reused_bytes = _encode_selective_v4_reference(reused_reference)
    new_bytes = _encode_selective_v4_reference(new_reference)
    canonical_manifest = reused_bytes + new_bytes
    if manifest_case == "same_row_count_drift":
        drifted_manifest = (
            _reorder_selective_v4_reference_bytes(reused_bytes) + new_bytes
        )
    else:
        drifted_manifest = reused_bytes
    path.write_bytes(drifted_manifest)
    return _ReferenceManifestProof(
        path=path,
        byte_count=len(canonical_manifest),
        sha256=hashlib.sha256(canonical_manifest).hexdigest(),
        row_count=2,
    )


async def _stage_v4_manifest_blocks(
    schema_name: str,
) -> tuple[bytes, SharedBlockReference, SharedBlockReference]:
    """Stage two object kinds for authenticated manifest rollback tests."""

    reused_payload = b"atomic-manifest-reused-target"
    new_payload = b"atomic-manifest-new-target"
    reused_hash = await _stage_selective_v4_reuse(
        schema_name,
        block_payload=reused_payload,
        object_kind="v4_atomic_test_a",
    )
    new_hash = await _append_selective_v4_payload(
        schema_name,
        payload=new_payload,
        object_kind="v4_atomic_test_z",
    )
    await _queue_selective_gc_candidate(schema_name, reused_hash)
    reused_reference = _selective_v4_reference(
        reused_hash,
        raw_byte_count=len(reused_payload),
        object_kind="v4_atomic_test_a",
    )
    new_reference = _selective_v4_reference(
        new_hash,
        raw_byte_count=len(new_payload),
        object_kind="v4_atomic_test_z",
        block_key=2,
    )
    return new_hash, reused_reference, new_reference


async def _assert_v4_before_map(quoted_schema: str) -> None:
    """Prove durable pins protect CAS before map validation completes."""

    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_block",
    ) == 1
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_gc_candidate",
    ) == 0
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_block_build_pin",
    ) == 1
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v4_snapshot_map_root",
    ) == 0


async def _assert_v4_after_map(quoted_schema: str) -> None:
    """Prove the atomic map transaction committed its reachable state."""

    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_block",
    ) == 2
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_gc_candidate",
    ) == 0
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_block_build_pin",
    ) == 0
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v4_snapshot_map_root",
    ) == 1


async def _assert_v4_gc_safe(
    schema_name: str,
    quoted_schema: str,
    block_hash: bytes,
) -> None:
    """Prove committed maps keep their CAS blocks through a real sweep."""

    reachable = await ptg2_shared_gc._v4_reachable_hashes(
        db,
        schema_name=schema_name,
        candidate_hashes={block_hash},
    )
    assert reachable == {block_hash}
    sweep = await _sweep_selective_blocks_once(schema_name)
    assert sweep.selected_hashes == ()
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_block",
    ) == 2
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_gc_candidate",
    ) == 0


async def _assert_v4_mapped_candidate_canceled(quoted_schema: str) -> None:
    """Prove map attachment atomically cancels a concurrent GC candidate."""

    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v4_snapshot_map_root",
    ) == 1
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v4_snapshot_map_pack",
    ) == 1
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_gc_candidate",
    ) == 0


def _start_gated_v4_publish(
    schema_name: str,
    snapshot_key: int,
    build_token: str,
    reference: SharedBlockReference,
) -> tuple[asyncio.Event, asyncio.Event, asyncio.Task]:
    """Start one atomic publisher behind explicit CAS and map gates."""

    cas_ready = asyncio.Event()
    allow_map = asyncio.Event()
    publisher = asyncio.create_task(
        _publish_v4_fixture(
            schema_name=schema_name,
            snapshot_key=snapshot_key,
            build_token=build_token,
            reference=reference,
            cas_ready=cas_ready,
            allow_map=allow_map,
        )
    )
    return cas_ready, allow_map, publisher


async def _stage_v4_reuse_reference(
    schema_name: str,
    block_payload: bytes,
) -> tuple[bytes, SharedBlockReference]:
    """Stage one reused block and return its exact map reference."""

    block_hash = await _stage_selective_v4_reuse(
        schema_name,
        block_payload=block_payload,
    )
    return block_hash, _selective_v4_reference(
        block_hash,
        raw_byte_count=len(block_payload),
    )


@pytest.mark.asyncio
async def test_real_postgres_batched_shared_publish_preprotects_reuses_from_gc(
    tmp_path,
    monkeypatch,
):
    """The mapping publisher protects every reuse before payload batches."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        reused_hash = await _insert_selective_durable_block(
            schema_name,
            b"reuse-before-payload-lane",
        )
        await _queue_selective_gc_candidate(schema_name, reused_hash)
        await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"reuse-before-payload-lane", 1),
            (2, b"new-payload", 1),
        )
        protection_finished, allow_publication = _hold_shared_mapping_attach(
            monkeypatch
        )
        publisher = asyncio.create_task(
            publish_shared_block_stage(
                schema_name=schema_name,
                stage_table="block_stage",
                snapshot_key=13,
                build_token="preprotect-shared",
                progress_callback=lambda _metric, _amount: None,
            )
        )
        await asyncio.wait_for(protection_finished.wait(), timeout=3)
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_block_build_pin",
        ) == 2
        swept = await _sweep_selective_blocks_once(schema_name)
        assert swept.selected_hashes == ()
        allow_publication.set()
        publication = await asyncio.wait_for(publisher, timeout=3)
        assert publication.mapping_count == 2
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_v3_block",
        ) == 2
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_v3_gc_candidate",
        ) == 0
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_block_build_pin",
        ) == 0


async def _stage_identical_v3_peers(
    tmp_path,
    schema_name: str,
) -> None:
    block_payload = b"v3-identical-peer"
    for stage_table in ("block_stage_source0", "block_stage_source1"):
        await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, block_payload, 1),
            stage_table=stage_table,
        )


def _gate_v3_source_publication(monkeypatch):
    source0_ready = asyncio.Event()
    release_source0 = asyncio.Event()
    original_attach = ptg2_shared_publish._publish_shared_block_stage_batched

    async def hold_only_source0(*args, **kwargs):
        if int(kwargs["snapshot_key"]) == 130:
            source0_ready.set()
            await release_source0.wait()
        return await original_attach(*args, **kwargs)

    monkeypatch.setattr(
        ptg2_shared_publish,
        "_publish_shared_block_stage_batched",
        hold_only_source0,
    )
    return source0_ready, release_source0


async def _assert_v3_peer_publication(
    schema_name: str,
    quoted_schema: str,
) -> None:
    source1 = await asyncio.wait_for(
        publish_shared_block_stage(
            schema_name=schema_name,
            stage_table="block_stage_source1",
            snapshot_key=131,
            build_token="v3-source-1",
        ),
        timeout=3,
    )
    assert source1.mapping_count == source1.unique_block_count == 1
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_snapshot_block",
    ) == 1
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_block_build_pin",
    ) == 1


async def _assert_v3_sources_published(source0, quoted_schema: str) -> None:
    source0_publication = await asyncio.wait_for(source0, timeout=3)
    assert source0_publication.mapping_count == 1
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_snapshot_block",
    ) == 2
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_block_build_pin",
    ) == 0


@pytest.mark.asyncio
async def test_real_postgres_v3_pinned_source_does_not_block_identical_peer(
    tmp_path,
    monkeypatch,
):
    """A V3 source paused after durable CAS cannot block an identical peer."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        await _stage_identical_v3_peers(tmp_path, schema_name)
        source0_ready, release_source0 = _gate_v3_source_publication(
            monkeypatch
        )
        source0 = asyncio.create_task(
            publish_shared_block_stage(
                schema_name=schema_name,
                stage_table="block_stage_source0",
                snapshot_key=130,
                build_token="v3-source-0",
            )
        )
        try:
            await asyncio.wait_for(source0_ready.wait(), timeout=3)
            await _assert_v3_peer_publication(
                schema_name,
                quoted_schema,
            )
        finally:
            release_source0.set()
        await _assert_v3_sources_published(source0, quoted_schema)


@pytest.mark.asyncio
async def test_real_postgres_batched_v4_cas_preprotects_reuses_from_gc(
    tmp_path,
    monkeypatch,
):
    """Durable V4 pins protect every reuse before CAS payload batches."""
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_v4_shared_layout_for_map_write",
        AsyncMock(),
    )
    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        reused_hash = await _insert_selective_durable_block(
            schema_name,
            b"v4-reuse-before-payload-lane",
        )
        await _queue_selective_gc_candidate(schema_name, reused_hash)
        await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"v4-reuse-before-payload-lane", 1),
            (2, b"new-v4-payload", 1),
        )
        pins_ready, allow_publication = _hold_v4_after_durable_pins(monkeypatch)
        publisher = asyncio.create_task(
            ptg2_shared_publish._publish_v4_cas_block_stage_compatibility(
                schema_name=schema_name,
                stage_table="block_stage",
                snapshot_key=14,
                build_token="preprotect-v4",
                progress_callback=lambda _metric, _amount: None,
            )
        )
        await asyncio.wait_for(pins_ready.wait(), timeout=3)
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_block_build_pin",
        ) == 2
        swept = await _sweep_selective_blocks_once(schema_name)
        assert swept.selected_hashes == ()
        allow_publication.set()
        publication = await asyncio.wait_for(publisher, timeout=3)
        assert publication.staged_row_count == 2
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_v3_block",
        ) == 2
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_v3_gc_candidate",
        ) == 0
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_block_build_pin",
        ) == 0


async def _assert_failed_v4_map_state(
    cancellation_observed,
    quoted_schema: str,
    new_block_hash: bytes,
    schema_name: str,
) -> None:
    assert cancellation_observed.is_set()
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_block",
    ) == 2
    assert await _selective_block_count(
        quoted_schema,
        new_block_hash,
    ) == 1
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_gc_candidate",
    ) == 0
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_block_build_pin",
    ) == 2
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v4_snapshot_map_root",
    ) == 0
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v4_snapshot_map_pack",
    ) == 0
    assert (await _sweep_selective_blocks_once(schema_name)).selected_hashes == ()


@pytest.mark.asyncio
async def test_real_postgres_v4_map_failure_retains_durable_cas_protection(
    monkeypatch,
):
    """A post-CAS map failure leaves durable CAS pinned and no map residue."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    snapshot_key = 21
    build_token = "atomic-map-failure"
    reused_payload = b"atomic-map-failure-target"
    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        await _install_selective_v4_map_schema(
            schema_name, snapshot_key, build_token
        )
        block_hash = await _stage_selective_v4_reuse(
            schema_name,
            block_payload=reused_payload,
        )
        new_block_hash = await _append_selective_v4_payload(
            schema_name,
            payload=b"atomic-map-failure-new-cas-row",
        )
        await _queue_selective_gc_candidate(schema_name, block_hash)
        invalid_reference = _selective_v4_reference(
            block_hash,
            raw_byte_count=len(reused_payload) + 1,
        )
        cancellation_observed = (
            _observe_candidate_cancellation_before_map_validation(
                monkeypatch,
                quoted_schema,
            )
        )

        with pytest.raises(
            RuntimeError,
            match="could not resolve every target CAS block",
        ):
            await _publish_v4_fixture(
                schema_name=schema_name,
                snapshot_key=snapshot_key,
                build_token=build_token,
                reference=invalid_reference,
            )

        await _assert_failed_v4_map_state(
            cancellation_observed,
            quoted_schema,
            new_block_hash,
            schema_name,
        )


@pytest.mark.asyncio
async def test_real_postgres_v4_coordinate_mismatch_rolls_back_atomic_publish(
    monkeypatch,
):
    """A post-map CAS/reference count mismatch rolls all publication back."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    snapshot_key = 25
    build_token = "atomic-coordinate-mismatch"
    reused_payload = b"atomic-coordinate-reused-target"
    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        await _install_selective_v4_map_schema(
            schema_name, snapshot_key, build_token
        )
        reused_hash = await _stage_selective_v4_reuse(
            schema_name,
            block_payload=reused_payload,
        )
        new_hash = await _append_selective_v4_payload(
            schema_name,
            payload=b"atomic-coordinate-unreferenced-cas-row",
        )
        await _queue_selective_gc_candidate(schema_name, reused_hash)
        reference = _selective_v4_reference(
            reused_hash,
            raw_byte_count=len(reused_payload),
        )
        published_map_blocks = _observe_v4_map_pack_publication(monkeypatch)

        with pytest.raises(
            RuntimeError,
            match="CAS and packed-map coordinate counts changed",
        ):
            await _publish_v4_fixture(
                schema_name=schema_name,
                snapshot_key=snapshot_key,
                build_token=build_token,
                reference=reference,
                expected_block_count=1,
            )

        assert len(published_map_blocks) == 1
        await _assert_v4_failed_map_pins(
            quoted_schema,
            new_block_hash=new_hash,
            map_block_hashes=tuple(published_map_blocks),
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manifest_case",
    ("same_row_count_drift", "truncated"),
)
async def test_real_postgres_v4_manifest_auth_failure_rolls_back_atomic_publish(
    tmp_path,
    monkeypatch,
    manifest_case,
):
    """Authenticated reference drift rolls CAS and packed-map state back."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    snapshot_key = 24
    build_token = f"atomic-manifest-{manifest_case}"
    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        await _install_selective_v4_map_schema(
            schema_name, snapshot_key, build_token
        )
        new_hash, reused_reference, new_reference = (
            await _stage_v4_manifest_blocks(schema_name)
        )
        manifest_path = tmp_path / f"references-{manifest_case}.jsonl"
        manifest_proof = _write_drifted_v4_manifest(
            manifest_path,
            manifest_case=manifest_case,
            reused_reference=reused_reference,
            new_reference=new_reference,
        )
        published_map_blocks = _observe_v4_map_pack_publication(monkeypatch)

        with pytest.raises(
            RuntimeError,
            match="graph reference authentication changed",
        ):
            await _publish_v4_fixture(
                schema_name=schema_name,
                snapshot_key=snapshot_key,
                build_token=build_token,
                reference=reused_reference,
                manifest_proof=manifest_proof,
            )

        expected_published_packs = (
            1 if manifest_case == "same_row_count_drift" else 0
        )
        assert len(published_map_blocks) == expected_published_packs
        await _assert_v4_failed_map_pins(
            quoted_schema,
            new_block_hash=new_hash,
            map_block_hashes=tuple(published_map_blocks),
        )


@pytest.mark.asyncio
async def test_real_postgres_v4_cas_precedes_validation_gc_window(
    monkeypatch,
):
    """A reused target is protected throughout pre-map validation work."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    snapshot_key, build_token = 22, "atomic-validation-window"
    reused_payload = b"atomic-validation-window-target"
    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        await _install_selective_v4_map_schema(
            schema_name, snapshot_key, build_token
        )
        block_hash = await _stage_selective_v4_reuse(
            schema_name,
            block_payload=reused_payload,
        )
        await _queue_selective_gc_candidate(schema_name, block_hash)
        reference = _selective_v4_reference(
            block_hash,
            raw_byte_count=len(reused_payload),
        )
        validation_started = asyncio.Event()
        allow_validation = asyncio.Event()
        publisher = asyncio.create_task(
            _publish_v4_fixture(
                schema_name=schema_name,
                snapshot_key=snapshot_key,
                build_token=build_token,
                reference=reference,
                cas_ready=validation_started,
                allow_map=allow_validation,
            )
        )
        try:
            await asyncio.wait_for(validation_started.wait(), timeout=3)
            sweep = await asyncio.wait_for(
                _sweep_selective_blocks_once(schema_name),
                timeout=3,
            )
            assert sweep.selected_hashes == ()
            await _assert_v4_before_map(quoted_schema)

            allow_validation.set()
            cas_publication, map_summary = await asyncio.wait_for(
                publisher,
                timeout=3,
            )
            assert cas_publication.staged_row_count == 1
            assert map_summary.coordinate_count == 1
            await _assert_v4_after_map(quoted_schema)
        finally:
            await _settle_atomic_test_tasks(
                allow_validation,
                publisher,
            )


async def _queue_failed_candidate_and_assert_pinned(
    cas_ready: asyncio.Event,
    schema_name: str,
    reference_manifest: Path,
    quoted_schema: str,
) -> None:
    await asyncio.wait_for(cas_ready.wait(), timeout=3)
    await asyncio.wait_for(
        ptg2_shared_snapshot_publish._queue_failed_v4_graph_blocks(
            schema_name=schema_name,
            reference_manifest_path=reference_manifest,
        ),
        timeout=3,
    )
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_v3_gc_candidate",
    ) == 1
    assert await _selective_relation_count(
        quoted_schema,
        "ptg2_block_build_pin",
    ) == 1
    assert (await _sweep_selective_blocks_once(schema_name)).selected_hashes == ()


@pytest.mark.asyncio
async def test_real_postgres_atomic_v4_map_wins_failed_enqueue_gc_race(
    tmp_path,
    monkeypatch,
):
    """A concurrent failed enqueue cannot make a pinned/map block collectible."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    snapshot_key, build_token = 23, "atomic-failed-enqueue-race"
    reused_payload = b"atomic-failed-enqueue-target"
    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        await _install_selective_v4_map_schema(
            schema_name, snapshot_key, build_token
        )
        block_hash, reference = await _stage_v4_reuse_reference(
            schema_name, reused_payload
        )
        reference_manifest = tmp_path / "failed-v4-references.jsonl"
        _write_selective_v4_reference_manifest(reference_manifest, reference)
        cas_ready, allow_map, publisher = _start_gated_v4_publish(
            schema_name,
            snapshot_key,
            build_token,
            reference,
        )
        try:
            await _queue_failed_candidate_and_assert_pinned(
                cas_ready,
                schema_name,
                reference_manifest,
                quoted_schema,
            )
            allow_map.set()
            publication = await asyncio.wait_for(
                publisher,
                timeout=3,
            )
            cas_publication, map_summary = publication

            assert cas_publication.staged_row_count == 1
            assert map_summary.coordinate_count == 1
            await _assert_v4_mapped_candidate_canceled(quoted_schema)

            await _assert_v4_gc_safe(
                schema_name,
                quoted_schema,
                block_hash,
            )
        finally:
            await _settle_atomic_test_tasks(
                allow_map,
                publisher,
            )


@pytest.mark.asyncio
async def test_real_postgres_batched_publishers_order_overlapping_reuse_locks(
    tmp_path,
    monkeypatch,
):
    """Reverse stage order cannot deadlock two overlapping publishers."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        await _stage_reverse_order_reuse_publishers(tmp_path, schema_name)
        async def publish(stage_table: str, snapshot_key: int):
            return await publish_shared_block_stage(
                schema_name=schema_name,
                stage_table=stage_table,
                snapshot_key=snapshot_key,
                build_token=f"concurrent-publisher-{snapshot_key}",
                progress_callback=lambda _metric, _amount: None,
            )

        forward, reverse = await asyncio.wait_for(
            asyncio.gather(
                publish("block_stage_forward", 15),
                publish("block_stage_reverse", 16),
            ),
            timeout=5,
        )
        await _assert_concurrent_reuse_publication(
            quoted_schema,
            forward,
            reverse,
        )


@pytest.mark.asyncio
async def test_real_postgres_reused_block_publish_wins_gc_race_atomically(
    tmp_path,
    monkeypatch,
):
    """Publisher protection cancels GC before its mapping becomes visible."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        block_hash = await _insert_selective_durable_block(schema_name, b"kept")
        await _queue_selective_gc_candidate(schema_name, block_hash)
        await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"kept", 1),
        )
        mapping_started, allow_mapping = _hold_shared_mapping_attach(
            monkeypatch
        )
        publisher = asyncio.create_task(
            publish_shared_block_stage(
                schema_name=schema_name,
                stage_table="block_stage",
                snapshot_key=7,
                build_token="build-7",
            )
        )
        await asyncio.wait_for(mapping_started.wait(), timeout=3)
        swept = await _sweep_selective_blocks_once(schema_name)
        assert swept.selected_hashes == ()
        allow_mapping.set()
        publication = await asyncio.wait_for(publisher, timeout=3)
        assert publication.mapping_count == 1
        assert await _selective_relation_count(
            quoted_schema, "ptg2_v3_block"
        ) == 1
        assert await _selective_relation_count(
            quoted_schema, "ptg2_v3_snapshot_block"
        ) == 1
        assert await _selective_relation_count(
            quoted_schema, "ptg2_v3_gc_candidate"
        ) == 0


async def _delete_selective_block_under_gc_lock(
    gc_database: Database,
    *,
    quoted_schema: str,
    block_hash: bytes,
    delete_started: asyncio.Event,
    allow_gc_commit: asyncio.Event,
) -> None:
    """Delete one candidate under locks and hold the transaction before commit."""

    async with gc_database.transaction() as session:
        await session.execute(
            gc_database.text(
                f"""
                SELECT block.block_hash
                  FROM {quoted_schema}.ptg2_v3_block AS block
                  JOIN {quoted_schema}.ptg2_v3_gc_candidate AS candidate
                    USING (block_hash)
                 WHERE block.block_hash = :block_hash
                 FOR UPDATE OF candidate, block
                """
            ),
            {"block_hash": block_hash},
        )
        await session.execute(
            gc_database.text(
                f"""
                DELETE FROM {quoted_schema}.ptg2_v3_block
                 WHERE block_hash = :block_hash
                """
            ),
            {"block_hash": block_hash},
        )
        delete_started.set()
        await allow_gc_commit.wait()


async def _hold_selective_candidate_then_rollback(
    lock_database: Database,
    *,
    quoted_schema: str,
    block_hash: bytes,
    locks_acquired: asyncio.Event,
    allow_rollback: asyncio.Event,
) -> None:
    """Hold publisher-conflicting block/candidate locks, then roll back."""

    try:
        async with lock_database.transaction() as session:
            await session.execute(
                lock_database.text(
                    f"SELECT block_hash FROM {quoted_schema}.ptg2_v3_block "
                    "WHERE block_hash = :block_hash FOR KEY SHARE"
                ),
                {"block_hash": block_hash},
            )
            await session.execute(
                lock_database.text(
                    f"SELECT block_hash FROM "
                    f"{quoted_schema}.ptg2_v3_gc_candidate "
                    "WHERE block_hash = :block_hash FOR UPDATE"
                ),
                {"block_hash": block_hash},
            )
            locks_acquired.set()
            await allow_rollback.wait()
            raise RuntimeError("intentional candidate-lock rollback")
    except RuntimeError as rollback_error:
        assert str(rollback_error) == "intentional candidate-lock rollback"


def _start_v4_reuse_publication(schema_name: str):
    """Start the batched V4 CAS publisher for the selective fixture."""

    return asyncio.create_task(
        ptg2_shared_publish._publish_v4_cas_block_stage_compatibility(
            schema_name=schema_name,
            stage_table="block_stage",
            snapshot_key=17,
            build_token="v4-candidate-lock-race",
            progress_callback=lambda _metric, _amount: None,
        )
    )


async def _start_candidate_lock_rollback(
    quoted_schema: str,
    block_hash: bytes,
):
    """Start and prove an independent candidate lock-holder transaction."""

    lock_database = Database()
    await lock_database.connect()
    locks_acquired = asyncio.Event()
    allow_rollback = asyncio.Event()
    lock_holder = asyncio.create_task(
        _hold_selective_candidate_then_rollback(
            lock_database,
            quoted_schema=quoted_schema,
            block_hash=block_hash,
            locks_acquired=locks_acquired,
            allow_rollback=allow_rollback,
        )
    )
    await asyncio.wait_for(locks_acquired.wait(), timeout=3)
    return lock_database, allow_rollback, lock_holder


async def _finish_candidate_lock_rollback(
    lock_database: Database,
    allow_rollback: asyncio.Event,
    lock_holder,
) -> None:
    """Release, verify, and disconnect the candidate lock-holder."""

    allow_rollback.set()
    try:
        await asyncio.wait_for(lock_holder, timeout=3)
    finally:
        allow_rollback.set()
        await lock_database.disconnect()


async def _assert_missing_cas_publication_error(publisher) -> None:
    """Require controlled validation before a foreign-key mapping attempt."""

    with pytest.raises(
        RuntimeError,
        match="has no payload or durable CAS row",
    ) as missing_cas:
        await asyncio.wait_for(publisher, timeout=3)
    assert type(missing_cas.value) is RuntimeError
    assert getattr(missing_cas.value, "sqlstate", None) != "23503"
    assert "ForeignKeyViolation" not in type(missing_cas.value).__name__


@pytest.mark.asyncio
async def test_real_postgres_v4_waits_for_candidate_lock_rollback(
    tmp_path,
    monkeypatch,
):
    """V4 blocks, then cancels a candidate whose lock-holder rolls back."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_v4_shared_layout_for_map_write",
        AsyncMock(),
    )
    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        block_hash = await _insert_selective_durable_block(
            schema_name,
            b"v4-candidate-lock-rollback",
        )
        await _queue_selective_gc_candidate(schema_name, block_hash)
        await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"v4-candidate-lock-rollback", 1),
        )
        lock_state = await _start_candidate_lock_rollback(
            quoted_schema,
            block_hash,
        )
        publisher = _start_v4_reuse_publication(schema_name)
        try:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(asyncio.shield(publisher), timeout=0.2)
        finally:
            await _finish_candidate_lock_rollback(*lock_state)
        publication = await asyncio.wait_for(publisher, timeout=3)
        assert publication.staged_row_count == 1
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_v3_gc_candidate",
        ) == 0
        swept = await _sweep_selective_blocks_once(schema_name)
        assert swept.selected_hashes == ()
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_v3_block",
        ) == 1


@pytest.mark.asyncio
async def test_real_postgres_v4_candidate_lock_timeout_rolls_back(
    tmp_path,
    monkeypatch,
):
    """A bounded candidate-lock timeout fails the entire V4 CAS transaction."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_v4_shared_layout_for_map_write",
        AsyncMock(),
    )
    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        block_hash = await _insert_selective_durable_block(
            schema_name,
            b"v4-candidate-lock-timeout",
        )
        await _queue_selective_gc_candidate(schema_name, block_hash)
        await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"v4-candidate-lock-timeout", 1),
        )
        lock_state = await _start_candidate_lock_rollback(
            quoted_schema,
            block_hash,
        )
        try:
            with pytest.raises(Exception, match="lock timeout") as caught:
                await (
                    ptg2_shared_publish._publish_v4_cas_block_stage_compatibility(
                        schema_name=schema_name,
                        stage_table="block_stage",
                        snapshot_key=18,
                        build_token="v4-candidate-lock-timeout",
                        progress_callback=lambda _metric, _amount: None,
                    )
                )
            lock_error = getattr(caught.value, "orig", caught.value)
            assert getattr(lock_error, "sqlstate", None) == "55P03"
        finally:
            await _finish_candidate_lock_rollback(*lock_state)
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_v3_block",
        ) == 1
        assert await _selective_relation_count(
            quoted_schema,
            "ptg2_v3_gc_candidate",
        ) == 1


@pytest.mark.asyncio
async def test_real_postgres_missing_reused_cas_is_rejected_before_23503_mapping(
    tmp_path,
    monkeypatch,
):
    """A completed GC delete fails before snapshot mapping can raise SQLSTATE 23503."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        block_hash = await _insert_selective_durable_block(schema_name, b"gone")
        await _queue_selective_gc_candidate(schema_name, block_hash)
        await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"gone", 1),
        )
        gc_database = Database()
        await gc_database.connect()
        delete_started = asyncio.Event()
        allow_gc_commit = asyncio.Event()
        gc_delete = asyncio.create_task(
            _delete_selective_block_under_gc_lock(
                gc_database,
                quoted_schema=quoted_schema,
                block_hash=block_hash,
                delete_started=delete_started,
                allow_gc_commit=allow_gc_commit,
            )
        )
        await asyncio.wait_for(delete_started.wait(), timeout=3)
        publisher = asyncio.create_task(
            publish_shared_block_stage(
                schema_name=schema_name,
                stage_table="block_stage",
                snapshot_key=8,
                build_token="build-8",
            )
        )
        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(asyncio.shield(publisher), timeout=0.2)
        allow_gc_commit.set()
        try:
            await asyncio.wait_for(gc_delete, timeout=3)
            await _assert_missing_cas_publication_error(publisher)
        finally:
            allow_gc_commit.set()
            await gc_database.disconnect()

        assert await _selective_relation_count(
            quoted_schema, "ptg2_v3_block"
        ) == 0
        assert await _selective_relation_count(
            quoted_schema, "ptg2_v3_snapshot_block"
        ) == 0


@pytest.mark.asyncio
async def test_real_postgres_selective_copy_rejects_durable_metadata_conflict(
    tmp_path,
    monkeypatch,
):
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")

    async with _selective_block_database(monkeypatch) as (
        schema_name,
        quoted_schema,
    ):
        await _insert_selective_durable_block(
            schema_name,
            b"conflict",
            entry_count=9,
        )
        metrics, stage_counts = await _stage_selective_block_copy(
            tmp_path,
            schema_name,
            (1, b"conflict", 1),
        )
        assert stage_counts == (1, 0, 0)
        assert metrics.existing_block_count == 1
        with pytest.raises(RuntimeError, match="conflicts with stored content metadata"):
            await publish_shared_block_stage(
                schema_name=schema_name,
                stage_table="block_stage",
                snapshot_key=5,
                build_token="build-5",
            )
        assert int(
            await db.scalar(
                f"SELECT COUNT(*) FROM {quoted_schema}.ptg2_v3_snapshot_block"
            )
            or 0
        ) == 0


async def _create_shared_block_publish_fixture(schema_name):
    """Create one deterministic shared-block publication fixture."""
    quoted_schema = f'"{schema_name}"'
    await db.execute_ddl(f"CREATE SCHEMA {quoted_schema}")
    await _create_shared_block_pin_tables(quoted_schema)
    await db.status(
        f"""
        INSERT INTO {quoted_schema}.ptg2_v3_snapshot_layout
            (snapshot_key, generation, state, build_token)
        VALUES
            (7, 'shared_blocks_v3', 'building', 'build-7'),
            (8, 'shared_blocks_v3', 'building', 'build-8')
        """
    )
    await _create_shared_mapping_and_gc_tables(quoted_schema)


async def _create_shared_block_pin_tables(quoted_schema: str) -> None:
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_v3_block (
            block_hash bytea PRIMARY KEY,
            format_version smallint NOT NULL,
            object_kind varchar(64) NOT NULL,
            codec varchar(16) NOT NULL,
            entry_count bigint NOT NULL,
            raw_byte_count bigint NOT NULL,
            stored_byte_count bigint NOT NULL,
            payload bytea NOT NULL,
            created_at timestamp with time zone NOT NULL
        )
        """
    )
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_layout (
            snapshot_key bigint PRIMARY KEY,
            generation varchar(32) NOT NULL,
            state varchar(16) NOT NULL,
            build_token varchar(96) NOT NULL
        )
        """
    )
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_block_build_pin (
            snapshot_key bigint NOT NULL REFERENCES
                {quoted_schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            build_token varchar(96) NOT NULL,
            pin_token varchar(96) NOT NULL,
            block_hash bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            heartbeat_at timestamptz NOT NULL DEFAULT transaction_timestamp(),
            lease_until timestamptz NOT NULL,
            PRIMARY KEY (snapshot_key, pin_token, block_hash)
        )
        """
    )


async def _create_shared_mapping_and_gc_tables(quoted_schema: str) -> None:
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_v3_snapshot_block (
            snapshot_key bigint NOT NULL,
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL,
            fragment_no integer NOT NULL,
            entry_count bigint NOT NULL,
            block_hash bytea NOT NULL,
            PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
        )
        """
    )
    await db.execute_ddl(
        f"""
        CREATE TABLE {quoted_schema}.ptg2_v3_gc_candidate (
            block_hash bytea PRIMARY KEY,
            eligible_at timestamp with time zone NOT NULL,
            queued_at timestamp with time zone NOT NULL DEFAULT now(),
            FOREIGN KEY (block_hash)
                REFERENCES {quoted_schema}.ptg2_v3_block (block_hash)
                ON DELETE CASCADE
        )
        """
    )


async def _load_shared_block_stage(schema_name, values_sql):
    await create_shared_block_stage(
        schema_name=schema_name,
        stage_table="block_stage",
    )
    await db.status(
        f"""
        INSERT INTO "{schema_name}".block_stage
            (block_hash, format_version, object_kind, block_key, fragment_no,
             entry_count, codec, raw_byte_count, stored_byte_count, payload)
        VALUES {values_sql}
        """
    )


def _shared_block_publish_hashes():
    canonical_payload_bytes = b"abc"
    canonical_hash = shared_block_hash(
        format_version=2,
        object_kind="serving",
        codec="none",
        payload=canonical_payload_bytes,
    ).hex()
    missing_hash = shared_block_hash(
        format_version=2,
        object_kind="serving",
        codec="none",
        payload=b"missing",
    ).hex()
    return canonical_payload_bytes, canonical_hash, missing_hash


async def _assert_shared_block_publish_reuse(
    schema_name,
    canonical_payload_bytes,
    block_hash,
):
    identical_rows = f"""
        (decode('{block_hash}', 'hex'), 2, 'serving', 1, 0,
         3, 'none', 3, 3, decode('{canonical_payload_bytes.hex()}', 'hex')),
        (decode('{block_hash}', 'hex'), 2, 'serving', 2, 0,
         3, 'none', 3, 3, decode('{canonical_payload_bytes.hex()}', 'hex'))
    """
    for _ in range(2):
        await _load_shared_block_stage(schema_name, identical_rows)
        publication = await publish_shared_block_stage(
            schema_name=schema_name,
            stage_table="block_stage",
            snapshot_key=7,
            build_token="build-7",
        )
        assert publication.object_kinds == ("serving",)
        assert publication.mapping_count == 2
        assert publication.unique_block_count == 1
        assert publication.logical_byte_count == 6
        assert publication.stored_byte_count == 6
    assert await _shared_relation_count(schema_name, "ptg2_v3_block") == 1
    assert await _shared_relation_count(
        schema_name,
        "ptg2_v3_snapshot_block",
    ) == 2


async def _shared_relation_count(schema_name, relation_name):
    return int(
        await db.scalar(f'SELECT COUNT(*) FROM "{schema_name}".{relation_name}')
        or 0
    )


async def _assert_shared_block_publish_conflicts(
    schema_name,
    canonical_hash,
    missing_hash,
):
    await _load_shared_block_stage(
        schema_name,
        f"(decode('{canonical_hash}', 'hex'), 2, 'serving', 1, 0,"
        " 3, 'none', 3, 3, NULL)",
    )
    publication = await publish_shared_block_stage(
        schema_name=schema_name,
        stage_table="block_stage",
        snapshot_key=7,
        build_token="build-7",
    )
    assert publication.mapping_count == publication.unique_block_count == 1
    for block_hash, snapshot_key, entry_count, byte_count, error_pattern in (
        (missing_hash, 8, 1, 7, "no payload or durable CAS row"),
        (
            canonical_hash,
            7,
            4,
            3,
            "conflicts with stored content metadata",
        ),
    ):
        await _load_shared_block_stage(
            schema_name,
            f"(decode('{block_hash}', 'hex'), 2, 'serving', 3, 0,"
            f" {entry_count}, 'none', {byte_count}, {byte_count}, NULL)",
        )
        with pytest.raises(
            RuntimeError,
            match=error_pattern,
        ):
            await publish_shared_block_stage(
                schema_name=schema_name,
                stage_table="block_stage",
                snapshot_key=snapshot_key,
                build_token=f"build-{snapshot_key}",
            )
    assert await _shared_relation_count(schema_name, "ptg2_v3_block") == 1
    assert await _shared_relation_count(
        schema_name,
        "ptg2_v3_snapshot_block",
    ) == 2


@pytest.mark.asyncio
async def test_real_postgres_shared_block_publish_preserves_content_conflict_checks(
    monkeypatch,
):
    """Content-addressed reuse retains metadata and durable-existence checks."""
    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1")
    schema_name = f"ptg2_block_publish_{uuid.uuid4().hex[:16]}"
    monkeypatch.setattr(
        ptg2_shared_publish,
        "lock_shared_layout_for_dense_write",
        AsyncMock(),
    )
    await db.disconnect()
    await db.connect()
    try:
        await _create_shared_block_publish_fixture(schema_name)
        canonical_payload_bytes, canonical_hash, missing_hash = (
            _shared_block_publish_hashes()
        )
        await _assert_shared_block_publish_reuse(
            schema_name,
            canonical_payload_bytes,
            canonical_hash,
        )
        await _assert_shared_block_publish_conflicts(
            schema_name,
            canonical_hash,
            missing_hash,
        )
    finally:
        await db.status(f'DROP SCHEMA IF EXISTS "{schema_name}" CASCADE')
        await db.disconnect()


@dataclass(frozen=True)
class _StrictSharedScan:
    coverage_scope_id: bytes
    scanner_tests: object
    scanner_binary: Path
    scan: dict
    provider_set_id: bytes
    price_set_ids: set[bytes]
    provider_set_metadata_path: Path


@dataclass(frozen=True)
class _StrictSharedFixture:
    tmp_path: Path
    scanner: _StrictSharedScan
    schema_name: str
    quoted_schema: str
    snapshot_id: str
    artifact_digest: str
    source_identity: SharedPhysicalArtifactIdentity
    source_assignment: SharedSnapshotSourceAssignment
    scanner_summary: dict
    serving_run_entries: list[dict]
    code_dictionary_entries: list[dict]
    provider_set_metadata_entries: tuple[dict, ...]


@dataclass(frozen=True)
class _StrictSharedLayout:
    publication: object
    graph_entries: object
    npi: int
    reserved_snapshot_key: int


@dataclass(frozen=True)
class _StrictServingKeys:
    provider_set_key: int
    provider_group_key: int
    code_key_by_reported_code: dict[str, int]


@dataclass(frozen=True)
class _StrictForwardRows:
    first: list
    second: list


@dataclass(frozen=True)
class _StrictReuse:
    discarded_snapshot_key: int
    stage_table: str
    snapshot_id: str


def _run_strict_shared_scanner(tmp_path, monkeypatch):
    coverage_scope_id = b"\xcc" * 32
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V3_COVERAGE_SCOPE_ID",
        coverage_scope_id.hex(),
    )
    scanner_tests = _load_module(
        SCANNER_TEST_PATH, "ptg2_shared_publish_scanner_support"
    )
    configured_scanner = os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_SCANNER_BIN")
    scanner_binary = (
        Path(configured_scanner).resolve()
        if configured_scanner
        else scanner_tests._built_scanner_binary()
    )
    assert scanner_binary.is_file() and os.access(scanner_binary, os.X_OK)
    scan = scanner_tests._run_scanner(
        scanner_binary,
        tmp_path,
        "shared-publish-source",
        arch="postgres_binary_v3",
        provider_references_first=True,
        grouped=False,
        multiple_prices=True,
        duplicate_first_price=True,
    )
    return coverage_scope_id, scanner_tests, scanner_binary, scan


def _assert_strict_scanner_artifacts(scan, tmp_path):
    serving_records = [
        SERVING_RECORD.unpack_from(scan["partition_bytes"], offset)
        for offset in range(0, len(scan["partition_bytes"]), SERVING_RECORD.size)
    ]
    assert len(serving_records) == 2
    assert all(code_id != b"\0" * 16 for code_id, *_rest in serving_records)
    assert {provider_count for *_ids, provider_count in serving_records} == {2}
    provider_set_ids = {
        provider_set_id for _code, provider_set_id, _price, _count in serving_records
    }
    price_set_ids = {
        price_set_id for _code, _provider, price_set_id, _count in serving_records
    }
    assert len(provider_set_ids) == 1
    assert len(price_set_ids) == 2
    provider_set_id = next(iter(provider_set_ids))
    metadata_path = tmp_path / "provider-set-metadata.copy"
    metadata_path.write_text(
        f"{provider_set_id.hex()}\t2\t{{}}\n",
        encoding="ascii",
    )
    assert scan["price_atom_frames"]
    assert scan["price_set_atom_frames"]
    assert scan["price_set_summary_frames"]
    assert all(
        Path(frame["path"]).stat().st_size > 0 for frame in scan["price_atom_frames"]
    )
    assert all(
        Path(frame["path"]).stat().st_size > 0
        for frame in scan["price_set_atom_frames"]
    )
    return provider_set_id, price_set_ids, metadata_path


def _prepare_strict_shared_scan(tmp_path, monkeypatch):
    coverage_scope_id, scanner_tests, scanner_binary, scan = (
        _run_strict_shared_scanner(tmp_path, monkeypatch)
    )
    provider_set_id, price_set_ids, metadata_path = (
        _assert_strict_scanner_artifacts(scan, tmp_path)
    )
    return _StrictSharedScan(
        coverage_scope_id=coverage_scope_id,
        scanner_tests=scanner_tests,
        scanner_binary=scanner_binary,
        scan=scan,
        provider_set_id=provider_set_id,
        price_set_ids=price_set_ids,
        provider_set_metadata_path=metadata_path,
    )


def _strict_provider_metadata_entries(scanner, source_identity, serving_entries):
    metadata_payload = scanner.provider_set_metadata_path.read_bytes()
    return (
        {
            "path": str(scanner.provider_set_metadata_path),
            "row_count": 1,
            "bytes": len(metadata_payload),
            "sha256": hashlib.sha256(metadata_payload).hexdigest(),
            "format": "ptg2_v3_provider_set_metadata_copy",
            "version": 1,
            "source_type": source_identity.source_type,
            "identity_kind": source_identity.identity_kind,
            "identity_sha256": source_identity.identity_sha256,
            "source_run_contract_sha256": serving_entries[0][
                "source_run_contract_sha256"
            ],
        },
    )


def _prepare_strict_shared_fixture(tmp_path, monkeypatch):
    scanner = _prepare_strict_shared_scan(tmp_path, monkeypatch)
    schema_name = f"ptg2_shared_publish_{uuid.uuid4().hex[:16]}"
    snapshot_id = f"shared-smoke-{uuid.uuid4().hex}"
    artifact_digest = hashlib.sha256(scanner.scan["artifact"].read_bytes()).hexdigest()
    source_identity = SharedPhysicalArtifactIdentity(
        "in_network", "logical_json_sha256_v1", "a" * 64
    )
    source_assignment = SharedSnapshotSourceAssignment(
        source_key=0,
        identity=source_identity,
        source_trace_set_hash="b" * 64,
        source_trace_hashes=("c" * 64,),
        raw_container_sha256=artifact_digest,
        logical_json_sha256="a" * 64,
        logical_hash_deferred=False,
    )
    scanner_summary = scanner.scanner_tests._single_frame(
        scanner.scan["frames"], "scanner_summary"
    )
    serving_entries = attach_v3_source_run_contract(
        scanner.scan["partition_frames"],
        source_identity=source_identity,
        scanner_summary=scanner_summary,
        scanner_config=scanner.scanner_tests._single_frame(
            scanner.scan["frames"], "scanner_config"
        ),
    )
    dictionary_entries = attach_v3_dictionary_contract(
        scanner.scan["code_dictionary_frames"],
        source_identity=source_identity,
        source_run_contract_sha256=serving_entries[0]["source_run_contract_sha256"],
        scanner_summary=scanner_summary,
    )
    metadata_entries = _strict_provider_metadata_entries(
        scanner, source_identity, serving_entries
    )
    return _StrictSharedFixture(
        tmp_path=tmp_path,
        scanner=scanner,
        schema_name=schema_name,
        quoted_schema=f'"{schema_name}"',
        snapshot_id=snapshot_id,
        artifact_digest=artifact_digest,
        source_identity=source_identity,
        source_assignment=source_assignment,
        scanner_summary=scanner_summary,
        serving_run_entries=serving_entries,
        code_dictionary_entries=dictionary_entries,
        provider_set_metadata_entries=metadata_entries,
    )


def _configure_strict_shared_environment(fixture, monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", fixture.schema_name)
    monkeypatch.setenv("DB_SCHEMA", fixture.schema_name)
    monkeypatch.setenv("HLTHPRT_PTG2_SNAPSHOT_ARCH", "postgres_binary_v3")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_MANIFEST_SERVING_LAYOUT",
        PTG2_MANIFEST_SERVING_LAYOUT_LEAN_PROVIDER_KEY,
    )
    monkeypatch.setenv("HLTHPRT_PTG2_BINARY_IDS", "true")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_RUST_SCANNER_BIN", str(fixture.scanner.scanner_binary)
    )
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_WORKERS", "1")
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES", "67108864"
    )
    monkeypatch.setenv(
        "HLTHPRT_PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES", "16777216"
    )
    monkeypatch.setenv("HLTHPRT_PTG2_SERVING_BINARY_PAYLOAD_COMPRESSION", "none")
    monkeypatch.setenv("HLTHPRT_PTG2_SERVING_BINARY_BLOCK_BYTES", "65536")


async def _create_strict_shared_reservation(fixture):
    await _create_shared_schema(fixture.schema_name)
    await db.status(
        f"""
        INSERT INTO {fixture.quoted_schema}.ptg2_snapshot
            (snapshot_id, status, manifest)
        VALUES (:snapshot_id, 'building', '{{}}'::json)
        """,
        snapshot_id=fixture.snapshot_id,
    )
    await db.status(
        f"""
        INSERT INTO {fixture.quoted_schema}.ptg2_source_trace_set
            (source_trace_set_hash, source_trace_hashes)
        VALUES (:source_trace_set_hash, CAST(:source_trace_hashes AS varchar[]))
        """,
        source_trace_set_hash=fixture.source_assignment.source_trace_set_hash,
        source_trace_hashes=list(fixture.source_assignment.source_trace_hashes),
    )
    published_source_rows = await publish_shared_v3_snapshot_sources(
        schema_name=fixture.schema_name,
        snapshot_id=fixture.snapshot_id,
        plan_scopes=[SharedLogicalPlanScope("plan-v3-runs", "ein", "group")],
        coverage_scope_id=fixture.scanner.coverage_scope_id,
        assignments=[fixture.source_assignment],
    )
    replayed_source_rows = await publish_shared_v3_snapshot_sources(
        schema_name=fixture.schema_name,
        snapshot_id=fixture.snapshot_id,
        plan_scopes=[SharedLogicalPlanScope("plan-v3-runs", "ein", "group")],
        coverage_scope_id=fixture.scanner.coverage_scope_id,
        assignments=[fixture.source_assignment],
    )
    assert replayed_source_rows == published_source_rows
    async with db.transaction() as session:
        reservation = await reserve_shared_layout(
            session,
            schema_name=fixture.schema_name,
            semantic_fingerprint=shared_semantic_fingerprint(
                {"fixture": "strict-shared-publish-v1"}
            ),
            build_token="strict-shared-publish-smoke",
        )
    assert reservation.reused is False
    return reservation


async def _copy_strict_price_frames(scan, stage_table):
    for frame in scan["price_atom_frames"]:
        await _copy_price_atom_file(
            Path(frame["path"]),
            target_table=_ptg2_manifest_support_stage_table(
                stage_table, "price_atom"
            ),
        )
    for frame in scan["price_set_atom_frames"]:
        await _copy_price_atom_member_file(
            Path(frame["path"]),
            target_table=_ptg2_manifest_support_stage_table(
                stage_table,
                "price_set_atom",
            ),
        )
    for frame in scan["price_set_summary_frames"]:
        await _copy_price_set_summary_file(
            Path(frame["path"]),
            target_table=_ptg2_manifest_support_stage_table(
                stage_table,
                "price_set_summary",
            ),
        )


async def _publish_strict_shared_fixture(fixture, reservation):
    stage_table = await _create_serving_stage_table(
        f"shared_{reservation.snapshot_key}"
    )
    # A single scanner source emits every atom ID once. Cross-source
    # canonicalization has separate PostgreSQL coverage.
    await _copy_strict_price_frames(fixture.scanner.scan, stage_table)
    provider_group_id = bytes.fromhex("00112233445566778899aabbccddeeff")
    npi = 1234567890
    graph_entries = _graph_artifacts(
        fixture.tmp_path / "shared-graph",
        provider_set_id=fixture.scanner.provider_set_id,
        provider_group_id=provider_group_id,
        npi=npi,
    )
    publication = await publish_strict_shared_v3_layout(
        schema_name=fixture.schema_name,
        manifest_stage_table=stage_table,
        reserved_snapshot_key=reservation.snapshot_key,
        build_token="strict-shared-publish-smoke",
        expected_coverage_scope_id=fixture.scanner.coverage_scope_id,
        logical_snapshot_id=fixture.snapshot_id,
        expected_source_identities=[fixture.source_identity],
        serving_run_entries=fixture.serving_run_entries,
        code_dictionary_entries=fixture.code_dictionary_entries,
        provider_set_metadata_entries=fixture.provider_set_metadata_entries,
        price_set_summary_source_count=1,
        graph_artifact_entries=graph_entries,
        source_audit_witness_entries=(
            fixture.scanner.scanner_tests._single_frame(
                fixture.scanner.scan["frames"], "source_audit_witness_file"
            ),
        ),
        expected_raw_source_sha256=(fixture.artifact_digest,),
        provider_identifier_quarantine=fixture.scanner_summary[
            "provider_identifier_quarantine"
        ],
        scratch_parent=fixture.tmp_path,
    )
    return _StrictSharedLayout(
        publication=publication,
        graph_entries=graph_entries,
        npi=npi,
        reserved_snapshot_key=reservation.snapshot_key,
    )


def _assert_strict_publication_summary(layout):
    publication = layout.publication
    assert publication.snapshot_key == layout.reserved_snapshot_key
    assert publication.layout_reused_at_seal is False
    assert publication.stored_byte_count > 0
    assert publication.mapping_count > 0
    assert 0 < publication.unique_block_count <= publication.mapping_count
    assert len(publication.mapping_digest) == 32
    assert set(publication.object_kinds) == {
        "by_code_provider_shard_v1",
        "by_code_price_page_v4",
        "by_code_price_dictionary",
        "provider_set_count_dictionary",
        "provider_set_codes_v3",
        "provider_set_page_v3_s2",
        "price_set_atom_memberships_v3",
        "price_atoms_v3",
        "graph_npi_groups_v1",
        "graph_group_npis_v1",
        "graph_group_provider_sets_v1",
        "graph_provider_set_groups_v1",
    }


def _assert_strict_serving_index(fixture, publication):
    serving_index = publication.serving_index
    assert serving_index["storage_generation"] == "shared_blocks_v3"
    assert serving_index["shared_block_layout"] == "dense_shared_blocks_v3"
    assert serving_index["source_count"] == 1
    assert serving_index["cold_lookup_contract"] == "ptg_v3_cold_v2"
    assert serving_index["serving_multiplicity_semantics"] == "source_multiset_v1"
    assert serving_index["provider_identifier_quarantine"] == (
        fixture.scanner_summary["provider_identifier_quarantine"]
    )
    assert serving_index["coverage_scope_id"] == (
        fixture.scanner.coverage_scope_id.hex()
    )
    assert serving_index["serving_binary_table"] is None
    assert {
        "finalizer_seconds",
        "price_key_ready_finalizer_wall_seconds",
        "serving_block_publish_seconds",
        "dictionary_publish_seconds",
        "provider_set_key_export_seconds",
        "provider_graph_convert_seconds",
        "provider_graph_publish_seconds",
        "independent_publish_wall_seconds",
        "price_publish_seconds",
        "audit_publish_seconds",
        "seal_seconds",
        "shared_publish_total_seconds",
    } <= serving_index["timings"].keys()


def _assert_strict_timing_values(publication):
    assert all(
        publication.serving_index["timings"][name] >= 0
        for name in (
            "finalizer_seconds",
            "price_key_ready_finalizer_wall_seconds",
            "serving_block_publish_seconds",
            "dictionary_publish_seconds",
            "provider_set_key_export_seconds",
            "provider_graph_convert_seconds",
            "provider_graph_publish_seconds",
            "independent_publish_wall_seconds",
            "price_publish_seconds",
            "audit_publish_seconds",
            "seal_seconds",
            "shared_publish_total_seconds",
        )
    )
    price_stage = publication.serving_index["price_stage"]
    assert price_stage["price_key_build_seconds"] >= 0
    assert price_stage["price_atom_source_mode"] == (
        "single_scanner_unique_provenance"
    )
    assert price_stage["normalization_seconds"] == 0
    assert price_stage["duplicate_rows_removed"] == 0


def _assert_strict_audit_sample(fixture, publication):
    audit_sample = publication.serving_index["audit_sample"]
    assert audit_sample["contract"] == "persisted_served_occurrence_sample_v2"
    assert audit_sample["method"] == "publish_time_stratified_v1"
    assert audit_sample["serving_multiplicity_semantics"] == "source_multiset_v1"
    assert audit_sample["provider_selection"] == (
        "hash_targeted_budgeted_owner_ordinals_v2"
    )
    assert audit_sample["hydration_candidate_selection"] == (
        "source_preserving_equal_interval_v1"
    )
    assert audit_sample["price_hydration_candidate_count"] == 2
    assert audit_sample["hydrated_candidate_count"] == 2
    assert 0 < audit_sample["provider_selection_count"] <= audit_sample[
        "provider_selection_budget"
    ]
    assert audit_sample["price_membership_block_span"] == publication.serving_index[
        "serving_binary"
    ]["price_set_atom_memberships_v3"]["block_span"]
    assert audit_sample["sample_count"] == 3
    assert audit_sample["candidate_count"] == 2
    assert len(audit_sample["sample_digest"]) == 64
    assert not any(
        path.name.startswith("ptg2-v3-shared-publish-")
        for path in fixture.tmp_path.iterdir()
    )


def _assert_strict_publication(fixture, layout):
    _assert_strict_publication_summary(layout)
    _assert_strict_serving_index(fixture, layout.publication)
    _assert_strict_timing_values(layout.publication)
    _assert_strict_audit_sample(fixture, layout.publication)


async def _read_strict_logical_source_set(fixture, publication):
    source_set_by_field = shared_source_set_metadata(
        [fixture.source_assignment.raw_container_sha256]
    )
    logical_source_set = await db.scalar(
        f"""
        SELECT manifest::jsonb #> '{{serving_index,source_set}}'
          FROM {fixture.quoted_schema}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=fixture.snapshot_id,
    )
    assert logical_source_set == source_set_by_field
    assert "source_set" not in publication.serving_index
    assert (
        await db.scalar(
            f"""
            SELECT layout_manifest #> '{{serving_index,source_set}}'
              FROM {fixture.quoted_schema}.ptg2_v3_snapshot_layout
             WHERE snapshot_key = :snapshot_key
            """,
            snapshot_key=publication.snapshot_key,
        )
        is None
    )
    return source_set_by_field, logical_source_set


async def _assert_strict_source_conflict(fixture):
    conflicting_snapshot_id = f"shared-conflict-{uuid.uuid4().hex}"
    conflicting_source_set = shared_source_set_metadata(["f" * 64])
    await db.status(
        f"""
        INSERT INTO {fixture.quoted_schema}.ptg2_snapshot
            (snapshot_id, status, manifest)
        VALUES (:snapshot_id, 'building', CAST(:manifest AS json))
        """,
        snapshot_id=conflicting_snapshot_id,
        manifest=json.dumps(
            {"serving_index": {"source_set": conflicting_source_set}},
            ensure_ascii=True,
            separators=(",", ":"),
        ),
    )
    with pytest.raises(RuntimeError, match="conflicting logical source-set seal"):
        await publish_shared_v3_snapshot_sources(
            schema_name=fixture.schema_name,
            snapshot_id=conflicting_snapshot_id,
            plan_scopes=[
                SharedLogicalPlanScope("plan-v3-conflict", "ein", "group")
            ],
            coverage_scope_id=fixture.scanner.coverage_scope_id,
            assignments=[fixture.source_assignment],
        )
    assert await db.scalar(
        f"""
        SELECT manifest::jsonb #> '{{serving_index,source_set}}'
          FROM {fixture.quoted_schema}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=conflicting_snapshot_id,
    ) == conflicting_source_set


async def _publish_strict_snapshot_manifest(fixture, publication, source_set):
    await db.status(
        f"""
        UPDATE {fixture.quoted_schema}.ptg2_snapshot
           SET status = 'published',
               manifest = CAST(:manifest AS json)
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=fixture.snapshot_id,
        manifest=json.dumps(
            {
                "serving_index": {
                    **publication.serving_index,
                    "source_key": "synthetic-source",
                    "source_set": source_set,
                }
            },
            ensure_ascii=True,
            separators=(",", ":"),
        ),
    )
    async with db.transaction() as session:
        await bind_snapshot_to_shared_layout(
            session,
            schema_name=fixture.schema_name,
            snapshot_id=fixture.snapshot_id,
            snapshot_key=publication.snapshot_key,
        )


async def _attest_strict_snapshot(fixture, publication, source_set_by_field):
    await db.status(
        f"""
        INSERT INTO {fixture.quoted_schema}.ptg2_v3_candidate_audit_attestation
            (snapshot_id, snapshot_key, source_key, plan_id,
             plan_market_type, coverage_scope_id, source_set_digest,
             audit_sample_digest, contract, tool_name, tool_version,
             report_digest, report, attested_at, expires_at, activated_at)
        VALUES
            (:snapshot_id, :snapshot_key, 'synthetic-source',
             'plan-v3-runs', 'group', :coverage_scope_id,
             :source_set_digest, :audit_sample_digest,
             :contract, 'integration-test', '1',
             :report_digest, '{{}}'::jsonb,
             transaction_timestamp(),
             transaction_timestamp() + interval '1 hour',
             transaction_timestamp())
        """,
        snapshot_id=fixture.snapshot_id,
        snapshot_key=publication.snapshot_key,
        coverage_scope_id=fixture.scanner.coverage_scope_id,
        source_set_digest=bytes.fromhex(
            source_set_by_field["raw_container_sha256_digest"]
        ),
        audit_sample_digest=bytes.fromhex(
            publication.serving_index["audit_sample"]["sample_digest"]
        ),
        contract=PTG2_CANDIDATE_ATTESTATION_CONTRACT_V3,
        report_digest=b"\x11" * 32,
    )


async def _assert_strict_table_counts(fixture):
    row_counts = await db.first(
        f"""
        SELECT
            (SELECT COUNT(*) FROM {fixture.quoted_schema}.ptg2_v3_code),
            (SELECT COUNT(*) FROM {fixture.quoted_schema}.ptg2_v3_provider_set),
            (SELECT COUNT(*) FROM {fixture.quoted_schema}.ptg2_v3_provider_group),
            (SELECT COUNT(*) FROM {fixture.quoted_schema}.ptg2_v3_npi_scope),
            (SELECT COUNT(*) FROM {fixture.quoted_schema}.ptg2_v3_snapshot_binding),
            (SELECT COUNT(*) FROM {fixture.quoted_schema}.ptg2_v3_snapshot_scope),
            (SELECT COUNT(*) FROM {fixture.quoted_schema}.ptg2_v3_audit_occurrence),
            (SELECT COUNT(DISTINCT coverage_scope_id)
               FROM {fixture.quoted_schema}.ptg2_v3_code)
        """
    )
    assert tuple(int(table_count) for table_count in row_counts) == (
        2,
        1,
        1,
        1,
        1,
        1,
        3,
        1,
    )


async def _read_strict_serving_keys(fixture):
    provider_set_key = int(
        await db.scalar(
            f"SELECT provider_set_key FROM {fixture.quoted_schema}.ptg2_v3_provider_set"
        )
    )
    provider_group_key = int(
        await db.scalar(
            f"SELECT provider_group_key FROM {fixture.quoted_schema}.ptg2_v3_provider_group"
        )
    )
    code_key_rows = await db.all(
        f"""
        SELECT reported_code, code_key, negotiation_arrangement
          FROM {fixture.quoted_schema}.ptg2_v3_code
         ORDER BY reported_code
        """
    )
    code_key_by_reported_code = {
        str(code_key_record[0]): int(code_key_record[1])
        for code_key_record in code_key_rows
    }
    assert set(code_key_by_reported_code) == {"99213", "99214"}
    assert {str(code_key_record[2]) for code_key_record in code_key_rows} == {"FFS"}
    return _StrictServingKeys(
        provider_set_key=provider_set_key,
        provider_group_key=provider_group_key,
        code_key_by_reported_code=code_key_by_reported_code,
    )


async def _assert_strict_audit_rows(fixture, publication, serving_keys):
    audit_rows = await db.all(
        f"""
        SELECT occurrence_id, atom_ordinal, atom_key
          FROM {fixture.quoted_schema}.ptg2_v3_audit_occurrence
         WHERE snapshot_key = :snapshot_key
           AND code_key = :code_key
         ORDER BY atom_ordinal
        """,
        snapshot_key=publication.snapshot_key,
        code_key=serving_keys.code_key_by_reported_code["99213"],
    )
    assert [int(audit_row[1]) for audit_row in audit_rows] == [0, 1]
    assert audit_rows[0][2] == audit_rows[1][2]
    assert bytes(audit_rows[0][0]) != bytes(audit_rows[1][0])
    layout_audit_sample = await db.scalar(
        f"""
        SELECT layout_manifest #> '{{serving_index,audit_sample}}'
          FROM {fixture.quoted_schema}.ptg2_v3_snapshot_layout
         WHERE snapshot_key = :snapshot_key
        """,
        snapshot_key=publication.snapshot_key,
    )
    assert layout_audit_sample == publication.serving_index["audit_sample"]


async def _load_strict_serving_tables(session, fixture, publication):
    original_schema = ptg2_tables.PTG2_SCHEMA
    ptg2_tables.PTG2_SCHEMA = fixture.schema_name
    try:
        serving_tables = await ptg2_tables.snapshot_serving_tables(
            session,
            fixture.snapshot_id,
        )
    finally:
        ptg2_tables.PTG2_SCHEMA = original_schema
    assert serving_tables.uses_shared_blocks
    assert serving_tables.shared_snapshot_key == publication.snapshot_key
    return serving_tables


async def _assert_strict_api_response(session, fixture, publication, serving_tables):
    class _Pagination:
        limit = 25
        offset = 0

    original_schema = ptg2_serving.PTG2_SCHEMA
    ptg2_serving.PTG2_SCHEMA = fixture.schema_name
    try:
        api_payload = await ptg2_serving.search_ptg2_serving_table(
            session,
            fixture.snapshot_id,
            {
                "plan_id": "plan-v3-runs",
                "code_system": "CPT",
                "code": "99213",
            },
            _Pagination(),
            serving_tables=serving_tables,
        )
    finally:
        ptg2_serving.PTG2_SCHEMA = original_schema
    assert api_payload is not None
    assert len(api_payload["items"]) == 1
    provenance_map = dict(api_payload["provenance"])
    database_evidence = provenance_map.pop("database_evidence")
    assert provenance_map == {
        "arch_version": "postgres_binary_v3",
        "storage_generation": "shared_blocks_v3",
        "database_backend": "postgresql",
        "plan_id": "plan-v3-runs",
        "snapshot_id": fixture.snapshot_id,
        "source_key": "synthetic-source",
        "mode": "product_search",
        "pricing_scope": "plan_scoped_ptg",
    }
    assert database_evidence["contract"] == "postgresql_session_v1"
    assert database_evidence["backend_session_active"] is True
    assert database_evidence["database_selected"] is True
    assert database_evidence["transaction_snapshot_observed"] is True
    assert int(database_evidence["server_version_num"]) > 0
    assert api_payload["items"][0]["negotiation_arrangement"] == "FFS"
    assert len(api_payload["items"][0]["prices"]) == 2
    assert api_payload["items"][0]["prices"][0] == api_payload["items"][0][
        "prices"
    ][1]


async def _read_strict_forward_rows(session, fixture, publication, serving_keys):
    first_rows = await lookup_serving_binary_by_code_from_db(
        session,
        serving_keys.code_key_by_reported_code["99213"],
        shared_snapshot_key=publication.snapshot_key,
        schema_name=fixture.schema_name,
        price_dictionary_item_count=2,
        price_dictionary_block_bytes=int(
            publication.serving_index["serving_binary"]["price_dictionary"][
                "block_bytes"
            ]
        ),
    )
    assert len(first_rows) == 1
    assert first_rows[0].provider_set_key == serving_keys.provider_set_key
    assert first_rows[0].provider_count == 2
    assert (
        bytes.fromhex(first_rows[0].price_set_global_id_128)
        in fixture.scanner.price_set_ids
    )
    second_rows = await lookup_serving_binary_by_code_from_db(
        session,
        serving_keys.code_key_by_reported_code["99214"],
        shared_snapshot_key=publication.snapshot_key,
        schema_name=fixture.schema_name,
        price_dictionary_item_count=2,
        price_dictionary_block_bytes=int(
            publication.serving_index["serving_binary"]["price_dictionary"][
                "block_bytes"
            ]
        ),
    )
    assert len(second_rows) == 1
    assert (
        bytes.fromhex(second_rows[0].price_set_global_id_128)
        in fixture.scanner.price_set_ids
    )
    assert second_rows[0].price_key != first_rows[0].price_key
    return _StrictForwardRows(first=first_rows, second=second_rows)


async def _assert_strict_price_reads(
    session,
    fixture,
    publication,
    serving_keys,
    forward_rows,
):
    assert await lookup_shared_provider_code_keys_from_db(
        session,
        publication.snapshot_key,
        (serving_keys.provider_set_key,),
        schema_name=fixture.schema_name,
    ) == {
        serving_keys.provider_set_key: tuple(
            sorted(serving_keys.code_key_by_reported_code.values())
        )
    }
    assert (
        await lookup_shared_code_page_from_db(
            session,
            publication.snapshot_key,
            serving_keys.code_key_by_reported_code["99213"],
            schema_name=fixture.schema_name,
        )
        is not None
    )
    price_keys = {
        int(forward_rows.first[0].price_key),
        int(forward_rows.second[0].price_key),
    }
    memberships = await lookup_shared_price_atom_memberships_from_db(
        session,
        publication.snapshot_key,
        price_keys,
        atom_key_bits=int(publication.serving_index["atom_key_bits"]),
        schema_name=fixture.schema_name,
    )
    assert set(memberships) == price_keys
    assert sorted(len(atom_keys) for atom_keys in memberships.values()) == [1, 2]
    duplicate_atom_keys = memberships[int(forward_rows.first[0].price_key)]
    assert len(duplicate_atom_keys) == 2
    assert duplicate_atom_keys[0] == duplicate_atom_keys[1]
    atoms = await lookup_shared_price_atoms_from_db(
        session,
        publication.snapshot_key,
        {
            atom_key
            for atom_keys in memberships.values()
            for atom_key in atom_keys
        },
        atom_key_bits=int(publication.serving_index["atom_key_bits"]),
        schema_name=fixture.schema_name,
    )
    assert {atom.negotiated_rate for atom in atoms.values()} == {"125.5", "250"}


async def _assert_strict_graph_reads(
    session,
    fixture,
    layout,
    serving_keys,
):
    assert await fetch_shared_graph_members(
        session,
        schema_name=fixture.schema_name,
        snapshot_key=layout.publication.snapshot_key,
        direction=PTG2_V3_GRAPH_NPI_TO_GROUP,
        owner_keys=(layout.npi,),
    ) == {layout.npi: (serving_keys.provider_group_key,)}
    assert await fetch_shared_graph_members(
        session,
        schema_name=fixture.schema_name,
        snapshot_key=layout.publication.snapshot_key,
        direction=PTG2_V3_GRAPH_GROUP_TO_NPI,
        owner_keys=(serving_keys.provider_group_key,),
    ) == {serving_keys.provider_group_key: (layout.npi,)}
    assert await fetch_shared_graph_members(
        session,
        schema_name=fixture.schema_name,
        snapshot_key=layout.publication.snapshot_key,
        direction=PTG2_V3_GRAPH_GROUP_TO_PROVIDER_SET,
        owner_keys=(serving_keys.provider_group_key,),
    ) == {serving_keys.provider_group_key: (serving_keys.provider_set_key,)}
    assert await fetch_shared_graph_members(
        session,
        schema_name=fixture.schema_name,
        snapshot_key=layout.publication.snapshot_key,
        direction=PTG2_V3_GRAPH_PROVIDER_SET_TO_GROUP,
        owner_keys=(serving_keys.provider_set_key,),
    ) == {serving_keys.provider_set_key: (serving_keys.provider_group_key,)}
    assert await fetch_shared_blocks(
        session,
        schema_name=fixture.schema_name,
        snapshot_key=layout.publication.snapshot_key,
        object_kind="by_code_provider_shard_v1",
        block_keys=(
            (serving_keys.code_key_by_reported_code["99213"] << 31)
            | (serving_keys.provider_set_key // 1024),
        ),
        require_all=True,
    )


async def _assert_strict_cache_free_reads(fixture, layout, serving_keys):
    async with db.session() as session:
        serving_tables = await _load_strict_serving_tables(
            session, fixture, layout.publication
        )
        await _assert_strict_api_response(
            session, fixture, layout.publication, serving_tables
        )
        forward_rows = await _read_strict_forward_rows(
            session, fixture, layout.publication, serving_keys
        )
        await _assert_strict_price_reads(
            session,
            fixture,
            layout.publication,
            serving_keys,
            forward_rows,
        )
        await _assert_strict_graph_reads(session, fixture, layout, serving_keys)


async def _prepare_strict_reuse_stage(fixture):
    async with db.transaction() as session:
        reservation = await reserve_shared_layout(
            session,
            schema_name=fixture.schema_name,
            semantic_fingerprint=shared_semantic_fingerprint(
                {"fixture": "strict-shared-publish-v1-second-logical-source"}
            ),
            build_token="strict-shared-publish-reuse-smoke",
        )
    assert reservation.reused is False
    discarded_snapshot_key = reservation.snapshot_key
    stage_table = await _create_serving_stage_table(
        f"shared_{discarded_snapshot_key}"
    )
    await _copy_strict_price_frames(fixture.scanner.scan, stage_table)
    return discarded_snapshot_key, stage_table


async def _insert_strict_reuse_source(fixture, snapshot_id, assignment):
    await db.status(
        f"""
        INSERT INTO {fixture.quoted_schema}.ptg2_snapshot
            (snapshot_id, status, manifest)
        VALUES (:snapshot_id, 'building', '{{}}'::json)
        """,
        snapshot_id=snapshot_id,
    )
    await db.status(
        f"""
        INSERT INTO {fixture.quoted_schema}.ptg2_source_trace_set
            (source_trace_set_hash, source_trace_hashes)
        VALUES (:source_trace_set_hash, CAST(:source_trace_hashes AS varchar[]))
        """,
        source_trace_set_hash=assignment.source_trace_set_hash,
        source_trace_hashes=list(assignment.source_trace_hashes),
    )
    await publish_shared_v3_snapshot_sources(
        schema_name=fixture.schema_name,
        snapshot_id=snapshot_id,
        plan_scopes=[
            SharedLogicalPlanScope(
                "plan-v3-runs-reused",
                "ein",
                "group",
            )
        ],
        coverage_scope_id=fixture.scanner.coverage_scope_id,
        assignments=[assignment],
    )


async def _prepare_strict_reuse_source(fixture, logical_source_set):
    snapshot_id = f"shared-reused-{uuid.uuid4().hex}"
    assignment = SharedSnapshotSourceAssignment(
        source_key=0,
        identity=fixture.source_identity,
        source_trace_set_hash="e" * 64,
        source_trace_hashes=("f" * 64,),
        raw_container_sha256="1" * 64,
        logical_json_sha256="a" * 64,
        logical_hash_deferred=False,
    )
    await _insert_strict_reuse_source(fixture, snapshot_id, assignment)
    reused_source_set = await db.scalar(
        f"""
        SELECT manifest::jsonb #> '{{serving_index,source_set}}'
          FROM {fixture.quoted_schema}.ptg2_snapshot
         WHERE snapshot_id = :snapshot_id
        """,
        snapshot_id=snapshot_id,
    )
    assert reused_source_set == shared_source_set_metadata(
        [assignment.raw_container_sha256]
    )
    assert reused_source_set != logical_source_set
    return snapshot_id


async def _publish_strict_reused_layout(fixture, layout, reuse):
    return await publish_strict_shared_v3_layout(
        schema_name=fixture.schema_name,
        manifest_stage_table=reuse.stage_table,
        reserved_snapshot_key=reuse.discarded_snapshot_key,
        build_token="strict-shared-publish-reuse-smoke",
        expected_coverage_scope_id=fixture.scanner.coverage_scope_id,
        logical_snapshot_id=reuse.snapshot_id,
        expected_source_identities=[fixture.source_identity],
        serving_run_entries=fixture.serving_run_entries,
        code_dictionary_entries=fixture.code_dictionary_entries,
        provider_set_metadata_entries=fixture.provider_set_metadata_entries,
        price_set_summary_source_count=1,
        graph_artifact_entries=layout.graph_entries,
        source_audit_witness_entries=(
            fixture.scanner.scanner_tests._single_frame(
                fixture.scanner.scan["frames"], "source_audit_witness_file"
            ),
        ),
        expected_raw_source_sha256=(fixture.artifact_digest,),
        provider_identifier_quarantine=fixture.scanner_summary[
            "provider_identifier_quarantine"
        ],
        scratch_parent=fixture.tmp_path,
    )


async def _assert_strict_reuse_marker(fixture, layout, reuse, reused_publication):
    assert reused_publication.layout_reused_at_seal is True
    assert reused_publication.snapshot_key == layout.publication.snapshot_key
    assert reused_publication.serving_index["audit_sample"] == (
        layout.publication.serving_index["audit_sample"]
    )
    cleanup_marker = await db.first(
        f"""
        SELECT canonical_snapshot_key,
               cleanup_pending_at IS NOT NULL AS cleanup_pending
          FROM {fixture.quoted_schema}.ptg2_layout_build_candidate
         WHERE snapshot_key = :snapshot_key
        """,
        snapshot_key=reuse.discarded_snapshot_key,
    )
    assert dict(cleanup_marker._mapping) == {
        "canonical_snapshot_key": layout.publication.snapshot_key,
        "cleanup_pending": True,
    }
    assert (
        int(
            await db.scalar(
                f"""
            SELECT COUNT(*)
              FROM {fixture.quoted_schema}.ptg2_v3_snapshot_layout
             WHERE snapshot_key = :snapshot_key
            """,
                snapshot_key=reuse.discarded_snapshot_key,
            )
            or 0
        )
        == 1
    )


async def _assert_strict_reuse_release(fixture, reuse):
    released = await ptg2_shared_gc.release_unbound_ptg2_shared_layouts(
        schema_name=fixture.schema_name,
        building_max_age_seconds=21_600,
        grace_seconds=0,
        max_layouts=1,
        require_shared=True,
        layout_keys=(reuse.discarded_snapshot_key,),
    )
    replayed_release = await ptg2_shared_gc.release_unbound_ptg2_shared_layouts(
        schema_name=fixture.schema_name,
        building_max_age_seconds=21_600,
        grace_seconds=0,
        max_layouts=1,
        require_shared=True,
        layout_keys=(reuse.discarded_snapshot_key,),
    )
    assert released.logical_layout_count == 1
    assert replayed_release.logical_layout_count == 0
    assert (
        int(
            await db.scalar(
                f"""
            SELECT COUNT(*)
              FROM {fixture.quoted_schema}.ptg2_v3_snapshot_layout
             WHERE snapshot_key = :snapshot_key
            """,
                snapshot_key=reuse.discarded_snapshot_key,
            )
            or 0
        )
        == 0
    )
    assert (
        int(
            await db.scalar(
                f"""
            SELECT COUNT(*)
              FROM {fixture.quoted_schema}.ptg2_v3_audit_occurrence
             WHERE snapshot_key = :snapshot_key
            """,
                snapshot_key=reuse.discarded_snapshot_key,
            )
            or 0
        )
        == 0
    )


async def _exercise_strict_shared_reuse(fixture, layout, logical_source_set):
    discarded_snapshot_key, stage_table = await _prepare_strict_reuse_stage(fixture)
    snapshot_id = await _prepare_strict_reuse_source(fixture, logical_source_set)
    reuse = _StrictReuse(
        discarded_snapshot_key=discarded_snapshot_key,
        stage_table=stage_table,
        snapshot_id=snapshot_id,
    )
    reused_publication = await _publish_strict_reused_layout(fixture, layout, reuse)
    await _assert_strict_reuse_marker(fixture, layout, reuse, reused_publication)
    await _assert_strict_reuse_release(fixture, reuse)


async def _exercise_strict_shared_database(fixture):
    await db.disconnect()
    await db.connect()
    try:
        reservation = await _create_strict_shared_reservation(fixture)
        layout = await _publish_strict_shared_fixture(fixture, reservation)
        _assert_strict_publication(fixture, layout)
        source_set_by_field, logical_source_set = (
            await _read_strict_logical_source_set(fixture, layout.publication)
        )
        await _assert_strict_source_conflict(fixture)
        await _publish_strict_snapshot_manifest(
            fixture, layout.publication, logical_source_set
        )
        await _attest_strict_snapshot(
            fixture, layout.publication, source_set_by_field
        )
        await _assert_strict_table_counts(fixture)
        serving_keys = await _read_strict_serving_keys(fixture)
        await _assert_strict_audit_rows(
            fixture, layout.publication, serving_keys
        )
        await _assert_strict_cache_free_reads(fixture, layout, serving_keys)
        await _exercise_strict_shared_reuse(fixture, layout, logical_source_set)
    finally:
        try:
            await db.execute_ddl(
                f"DROP SCHEMA IF EXISTS {fixture.quoted_schema} CASCADE"
            )
        finally:
            await db.disconnect()


@pytest.mark.asyncio
async def test_real_postgres_strict_shared_v3_publish_and_cache_free_reads(
    tmp_path,
    monkeypatch,
):
    """Publish a strict shared layout and verify cache-free reads and reuse."""

    if os.getenv("HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST") != "1":
        pytest.skip(
            "set HLTHPRT_PTG2_SHARED_PUBLISH_POSTGRES_TEST=1 for the isolated PostgreSQL test"
        )

    fixture = _prepare_strict_shared_fixture(tmp_path, monkeypatch)
    _configure_strict_shared_environment(fixture, monkeypatch)
    await _exercise_strict_shared_database(fixture)
