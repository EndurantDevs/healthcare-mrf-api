# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Guarded compiler-to-reader PostgreSQL proof for packed PTG V4."""

from __future__ import annotations

from collections import OrderedDict
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import statistics
import struct
import time
import uuid

import pytest
import sqlalchemy as sa

from api import ptg2_v4_graph as graph
from db.connection import Database
from process.ptg_parts import ptg2_shared_publish
from process.ptg_parts import ptg2_shared_snapshot_publish as snapshot_publish
from process.ptg_parts.ptg2_v4_graph_compiler import (
    compile_provider_graph_v4_rust,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_SHARED_GENERATION,
    reserve_v4_shared_layout,
    seal_v4_shared_layout,
)
from tests.ptg2_v4_migration_catalog_support import (
    v3_provider_set_prerequisite_ddl,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260723100000_ptg2_v4_snapshot_map_pack.py"
)
_STANDARD_FORMAT = (
    "magic8:uint32_le_version:uint64_le_entry_count:"
    "index(owner16:uint64_le_offset:uint32_le_count):members16"
)
_GROUP_COUNT = 5_000
_SET_COUNT = 16
_NPI = 1_234_567_890


class _OpRecorder:
    def __init__(self) -> None:
        self.executed: list[str] = []

    def execute(self, statement) -> None:
        self.executed.append(str(statement))


def _load_v4_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_v4_postgres_e2e_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _quoted(identifier: str) -> str:
    return '"' + str(identifier).replace('"', '""') + '"'


def _global(domain: int, value: int) -> bytes:
    return bytes([domain]) + bytes(7) + int(value).to_bytes(8, "big")


def _npi(value: int) -> bytes:
    return bytes(8) + int(value).to_bytes(8, "big")


def _write_membership(
    path: Path,
    *,
    name: str,
    pairs: list[tuple[bytes, bytes]],
) -> dict[str, object]:
    by_owner: dict[bytes, set[bytes]] = {}
    for owner, member in pairs:
        by_owner.setdefault(owner, set()).add(member)
    normalized_memberships = [
        (owner, sorted(members))
        for owner, members in sorted(by_owner.items())
    ]
    membership_payload = bytearray(b"PTG2MNSC")
    membership_payload.extend(struct.pack("<IQ", 1, len(normalized_memberships)))
    offset = 0
    for owner, members in normalized_memberships:
        membership_payload.extend(owner)
        membership_payload.extend(struct.pack("<QI", offset, len(members)))
        offset += len(members)
    for _owner, members in normalized_memberships:
        for member in members:
            membership_payload.extend(member)
    path.write_bytes(membership_payload)
    return {
        "name": name,
        "source_shard_id": "postgres-e2e",
        "path": str(path),
        "record_format": _STANDARD_FORMAT,
        "sha256": hashlib.sha256(membership_payload).hexdigest(),
        "byte_count": len(membership_payload),
        "owner_count": len(normalized_memberships),
        "member_count": offset,
    }


def _factor_fixture(tmp_path: Path) -> tuple[list[dict[str, object]], Path]:
    component = _global(2, 1)
    groups = [_global(3, index + 1) for index in range(_GROUP_COUNT)]
    provider_sets = [_global(1, index + 1) for index in range(_SET_COUNT)]
    npi = _npi(_NPI)
    artifacts = [
        _write_membership(
            tmp_path / "set-component.sidecar",
            name="provider_set_component",
            pairs=[(provider_set, component) for provider_set in provider_sets],
        ),
        _write_membership(
            tmp_path / "component-group.sidecar",
            name="provider_component_group",
            pairs=[(component, group) for group in groups],
        ),
        _write_membership(
            tmp_path / "group-npi.sidecar",
            name="provider_group_npi",
            pairs=[(group, npi) for group in groups],
        ),
        _write_membership(
            tmp_path / "npi-group.sidecar",
            name="provider_npi_group",
            pairs=[(npi, group) for group in groups],
        ),
    ]
    provider_map = tmp_path / "provider-set-map.tsv"
    provider_map.write_text(
        "".join(
            f"{provider_set.hex()}\t{index}\n"
            for index, provider_set in enumerate(provider_sets, start=1)
        ),
        encoding="ascii",
    )
    return artifacts, provider_map


def _direct_factor_fixture(
    tmp_path: Path,
) -> tuple[list[dict[str, object]], Path]:
    provider_sets = [_global(1, 1), _global(1, 2)]
    components = [_global(2, 1), _global(2, 2)]
    groups = [_global(3, 1), _global(3, 2)]
    npis = [_npi(1_111_111_111), _npi(2_222_222_222)]
    artifacts = [
        _write_membership(
            tmp_path / "direct-set-component.sidecar",
            name="provider_set_component",
            pairs=list(zip(provider_sets, components, strict=True)),
        ),
        _write_membership(
            tmp_path / "direct-component-group.sidecar",
            name="provider_component_group",
            pairs=list(zip(components, groups, strict=True)),
        ),
        _write_membership(
            tmp_path / "direct-group-npi.sidecar",
            name="provider_group_npi",
            pairs=list(zip(groups, npis, strict=True)),
        ),
        _write_membership(
            tmp_path / "direct-npi-group.sidecar",
            name="provider_npi_group",
            pairs=list(zip(npis, groups, strict=True)),
        ),
    ]
    provider_map = tmp_path / "direct-provider-set-map.tsv"
    provider_map.write_text(
        "".join(
            f"{provider_set.hex()}\t{index}\n"
            for index, provider_set in enumerate(provider_sets)
        ),
        encoding="ascii",
    )
    return artifacts, provider_map


async def _insert_provider_set_rows(
    session,
    *,
    schema_name: str,
    snapshot_key: int,
    provider_sets_by_key: dict[int, bytes],
) -> None:
    """Persist the V3 provider-set identities consumed by V4 diagnostics."""

    schema = _quoted(schema_name)
    parameter_rows = [
        {
            "snapshot_key": int(snapshot_key),
            "provider_set_key": int(provider_set_key),
            "provider_set_global_id_128": bytes(provider_set_global_id),
            "provider_count": 1,
        }
        for provider_set_key, provider_set_global_id in sorted(
            provider_sets_by_key.items()
        )
    ]
    await session.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.ptg2_v3_provider_set
                (snapshot_key, provider_set_key,
                 provider_set_global_id_128, provider_count)
            VALUES
                (:snapshot_key, :provider_set_key,
                 :provider_set_global_id_128, :provider_count)
            """
        ),
        parameter_rows,
    )


def _compiler_binary() -> Path:
    return Path(
        os.getenv("HLTHPRT_PTG2_PROVIDER_GRAPH_V4_BIN")
        or ROOT
        / "support"
        / "ptg2_scanner"
        / "target"
        / "debug"
        / "ptg2_provider_graph_v4"
    )


def _isolate_graph_caches(monkeypatch) -> None:
    monkeypatch.setattr(graph, "_MAP_COORDINATE_CACHE", graph._ByteLRU(8 << 20))
    monkeypatch.setattr(graph, "_PHYSICAL_BLOCK_CACHE", graph._ByteLRU(8 << 20))
    monkeypatch.setattr(graph, "_ROOT_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_RELATION_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_HEAVY_OWNER_CACHE", OrderedDict())
    monkeypatch.setattr(graph, "_HEAVY_OWNER_NEGATIVE_CACHE", OrderedDict())


async def _create_v4_test_schema(
    database: Database,
    *,
    schema_name: str,
    monkeypatch,
) -> None:
    """Create the minimal migrated V4 catalog required by the E2E tests."""

    schema = _quoted(schema_name)
    await database.execute_ddl(f"CREATE SCHEMA {schema}")
    for statement in (
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_layout (
            snapshot_key bigint GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
            storage_shard_id smallint NOT NULL DEFAULT 0,
            build_token varchar(96) NOT NULL,
            generation varchar(32) NOT NULL,
            state varchar(16) NOT NULL,
            mapping_digest bytea,
            support_digest bytea,
            layout_manifest jsonb NOT NULL DEFAULT '{{}}'::jsonb,
            logical_byte_count bigint NOT NULL DEFAULT 0,
            created_at timestamptz NOT NULL DEFAULT now(),
            heartbeat_at timestamptz NOT NULL DEFAULT now(),
            lease_until timestamptz,
            published_at timestamptz
        )
        """,
        f"""
        CREATE UNIQUE INDEX ptg2_v4_e2e_sealed_mapping_idx
            ON {schema}.ptg2_v3_snapshot_layout
               (generation, mapping_digest, support_digest)
         WHERE state = 'sealed'
           AND mapping_digest IS NOT NULL
           AND support_digest IS NOT NULL
        """,
        v3_provider_set_prerequisite_ddl(schema),
        f"""
        CREATE TABLE {schema}.ptg2_v3_layout_fingerprint (
            semantic_fingerprint bytea PRIMARY KEY,
            snapshot_key bigint NOT NULL REFERENCES
                {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_provider_group (
            snapshot_key bigint NOT NULL,
            provider_group_key integer NOT NULL,
            provider_group_global_id_128 bytea NOT NULL,
            PRIMARY KEY (snapshot_key, provider_group_key),
            UNIQUE (snapshot_key, provider_group_global_id_128)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_npi_scope (
            snapshot_key bigint NOT NULL,
            npi bigint NOT NULL,
            PRIMARY KEY (snapshot_key, npi)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_block (
            block_hash bytea PRIMARY KEY,
            format_version smallint NOT NULL,
            object_kind varchar(64) NOT NULL,
            codec varchar(16) NOT NULL,
            entry_count bigint NOT NULL,
            raw_byte_count bigint NOT NULL,
            stored_byte_count bigint NOT NULL,
            payload bytea NOT NULL,
            created_at timestamptz NOT NULL DEFAULT now()
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_block (
            snapshot_key bigint NOT NULL REFERENCES
                {schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
            object_kind varchar(64) NOT NULL,
            block_key bigint NOT NULL,
            fragment_no integer NOT NULL,
            entry_count bigint NOT NULL,
            block_hash bytea NOT NULL REFERENCES
                {schema}.ptg2_v3_block(block_hash),
            PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
        )
        """,
        f"""
        CREATE TABLE {schema}.ptg2_v3_gc_candidate (
            block_hash bytea PRIMARY KEY REFERENCES
                {schema}.ptg2_v3_block(block_hash) ON DELETE CASCADE,
            eligible_at timestamptz NOT NULL,
            queued_at timestamptz NOT NULL DEFAULT now()
        )
        """,
    ):
        await database.execute_ddl(statement)

    migration = _load_v4_migration()
    recorder = _OpRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setattr(migration, "_schema", lambda: schema_name)
    migration.upgrade()
    for statement in recorder.executed:
        await database.execute_ddl(statement)


def _base_layout_manifest() -> dict[str, object]:
    return {
        "serving_index": {
            "arch_version": "postgres_binary_v3",
            "type": "ptg2_shared_blocks_v3",
            "storage_generation": "shared_blocks_v3",
            "provider_scope_strategy": "postgres_shared_graph",
            "shared_block_layout": "dense_shared_blocks_v3",
            "serving_binary": {
                "format": "postgres_binary_v3",
                "price_dictionary": {"preserved": True},
            },
        }
    }


@pytest.mark.asyncio
async def test_v4_compiler_publish_seal_and_reader_are_exact_on_postgres(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Prove the exact pattern and heavy-bitmap paths through durable CAS."""

    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for PostgreSQL E2E")

    binary_path = _compiler_binary()
    assert binary_path.is_file(), f"missing V4 compiler binary: {binary_path}"
    artifacts, provider_map = _factor_fixture(tmp_path)
    compilation_started = time.perf_counter()
    compilation = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=tmp_path / "compiled-v4",
        options={"member_page_bytes": 64},
        binary_path=binary_path,
    )
    compilation_ms = (time.perf_counter() - compilation_started) * 1_000
    assert compilation.selected_layout == "pattern"
    assert compilation.observe["group_count"] == _GROUP_COUNT
    assert compilation.observe["provider_set_count"] == _SET_COUNT
    heavy_npi = next(
        bitmap_summary
        for bitmap_summary in compilation.heavy_bitmaps
        if bitmap_summary["relation"] == "npi_groups_exact"
        and bitmap_summary["owner_key"] == 0
    )
    assert int(heavy_npi["block_count"]) > 1
    reference_rows = [
        json.loads(line)
        for line in compilation.reference_manifest_path.read_text(
            encoding="utf-8"
        ).splitlines()
    ]
    heavy_references = [
        reference_entry
        for reference_entry in reference_rows
        if reference_entry["object_kind"] == heavy_npi["object_kind"]
        and int(reference_entry["block_key"]) == 0
    ]
    assert len(heavy_references) == int(heavy_npi["block_count"])
    assert (
        sum(int(reference_entry["entry_count"]) for reference_entry in heavy_references)
        == _GROUP_COUNT
    )

    schema_name = f"ptg2_v4_e2e_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    database = Database()
    await database.connect()
    monkeypatch.setattr(ptg2_shared_publish, "db", database)
    monkeypatch.setattr(snapshot_publish, "db", database)
    _isolate_graph_caches(monkeypatch)
    try:
        await _create_v4_test_schema(
            database,
            schema_name=schema_name,
            monkeypatch=monkeypatch,
        )
        build_token = f"v4-e2e-{uuid.uuid4().hex}"
        async with database.transaction() as session:
            reservation = await reserve_v4_shared_layout(
                session,
                schema_name=schema_name,
                semantic_fingerprint=hashlib.sha256(build_token.encode()).digest(),
                build_token=build_token,
            )
            await _insert_provider_set_rows(
                session,
                schema_name=schema_name,
                snapshot_key=reservation.snapshot_key,
                provider_sets_by_key={
                    provider_set_key: _global(1, provider_set_key)
                    for provider_set_key in range(1, _SET_COUNT + 1)
                },
            )
        publication_started = time.perf_counter()
        publication = await snapshot_publish._publish_v4_graph(
            compilation,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
            build_token=build_token,
            compressed_acquisition_bytes=1024,
            empty_npi_tin_only_normalization_count=0,
        )
        async with database.transaction() as session:
            sealed = await seal_v4_shared_layout(
                session,
                schema_name=schema_name,
                snapshot_key=reservation.snapshot_key,
                build_token=build_token,
                expected_summary=publication.map_summary,
                support_digest=publication.support_digest,
                layout_manifest=_base_layout_manifest(),
            )
        publication_ms = (time.perf_counter() - publication_started) * 1_000
        assert sealed.snapshot_key == reservation.snapshot_key
        assert publication.representation == "pattern_v1"
        assert publication.mapping_count > 0
        assert publication.unique_block_count > 0
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 0
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_npi_scope "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 0
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v4_npi_scope "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 1
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_provider_set "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == _SET_COUNT

        expected_groups = tuple(range(_GROUP_COUNT))
        before_metrics = graph.v4_graph_metrics_snapshot()
        cold_started = time.perf_counter()
        async with database.transaction() as session:
            npi_keys = await graph.v4_npi_keys_for_values(
                session,
                snapshot_key=sealed.snapshot_key,
                npis=(_NPI,),
                schema_name=schema_name,
            )
            exact_groups = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="npi_groups_exact",
                owner_keys=(npi_keys[_NPI],),
                schema_name=schema_name,
            )
        cold_ms = (time.perf_counter() - cold_started) * 1_000
        after_cold_metrics = graph.v4_graph_metrics_snapshot()
        assert npi_keys == {_NPI: 0}
        assert exact_groups == {0: expected_groups}
        assert (
            after_cold_metrics["bitmap_owner_hits"]
            == before_metrics["bitmap_owner_hits"] + 1
        )
        assert after_cold_metrics["database_blocks"] > before_metrics["database_blocks"]

        async with database.transaction() as session:
            npi_patterns = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="npi_patterns",
                owner_keys=(0,),
                schema_name=schema_name,
            )
            pattern_groups = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="pattern_groups",
                owner_keys=npi_patterns[0],
                schema_name=schema_name,
            )
            pattern_sets = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="pattern_sets",
                owner_keys=npi_patterns[0],
                schema_name=schema_name,
            )
            set_patterns = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="set_patterns",
                owner_keys=range(1, _SET_COUNT + 1),
                schema_name=schema_name,
            )
        assert npi_patterns == {0: (0,)}
        assert pattern_groups == {0: expected_groups}
        assert pattern_sets == {0: tuple(range(1, _SET_COUNT + 1))}
        assert set_patterns == {
            provider_set_key: (0,)
            for provider_set_key in range(1, _SET_COUNT + 1)
        }
        assert set().union(*(set(groups) for groups in pattern_groups.values())) == set(
            expected_groups
        )

        warm_durations_ms: list[float] = []
        warm_metrics_before = graph.v4_graph_metrics_snapshot()
        async with database.transaction() as session:
            for _ in range(7):
                started = time.perf_counter()
                warm = await graph.lookup_v4_relation_members(
                    session,
                    snapshot_key=sealed.snapshot_key,
                    relation="npi_groups_exact",
                    owner_keys=(0,),
                    schema_name=schema_name,
                )
                warm_durations_ms.append((time.perf_counter() - started) * 1_000)
                assert warm == {0: expected_groups}
        warm_metrics_after = graph.v4_graph_metrics_snapshot()
        warm_p50_ms = statistics.median(warm_durations_ms)
        assert warm_metrics_after["database_bytes"] == warm_metrics_before["database_bytes"]
        assert (
            warm_metrics_after["bitmap_owner_hits"]
            == warm_metrics_before["bitmap_owner_hits"] + len(warm_durations_ms)
        )
        assert warm_p50_ms < 50

        physical_bytes = int(
            await database.scalar(
                f"""
                SELECT SUM(pg_total_relation_size(relation_name::regclass))::bigint
                  FROM unnest(ARRAY[
                       '{schema_name}.ptg2_v3_block',
                       '{schema_name}.ptg2_v3_provider_group',
                       '{schema_name}.ptg2_v4_npi_scope',
                       '{schema_name}.ptg2_v4_snapshot_map_root',
                       '{schema_name}.ptg2_v4_snapshot_map_pack',
                       '{schema_name}.ptg2_v4_provider_component',
                       '{schema_name}.ptg2_v4_pattern',
                       '{schema_name}.ptg2_v4_relation_manifest',
                       '{schema_name}.ptg2_v4_heavy_owner'
                  ]) AS relation_name
                """
            )
            or 0
        )
        performance_evidence_map = {
            "block_count": compilation.block_count,
            "cold_reader_ms": round(cold_ms, 3),
            "compiler_ms": round(compilation_ms, 3),
            "coordinate_count": publication.map_summary.coordinate_count,
            "group_count": _GROUP_COUNT,
            "heavy_owner_count": len(compilation.heavy_bitmaps),
            "layout": compilation.selected_layout,
            "physical_bytes": physical_bytes,
            "publication_and_seal_ms": round(publication_ms, 3),
            "set_count": _SET_COUNT,
            "warm_reader_p50_ms": round(warm_p50_ms, 3),
            "warm_reader_max_ms": round(max(warm_durations_ms), 3),
        }
        print(
            "PTG2_V4_POSTGRES_E2E "
            + json.dumps(performance_evidence_map, sort_keys=True)
        )
        assert physical_bytes > 0
    finally:
        compilation.cleanup()
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_v4_direct_layout_publishes_only_exact_direct_relations_on_postgres(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Prove the smaller direct layout remains exact in both directions."""

    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for PostgreSQL E2E")

    binary_path = _compiler_binary()
    assert binary_path.is_file(), f"missing V4 compiler binary: {binary_path}"
    artifacts, provider_map = _direct_factor_fixture(tmp_path)
    compilation = await compile_provider_graph_v4_rust(
        graph_artifact_entries=artifacts,
        provider_set_key_map_path=provider_map,
        output_directory=tmp_path / "compiled-direct-v4",
        binary_path=binary_path,
    )
    assert compilation.selected_layout == "direct"
    relation_names = {
        str(relation["relation"])
        for relation in compilation.relation_summaries
    }
    assert {"group_sets_direct", "set_groups_direct"} <= relation_names
    assert not relation_names.intersection(graph.PTG2_V4_PATTERN_RELATIONS)

    schema_name = f"ptg2_v4_direct_e2e_{uuid.uuid4().hex}"
    schema = _quoted(schema_name)
    database = Database()
    await database.connect()
    monkeypatch.setattr(ptg2_shared_publish, "db", database)
    monkeypatch.setattr(snapshot_publish, "db", database)
    _isolate_graph_caches(monkeypatch)
    try:
        await _create_v4_test_schema(
            database,
            schema_name=schema_name,
            monkeypatch=monkeypatch,
        )
        build_token = f"v4-direct-e2e-{uuid.uuid4().hex}"
        async with database.transaction() as session:
            reservation = await reserve_v4_shared_layout(
                session,
                schema_name=schema_name,
                semantic_fingerprint=hashlib.sha256(build_token.encode()).digest(),
                build_token=build_token,
            )
            await _insert_provider_set_rows(
                session,
                schema_name=schema_name,
                snapshot_key=reservation.snapshot_key,
                provider_sets_by_key={
                    provider_set_key: _global(1, provider_set_key + 1)
                    for provider_set_key in range(2)
                },
            )
        publication = await snapshot_publish._publish_v4_graph(
            compilation,
            schema_name=schema_name,
            snapshot_key=reservation.snapshot_key,
            build_token=build_token,
            compressed_acquisition_bytes=1024,
            empty_npi_tin_only_normalization_count=0,
        )
        async with database.transaction() as session:
            sealed = await seal_v4_shared_layout(
                session,
                schema_name=schema_name,
                snapshot_key=reservation.snapshot_key,
                build_token=build_token,
                expected_summary=publication.map_summary,
                support_digest=publication.support_digest,
                layout_manifest=_base_layout_manifest(),
            )

        assert publication.representation == "direct_v1"
        async with database.transaction() as session:
            set_groups = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="set_groups_direct",
                owner_keys=(0, 1),
                schema_name=schema_name,
            )
            group_sets = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="group_sets_direct",
                owner_keys=(0, 1),
                schema_name=schema_name,
            )
            npi_groups = await graph.lookup_v4_relation_members(
                session,
                snapshot_key=sealed.snapshot_key,
                relation="npi_groups_exact",
                owner_keys=(0, 1),
                schema_name=schema_name,
            )
        assert set_groups == {0: (0,), 1: (1,)}
        assert group_sets == {0: (0,), 1: (1,)}
        assert npi_groups == {0: (0,), 1: (1,)}

        persisted_relations = {
            str(relation_row[0])
            for relation_row in await database.all(
                f"SELECT relation FROM {schema}.ptg2_v4_relation_manifest "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=sealed.snapshot_key,
            )
        }
        assert persisted_relations == relation_names
        assert not persisted_relations.intersection(graph.PTG2_V4_PATTERN_RELATIONS)
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_snapshot_block "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 0
        assert await database.scalar(
            f"SELECT COUNT(*) FROM {schema}.ptg2_v3_provider_set "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=sealed.snapshot_key,
        ) == 2
    finally:
        compilation.cleanup()
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
        finally:
            await database.disconnect()
