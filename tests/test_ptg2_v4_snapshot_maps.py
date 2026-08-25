# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import asyncio
import importlib.util
import os
from pathlib import Path
import struct
import uuid

import pytest
import sqlalchemy as sa

from api.ptg2_code_filters import INFERRED_PROVIDER_TAXONOMY_RULES
from db.connection import Database
from db.models import (
    PTG2V4HeavyOwner,
    PTG2V4NPIScope,
    PTG2V4Pattern,
    PTG2V4ProviderComponent,
    PTG2V4RelationManifest,
    PTG2V4SnapshotMapPack,
    PTG2V4SnapshotMapRoot,
)
from process.ptg_parts import (
    ptg2_block_build_pins,
    ptg2_shared_gc,
    ptg2_shared_publish,
    ptg2_v4_snapshot_maps,
)
from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_DENSE_LAYOUT_TABLES,
    SharedBlock,
)
from process.ptg_parts.ptg2_shared_publish import (
    _publish_v4_cas_block_stage_compatibility,
)
from process.ptg_parts.ptg2_v4_snapshot_maps import (
    PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS,
    PTG2_V4_MAP_BLOCK_KIND,
    PTG2_V4_MAP_FORMAT,
    PTG2_V4_PROJECTION_ID_SCOPE,
    PTG2_V4_SHARED_GENERATION,
    bind_snapshot_to_v4_layout,
    decode_v4_snapshot_map_pack,
    iter_v4_snapshot_map_packs,
    publish_v4_heavy_owners,
    publish_v4_npi_dictionary,
    publish_v4_patterns,
    publish_v4_provider_components,
    publish_v4_relation_manifests,
    publish_v4_snapshot_maps,
    reserve_v4_shared_layout,
    seal_v4_shared_layout,
    summarize_persisted_v4_snapshot_maps,
    summarize_v4_snapshot_map_packs,
    v4_layout_fingerprint,
    v4_map_pack_target_hashes,
    touch_v4_shared_layout_build,
)
from process.ptg_parts.ptg2_v4_taxonomy_candidates import (
    publish_v4_inferred_taxonomy_candidates,
)
from tests.ptg2_v4_migration_catalog_support import (
    attempt_guard_prerequisite_ddl,
    v3_provider_set_prerequisite_ddl,
)
from tests.ptg2_v4_coverage_support import (
    synthetic_adaptive_layout_decision,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "20260723100000_ptg2_v4_snapshot_map_pack.py"
)
TAXONOMY_MIGRATION_PATH = (
    ROOT / "alembic" / "versions" / "20260724120000_ptg2_v4_taxonomy_candidates.py"
)


def _load_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_v4_snapshot_map_pack_migration",
        MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _load_taxonomy_migration():
    spec = importlib.util.spec_from_file_location(
        "ptg2_v4_taxonomy_candidates_migration",
        TAXONOMY_MIGRATION_PATH,
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class _OpRecorder:
    def __init__(self):
        self.executed = []

    def execute(self, statement):
        self.executed.append(str(statement))


def _normalized(statement):
    return " ".join(str(statement).split())


def _references():
    return (
        SharedBlock("alpha_v1", 1, 0, 2, "none", 3, b"one").reference(),
        SharedBlock("alpha_v1", 1, 1, 3, "none", 3, b"two").reference(),
        SharedBlock("alpha_v1", 2, 0, 5, "none", 5, b"three").reference(),
        SharedBlock("beta_v1", 0, 0, 7, "none", 4, b"four").reference(),
        SharedBlock("beta_v1", 9, 0, 11, "none", 4, b"five").reference(),
    )


def _sealed_graph_blocks():
    member_payload = struct.pack("<IIIII", 10, 11, 12, 13, 14)
    locator_payload = struct.pack("<QI", 0, 0) + struct.pack("<QI", 0, 5)
    heavy_payload = (
        b"PTG2V4BM" + struct.pack("<IIII", 1, 0, 10, 5) + bytes((0x1F, 0x00))
    )
    return tuple(
        sorted(
            (
                SharedBlock("alpha_members_v1", 0, 0, 5, "none", 20, member_payload),
                SharedBlock("alpha_locators_v1", 1, 0, 2, "none", 24, locator_payload),
                SharedBlock("alpha_heavy_v1", 1, 0, 5, "none", 26, heavy_payload),
            ),
            key=lambda block: (
                block.object_kind,
                block.block_key,
                block.fragment_no,
            ),
        )
    )


def _v4_positive_diagnostic_limits() -> dict[str, int]:
    """Return positive sealed limits for the synthetic graph diagnostic."""

    return {
        "npi_prefix_target": 201,
        "max_set_patterns_per_set": 1024,
        "max_set_components_per_fallback_set": 4096,
        "max_online_group_keys_per_set": 4096,
        "max_online_source_owners_per_set": 4096,
        "max_online_source_members_per_set": 16384,
        "max_online_source_pages_per_set": 64,
        "max_online_source_bytes_per_set": 1024 * 1024,
        "online_group_npi_batch_size": 32,
        "max_online_group_npi_members_per_set": 32768,
        "max_online_group_npi_locator_pages_per_set": 16,
        "max_online_group_npi_member_pages_per_set": 128,
        "max_online_group_npi_bytes_per_set": 4 * 1024 * 1024,
        "max_online_group_npi_batches_per_set": 4,
        "provider_expansion_rate_page_rows": 64,
        "max_online_provider_expansion_rate_rows": 256,
        "max_online_provider_expansion_provider_sets": 64,
        "max_online_provider_expansion_graph_batches": 64,
    }


def _v4_graph_diagnostic_fields(
    snapshot_key: int,
    stored_graph_bytes: int,
) -> dict[str, object]:
    """Build a constraint-valid empty prefix diagnostic."""

    diagnostic_by_field: dict[str, object] = {
        field_name: 0 for field_name in PTG2_V4_GRAPH_DIAGNOSTIC_FIELDS
    }
    diagnostic_by_field.update(_v4_positive_diagnostic_limits())
    diagnostic_by_field.update(
        {
            "worst_provider_set_key": None,
            "worst_member_digest": None,
            "worst_online_provider_set_key": None,
            "worst_online_member_digest": None,
            "worst_uses_override": False,
            "worst_uses_component_fallback": False,
            "worst_online_groups_to_target_exact": False,
            "worst_online_uses_component_fallback": False,
        }
    )
    return {
        "snapshot_key": int(snapshot_key),
        "compressed_acquisition_bytes": stored_graph_bytes,
        "input_factor_bytes": stored_graph_bytes,
        "factor_edge_count": 10,
        "empty_npi_tin_only_normalization_count": 0,
        **diagnostic_by_field,
    }


async def _insert_v4_graph_diagnostic_fixture(
    session,
    *,
    schema: str,
    snapshot_key: int,
    graph_blocks,
) -> None:
    """Persist a constraint-valid diagnostic for the synthetic alpha graph."""

    stored_graph_bytes = sum(
        int(graph_block.stored_byte_count) for graph_block in graph_blocks
    )
    diagnostic_by_field = _v4_graph_diagnostic_fields(
        snapshot_key,
        stored_graph_bytes,
    )
    columns = ", ".join(diagnostic_by_field)
    parameters = ", ".join(f":{field}" for field in diagnostic_by_field)
    await session.execute(
        sa.text(
            f"""
            INSERT INTO {schema}.ptg2_v4_provider_graph_diagnostic
                ({columns})
            VALUES
                ({parameters})
            """
        ),
        diagnostic_by_field,
    )


def _constraint(table, name, constraint_type):
    return next(
        constraint
        for constraint in table.constraints
        if isinstance(constraint, constraint_type) and constraint.name == name
    )


def test_v4_migration_is_additive_after_the_current_head(monkeypatch):
    migration = _load_migration()
    recorder = _OpRecorder()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "ptg_v4_test")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    monkeypatch.setattr(migration, "op", recorder)

    migration.upgrade()

    sql = " ".join(_normalized(statement) for statement in recorder.executed)
    assert migration.down_revision == "20260722170000_projection_child_decoded_census"
    assert 'ALTER TABLE "ptg_v4_test"."ptg2_v3_npi_scope"' not in sql
    assert 'CREATE TABLE "ptg_v4_test"."ptg2_v4_npi_scope"' in sql
    assert 'CREATE TABLE "ptg_v4_test"."ptg2_v4_snapshot_map_root"' in sql
    assert 'CREATE TABLE "ptg_v4_test"."ptg2_v4_snapshot_map_pack"' in sql
    assert 'CREATE TABLE "ptg_v4_test"."ptg2_v4_provider_component"' in sql
    assert 'CREATE TABLE "ptg_v4_test"."ptg2_v4_pattern"' in sql
    assert 'CREATE TABLE "ptg_v4_test"."ptg2_v4_relation_manifest"' in sql
    assert 'CREATE TABLE "ptg_v4_test"."ptg2_v4_heavy_owner"' in sql
    assert "packed_coordinate_hash_v1" in sql
    assert "snapshot_local_v1" in sql
    assert "ptg2_v4_snapshot_map_pack_overlap" in sql
    assert "ptg2_v4_snapshot_map_pack_sealed_delete" in sql
    assert "ptg2_v4_snapshot_metadata_sealed_delete" in sql
    assert "ptg2_v4_snapshot_map_root_sealed_delete" in sql
    assert sql.count("BEFORE INSERT OR UPDATE OR DELETE") == 9
    assert "ptg2_v3_graph_owner" not in sql

    recorder.executed.clear()
    migration.downgrade()
    downgrade_sql = " ".join(_normalized(statement) for statement in recorder.executed)
    assert (
        'DROP TABLE IF EXISTS "ptg_v4_test"."ptg2_v4_snapshot_map_pack"'
        in downgrade_sql
    )
    assert (
        'DROP TABLE IF EXISTS "ptg_v4_test"."ptg2_v4_snapshot_map_root"'
        in downgrade_sql
    )
    assert 'DROP TABLE IF EXISTS "ptg_v4_test"."ptg2_v4_heavy_owner"' in downgrade_sql
    assert (
        'DROP TABLE IF EXISTS "ptg_v4_test"."ptg2_v4_relation_manifest"'
        in downgrade_sql
    )
    assert 'DROP TABLE IF EXISTS "ptg_v4_test"."ptg2_v4_npi_scope"' in downgrade_sql
    assert "ptg2_v3_npi_scope" not in downgrade_sql


def test_v4_migration_rejects_conflicting_schema_aliases(monkeypatch):
    migration = _load_migration()
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "runtime")
    monkeypatch.setenv("DB_SCHEMA", "legacy")

    with pytest.raises(RuntimeError, match="must identify the same schema"):
        migration._schema()


def test_v4_models_match_root_pack_and_dense_npi_contracts():
    """Keep V4 map roots, packs, and NPI keys generation-local."""

    root = PTG2V4SnapshotMapRoot.__table__
    pack = PTG2V4SnapshotMapPack.__table__
    npi = PTG2V4NPIScope.__table__

    assert tuple(column.name for column in root.primary_key.columns) == (
        "snapshot_key",
    )
    assert tuple(column.name for column in pack.primary_key.columns) == (
        "snapshot_key",
        "object_kind",
        "pack_no",
    )
    assert tuple(column.name for column in npi.primary_key.columns) == (
        "snapshot_key",
        "npi_key",
    )
    root_fk = _constraint(
        root,
        "ptg2_v4_snapshot_map_root_layout_fkey",
        sa.ForeignKeyConstraint,
    )
    assert root_fk.ondelete == "CASCADE"
    pack_block_fk = _constraint(
        pack,
        "ptg2_v4_snapshot_map_pack_block_fkey",
        sa.ForeignKeyConstraint,
    )
    assert pack_block_fk.ondelete == "RESTRICT"
    npi_unique = _constraint(
        npi,
        "ptg2_v4_npi_scope_npi_key",
        sa.UniqueConstraint,
    )
    assert tuple(column.name for column in npi_unique.columns) == (
        "snapshot_key",
        "npi",
    )
    npi_root_fk = _constraint(
        npi,
        "ptg2_v4_npi_scope_root_fkey",
        sa.ForeignKeyConstraint,
    )
    assert npi_root_fk.ondelete == "CASCADE"


def test_v4_projection_models_match_metadata_contracts():
    """Keep projection metadata keyed within one immutable snapshot."""

    component = PTG2V4ProviderComponent.__table__
    pattern = PTG2V4Pattern.__table__
    relation = PTG2V4RelationManifest.__table__
    heavy_owner = PTG2V4HeavyOwner.__table__

    assert tuple(column.name for column in component.primary_key.columns) == (
        "snapshot_key",
        "component_key",
    )
    assert tuple(column.name for column in pattern.primary_key.columns) == (
        "snapshot_key",
        "pattern_key",
    )
    assert tuple(column.name for column in relation.primary_key.columns) == (
        "snapshot_key",
        "relation",
    )
    assert relation.c.logical_member_count.nullable is False
    assert relation.c.vector_member_count.nullable is False
    assert "member_count" not in relation.c
    assert tuple(column.name for column in heavy_owner.primary_key.columns) == (
        "snapshot_key",
        "relation",
        "owner_key",
    )


def test_v4_map_pack_round_trip_and_gc_reachability_are_exact():
    packs = tuple(iter_v4_snapshot_map_packs(_references(), max_coordinates_per_pack=2))

    assert [
        (pack.object_kind, pack.pack_no, pack.coordinate_count) for pack in packs
    ] == [
        ("alpha_v1", 0, 2),
        ("alpha_v1", 1, 1),
        ("beta_v1", 0, 2),
    ]
    first = packs[0]
    assert first.map_block.object_kind == PTG2_V4_MAP_BLOCK_KIND
    assert first.map_block.codec == "none"
    decoded = decode_v4_snapshot_map_pack(
        first.map_block.payload,
        expected_object_kind="alpha_v1",
    )
    assert [coordinate.coordinate for coordinate in decoded] == [
        ("alpha_v1", 1, 0),
        ("alpha_v1", 1, 1),
    ]
    assert v4_map_pack_target_hashes(first.map_block.payload) == tuple(
        reference.block_hash for reference in first.references
    )


def test_v4_mapping_digest_does_not_depend_on_pack_boundaries():
    small_packs = tuple(
        iter_v4_snapshot_map_packs(_references(), max_coordinates_per_pack=1)
    )
    wide_packs = tuple(
        iter_v4_snapshot_map_packs(_references(), max_coordinates_per_pack=4)
    )
    small = summarize_v4_snapshot_map_packs(small_packs)
    wide = summarize_v4_snapshot_map_packs(wide_packs)

    assert small.map_digest == wide.map_digest
    assert small.coordinate_count == wide.coordinate_count == 5
    assert small.entry_count == wide.entry_count == 28
    assert small.logical_byte_count == wide.logical_byte_count == 19
    assert small.map_pack_count != wide.map_pack_count


def test_v4_map_encoder_rejects_non_deterministic_coordinate_order():
    references = _references()

    with pytest.raises(ValueError, match="strictly ordered"):
        tuple(iter_v4_snapshot_map_packs(reversed(references)))


def test_v4_layout_fingerprint_namespaces_physical_generation_only():
    semantic = b"s" * 32

    assert v4_layout_fingerprint(semantic) == v4_layout_fingerprint(semantic)
    assert v4_layout_fingerprint(semantic) != semantic
    with pytest.raises(ValueError, match="32 bytes"):
        v4_layout_fingerprint(b"short")


class _V4PostgresScenario:
    """Keep the real-PostgreSQL V4 contract in one ordered disposable lifecycle."""

    def __init__(self, monkeypatch):
        self.monkeypatch = monkeypatch

    async def prepare(self):
        self.schema_name = f"ptg2_v4_map_{uuid.uuid4().hex}"
        self.schema = '"' + self.schema_name.replace('"', '""') + '"'
        self.database = Database()
        await self.database.connect()
        migration = _load_migration()
        self.recorder = _OpRecorder()
        self.monkeypatch.setattr(migration, "op", self.recorder)
        self.monkeypatch.setattr(migration, "_schema", lambda: self.schema_name)
        migration.upgrade()
        taxonomy_migration = _load_taxonomy_migration()
        self.monkeypatch.setattr(taxonomy_migration, "op", self.recorder)
        self.monkeypatch.setattr(
            taxonomy_migration, "_schema", lambda: self.schema_name
        )
        taxonomy_migration.upgrade()

    async def _create_base_schema(self):
        await self.database.execute_ddl(f"CREATE SCHEMA {self.schema}")
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_v3_snapshot_layout (
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
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE UNIQUE INDEX ptg2_v4_test_sealed_mapping_idx
                ON {self.schema}.ptg2_v3_snapshot_layout
                (generation, mapping_digest, support_digest)
             WHERE state = 'sealed'
               AND mapping_digest IS NOT NULL
               AND support_digest IS NOT NULL
            """
        )

    async def _create_build_prerequisites(self):
        await self.database.execute_ddl(v3_provider_set_prerequisite_ddl(self.schema))
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_v3_layout_fingerprint (
                semantic_fingerprint bytea PRIMARY KEY,
                snapshot_key bigint NOT NULL REFERENCES
                    {self.schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
                created_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_layout_build_candidate (
                snapshot_key bigint PRIMARY KEY REFERENCES
                    {self.schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
                semantic_fingerprint bytea NOT NULL,
                cleanup_pending_at timestamptz,
                canonical_snapshot_key bigint,
                created_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_block_build_pin (
                snapshot_key bigint NOT NULL REFERENCES
                    {self.schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
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

    async def _create_source_prerequisites(self):
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_v3_npi_scope (
                snapshot_key bigint NOT NULL,
                npi bigint NOT NULL,
                CONSTRAINT ptg2_v4_test_npi_scope_pkey
                    PRIMARY KEY (snapshot_key, npi)
            ) PARTITION BY HASH (snapshot_key)
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_v3_npi_scope_p0
                PARTITION OF {self.schema}.ptg2_v3_npi_scope
                FOR VALUES WITH (MODULUS 1, REMAINDER 0)
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.npi (
                npi bigint PRIMARY KEY,
                entity_type_code integer
            )
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.npi_taxonomy (
                npi bigint NOT NULL,
                checksum integer NOT NULL,
                healthcare_provider_taxonomy_code varchar,
                PRIMARY KEY (npi, checksum)
            )
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_snapshot (
                snapshot_id varchar(96) PRIMARY KEY
            )
            """
        )

    async def _create_shared_prerequisites(self):
        await self.database.execute_ddl(attempt_guard_prerequisite_ddl(self.schema))
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_v3_snapshot_binding (
                snapshot_id varchar(96) PRIMARY KEY REFERENCES
                    {self.schema}.ptg2_snapshot(snapshot_id) ON DELETE CASCADE,
                snapshot_key bigint NOT NULL REFERENCES
                    {self.schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE RESTRICT,
                created_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_v3_block (
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
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_v3_snapshot_block (
                snapshot_key bigint NOT NULL REFERENCES
                    {self.schema}.ptg2_v3_snapshot_layout(snapshot_key) ON DELETE CASCADE,
                object_kind varchar(64) NOT NULL,
                block_key bigint NOT NULL,
                fragment_no integer NOT NULL,
                entry_count bigint NOT NULL,
                block_hash bytea NOT NULL REFERENCES
                    {self.schema}.ptg2_v3_block(block_hash),
                PRIMARY KEY (snapshot_key, object_kind, block_key, fragment_no)
            )
            """
        )
        await self.database.execute_ddl(
            f"""
            CREATE TABLE {self.schema}.ptg2_v3_gc_candidate (
                block_hash bytea PRIMARY KEY REFERENCES
                    {self.schema}.ptg2_v3_block(block_hash) ON DELETE CASCADE,
                eligible_at timestamptz NOT NULL,
                queued_at timestamptz NOT NULL DEFAULT now()
            )
            """
        )

    async def _finish_schema(self):
        for table_name in PTG2_V3_DENSE_LAYOUT_TABLES:
            if table_name in {
                "ptg2_v3_npi_scope",
                "ptg2_v3_provider_set",
            }:
                continue
            await self.database.execute_ddl(
                f'CREATE TABLE {self.schema}."{table_name}" (snapshot_key bigint)'
            )
        for statement in self.recorder.executed:
            await self.database.execute_ddl(statement)

    async def _create_schema(self):
        await self._create_base_schema()
        await self._create_build_prerequisites()
        await self._create_source_prerequisites()
        await self._create_shared_prerequisites()
        await self._finish_schema()

    async def _reserve_and_store_blocks(self, session):
        reservation = await reserve_v4_shared_layout(
            session,
            schema_name=self.schema_name,
            semantic_fingerprint=self.semantic_fingerprint,
            build_token="build-v4",
        )
        self.snapshot_key = reservation.snapshot_key
        await touch_v4_shared_layout_build(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            build_token="build-v4",
        )
        await session.execute(
            sa.text(
                f"INSERT INTO {self.schema}.ptg2_snapshot (snapshot_id) "
                "VALUES ('snapshot-v4')"
            )
        )
        for graph_block in self.graph_blocks:
            await session.execute(
                sa.text(
                    f"""
                    INSERT INTO {self.schema}.ptg2_v3_block
                        (block_hash, format_version, object_kind, codec,
                         entry_count, raw_byte_count, stored_byte_count, payload)
                    VALUES
                        (:block_hash, 2, :object_kind, 'none', :entry_count,
                         :raw_byte_count, :stored_byte_count, :payload)
                    ON CONFLICT (block_hash) DO NOTHING
                    """
                ),
                {
                    "block_hash": graph_block.block_hash,
                    "object_kind": graph_block.object_kind,
                    "entry_count": graph_block.entry_count,
                    "raw_byte_count": graph_block.raw_byte_count,
                    "stored_byte_count": graph_block.stored_byte_count,
                    "payload": graph_block.payload,
                },
            )

    async def _reject_hostile_publication(self, session):
        # A hostile row can preserve the hash, byte counts, and entry
        # count while substituting a different CAS namespace. Publication
        # must bind the packed coordinate to the full CAS identity.
        with pytest.raises(RuntimeError, match="resolve every target CAS block"):
            async with session.begin_nested():
                await session.execute(
                    sa.text(
                        f"""
                        UPDATE {self.schema}.ptg2_v3_block
                           SET object_kind = 'hostile_same_size_v1'
                         WHERE block_hash = :block_hash
                        """
                    ),
                    {"block_hash": self.references[0].block_hash},
                )
                await publish_v4_snapshot_maps(
                    session,
                    schema_name=self.schema_name,
                    snapshot_key=self.snapshot_key,
                    build_token="build-v4",
                    representation="pattern_v1",
                    references=self.references,
                    max_coordinates_per_pack=2,
                )

    async def _publish_core_metadata(self, session):
        self.summary = await publish_v4_snapshot_maps(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            build_token="build-v4",
            representation="pattern_v1",
            references=self.references,
            max_coordinates_per_pack=2,
        )
        self.npi_publication = await publish_v4_npi_dictionary(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            build_token="build-v4",
            entries=((0, 1234567890), (1, 1234567891)),
            batch_rows=1,
        )
        self.component_publication = await publish_v4_provider_components(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            build_token="build-v4",
            entries=((0, b"c" * 16),),
            batch_rows=1,
        )
        self.pattern_publication = await publish_v4_patterns(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            build_token="build-v4",
            entries=((0, b"p" * 32, 2),),
            batch_rows=1,
        )

    async def _publish_relation_metadata(self, session):
        self.relation_publication = await publish_v4_relation_manifests(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            build_token="build-v4",
            entries=(
                {
                    "relation": "alpha_relation",
                    "member_object_kind": "alpha_members_v1",
                    "locator_object_kind": "alpha_locators_v1",
                    "owner_base": 1,
                    "owner_count": 2,
                    "logical_member_count": 10,
                    "vector_member_count": 5,
                    "member_width": 4,
                    "member_page_bytes": 20,
                    "locator_page_bytes": 24,
                    "locator_owner_span": 2,
                },
                {
                    "relation": "set_npi_prefix_override",
                    "member_object_kind": ("v4_set_npi_prefix_override_members_v1"),
                    "locator_object_kind": (
                        "v4_set_npi_prefix_override_locators_v1"
                    ),
                    "owner_base": 0,
                    "owner_count": 0,
                    "logical_member_count": 0,
                    "vector_member_count": 0,
                    "member_width": 4,
                    "member_page_bytes": 20,
                    "locator_page_bytes": 24,
                    "locator_owner_span": 1,
                },
            ),
        )
        self.heavy_owner_publication = await publish_v4_heavy_owners(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            build_token="build-v4",
            entries=(
                {
                    "relation": "alpha_relation",
                    "owner_key": 1,
                    "object_kind": "alpha_heavy_v1",
                    "member_count": 5,
                    "member_base": 0,
                    "member_span": 10,
                    "fragment_count": 1,
                },
            ),
        )

    async def _publish_diagnostic_and_taxonomy(self, session):
        await _insert_v4_graph_diagnostic_fixture(
            session,
            schema=self.schema,
            snapshot_key=self.snapshot_key,
            graph_blocks=self.graph_blocks,
        )
        self.taxonomy_publication = await publish_v4_inferred_taxonomy_candidates(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            build_token="build-v4",
            rules=INFERRED_PROVIDER_TAXONOMY_RULES,
            npi_count=self.npi_publication.row_count,
            representation="pattern_v1",
            pattern_count=self.pattern_publication.row_count,
        )
        self.persisted_summary = await summarize_persisted_v4_snapshot_maps(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            batch_rows=1,
        )

    async def _reject_changed_entry_count(self, session):
        with pytest.raises(RuntimeError, match="target CAS identity"):
            async with session.begin_nested():
                await session.execute(
                    sa.text(
                        f"""
                        UPDATE {self.schema}.ptg2_v3_block
                           SET entry_count = entry_count + 1
                         WHERE block_hash = :block_hash
                        """
                    ),
                    {"block_hash": self.references[0].block_hash},
                )
                await seal_v4_shared_layout(
                    session,
                    schema_name=self.schema_name,
                    snapshot_key=self.snapshot_key,
                    build_token="build-v4",
                    expected_summary=self.summary,
                    support_digest=b"u" * 32,
                    layout_manifest={},
                    summary_batch_rows=1,
                )

    async def _reject_changed_object_kind(self, session):
        with pytest.raises(RuntimeError, match="target CAS identity"):
            async with session.begin_nested():
                await session.execute(
                    sa.text(
                        f"""
                        UPDATE {self.schema}.ptg2_v3_block
                           SET object_kind = 'hostile_same_size_v1'
                         WHERE block_hash = :block_hash
                        """
                    ),
                    {"block_hash": self.references[0].block_hash},
                )
                await seal_v4_shared_layout(
                    session,
                    schema_name=self.schema_name,
                    snapshot_key=self.snapshot_key,
                    build_token="build-v4",
                    expected_summary=self.summary,
                    support_digest=b"u" * 32,
                    layout_manifest={},
                    summary_batch_rows=1,
                )

    async def _reject_changed_codec(self, session):
        with pytest.raises(RuntimeError, match="target CAS metadata"):
            async with session.begin_nested():
                await session.execute(
                    sa.text(
                        f"""
                        UPDATE {self.schema}.ptg2_v3_block
                           SET codec = 'gzip'
                         WHERE block_hash = :block_hash
                        """
                    ),
                    {"block_hash": self.references[0].block_hash},
                )
                await seal_v4_shared_layout(
                    session,
                    schema_name=self.schema_name,
                    snapshot_key=self.snapshot_key,
                    build_token="build-v4",
                    expected_summary=self.summary,
                    support_digest=b"u" * 32,
                    layout_manifest={},
                    summary_batch_rows=1,
                )

    async def _reject_changed_raw_size(self, session):
        with pytest.raises(RuntimeError, match="target CAS metadata"):
            async with session.begin_nested():
                await session.execute(
                    sa.text(
                        f"""
                        UPDATE {self.schema}.ptg2_v3_block
                           SET raw_byte_count = raw_byte_count + 1
                         WHERE block_hash = :block_hash
                        """
                    ),
                    {"block_hash": self.references[0].block_hash},
                )
                await seal_v4_shared_layout(
                    session,
                    schema_name=self.schema_name,
                    snapshot_key=self.snapshot_key,
                    build_token="build-v4",
                    expected_summary=self.summary,
                    support_digest=b"u" * 32,
                    layout_manifest={},
                    summary_batch_rows=1,
                )

    async def _reject_missing_heavy_owner(self, session):
        with pytest.raises(
            RuntimeError,
            match="logical/vector counts disagree with heavy owners",
        ):
            async with session.begin_nested():
                await session.execute(
                    sa.text(
                        f"""
                        DELETE FROM {self.schema}.ptg2_v4_heavy_owner
                         WHERE snapshot_key = :snapshot_key
                        """
                    ),
                    {"snapshot_key": self.snapshot_key},
                )
                await seal_v4_shared_layout(
                    session,
                    schema_name=self.schema_name,
                    snapshot_key=self.snapshot_key,
                    build_token="build-v4",
                    expected_summary=self.summary,
                    support_digest=b"u" * 32,
                    layout_manifest={},
                    summary_batch_rows=1,
                )

    async def _seal_and_bind(self, session):
        self.sealed = await seal_v4_shared_layout(
            session,
            schema_name=self.schema_name,
            snapshot_key=self.snapshot_key,
            build_token="build-v4",
            expected_summary=self.summary,
            support_digest=b"u" * 32,
            layout_manifest={
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
                    "provider_graph": {
                        "adaptive_layout": (synthetic_adaptive_layout_decision())
                    },
                }
            },
            summary_batch_rows=1,
        )
        await bind_snapshot_to_v4_layout(
            session,
            schema_name=self.schema_name,
            snapshot_id="snapshot-v4",
            snapshot_key=self.sealed.snapshot_key,
        )

    async def _publish_initial_snapshot(self):
        self.graph_blocks = _sealed_graph_blocks()
        self.references = tuple(block.reference() for block in self.graph_blocks)
        self.semantic_fingerprint = b"s" * 32
        async with self.database.transaction() as session:
            await self._reserve_and_store_blocks(session)
            await self._reject_hostile_publication(session)
            await self._publish_core_metadata(session)
            await self._publish_relation_metadata(session)
            await self._publish_diagnostic_and_taxonomy(session)
            await self._reject_changed_entry_count(session)
            await self._reject_changed_object_kind(session)
            await self._reject_changed_codec(session)
            await self._reject_changed_raw_size(session)
            await self._reject_missing_heavy_owner(session)
            await self._seal_and_bind(session)
        assert self.persisted_summary == self.summary
        assert self.sealed.snapshot_key == self.snapshot_key
        assert not self.sealed.reused

    async def _assert_sealed_delete_guards(self):
        async with self.database.transaction() as session:
            guarded_deletes = (
                (
                    "ptg2_v4_snapshot_map_pack",
                    "ptg2_v4_snapshot_map_pack_sealed_delete",
                ),
                (
                    "ptg2_v4_relation_manifest",
                    "ptg2_v4_snapshot_metadata_sealed_delete",
                ),
                (
                    "ptg2_v4_heavy_owner",
                    "ptg2_v4_snapshot_metadata_sealed_delete",
                ),
                (
                    "ptg2_v4_snapshot_map_root",
                    "ptg2_v4_snapshot_map_root_sealed_delete",
                ),
            )
            for table_name, error_match in guarded_deletes:
                with pytest.raises(sa.exc.DBAPIError, match=error_match):
                    async with session.begin_nested():
                        await session.execute(
                            sa.text(
                                f'DELETE FROM {self.schema}."{table_name}" '
                                "WHERE snapshot_key = :snapshot_key"
                            ),
                            {"snapshot_key": self.snapshot_key},
                        )

    async def _assert_layout_reuse(self):
        async with self.database.transaction() as session:
            reused = await reserve_v4_shared_layout(
                session,
                schema_name=self.schema_name,
                semantic_fingerprint=self.semantic_fingerprint,
                build_token="unused-build-token",
            )
        assert reused.reused
        assert reused.snapshot_key == self.snapshot_key

    async def _insert_overlap_fixture(self, session):
        self.overlap_snapshot_key = (
            await session.execute(
                sa.text(
                    f"""
                    INSERT INTO {self.schema}.ptg2_v3_snapshot_layout
                        (build_token, generation, state)
                    VALUES ('overlap-v4', :generation, 'building')
                    RETURNING snapshot_key
                    """
                ),
                {"generation": PTG2_V4_SHARED_GENERATION},
            )
        ).scalar_one()
        await session.execute(
            sa.text(
                f"""
                INSERT INTO {self.schema}.ptg2_v4_snapshot_map_root
                    (snapshot_key, state, format_version, map_format,
                     representation, projection_id_scope)
                VALUES
                    (:snapshot_key, 'building', 1, :map_format,
                     'pattern_v1', :projection_id_scope)
                """
            ),
            {
                "snapshot_key": self.overlap_snapshot_key,
                "map_format": PTG2_V4_MAP_FORMAT,
                "projection_id_scope": PTG2_V4_PROJECTION_ID_SCOPE,
            },
        )

    async def _assert_overlap_rejected(self, session):
        first_target_hash = self.references[0].block_hash
        await session.execute(
            sa.text(
                f"""
                INSERT INTO {self.schema}.ptg2_v4_snapshot_map_pack
                    (snapshot_key, object_kind, pack_no,
                     first_block_key, first_fragment_no,
                     last_block_key, last_fragment_no,
                     coordinate_count, entry_count, logical_byte_count,
                     map_block_hash)
                VALUES
                    (:snapshot_key, 'overlap_v1', 0, 1, 0, 2, 0,
                     1, 1, 1, :map_block_hash)
                """
            ),
            {
                "snapshot_key": self.overlap_snapshot_key,
                "map_block_hash": first_target_hash,
            },
        )
        with pytest.raises(sa.exc.DBAPIError, match="map_pack_overlap"):
            async with session.begin_nested():
                await session.execute(
                    sa.text(
                        f"""
                        INSERT INTO {self.schema}.ptg2_v4_snapshot_map_pack
                            (snapshot_key, object_kind, pack_no,
                             first_block_key, first_fragment_no,
                             last_block_key, last_fragment_no,
                             coordinate_count, entry_count, logical_byte_count,
                             map_block_hash)
                        VALUES
                            (:snapshot_key, 'overlap_v1', 1, 1, 1, 3, 0,
                             1, 1, 1, :map_block_hash)
                        """
                    ),
                    {
                        "snapshot_key": self.overlap_snapshot_key,
                        "map_block_hash": first_target_hash,
                    },
                )

    async def _exercise_overlap_guard(self):
        async with self.database.transaction() as session:
            await self._insert_overlap_fixture(session)
            await self._assert_overlap_rejected(session)
        async with self.database.transaction() as session:
            await session.execute(
                sa.text(
                    f"DELETE FROM {self.schema}.ptg2_v3_snapshot_layout "
                    "WHERE snapshot_key = :snapshot_key"
                ),
                {"snapshot_key": self.overlap_snapshot_key},
            )
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v4_snapshot_map_root "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=self.overlap_snapshot_key,
            )
            == 0
        )

    async def _assert_publication_counts(self):
        assert self.npi_publication.row_count == 2
        assert self.component_publication.row_count == 1
        assert self.pattern_publication.row_count == 1
        assert self.relation_publication.row_count == 2
        assert self.heavy_owner_publication.row_count == 1
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v4_npi_scope "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=self.snapshot_key,
            )
            == 2
        )
        assert self.summary.coordinate_count == 3
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v4_snapshot_map_pack "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=self.snapshot_key,
            )
            == 3
        )
        assert (
            await self.database.scalar(
                f"SELECT state FROM {self.schema}.ptg2_v4_snapshot_map_root "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=self.snapshot_key,
            )
            == "complete"
        )
        manifest = await self.database.scalar(
            f"SELECT layout_manifest FROM {self.schema}.ptg2_v3_snapshot_layout "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=self.snapshot_key,
        )
        self.serving_index = manifest["serving_index"]

    async def _assert_serving_manifest(self):
        assert self.serving_index["arch_version"] == "postgres_binary_v3"
        assert self.serving_index["storage_generation"] == PTG2_V4_SHARED_GENERATION
        assert self.serving_index["type"] == "ptg2_shared_blocks_v4"
        assert self.serving_index["provider_scope_strategy"] == "postgres_packed_graph_v4"
        assert self.serving_index["shared_block_layout"] == "packed_snapshot_maps_v4"
        assert self.serving_index["serving_binary"]["price_dictionary"] == {
            "preserved": True
        }
        provider_graph_by_field = self.serving_index["serving_binary"]["provider_graph_v4"]
        assert provider_graph_by_field["map_digest"] == self.summary.map_digest.hex()
        assert provider_graph_by_field["resource_admission"] == {
            "compressed_acquisition_bytes": 70,
            "input_factor_bytes": 70,
            "factor_edge_count": 10,
            "empty_npi_tin_only_normalization_count": 0,
        }
        assert self.taxonomy_publication.rule_count == len(INFERRED_PROVIDER_TAXONOMY_RULES)
        assert self.taxonomy_publication.member_count == 0
        assert self.taxonomy_publication.observe_only_rule_count == 0
        assert provider_graph_by_field["inferred_taxonomy_candidates"] == dict(
            self.taxonomy_publication.manifest
        )
        assert provider_graph_by_field["hot_prefix"]["override_owner_count"] == 0
        snapshot_map = self.serving_index["snapshot_map"]
        assert snapshot_map["component_count"] == 1
        assert snapshot_map["npi_count"] == 2
        assert snapshot_map["pattern_count"] == 1
        assert snapshot_map["relation_count"] == 2
        assert snapshot_map["heavy_owner_count"] == 1
        assert snapshot_map["relation_manifest_table"] == ("ptg2_v4_relation_manifest")
        assert snapshot_map["heavy_owner_table"] == "ptg2_v4_heavy_owner"
        assert snapshot_map["npi_table"] == "ptg2_v4_npi_scope"
        assert (
            await self.database.scalar(
                f"SELECT snapshot_key FROM {self.schema}.ptg2_v3_snapshot_binding "
                "WHERE snapshot_id = 'snapshot-v4'"
            )
            == self.snapshot_key
        )

    async def _assert_gc_protection(self):
        map_block_hash = await self.database.scalar(
            f"SELECT map_block_hash FROM {self.schema}.ptg2_v4_snapshot_map_pack "
            "WHERE snapshot_key = :snapshot_key ORDER BY object_kind, pack_no LIMIT 1",
            snapshot_key=self.snapshot_key,
        )
        async with self.database.acquire() as connection:
            await connection.status(
                f"""
                INSERT INTO {self.schema}.ptg2_v3_gc_candidate
                    (block_hash, eligible_at, queued_at)
                VALUES
                    (:target_hash, transaction_timestamp() - INTERVAL '1 second',
                     transaction_timestamp()),
                    (:map_block_hash, transaction_timestamp() - INTERVAL '1 second',
                     transaction_timestamp())
                ON CONFLICT (block_hash) DO UPDATE
                    SET eligible_at = EXCLUDED.eligible_at
                """,
                target_hash=self.references[0].block_hash,
                map_block_hash=map_block_hash,
            )
            protected = await ptg2_shared_gc._sweep_ready(
                connection,
                schema_name=self.schema_name,
                max_bytes=10_000_000,
                max_rows=10,
            )
        assert protected.selected_hashes == ()
        assert (
            await self.database.scalar(f"SELECT COUNT(*) FROM {self.schema}.ptg2_v3_gc_candidate")
            == 0
        )
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v3_block "
                "WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))",
                block_hashes=[self.references[0].block_hash, map_block_hash],
            )
            == 2
        )

    async def _create_gc_race_fixture(self):
        # A publisher that already holds target KEY SHARE must make the
        # candidate/block FOR UPDATE SKIP LOCKED sweep skip that target.  The
        # same packed map must then keep both its target and map block until
        # the unbound expired layout is released and queues both hashes.
        self.race_block = SharedBlock(
            "gc_race_v1",
            0,
            0,
            1,
            "none",
            4,
            b"race",
        )
        async with self.database.transaction() as session:
            self.race_reservation = await reserve_v4_shared_layout(
                session,
                schema_name=self.schema_name,
                semantic_fingerprint=b"r" * 32,
                build_token="gc-race-v4",
            )
            await session.execute(
                sa.text(
                    f"""
                    INSERT INTO {self.schema}.ptg2_v3_block
                        (block_hash, format_version, object_kind, codec,
                         entry_count, raw_byte_count, stored_byte_count, payload)
                    VALUES
                        (:block_hash, 2, :object_kind, 'none', :entry_count,
                         :raw_byte_count, :stored_byte_count, :payload)
                    """
                ),
                {
                    "block_hash": self.race_block.block_hash,
                    "object_kind": self.race_block.object_kind,
                    "entry_count": self.race_block.entry_count,
                    "raw_byte_count": self.race_block.raw_byte_count,
                    "stored_byte_count": self.race_block.stored_byte_count,
                    "payload": self.race_block.payload,
                },
            )
            await session.execute(
                sa.text(
                    f"""
                    INSERT INTO {self.schema}.ptg2_v3_gc_candidate
                        (block_hash, eligible_at, queued_at)
                    VALUES
                        (:block_hash,
                         transaction_timestamp() - INTERVAL '1 second',
                         transaction_timestamp())
                    """
                ),
                {"block_hash": self.race_block.block_hash},
            )

    async def _exercise_gc_race_lock(self):
        self.target_locked = asyncio.Event()
        self.continue_publication = asyncio.Event()
        self.verify_target_blocks = ptg2_v4_snapshot_maps._verify_target_blocks

        async def _pause_with_target_lock(*args, **kwargs):
            await self.verify_target_blocks(*args, **kwargs)
            self.target_locked.set()
            await self.continue_publication.wait()

        self.monkeypatch.setattr(
            ptg2_v4_snapshot_maps,
            "_verify_target_blocks",
            _pause_with_target_lock,
        )

        async def _publish_race_map():
            async with self.database.transaction() as session:
                return await publish_v4_snapshot_maps(
                    session,
                    schema_name=self.schema_name,
                    snapshot_key=self.race_reservation.snapshot_key,
                    build_token="gc-race-v4",
                    representation="direct_v1",
                    references=(self.race_block.reference(),),
                    max_coordinates_per_pack=1,
                )

        self.publication_task = asyncio.create_task(_publish_race_map())
        await asyncio.wait_for(self.target_locked.wait(), timeout=2)
        async with self.database.acquire() as connection:
            skipped = await ptg2_shared_gc._sweep_ready(
                connection,
                schema_name=self.schema_name,
                max_bytes=10_000_000,
                max_rows=10,
            )
        assert skipped.selected_hashes == ()
        self.continue_publication.set()
        self.race_summary = await asyncio.wait_for(self.publication_task, timeout=2)
        self.monkeypatch.setattr(
            ptg2_v4_snapshot_maps,
            "_verify_target_blocks",
            self.verify_target_blocks,
        )

    async def _assert_gc_race_publication(self):
        assert self.race_summary.coordinate_count == 1
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v3_gc_candidate "
                "WHERE block_hash = :block_hash",
                block_hash=self.race_block.block_hash,
            )
            == 0
        )
        self.race_map_hash = await self.database.scalar(
            f"SELECT map_block_hash FROM {self.schema}.ptg2_v4_snapshot_map_pack "
            "WHERE snapshot_key = :snapshot_key",
            snapshot_key=self.race_reservation.snapshot_key,
        )

    async def _release_gc_race_layout(self):
        async with self.database.acquire() as connection:
            await connection.status(
                f"""
                UPDATE {self.schema}.ptg2_v3_snapshot_layout
                   SET heartbeat_at =
                           transaction_timestamp() - INTERVAL '7 hours',
                       lease_until =
                           transaction_timestamp() - INTERVAL '1 second'
                 WHERE snapshot_key = :snapshot_key
                """,
                snapshot_key=self.race_reservation.snapshot_key,
            )
            released_race = await ptg2_shared_gc._release_layouts_ready(
                connection,
                schema_name=self.schema_name,
                building_max_age_seconds=21_600,
                grace_seconds=0,
                max_layouts=10,
                layout_keys=(self.race_reservation.snapshot_key,),
            )
        assert released_race.logical_layout_count == 1
        assert released_race.candidate_hash_count == 2
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v4_snapshot_map_root "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=self.race_reservation.snapshot_key,
            )
            == 0
        )
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v4_snapshot_map_pack "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=self.race_reservation.snapshot_key,
            )
            == 0
        )

    async def _reclaim_gc_race_blocks(self):
        async with self.database.acquire() as connection:
            reclaimed_race = await ptg2_shared_gc._sweep_ready(
                connection,
                schema_name=self.schema_name,
                max_bytes=10_000_000,
                max_rows=10,
            )
        assert set(reclaimed_race.selected_hashes) == {
            self.race_block.block_hash,
            self.race_map_hash,
        }
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v3_block "
                "WHERE block_hash = ANY(CAST(:block_hashes AS bytea[]))",
                block_hashes=[self.race_block.block_hash, self.race_map_hash],
            )
            == 0
        )

    async def _exercise_gc_race(self):
        await self._create_gc_race_fixture()
        await self._exercise_gc_race_lock()
        await self._assert_gc_race_publication()
        await self._release_gc_race_layout()
        await self._reclaim_gc_race_blocks()

    async def _create_cas_stage(self):
        async with self.database.transaction() as session:
            self.cas_snapshot_key = (
                await session.execute(
                    sa.text(
                        f"""
                        INSERT INTO {self.schema}.ptg2_v3_snapshot_layout
                            (build_token, generation, state)
                        VALUES ('cas-v4', :generation, 'building')
                        RETURNING snapshot_key
                        """
                    ),
                    {"generation": PTG2_V4_SHARED_GENERATION},
                )
            ).scalar_one()
        self.cas_stage = "ptg2_v3_block_stage_v4casproof"
        await self.database.execute_ddl(
            f"""
            CREATE UNLOGGED TABLE {self.schema}."{self.cas_stage}" (
                block_hash bytea NOT NULL,
                format_version smallint NOT NULL,
                object_kind varchar(64) NOT NULL,
                block_key bigint NOT NULL,
                fragment_no integer NOT NULL,
                entry_count bigint NOT NULL,
                codec varchar(16) NOT NULL,
                raw_byte_count bigint NOT NULL,
                stored_byte_count bigint NOT NULL,
                payload bytea
            )
            """
        )

    async def _publish_cas_stage(self):
        self.cas_block = SharedBlock("cas_v1", 0, 0, 3, "none", 3, b"cas")
        async with self.database.acquire() as connection:
            await connection.status(
                f"""
                INSERT INTO {self.schema}."{self.cas_stage}"
                    (block_hash, format_version, object_kind, block_key,
                     fragment_no, entry_count, codec, raw_byte_count,
                     stored_byte_count, payload)
                VALUES
                    (:block_hash, 2, 'cas_v1', 0, 0, 3, 'none', 3, 3, :payload)
                """,
                block_hash=self.cas_block.block_hash,
                payload=self.cas_block.payload,
            )
        self.monkeypatch.setattr(ptg2_block_build_pins, "db", self.database)
        self.monkeypatch.setattr(ptg2_shared_publish, "db", self.database)
        cas_publication = await _publish_v4_cas_block_stage_compatibility(
            schema_name=self.schema_name,
            stage_table=self.cas_stage,
            snapshot_key=self.cas_snapshot_key,
            build_token="cas-v4",
        )
        assert cas_publication.object_kinds == ("cas_v1",)
        assert cas_publication.staged_row_count == 1
        assert cas_publication.unique_block_count == 1
        assert cas_publication.logical_byte_count == 3
        assert cas_publication.unique_stored_byte_count == 3
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v3_snapshot_block "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=self.cas_snapshot_key,
            )
            == 0
        )

    async def _create_reused_cas_stage(self):
        # Selective publication represents an already durable CAS payload as
        # NULL in the stage.  A second V4 layout must reconcile that metadata
        # without republishing the bytes or creating V3 snapshot mappings.
        async with self.database.transaction() as session:
            self.reused_cas_snapshot_key = (
                await session.execute(
                    sa.text(
                        f"""
                        INSERT INTO {self.schema}.ptg2_v3_snapshot_layout
                            (build_token, generation, state)
                        VALUES ('cas-v4-reused', :generation, 'building')
                        RETURNING snapshot_key
                        """
                    ),
                    {"generation": PTG2_V4_SHARED_GENERATION},
                )
            ).scalar_one()
        self.reused_cas_stage = "ptg2_v3_block_stage_v4casreuse"
        await self.database.execute_ddl(
            f"""
            CREATE UNLOGGED TABLE {self.schema}."{self.reused_cas_stage}" (
                block_hash bytea NOT NULL,
                format_version smallint NOT NULL,
                object_kind varchar(64) NOT NULL,
                block_key bigint NOT NULL,
                fragment_no integer NOT NULL,
                entry_count bigint NOT NULL,
                codec varchar(16) NOT NULL,
                raw_byte_count bigint NOT NULL,
                stored_byte_count bigint NOT NULL,
                payload bytea
            )
            """
        )

    async def _publish_reused_cas_stage(self):
        async with self.database.acquire() as connection:
            await connection.status(
                f"""
                INSERT INTO {self.schema}."{self.reused_cas_stage}"
                    (block_hash, format_version, object_kind, block_key,
                     fragment_no, entry_count, codec, raw_byte_count,
                     stored_byte_count, payload)
                VALUES
                    (:block_hash, 2, 'cas_v1', 0, 0, 3, 'none', 3, 3, NULL)
                """,
                block_hash=self.cas_block.block_hash,
            )
        reused_cas_publication = await _publish_v4_cas_block_stage_compatibility(
            schema_name=self.schema_name,
            stage_table=self.reused_cas_stage,
            snapshot_key=self.reused_cas_snapshot_key,
            build_token="cas-v4-reused",
        )
        assert reused_cas_publication.staged_row_count == 1
        assert reused_cas_publication.unique_block_count == 1
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v3_block "
                "WHERE block_hash = :block_hash",
                block_hash=self.cas_block.block_hash,
            )
            == 1
        )

    async def _exercise_cas_compatibility(self):
        await self._create_cas_stage()
        await self._publish_cas_stage()
        await self._create_reused_cas_stage()
        await self._publish_reused_cas_stage()

    async def _delete_published_layout(self):
        async with self.database.transaction() as session:
            await session.execute(
                sa.text(
                    f"DELETE FROM {self.schema}.ptg2_v3_snapshot_binding "
                    "WHERE snapshot_key = :snapshot_key"
                ),
                {"snapshot_key": self.snapshot_key},
            )
            await session.execute(
                sa.text(
                    f"DELETE FROM {self.schema}.ptg2_v3_snapshot_layout "
                    "WHERE snapshot_key = :snapshot_key"
                ),
                {"snapshot_key": self.snapshot_key},
            )
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v4_snapshot_map_root "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=self.snapshot_key,
            )
            == 0
        )
        assert (
            await self.database.scalar(
                f"SELECT COUNT(*) FROM {self.schema}.ptg2_v4_snapshot_map_pack "
                "WHERE snapshot_key = :snapshot_key",
                snapshot_key=self.snapshot_key,
            )
            == 0
        )

    async def run(self):
        await self._create_schema()
        await self._publish_initial_snapshot()
        await self._assert_sealed_delete_guards()
        await self._assert_layout_reuse()
        await self._exercise_overlap_guard()
        await self._assert_publication_counts()
        await self._assert_serving_manifest()
        await self._assert_gc_protection()
        await self._exercise_gc_race()
        await self._exercise_cas_compatibility()
        await self._delete_published_layout()

    async def cleanup(self):
        try:
            await self.database.execute_ddl(f"DROP SCHEMA IF EXISTS {self.schema} CASCADE")
        finally:
            await self.database.disconnect()


@pytest.mark.asyncio
async def test_real_postgres_v4_root_pack_overlap_and_publication(monkeypatch):
    """Exercise the migration and publisher on explicitly enabled local PostgreSQL."""

    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST=1 for local PostgreSQL")

    scenario = _V4PostgresScenario(monkeypatch)
    await scenario.prepare()
    try:
        await scenario.run()
    finally:
        await scenario.cleanup()
