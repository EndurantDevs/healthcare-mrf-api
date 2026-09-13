# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof for local PTG archive-layout preparation."""

from __future__ import annotations

import importlib.util
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from db.connection import Database
from db.migration_ptg2_frozen_source_file_binding import install_frozen_source_file_binding
from db.models._legacy import Base
from process.ptg_parts import result_archive_adoption as adoption
from tests import test_ptg2_v4_postgres_e2e as v4_e2e

_FIXTURE_REKEYED_TABLES = frozenset(
    {
        "ptg2_v3_provider_group",
        "ptg2_v3_provider_set",
        "ptg2_v4_snapshot_map_pack",
        "ptg2_v4_npi_scope",
        "ptg2_v4_provider_component",
        "ptg2_v4_pattern",
        "ptg2_v4_relation_manifest",
        "ptg2_v4_heavy_owner",
        "ptg2_v4_provider_set_npi_prefix",
        "ptg2_v4_provider_graph_diagnostic",
        "ptg2_v4_inferred_taxonomy_candidate",
        "ptg2_provider_tax_identity_manifest",
        "ptg2_provider_tax_identity",
        "ptg2_provider_group_tax_identity",
    }
)
_MODEL_CLOSURE_TABLES = (
    "ptg2_v3_code",
    "ptg2_v3_source_audit_witness",
    "ptg2_v3_source_audit_witness_part",
    "ptg2_allowed_amount_plan",
    "ptg2_allowed_amount_item",
    "ptg2_allowed_amount_payment",
    "ptg2_allowed_amount_provider_payment",
)
_FINALIZER_MIGRATION_PATH = (
    Path(__file__).resolve().parents[1] / "alembic" / "versions" / "20260825120000_ptg_v4_finalizer_map_pack.py"
)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _require_native_postgres() -> None:
    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set guarded PTG V4 PostgreSQL test variables for native proof")


async def _create_destination_snapshot_table(database: Database, schema: str) -> None:
    await database.execute_ddl(
        f"CREATE TABLE {schema}.ptg2_snapshot (snapshot_id text PRIMARY KEY, import_run_id varchar(96))"
    )
    await database.execute_ddl(
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
            snapshot_id text PRIMARY KEY,
            snapshot_key bigint NOT NULL,
            created_at timestamptz NOT NULL DEFAULT transaction_timestamp()
        )
        """
    )
    await database.execute_ddl(
        f"""
        CREATE OR REPLACE FUNCTION {schema}.guard_ptg2_v4_attempt(
            requested_snapshot_id text,
            requested_internal_run_id text,
            allow_reconciled boolean DEFAULT false
        ) RETURNS void LANGUAGE plpgsql AS $$ BEGIN END $$
        """
    )


async def _install_model_closure_tables(database: Database, *, schema_name: str) -> None:
    """Install current model columns for logical evidence outside layout rekeying."""

    metadata = MetaData(schema=schema_name)
    for table_name in _MODEL_CLOSURE_TABLES:
        source_table = Base.metadata.tables[f"mrf.{table_name}"]
        target_table = source_table.to_metadata(metadata, schema=schema_name)
        statement = str(
            CreateTable(target_table, include_foreign_key_constraints=[]).compile(dialect=postgresql.dialect())
        )
        await database.execute_ddl(statement)


async def _install_finalizer_map_tables(database: Database, *, schema_name: str, monkeypatch) -> None:
    """Apply the current packed-finalizer DDL to a disposable native fixture."""

    module_spec = importlib.util.spec_from_file_location("receiver_finalizer_migration", _FINALIZER_MIGRATION_PATH)
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    recorder = v4_e2e._OpRecorder()
    monkeypatch.setattr(migration, "op", recorder)
    monkeypatch.setattr(migration, "_schema", lambda: schema_name)
    migration.upgrade()
    for statement in recorder.executed:
        await database.execute_ddl(statement)


async def _install_full_closure_schema(database: Database, *, schema_name: str, monkeypatch) -> None:
    """Add all physical table families present in the current archive closure."""

    await _install_model_closure_tables(database, schema_name=schema_name)
    recorder = v4_e2e._OpRecorder()
    install_frozen_source_file_binding(recorder, schema_name)
    for statement in recorder.executed:
        await database.execute_ddl(statement)
    await v4_e2e._install_v4_source_evidence_schema(
        database,
        schema_name=schema_name,
        monkeypatch=monkeypatch,
    )
    await _install_finalizer_map_tables(database, schema_name=schema_name, monkeypatch=monkeypatch)


async def _seed_logical_closure_dependencies(
    database: Database,
    *,
    schema_name: str,
    source_snapshot_key: int,
) -> None:
    """Seed selected and unrelated current closure rows with synthetic identities."""

    schema = _quoted(schema_name)
    await database.status(
        f"INSERT INTO {schema}.ptg2_snapshot (snapshot_id, import_run_id) VALUES ('staged-snapshot', 'ptg2:source-file')"
    )
    await _seed_frozen_binding(database, schema=schema)
    await _seed_staged_code(
        database,
        schema=schema,
        source_snapshot_key=source_snapshot_key,
    )
    await _seed_allowed_amount_rows(database, schema=schema)


async def _seed_frozen_binding(database: Database, *, schema: str) -> None:
    """Seed the immutable local frozen-binding relation with synthetic evidence."""

    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_frozen_source_file_binding
            (source_file_import_id, internal_run_id, binding_contract,
             frozen_rate_file_set_contract, frozen_rate_file_set_sha256,
             frozen_rate_file_count, source_key, import_month, plan_ids,
             plan_market_types, binding_sha256, binding_payload)
        VALUES
            ('source-file', 'ptg2:source-file', 'ptg_frozen_source_file_binding_v1',
             'ptg_frozen_rate_file_set_v1', :file_digest, 2, 'source-key',
             DATE '2026-09-01', '["plan-a"]'::jsonb, '["market-a"]'::jsonb,
             :binding_digest,
             jsonb_build_object(
                 'contract', 'ptg_frozen_source_file_binding_v1',
                 'source_file_import_id', 'source-file',
                 'frozen_rate_file_set_contract', 'ptg_frozen_rate_file_set_v1',
                 'frozen_rate_file_set_sha256', CAST(:file_digest_payload AS text),
                 'frozen_rate_file_count', 2, 'source_key', 'source-key',
                 'import_month', '2026-09-01', 'plan_ids', jsonb_build_array('plan-a'),
                 'plan_market_types', jsonb_build_array('market-a')
             ))
        """,
        file_digest="a" * 64,
        file_digest_payload="a" * 64,
        binding_digest="b" * 64,
    )


async def _seed_staged_code(
    database: Database,
    *,
    schema: str,
    source_snapshot_key: int,
) -> None:
    """Seed one selected code row and one unrelated snapshot-key row."""

    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_v3_code
            (snapshot_key, code_key, code_global_id_128, coverage_scope_id, rate_count)
        VALUES (:snapshot_key, 7, :code_id, :coverage_id, 0),
               (:unrelated_snapshot_key, 8, :unrelated_code_id, :coverage_id, 0)
        """,
        snapshot_key=source_snapshot_key,
        unrelated_snapshot_key=source_snapshot_key + 1000,
        code_id=b"c" * 16,
        unrelated_code_id=b"d" * 16,
        coverage_id=b"v" * 32,
    )


async def _seed_allowed_amount_rows(database: Database, *, schema: str) -> None:
    """Seed the four selected and unrelated allowed-amount evidence tables."""

    for snapshot_id, offset in (("staged-snapshot", 0), ("unrelated-snapshot", 10)):
        await database.status(
            f"""
            INSERT INTO {schema}.ptg2_allowed_amount_plan
                (snapshot_id, plan_hash, file_id, plan_id)
            VALUES (:snapshot_id, :plan_hash, 1, 'plan-a')
            """,
            snapshot_id=snapshot_id,
            plan_hash=1 + offset,
        )
        await database.status(
            f"""
            INSERT INTO {schema}.ptg2_allowed_amount_item
                (snapshot_id, allowed_item_hash, file_id)
            VALUES (:snapshot_id, :item_hash, 1)
            """,
            snapshot_id=snapshot_id,
            item_hash=2 + offset,
        )
        await database.status(
            f"""
            INSERT INTO {schema}.ptg2_allowed_amount_payment
                (snapshot_id, payment_hash, allowed_item_hash)
            VALUES (:snapshot_id, :payment_hash, :item_hash)
            """,
            snapshot_id=snapshot_id,
            payment_hash=3 + offset,
            item_hash=2 + offset,
        )
        await database.status(
            f"""
            INSERT INTO {schema}.ptg2_allowed_amount_provider_payment
                (snapshot_id, provider_payment_hash, payment_hash, npi)
            VALUES (:snapshot_id, :provider_payment_hash, :payment_hash, ARRAY[1234567890]::bigint[])
            """,
            snapshot_id=snapshot_id,
            provider_payment_hash=4 + offset,
            payment_hash=3 + offset,
        )


async def _seed_preparation_catalog(
    database: Database,
    *,
    schema_name: str,
    tmp_path: Path,
    monkeypatch,
) -> int:
    compilation, _relation_names = await v4_e2e._compile_direct_v4_fixture(tmp_path)
    try:
        _publication, sealed = await v4_e2e._publish_direct_v4_fixture(
            database,
            schema_name=schema_name,
            compilation=compilation,
            monkeypatch=monkeypatch,
        )
    finally:
        compilation.cleanup()
    schema = _quoted(schema_name)
    await _create_destination_snapshot_table(database, schema)
    await database.status(
        f"INSERT INTO {schema}.ptg2_v3_snapshot_binding (snapshot_id, snapshot_key) VALUES ('staged-snapshot', :snapshot_key)",
        snapshot_key=sealed.snapshot_key,
    )
    return int(sealed.snapshot_key)


async def _seed_full_preparation_catalog(
    database: Database,
    *,
    schema_name: str,
    tmp_path: Path,
    monkeypatch,
) -> int:
    """Create a sealed layout plus the current non-layout closure families."""

    source_snapshot_key = await _seed_preparation_catalog(
        database,
        schema_name=schema_name,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )
    await _install_full_closure_schema(database, schema_name=schema_name, monkeypatch=monkeypatch)
    await _seed_logical_closure_dependencies(
        database,
        schema_name=schema_name,
        source_snapshot_key=source_snapshot_key,
    )
    return source_snapshot_key


async def _create_destination_catalog(database: Database, *, schema_name: str, snapshot_id: str, monkeypatch) -> None:
    await v4_e2e._create_v4_test_schema(database, schema_name=schema_name, monkeypatch=monkeypatch)
    schema = _quoted(schema_name)
    await _create_destination_snapshot_table(database, schema)
    await database.status(
        f"INSERT INTO {schema}.ptg2_snapshot (snapshot_id) VALUES (:snapshot_id)",
        snapshot_id=snapshot_id,
    )


async def _create_full_destination_catalog(
    database: Database,
    *,
    schema_name: str,
    snapshot_id: str,
    monkeypatch,
) -> None:
    """Create a destination fixture with every table family in the closure."""

    await _create_destination_catalog(
        database,
        schema_name=schema_name,
        snapshot_id=snapshot_id,
        monkeypatch=monkeypatch,
    )
    await _install_full_closure_schema(database, schema_name=schema_name, monkeypatch=monkeypatch)


async def _assert_prepared_destination(
    database: Database,
    *,
    destination_schema: str,
    staging_schema: str,
    source_snapshot_key: int,
    prepared,
) -> None:
    assert prepared.destination_snapshot_key != source_snapshot_key
    assert prepared.requires_fresh_destination_attestation is True
    assert (
        await database.scalar(
            f"SELECT snapshot_key FROM {destination_schema}.ptg2_v3_snapshot_binding WHERE snapshot_id = 'destination-snapshot'"
        )
        == prepared.destination_snapshot_key
    )
    assert (
        await database.scalar(
            f"SELECT COUNT(*) FROM {destination_schema}.ptg2_v3_block WHERE block_hash = :hash",
            hash=b"u" * 32,
        )
        == 1
    )
    assert (
        await database.scalar(
            f"SELECT COUNT(*) FROM {destination_schema}.ptg2_v3_block WHERE block_hash IN (SELECT block_hash FROM {staging_schema}.ptg2_v3_block)"
        )
        > 0
    )


async def _assert_full_family_boundary(
    database: Database,
    *,
    stage: str,
    destination: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
    destination_schema_name: str,
) -> None:
    """Verify physical rekeying while retaining logical evidence for its owner."""

    assert (
        await database.scalar(
            f"SELECT COUNT(*) FROM {stage}.ptg2_v3_provider_set WHERE snapshot_key = :snapshot_key",
            snapshot_key=source_snapshot_key,
        )
        > 0
    )
    assert (
        await database.scalar(
            f"SELECT COUNT(*) FROM {stage}.ptg2_frozen_source_file_binding WHERE internal_run_id = 'ptg2:source-file'"
        )
        == 1
    )
    for table_name in _MODEL_CLOSURE_TABLES[3:]:
        assert (
            await database.scalar(f"SELECT COUNT(*) FROM {stage}.{table_name} WHERE snapshot_id = 'staged-snapshot'")
            == 1
        )
        assert (
            await database.scalar(f"SELECT COUNT(*) FROM {stage}.{table_name} WHERE snapshot_id = 'unrelated-snapshot'")
            == 1
        )
        assert await database.scalar(f"SELECT COUNT(*) FROM {destination}.{table_name}") == 0
    assert (
        await database.scalar(
            f"SELECT COUNT(*) FROM {destination}.ptg2_v3_code WHERE snapshot_key = :snapshot_key",
            snapshot_key=destination_snapshot_key,
        )
        == 1
    )
    assert await database.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_v3_code WHERE code_key = 8") == 0
    assert await database.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_frozen_source_file_binding") == 0
    for table_name in adoption._REKEYED_TABLES:
        assert (
            await database.scalar(
                """
            SELECT COUNT(*)
              FROM information_schema.columns
             WHERE table_schema = :schema_name AND table_name = :table_name
            """,
                schema_name=destination_schema_name,
                table_name=table_name,
            )
            > 0
        )


async def _prepare_layout(
    database: Database,
    *,
    destination_schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_id: str,
):
    """Run local layout preparation inside one caller-owned transaction."""

    async with database.transaction() as session:
        return await adoption.prepare_result_archive_layout(
            session,
            schema_name=destination_schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id=destination_snapshot_id,
            build_token=f"receiver-{uuid.uuid4().hex}",
        )


async def _assert_reuse_rejects_support_mismatch(
    database: Database,
    *,
    stage: str,
    destination_schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
) -> None:
    """Require a reused reservation to match the staged support receipt."""

    source_support_digest = await database.scalar(
        f"SELECT support_digest FROM {stage}.ptg2_v3_snapshot_layout WHERE snapshot_key = :snapshot_key",
        snapshot_key=source_snapshot_key,
    )
    await database.status(
        f"UPDATE {stage}.ptg2_v3_snapshot_layout SET support_digest = :support_digest WHERE snapshot_key = :snapshot_key",
        support_digest=b"s" * 32,
        snapshot_key=source_snapshot_key,
    )
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="metadata differs"):
        await _prepare_layout(
            database,
            destination_schema_name=destination_schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id="destination-three",
        )
    await database.status(
        f"UPDATE {stage}.ptg2_v3_snapshot_layout SET support_digest = :support_digest WHERE snapshot_key = :snapshot_key",
        support_digest=source_support_digest,
        snapshot_key=source_snapshot_key,
    )


async def _corrupt_staged_target_payload(database: Database, *, stage: str) -> None:
    """Change one same-length target payload while retaining its claimed hash."""

    await database.status(
        f"""
        UPDATE {stage}.ptg2_v3_block
           SET payload = set_byte(payload, 0, (get_byte(payload, 0) + 1) % 256)
         WHERE block_hash = (
             SELECT block_hash
               FROM {stage}.ptg2_v3_block
              WHERE object_kind <> 'snapshot_coordinate_map_v1'
              ORDER BY block_hash
              LIMIT 1
         )
        """
    )


async def _assert_reuse_rejects_payload_mismatch(
    database: Database,
    *,
    stage: str,
    destination: str,
    destination_schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
) -> None:
    """Reject same-length staged payload corruption before a third bind occurs."""

    await _corrupt_staged_target_payload(database, stage=stage)
    with pytest.raises(adoption.ResultArchiveAdoptionError, match="CAS hash collides"):
        await _prepare_layout(
            database,
            destination_schema_name=destination_schema_name,
            staging_schema_name=staging_schema_name,
            source_snapshot_key=source_snapshot_key,
            destination_snapshot_id="destination-four",
        )
    for snapshot_id in ("destination-three", "destination-four"):
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {destination}.ptg2_v3_snapshot_binding WHERE snapshot_id = :snapshot_id",
                snapshot_id=snapshot_id,
            )
            == 0
        )


async def _prepare_reuse_layouts(
    database: Database,
    *,
    destination_schema_name: str,
    staging_schema_name: str,
    source_snapshot_key: int,
):
    """Prepare once and then bind a second local candidate to the reused key."""

    destination = _quoted(destination_schema_name)
    await database.status(
        f"""
        INSERT INTO {destination}.ptg2_snapshot (snapshot_id)
        VALUES ('destination-two'), ('destination-three'), ('destination-four')
        """
    )
    first = await _prepare_layout(
        database,
        destination_schema_name=destination_schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_id="destination-one",
    )
    reused = await _prepare_layout(
        database,
        destination_schema_name=destination_schema_name,
        staging_schema_name=staging_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_id="destination-two",
    )
    return destination, first, reused


@pytest.mark.asyncio
async def test_native_archive_preparation_remaps_local_layout_and_preserves_cas(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Seal a destination-local key without touching unrelated destination rows."""

    _require_native_postgres()
    stage_name = f"ptg_archive_stage_{uuid.uuid4().hex[:16]}"
    destination_name = f"ptg_archive_destination_{uuid.uuid4().hex[:16]}"
    database = Database()
    await database.connect()
    v4_e2e._bind_source_local_database(monkeypatch, database)
    monkeypatch.setattr(
        adoption,
        "_REKEYED_TABLES",
        tuple(table for table in adoption._REKEYED_TABLES if table in _FIXTURE_REKEYED_TABLES),
    )
    try:
        source_key = await _seed_preparation_catalog(
            database, schema_name=stage_name, tmp_path=tmp_path, monkeypatch=monkeypatch
        )
        await _create_destination_catalog(
            database, schema_name=destination_name, snapshot_id="destination-snapshot", monkeypatch=monkeypatch
        )
        destination = _quoted(destination_name)
        async with database.transaction() as session:
            await v4_e2e.reserve_v4_shared_layout(
                session,
                schema_name=destination_name,
                semantic_fingerprint=b"d" * 32,
                build_token="unrelated-layout",
            )
        await database.status(
            f"INSERT INTO {destination}.ptg2_v3_block (block_hash, format_version, object_kind, codec, entry_count, raw_byte_count, stored_byte_count, payload) VALUES (:hash, 2, 'unrelated', 'none', 1, 1, 1, 'u')",
            hash=b"u" * 32,
        )
        async with database.transaction() as session:
            prepared = await adoption.prepare_result_archive_layout(
                session,
                schema_name=destination_name,
                staging_schema_name=stage_name,
                source_snapshot_key=source_key,
                destination_snapshot_id="destination-snapshot",
                build_token=f"receiver-{uuid.uuid4().hex}",
            )
        await _assert_prepared_destination(
            database,
            destination_schema=destination,
            staging_schema=_quoted(stage_name),
            source_snapshot_key=source_key,
            prepared=prepared,
        )
    finally:
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(destination_name)} CASCADE")
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(stage_name)} CASCADE")
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_native_archive_preparation_copies_full_current_layout_family(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Rekey every current layout family while leaving logical evidence to its owner."""

    _require_native_postgres()
    stage_name = f"ptg_archive_stage_{uuid.uuid4().hex[:16]}"
    destination_name = f"ptg_archive_destination_{uuid.uuid4().hex[:16]}"
    database = Database()
    await database.connect()
    v4_e2e._bind_source_local_database(monkeypatch, database)
    try:
        source_key = await _seed_full_preparation_catalog(
            database,
            schema_name=stage_name,
            tmp_path=tmp_path,
            monkeypatch=monkeypatch,
        )
        await _create_full_destination_catalog(
            database,
            schema_name=destination_name,
            snapshot_id="destination-snapshot",
            monkeypatch=monkeypatch,
        )
        stage = _quoted(stage_name)
        destination = _quoted(destination_name)
        async with database.transaction() as session:
            prepared = await adoption.prepare_result_archive_layout(
                session,
                schema_name=destination_name,
                staging_schema_name=stage_name,
                source_snapshot_key=source_key,
                destination_snapshot_id="destination-snapshot",
                build_token=f"receiver-{uuid.uuid4().hex}",
            )
        assert prepared.mapping_digest
        await _assert_full_family_boundary(
            database,
            stage=stage,
            destination=destination,
            source_snapshot_key=source_key,
            destination_snapshot_key=prepared.destination_snapshot_key,
            destination_schema_name=destination_name,
        )
    finally:
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(destination_name)} CASCADE")
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(stage_name)} CASCADE")
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_native_archive_preparation_rejects_collision_without_partial_layout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A differing local row for one CAS hash leaves the caller transaction empty."""

    _require_native_postgres()
    stage_name = f"ptg_archive_stage_{uuid.uuid4().hex[:16]}"
    destination_name = f"ptg_archive_destination_{uuid.uuid4().hex[:16]}"
    database = Database()
    await database.connect()
    v4_e2e._bind_source_local_database(monkeypatch, database)
    monkeypatch.setattr(adoption, "_REKEYED_TABLES", ())
    try:
        source_key = await _seed_preparation_catalog(
            database, schema_name=stage_name, tmp_path=tmp_path, monkeypatch=monkeypatch
        )
        await _create_destination_catalog(
            database, schema_name=destination_name, snapshot_id="destination-snapshot", monkeypatch=monkeypatch
        )
        stage = _quoted(stage_name)
        destination = _quoted(destination_name)
        block_hash = await database.scalar(f"SELECT block_hash FROM {stage}.ptg2_v3_block LIMIT 1")
        await database.status(
            f"INSERT INTO {destination}.ptg2_v3_block (block_hash, format_version, object_kind, codec, entry_count, raw_byte_count, stored_byte_count, payload) VALUES (:hash, 2, 'collision', 'none', 1, 1, 1, 'x')",
            hash=block_hash,
        )
        with pytest.raises(adoption.ResultArchiveAdoptionError, match="CAS hash collides"):
            async with database.transaction() as session:
                await adoption.prepare_result_archive_layout(
                    session,
                    schema_name=destination_name,
                    staging_schema_name=stage_name,
                    source_snapshot_key=source_key,
                    destination_snapshot_id="destination-snapshot",
                    build_token=f"receiver-{uuid.uuid4().hex}",
                )
        assert await database.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_v3_snapshot_layout") == 0
    finally:
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(destination_name)} CASCADE")
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(stage_name)} CASCADE")
        finally:
            await database.disconnect()


@pytest.mark.asyncio
async def test_native_archive_preparation_reuse_rechecks_staged_cas_before_binding(
    tmp_path: Path,
    monkeypatch,
) -> None:
    """A reused local key requires matching staged payloads before it can bind."""

    _require_native_postgres()
    stage_name = f"ptg_archive_stage_{uuid.uuid4().hex[:16]}"
    destination_name = f"ptg_archive_destination_{uuid.uuid4().hex[:16]}"
    database = Database()
    await database.connect()
    v4_e2e._bind_source_local_database(monkeypatch, database)
    monkeypatch.setattr(
        adoption,
        "_REKEYED_TABLES",
        tuple(table for table in adoption._REKEYED_TABLES if table in _FIXTURE_REKEYED_TABLES),
    )
    try:
        source_key = await _seed_preparation_catalog(
            database, schema_name=stage_name, tmp_path=tmp_path, monkeypatch=monkeypatch
        )
        await _create_destination_catalog(
            database, schema_name=destination_name, snapshot_id="destination-one", monkeypatch=monkeypatch
        )
        destination, first, reused = await _prepare_reuse_layouts(
            database,
            destination_schema_name=destination_name,
            staging_schema_name=stage_name,
            source_snapshot_key=source_key,
        )
        assert reused.destination_snapshot_key == first.destination_snapshot_key
        assert reused.mapping_digest == first.mapping_digest
        assert await database.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_v3_snapshot_layout") == 1

        stage = _quoted(stage_name)
        await _assert_reuse_rejects_support_mismatch(
            database,
            stage=stage,
            destination_schema_name=destination_name,
            staging_schema_name=stage_name,
            source_snapshot_key=source_key,
        )
        await _assert_reuse_rejects_payload_mismatch(
            database,
            stage=stage,
            destination=destination,
            destination_schema_name=destination_name,
            staging_schema_name=stage_name,
            source_snapshot_key=source_key,
        )
    finally:
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(destination_name)} CASCADE")
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(stage_name)} CASCADE")
        finally:
            await database.disconnect()
