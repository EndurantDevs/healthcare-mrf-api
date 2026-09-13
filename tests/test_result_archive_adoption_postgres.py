# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof for local PTG archive-layout preparation."""

from __future__ import annotations

import importlib.util
import json
import os
import uuid
from pathlib import Path

import pytest
from sqlalchemy import MetaData, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.schema import CreateTable

from api.ptg2_shared_blocks import fetch_shared_blocks
from db.connection import Database
from db.migration_ptg2_frozen_source_file_binding import install_frozen_source_file_binding
from db.models._legacy import Base
from process.ptg_parts import ptg2_v4_finalizer_publish as finalizer_publish
from process.ptg_parts import result_archive_adoption as adoption
from process.ptg_parts.ptg2_v4_finalizer_maps import (
    PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS,
    has_valid_finalizer_map,
)
from scripts.research import ptg2_packed_finalizer_abba_lifecycle as finalizer_lifecycle
from scripts.research.ptg2_packed_finalizer_abba_artifacts import generate_artifacts
from tests import test_ptg2_v4_postgres_e2e as v4_e2e
from tests.test_ptg2_packed_finalizer_wrapper_postgres import _tiny_shape

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
    await _seed_source_audit_witness(
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


async def _seed_source_audit_witness(
    database: Database,
    *,
    schema: str,
    source_snapshot_key: int,
) -> None:
    """Seed selected and unrelated model-shaped source-audit records."""

    for snapshot_key, marker in (
        (source_snapshot_key, b"s"),
        (source_snapshot_key + 1000, b"u"),
    ):
        witness_payload = marker + b"-audit-witness"
        await database.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_source_audit_witness
                (snapshot_key, contract, selection_method, source_set_digest,
                 sample_digest, queryable_occurrence_population_count,
                 provider_population_count, occurrence_witness_count,
                 provider_witness_count, payload_sha256, payload)
            VALUES (:snapshot_key, 'synthetic_audit_v1', 'synthetic',
                    :source_digest, :sample_digest, 1, 0, 1, 0,
                    :payload_digest, :payload)
            """,
            snapshot_key=snapshot_key,
            source_digest=marker * 32,
            sample_digest=(marker + b"d") * 16,
            payload_digest=(marker + b"p") * 16,
            payload=witness_payload,
        )
        await database.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_source_audit_witness_part
                (snapshot_key, part_number, part_sha256, payload)
            VALUES (:snapshot_key, 1, :part_digest, :payload)
            """,
            snapshot_key=snapshot_key,
            part_digest=(marker + b"q") * 16,
            payload=marker + b"-audit-part",
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
    await _publish_native_finalizer_fixture(
        database,
        schema_name=schema_name,
        source_snapshot_key=source_snapshot_key,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )
    return source_snapshot_key


async def _publish_native_finalizer_fixture(
    database: Database,
    *,
    schema_name: str,
    source_snapshot_key: int,
    tmp_path: Path,
    monkeypatch,
) -> None:
    """Attach a producer-created packed finalizer map to the sealed source layout."""

    source_build_token, finalizer_build_token = await _enter_finalizer_build(
        database,
        schema_name=schema_name,
        source_snapshot_key=source_snapshot_key,
    )
    manifest = await _publish_finalizer_artifacts(
        database,
        schema_name=schema_name,
        source_snapshot_key=source_snapshot_key,
        finalizer_build_token=finalizer_build_token,
        tmp_path=tmp_path,
        monkeypatch=monkeypatch,
    )
    await _seal_finalizer_fixture(
        database,
        schema_name=schema_name,
        source_snapshot_key=source_snapshot_key,
        source_build_token=source_build_token,
        manifest=manifest,
    )


async def _enter_finalizer_build(
    database: Database,
    *,
    schema_name: str,
    source_snapshot_key: int,
) -> tuple[str, str]:
    """Reopen the disposable native layout for its producer-backed finalizer."""

    schema = _quoted(schema_name)
    source_build_token = await database.scalar(
        f"SELECT build_token FROM {schema}.ptg2_v3_snapshot_layout WHERE snapshot_key = :snapshot_key",
        snapshot_key=source_snapshot_key,
    )
    finalizer_build_token = f"receiver-finalizer-{uuid.uuid4().hex[:16]}"
    await database.status(
        f"""
        UPDATE {schema}.ptg2_v3_snapshot_layout
           SET state = 'building', build_token = :build_token
         WHERE snapshot_key = :snapshot_key
        """,
        build_token=finalizer_build_token,
        snapshot_key=source_snapshot_key,
    )
    return str(source_build_token), finalizer_build_token


async def _publish_finalizer_artifacts(
    database: Database,
    *,
    schema_name: str,
    source_snapshot_key: int,
    finalizer_build_token: str,
    tmp_path: Path,
    monkeypatch,
) -> dict[str, object]:
    """Run the production finalizer producer against generated tiny artifacts."""

    work_directory = tmp_path / f"finalizer-work-{uuid.uuid4().hex[:8]}"
    artifacts = generate_artifacts(tmp_path / f"finalizer-artifacts-{uuid.uuid4().hex[:8]}", _tiny_shape())
    work_directory.mkdir()
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_WORKERS", "1")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_IDENTITY_MAP_MAX_BYTES", "67108864")
    monkeypatch.setenv("HLTHPRT_PTG2_V3_FINALIZER_TOTAL_SORT_MEMORY_BYTES", "16777216")
    monkeypatch.setattr(finalizer_lifecycle, "db", database)
    monkeypatch.setattr(finalizer_publish, "db", database)
    try:
        request = finalizer_lifecycle.ArmRequest(
            "receiver",
            True,
            schema_name,
            source_snapshot_key,
            finalizer_build_token,
            work_directory,
            artifacts,
        )
        publication_result, _elapsed_seconds, _timeline = await finalizer_lifecycle._publish_finalizer(request)
        manifest = publication_result.publication.manifest()
    finally:
        artifacts.cleanup()
        assert not any(work_directory.iterdir())
        work_directory.rmdir()
    return manifest


async def _seal_finalizer_fixture(
    database: Database,
    *,
    schema_name: str,
    source_snapshot_key: int,
    source_build_token: str,
    manifest: dict[str, object],
) -> None:
    """Attach the producer receipt and verify it through the native map reader."""

    schema = _quoted(schema_name)
    await database.status(
        f"""
        UPDATE {schema}.ptg2_v3_snapshot_layout
           SET state = 'sealed', build_token = :source_build_token,
               layout_manifest = jsonb_set(
                   layout_manifest, '{{serving_index}}',
                   COALESCE(layout_manifest->'serving_index', '{{}}'::jsonb)
                       || jsonb_build_object('finalizer_mapping', CAST(:manifest AS jsonb))
               )
         WHERE snapshot_key = :snapshot_key
        """,
        source_build_token=source_build_token,
        manifest=json.dumps(manifest, sort_keys=True),
        snapshot_key=source_snapshot_key,
    )
    async with database.transaction() as session:
        assert await has_valid_finalizer_map(
            session,
            schema_name=schema_name,
            snapshot_key=source_snapshot_key,
            layout_manifest={"serving_index": {"finalizer_mapping": manifest}},
        )


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
    await _assert_logical_closure_boundary(
        database,
        stage=stage,
        destination=destination,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    await _assert_rekeyed_table_contract(
        database,
        destination_schema_name=destination_schema_name,
    )
    await _assert_destination_finalizer_reader(
        database,
        stage_schema_name=stage.strip('"'),
        destination_schema_name=destination_schema_name,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )


async def _assert_logical_closure_boundary(
    database: Database,
    *,
    stage: str,
    destination: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Separate logical snapshot evidence from rekeyed physical relations."""

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
    for table_name in _MODEL_CLOSURE_TABLES[1:3]:
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {stage}.{table_name} WHERE snapshot_key = :snapshot_key",
                snapshot_key=source_snapshot_key,
            )
            == 1
        )
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {stage}.{table_name} WHERE snapshot_key = :snapshot_key",
                snapshot_key=source_snapshot_key + 1000,
            )
            == 1
        )
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {destination}.{table_name} WHERE snapshot_key = :snapshot_key",
                snapshot_key=destination_snapshot_key,
            )
            == 1
        )


async def _assert_rekeyed_table_contract(
    database: Database,
    *,
    destination_schema_name: str,
) -> None:
    """Require the destination fixture to expose every current physical family."""

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


async def _assert_destination_finalizer_reader(
    database: Database,
    *,
    stage_schema_name: str,
    destination_schema_name: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Read producer receipts and packed finalizer payloads through destination APIs."""

    source_schema = _quoted(stage_schema_name)
    destination_schema = _quoted(destination_schema_name)
    await _assert_finalizer_receipts_match(
        database,
        source_schema=source_schema,
        destination_schema=destination_schema,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    await _assert_finalizer_maps_are_populated(
        database,
        source_schema=source_schema,
        destination_schema=destination_schema,
        source_snapshot_key=source_snapshot_key,
        destination_snapshot_key=destination_snapshot_key,
    )
    await _assert_destination_finalizer_payload_reads(
        database,
        destination_schema=destination_schema,
        destination_schema_name=destination_schema_name,
        destination_snapshot_key=destination_snapshot_key,
    )


async def _assert_finalizer_receipts_match(
    database: Database,
    *,
    source_schema: str,
    destination_schema: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Require the destination root to preserve every producer completion receipt."""

    finalizer_fields = (
        "map_digest, canonical_mapping_digest, canonical_byte_count, "
        "target_identity_digest, object_kind_count, map_pack_count, "
        "coordinate_count, entry_count, logical_byte_count, "
        "stored_map_byte_count, target_block_count"
    )
    source_receipt = await database.first(
        f"SELECT {finalizer_fields} FROM {source_schema}.ptg2_v4_finalizer_map_root WHERE snapshot_key = :snapshot_key",
        snapshot_key=source_snapshot_key,
    )
    destination_receipt = await database.first(
        f"SELECT {finalizer_fields} FROM {destination_schema}.ptg2_v4_finalizer_map_root "
        "WHERE snapshot_key = :snapshot_key",
        snapshot_key=destination_snapshot_key,
    )
    assert source_receipt is not None
    assert destination_receipt == source_receipt


async def _assert_finalizer_maps_are_populated(
    database: Database,
    *,
    source_schema: str,
    destination_schema: str,
    source_snapshot_key: int,
    destination_snapshot_key: int,
) -> None:
    """Reject a fixture that merely carries an empty finalizer table family."""

    for schema, snapshot_key in (
        (source_schema, source_snapshot_key),
        (destination_schema, destination_snapshot_key),
    ):
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_pack WHERE snapshot_key = :snapshot_key",
                snapshot_key=snapshot_key,
            )
            > 0
        )
        assert (
            await database.scalar(
                f"SELECT COUNT(*) FROM {schema}.ptg2_v4_finalizer_map_target WHERE snapshot_key = :snapshot_key",
                snapshot_key=snapshot_key,
            )
            > 0
        )


async def _assert_destination_finalizer_payload_reads(
    database: Database,
    *,
    destination_schema: str,
    destination_schema_name: str,
    destination_snapshot_key: int,
) -> None:
    """Validate the copied map and every finalizer object kind via native readers."""

    async with database.transaction() as session:
        layout_manifest = (
            await session.execute(
                text(
                    f"SELECT layout_manifest FROM {destination_schema}.ptg2_v3_snapshot_layout "
                    "WHERE snapshot_key = :snapshot_key"
                ),
                {"snapshot_key": destination_snapshot_key},
            )
        ).scalar_one()
        assert await has_valid_finalizer_map(
            session,
            schema_name=destination_schema_name,
            snapshot_key=destination_snapshot_key,
            layout_manifest=layout_manifest,
        )
        for object_kind in PTG2_V4_FINALIZER_PACKED_OBJECT_KINDS:
            blocks = await fetch_shared_blocks(
                session,
                schema_name=destination_schema_name,
                snapshot_key=destination_snapshot_key,
                object_kind=object_kind,
                block_keys=(0,),
                require_all=True,
            )
            assert tuple(blocks) == (0,)
            assert len(blocks[0]) == 1


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
