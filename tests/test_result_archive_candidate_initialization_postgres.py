# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof for destination PTG archive initialization."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import MetaData
from sqlalchemy.dialects import postgresql
from sqlalchemy.engine import make_url
from sqlalchemy.schema import CreateTable

from db.connection import db
from db.migration_ptg2_frozen_source_file_binding import install_frozen_source_file_binding
from db.models._legacy import Base
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_candidate_preparation as preparation
from process.ptg_parts import result_archive_receive_binding as receive_binding
from process.ptg_parts.frozen_rate_binding import frozen_internal_run_id, frozen_rate_binding_from_params
from process.ptg_parts.result_archive_source_authority import (
    PtgResultArchiveSourceAuthority,
    result_archive_manifest_sha256,
)
from tests.test_result_archive_candidate_preparation_postgres import (
    _candidate_manifest,
    _frozen_params,
    _seed_allowed_rows,
    _seed_candidate_sources,
)

_OPT_IN = "HLTHPRT_PTG2_ARCHIVE_CANDIDATE_POSTGRES_TEST"
_DSN_ENV = "HLTHPRT_PTG2_ARCHIVE_CANDIDATE_POSTGRES_DSN"
_MODEL_TABLES = (
    "ptg2_import_run",
    "ptg2_snapshot",
    "ptg2_v3_snapshot_binding",
    "ptg2_v3_snapshot_scope",
    "ptg2_v3_snapshot_plan_scope",
    "ptg2_v3_candidate_audit_attestation",
    "ptg2_source_identity",
    "ptg2_content_identity",
    "ptg2_source_file_version",
    "ptg2_source_trace",
    "ptg2_source_trace_set",
    "ptg2_v3_snapshot_source",
    "ptg2_allowed_amount_plan",
    "ptg2_allowed_amount_item",
    "ptg2_allowed_amount_payment",
    "ptg2_allowed_amount_provider_payment",
)


class _DdlRecorder:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _native_admin_url():
    if os.getenv(_OPT_IN) != "1":
        pytest.skip(f"set {_OPT_IN}=1 for the native PostgreSQL proof")
    raw_dsn = os.getenv(_DSN_ENV)
    if not raw_dsn:
        pytest.skip(f"set {_DSN_ENV} for the native PostgreSQL proof")
    url = make_url(raw_dsn).set(drivername="postgresql")
    if url.host not in {"127.0.0.1", "localhost", "postgres"} or url.port not in {5432, 5440, None}:
        pytest.fail(f"{_DSN_ENV} must identify a guarded PostgreSQL test service")
    return url


async def _install_schema(schema_name: str) -> None:
    assert db.engine is not None
    schema = _quoted(schema_name)
    async with db.engine.begin() as connection:
        await connection.exec_driver_sql(f"CREATE SCHEMA {schema}")
        metadata = MetaData(schema=schema_name)
        for table_name in _MODEL_TABLES:
            source_table = Base.metadata.tables[f"mrf.{table_name}"]
            target_table = source_table.to_metadata(metadata, schema=schema_name)
            statement = str(
                CreateTable(target_table, include_foreign_key_constraints=[]).compile(dialect=postgresql.dialect())
            )
            await connection.exec_driver_sql(statement)
        recorder = _DdlRecorder()
        install_frozen_source_file_binding(recorder, schema_name)
        for statement in recorder.statements:
            await connection.exec_driver_sql(statement)


@dataclass(frozen=True)
class _NativeFixture:
    database_name: str
    stage_schema: str
    destination_schema: str
    frozen_params: dict
    authority: dict


async def _seed_stage_logical_rows(fixture: _NativeFixture, stage_manifest: dict) -> None:
    """Seed the immutable source run, snapshot, binding, and scope."""

    stage = _quoted(fixture.stage_schema)
    await db.status(
        f"""
        INSERT INTO {stage}.ptg2_import_run
            (import_run_id, import_month, status, started_at, finished_at,
             heartbeat_at, options, report)
        VALUES (:run_id, DATE '2026-09-01', 'validated', now(), now(), now(),
                CAST(:options AS jsonb), '{{}}'::jsonb)
        """,
        run_id=frozen_internal_run_id("source-filing"),
        options=json.dumps({"source_file_import_id": "source-filing"}),
    )
    await db.status(
        f"""
        INSERT INTO {stage}.ptg2_snapshot
            (snapshot_id, import_run_id, import_month, status, created_at,
             validated_at, published_at, manifest)
        VALUES ('source-snapshot', :run_id, DATE '2026-09-01', 'published',
                now(), now(), now(), CAST(:manifest AS jsonb))
        """,
        run_id=frozen_internal_run_id("source-filing"),
        manifest=json.dumps(stage_manifest),
    )
    await db.status(
        f"INSERT INTO {stage}.ptg2_v3_snapshot_binding (snapshot_id, snapshot_key) VALUES ('source-snapshot', 71)"
    )
    await db.status(
        f"INSERT INTO {stage}.ptg2_v3_snapshot_scope "
        "(snapshot_id, plan_id, plan_market_type, coverage_scope_id) "
        "VALUES ('source-snapshot', 'plan-a', 'market-a', :scope_id)",
        scope_id=b"c" * 32,
    )
    await db.status(
        f"INSERT INTO {stage}.ptg2_v3_snapshot_plan_scope "
        "(snapshot_id, plan_id, plan_market_type) "
        "VALUES ('source-snapshot', 'plan-a', 'market-a')"
    )


async def _seed_stage_frozen_binding(fixture: _NativeFixture, stage_binding: dict) -> None:
    """Seed the exact frozen binding named by the source receipt."""

    stage = _quoted(fixture.stage_schema)
    await db.status(
        f"""
        INSERT INTO {stage}.ptg2_frozen_source_file_binding
            (source_file_import_id, internal_run_id, binding_contract,
             frozen_rate_file_set_contract, frozen_rate_file_set_sha256,
             frozen_rate_file_count, source_key, import_month, plan_ids,
             plan_market_types, binding_sha256, binding_payload)
        VALUES ('source-filing', :run_id, :binding_contract,
                :set_contract, :set_sha256, :file_count, :source_key,
                DATE '2026-09-01', CAST(:plan_ids AS jsonb),
                CAST(:market_types AS jsonb), :binding_sha256,
                CAST(:binding_payload AS jsonb))
        """,
        run_id=frozen_internal_run_id("source-filing"),
        binding_contract=stage_binding["contract"],
        set_contract=stage_binding["frozen_rate_file_set_contract"],
        set_sha256=stage_binding["frozen_rate_file_set_sha256"],
        file_count=stage_binding["frozen_rate_file_count"],
        source_key=stage_binding["source_key"],
        plan_ids=json.dumps(stage_binding["plan_ids"]),
        market_types=json.dumps(stage_binding["plan_market_types"]),
        binding_sha256=fixture.authority["frozen_binding_sha256"],
        binding_payload=json.dumps(stage_binding),
    )


async def _seed_stage(fixture: _NativeFixture) -> None:
    """Seed the complete authenticated staging input."""

    frozen_input, frozen_manifest_params, descriptors = _frozen_params()
    assert frozen_input == fixture.frozen_params
    stage_params_by_name = {
        **frozen_manifest_params,
        "import_id": "source-filing",
        "source_file_import_id": "source-filing",
    }
    stage_binding = frozen_rate_binding_from_params(stage_params_by_name)
    assert stage_binding is not None
    stage_manifest = _candidate_manifest(frozen_params=stage_params_by_name, descriptors=descriptors)
    stage_manifest["serving_index"] = {
        "storage_generation": "shared_blocks_v4",
        "shared_snapshot_key": 71,
        "coverage_scope_id": (b"c" * 32).hex(),
    }
    await _seed_stage_logical_rows(fixture, stage_manifest)
    await _seed_stage_frozen_binding(fixture, stage_binding)
    await _seed_candidate_sources(
        db,
        schema_name=fixture.stage_schema,
        snapshot_id="source-snapshot",
        descriptors=descriptors,
    )
    await db.status(
        f"UPDATE {_quoted(fixture.stage_schema)}.ptg2_source_file_version "
        "SET raw_storage_uri = 'archive://restored/' || source_file_version_id, "
        "verified_at = TIMESTAMP '2026-09-01 12:00:00'"
    )
    await _seed_allowed_rows(db, schema_name=fixture.stage_schema, snapshot_id="source-snapshot", offset=0)


def _fixture_admission() -> tuple[dict, dict]:
    """Build local admission and its authenticated source receipt."""

    frozen_input, frozen_manifest_params, descriptors = _frozen_params()
    stage_params_by_name = {
        **frozen_manifest_params,
        "import_id": "source-filing",
        "source_file_import_id": "source-filing",
    }
    stage_binding = frozen_rate_binding_from_params(stage_params_by_name)
    assert stage_binding is not None
    stage_manifest = _candidate_manifest(frozen_params=stage_params_by_name, descriptors=descriptors)
    stage_manifest["serving_index"] = {
        "storage_generation": "shared_blocks_v4",
        "shared_snapshot_key": 71,
        "coverage_scope_id": (b"c" * 32).hex(),
    }
    authority = PtgResultArchiveSourceAuthority(
        operation_id="candidate-operation",
        snapshot_id="source-snapshot",
        source_file_import_id="source-filing",
        source_key="source_a",
        snapshot_manifest_sha256=result_archive_manifest_sha256(stage_manifest),
        frozen_binding_sha256=initialization.frozen_rate_binding_sha256(stage_binding),
    ).as_dict()
    return frozen_input, authority


@pytest_asyncio.fixture
async def native_candidate(monkeypatch):
    """Create and prove cleanup of one UUID-owned native PostgreSQL database."""

    admin_url = _native_admin_url()
    database_name = "ptg_archive_candidate_" + uuid.uuid4().hex
    database_url = admin_url.set(database=database_name)
    admin = await asyncpg.connect(admin_url.render_as_string(hide_password=False), timeout=10)
    is_database_created = False
    try:
        assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", database_name) == 0
        await admin.execute(f"CREATE DATABASE {_quoted(database_name)} TEMPLATE template0")
        is_database_created = True
        monkeypatch.setenv("HLTHPRT_DB_DRIVER", "postgresql+asyncpg")
        monkeypatch.setenv("HLTHPRT_DB_HOST", str(database_url.host))
        monkeypatch.setenv("HLTHPRT_DB_PORT", str(database_url.port or 5432))
        monkeypatch.setenv("HLTHPRT_DB_USER", str(database_url.username))
        monkeypatch.setenv("HLTHPRT_DB_PASSWORD", str(database_url.password or ""))
        monkeypatch.setenv("HLTHPRT_DB_DATABASE", database_name)
        destination_schema = "candidate_destination"
        monkeypatch.setenv("HLTHPRT_DB_SCHEMA", destination_schema)
        monkeypatch.delenv("DB_SCHEMA", raising=False)
        await db.connect()
        await _install_schema("candidate_stage")
        await _install_schema(destination_schema)
        frozen_input, authority = _fixture_admission()
        fixture = _NativeFixture(
            database_name,
            "candidate_stage",
            destination_schema,
            frozen_input,
            authority,
        )
        await _seed_stage(fixture)
        yield fixture
    finally:
        await db.disconnect()
        if is_database_created:
            await admin.execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=$1 AND pid <> pg_backend_pid()",
                database_name,
            )
            await admin.execute(f"DROP DATABASE IF EXISTS {_quoted(database_name)}")
            assert await admin.fetchval("SELECT count(*) FROM pg_database WHERE datname=$1", database_name) == 0
        await admin.close(timeout=5)


@asynccontextmanager
async def _caller_transaction():
    assert db.session_factory is not None
    session = db.session_factory()
    try:
        async with session.begin():
            yield session
    finally:
        await session.close()


async def _initialize_and_prepare(fixture: _NativeFixture):
    async with _caller_transaction() as session:
        initialized = await initialization.initialize_result_archive_candidate(
            session,
            schema_name=fixture.destination_schema,
            staging_schema_name=fixture.stage_schema,
            source_snapshot_key=71,
            destination_snapshot_id="local-candidate",
            frozen_binding_params=fixture.frozen_params,
            authenticated_source_archive_metadata=fixture.authority,
        )
        prepared = await preparation.prepare_result_archive_candidate_evidence(
            session,
            schema_name=fixture.destination_schema,
            staging_schema_name=fixture.stage_schema,
            source_snapshot_key=71,
            destination_snapshot_id="local-candidate",
            frozen_binding_params=fixture.frozen_params,
        )
    return initialized, prepared


async def _receive_initialize_and_prepare(fixture: _NativeFixture):
    async with _caller_transaction() as session:
        frozen_params = await receive_binding.receive_frozen_binding_params(
            session,
            schema_name=fixture.destination_schema,
            staging_schema_name=fixture.stage_schema,
            source_snapshot_key=71,
            destination_snapshot_id="received-candidate",
            source_key="source_a",
            authenticated_source_archive_metadata=fixture.authority,
        )
        initialized = await initialization.initialize_result_archive_candidate(
            session,
            schema_name=fixture.destination_schema,
            staging_schema_name=fixture.stage_schema,
            source_snapshot_key=71,
            destination_snapshot_id="received-candidate",
            frozen_binding_params=frozen_params,
            authenticated_source_archive_metadata=fixture.authority,
        )
        prepared = await preparation.prepare_result_archive_candidate_evidence(
            session,
            schema_name=fixture.destination_schema,
            staging_schema_name=fixture.stage_schema,
            source_snapshot_key=71,
            destination_snapshot_id="received-candidate",
            frozen_binding_params=frozen_params,
        )
    return frozen_params, initialized, prepared


async def _seed_preexisting_destination_source(fixture: _NativeFixture) -> None:
    """Seed the same semantic source with destination-local provenance."""

    destination = _quoted(fixture.destination_schema)
    await db.status(
        f"""
        INSERT INTO {destination}.ptg2_source_identity
            (source_identity_hash, source_type, canonical_url, original_url,
             payload, created_at)
        VALUES (:identity, 'in_network', 'https://example.test/rates-1',
                'https://local.example/rates-1', '{{"local": true}}'::json,
                TIMESTAMP '2020-01-01 00:00:00')
        """,
        identity="e" * 64,
    )
    await db.status(
        f"""
        INSERT INTO {destination}.ptg2_source_file_version
            (source_file_version_id, source_identity_hash, raw_storage_uri,
             raw_sha256, logical_sha256, content_length, etag, last_modified,
             verification_mode, verified_at, created_at, payload)
        VALUES (:version_id, :identity, 'destination://retained/source-1',
                :raw_sha256, :logical_sha256, 101, '"a"',
                '2026-09-01T00:00:00Z', 'downloaded',
                TIMESTAMP '2020-01-01 00:00:00',
                TIMESTAMP '2020-01-01 00:00:00',
                CAST(:payload AS json))
        """,
        version_id="1" * 16,
        identity="e" * 64,
        raw_sha256="a" * 64,
        logical_sha256="c" * 64,
        payload=json.dumps(
            {
                "import_run_id": "local-existing",
                "raw_byte_count": 101,
                "logical_hash_deferred": False,
            }
        ),
    )


async def _assert_local_candidate_boundary(fixture: _NativeFixture, destination: str) -> None:
    """Require a building local attempt without copied source authority."""

    local_snapshot = await db.first(
        f"SELECT import_run_id, status, manifest FROM {destination}.ptg2_snapshot WHERE snapshot_id = 'local-candidate'"
    )
    assert local_snapshot is not None
    local_snapshot_by_name = dict(local_snapshot._mapping)
    local_manifest_by_name = dict(local_snapshot_by_name["manifest"])
    assert local_snapshot_by_name["import_run_id"] == frozen_internal_run_id("local-filing")
    assert local_snapshot_by_name["status"] == "building"
    assert "serving_index" not in local_manifest_by_name
    assert local_manifest_by_name["result_archive_source"] == {
        "contract": initialization.RESULT_ARCHIVE_CANDIDATE_INITIALIZATION_CONTRACT,
        "snapshot_manifest_sha256": fixture.authority["snapshot_manifest_sha256"],
        "frozen_binding_sha256": fixture.authority["frozen_binding_sha256"],
    }
    assert (
        await db.scalar(
            f"SELECT COUNT(*) FROM {destination}.ptg2_import_run WHERE import_run_id = :source_run_id",
            source_run_id=frozen_internal_run_id("source-filing"),
        )
        == 0
    )
    assert (
        await db.scalar(
            f"SELECT COUNT(*) FROM {destination}.ptg2_v3_candidate_audit_attestation "
            "WHERE snapshot_id = 'local-candidate'"
        )
        == 0
    )


async def _assert_destination_provenance_preserved(destination: str) -> None:
    """Require local storage evidence and unrelated siblings to survive."""

    retained_version = await db.first(
        f"SELECT raw_storage_uri, verified_at, created_at, payload "
        f"FROM {destination}.ptg2_source_file_version WHERE source_file_version_id = :version_id",
        version_id="1" * 16,
    )
    assert retained_version is not None
    retained_version_by_name = dict(retained_version._mapping)
    assert retained_version_by_name["raw_storage_uri"] == "destination://retained/source-1"
    assert retained_version_by_name["payload"]["import_run_id"] == "local-existing"
    retained_identity = await db.first(
        f"SELECT original_url, payload, created_at FROM {destination}.ptg2_source_identity "
        "WHERE source_identity_hash = :identity",
        identity="e" * 64,
    )
    assert retained_identity is not None
    retained_identity_by_name = dict(retained_identity._mapping)
    assert retained_identity_by_name["original_url"] == "https://local.example/rates-1"
    assert retained_identity_by_name["payload"] == {"local": True}
    assert str(retained_identity_by_name["created_at"]) == "2020-01-01 00:00:00"
    assert (
        await db.scalar(
            f"SELECT COUNT(*) FROM {destination}.ptg2_source_file_version "
            "WHERE raw_storage_uri LIKE 'archive://restored/%'"
        )
        == 0
    )
    assert (
        await db.scalar(
            f"SELECT COUNT(*) FROM {destination}.ptg2_source_identity WHERE source_identity_hash = :identity",
            identity="9" * 64,
        )
        == 1
    )
    assert (
        await db.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_allowed_amount_plan WHERE snapshot_id = 'sibling'")
        == 1
    )


@pytest.mark.asyncio
async def test_initialization_requires_caller_transaction_before_database_access() -> None:
    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="requires an already-open caller transaction",
    ):
        await initialization.initialize_result_archive_candidate(
            object(),
            schema_name="candidate_destination",
            staging_schema_name="candidate_stage",
            source_snapshot_key=71,
            destination_snapshot_id="local-candidate",
            frozen_binding_params={},
            authenticated_source_archive_metadata={},
        )


@pytest.mark.asyncio
async def test_native_initialization_feeds_preparation_replays_and_preserves_siblings(native_candidate) -> None:
    fixture = native_candidate
    destination = _quoted(fixture.destination_schema)
    await db.status(
        f"INSERT INTO {destination}.ptg2_source_identity "
        "(source_identity_hash, source_type, canonical_url) "
        "VALUES (:identity, 'in_network', 'https://sibling.example/rates')",
        identity="9" * 64,
    )
    await _seed_preexisting_destination_source(fixture)
    await _seed_allowed_rows(db, schema_name=fixture.destination_schema, snapshot_id="sibling", offset=50)

    initialized, prepared = await _initialize_and_prepare(fixture)
    replayed, replayed_preparation = await _initialize_and_prepare(fixture)

    assert initialized.reused is False
    assert replayed == initialization.InitializedResultArchiveCandidate(**{**initialized.__dict__, "reused": True})
    assert replayed_preparation == prepared
    assert prepared.destination_snapshot_id == "local-candidate"
    await _assert_local_candidate_boundary(fixture, destination)
    await _assert_destination_provenance_preserved(destination)


@pytest.mark.asyncio
async def test_native_receive_binding_feeds_destination_initialization_and_exact_replay(native_candidate) -> None:
    fixture = native_candidate

    frozen_params, initialized, prepared = await _receive_initialize_and_prepare(fixture)
    replayed_params, replayed, replayed_preparation = await _receive_initialize_and_prepare(fixture)

    filing_id = frozen_params["source_file_import_id"]
    assert frozen_params == replayed_params
    assert filing_id == frozen_params["import_id"]
    assert filing_id.startswith("archive-")
    assert len(filing_id.encode("utf-8")) == 64
    assert filing_id != "source-filing"
    assert initialized.destination_import_run_id == frozen_internal_run_id(filing_id)
    assert initialized.destination_import_run_id.startswith("ptg2:archive-")
    assert initialized.reused is False
    assert replayed == initialization.InitializedResultArchiveCandidate(**{**initialized.__dict__, "reused": True})
    assert prepared == replayed_preparation


@pytest.mark.asyncio
async def test_native_receive_binding_rejects_unapproved_source_without_destination_writes(native_candidate) -> None:
    fixture = native_candidate
    destination = _quoted(fixture.destination_schema)

    async with _caller_transaction() as session:
        with pytest.raises(
            initialization.ResultArchiveCandidateInitializationError,
            match="received source key differs",
        ):
            await receive_binding.receive_frozen_binding_params(
                session,
                schema_name=fixture.destination_schema,
                staging_schema_name=fixture.stage_schema,
                source_snapshot_key=71,
                destination_snapshot_id="received-candidate",
                source_key="different-source",
                authenticated_source_archive_metadata=fixture.authority,
            )

    assert await db.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_snapshot") == 0
    assert await db.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_import_run") == 0


@pytest.mark.asyncio
async def test_native_receive_binding_rejects_altered_staged_manifest_without_destination_writes(
    native_candidate,
) -> None:
    fixture = native_candidate
    stage = _quoted(fixture.stage_schema)
    destination = _quoted(fixture.destination_schema)
    await db.status(
        f"UPDATE {stage}.ptg2_snapshot "
        "SET manifest = jsonb_set(manifest::jsonb, '{frozen_rate_file_set_sha256}', "
        "to_jsonb(CAST(:digest AS text)))::json",
        digest="f" * 64,
    )

    async with _caller_transaction() as session:
        with pytest.raises(
            initialization.ResultArchiveCandidateInitializationError,
            match="restored source authority does not match",
        ):
            await receive_binding.receive_frozen_binding_params(
                session,
                schema_name=fixture.destination_schema,
                staging_schema_name=fixture.stage_schema,
                source_snapshot_key=71,
                destination_snapshot_id="received-candidate",
                source_key="source_a",
                authenticated_source_archive_metadata=fixture.authority,
            )

    assert await db.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_snapshot") == 0
    assert await db.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_import_run") == 0


@pytest.mark.asyncio
async def test_native_initialization_rejects_conflicting_local_run_without_partial_candidate(native_candidate) -> None:
    fixture = native_candidate
    destination = _quoted(fixture.destination_schema)
    await db.status(
        f"""
        INSERT INTO {destination}.ptg2_import_run
            (import_run_id, import_month, status, started_at, heartbeat_at,
             options, report)
        VALUES ('ptg2:local-filing', DATE '2026-09-01', 'running', now(), now(),
                '{{"conflict": true}}'::jsonb, '{{}}'::jsonb)
        """
    )
    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="local import run conflicts with retry",
    ):
        await _initialize_and_prepare(fixture)
    assert (
        await db.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_snapshot WHERE snapshot_id = 'local-candidate'") == 0
    )
    assert await db.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_frozen_source_file_binding") == 0


@pytest.mark.asyncio
async def test_native_initialization_rejects_conflicting_semantic_source_identity(native_candidate) -> None:
    fixture = native_candidate
    destination = _quoted(fixture.destination_schema)
    await db.status(
        f"INSERT INTO {destination}.ptg2_source_identity "
        "(source_identity_hash, source_type, canonical_url) "
        "VALUES (:identity, 'in_network', 'https://conflict.example/rates')",
        identity="e" * 64,
    )
    with pytest.raises(
        initialization.ResultArchiveCandidateInitializationError,
        match="ptg2_source_identity conflicts with destination state",
    ):
        await _initialize_and_prepare(fixture)
    assert (
        await db.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_snapshot WHERE snapshot_id = 'local-candidate'") == 0
    )
