# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Native PostgreSQL proof for restored PTG candidate evidence preparation."""

from __future__ import annotations

import json
import os
import uuid
from contextlib import asynccontextmanager
from dataclasses import dataclass

import pytest
import pytest_asyncio
from sqlalchemy import MetaData, text
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import DBAPIError
from sqlalchemy.schema import CreateTable

from db.connection import Database
from db.migration_ptg2_frozen_source_file_binding import install_frozen_source_file_binding
from db.models._legacy import Base
from process.ptg_parts import result_archive_candidate_preparation as candidate_preparation
from process.ptg_parts.frozen_rate_binding import frozen_rate_binding_from_params
from process.ptg_parts.frozen_rate_files import (
    FROZEN_RATE_FILE_PROOF_CONTRACT,
    FROZEN_RATE_FILE_SET_CONTRACT,
    frozen_rate_file_proof_sha256,
    frozen_rate_file_set_sha256,
)

_ALLOWED_AMOUNT_TABLES = (
    "ptg2_allowed_amount_plan",
    "ptg2_allowed_amount_item",
    "ptg2_allowed_amount_payment",
    "ptg2_allowed_amount_provider_payment",
)
_CANDIDATE_SOURCE_TABLES = (
    "ptg2_source_identity",
    "ptg2_source_file_version",
    "ptg2_source_trace",
    "ptg2_source_trace_set",
    "ptg2_v3_snapshot_source",
)
_STAGED_AMOUNT_MUTATION_COLUMNS = (
    ("ptg2_allowed_amount_plan", "plan_id"),
    ("ptg2_allowed_amount_item", "file_id"),
    ("ptg2_allowed_amount_payment", "allowed_item_hash"),
    ("ptg2_allowed_amount_provider_payment", "npi"),
)


def _require_native_postgres() -> None:
    if os.getenv("HLTHPRT_PTG2_V4_MAP_POSTGRES_TEST") != "1":
        pytest.skip("set guarded PTG V4 PostgreSQL test variables for native proof")


def _quoted(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


async def _install_model_tables(
    database: Database,
    *,
    schema_name: str,
    table_names: tuple[str, ...],
) -> None:
    metadata = MetaData(schema=schema_name)
    for table_name in table_names:
        source_table = Base.metadata.tables[f"mrf.{table_name}"]
        target_table = source_table.to_metadata(metadata, schema=schema_name)
        statement = str(
            CreateTable(target_table, include_foreign_key_constraints=[]).compile(dialect=postgresql.dialect())
        )
        await database.execute_ddl(statement)


async def _install_schema(
    database: Database,
    *,
    schema_name: str,
    include_candidate_sources: bool,
) -> None:
    schema = _quoted(schema_name)
    await database.execute_ddl(f"CREATE SCHEMA {schema}")
    await database.execute_ddl(
        f"""
        CREATE TABLE {schema}.ptg2_snapshot (
            snapshot_id varchar(96) PRIMARY KEY,
            import_run_id varchar(96) NOT NULL,
            manifest jsonb NOT NULL
        )
        """
    )
    await database.execute_ddl(
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_binding (
            snapshot_id varchar(96) PRIMARY KEY,
            snapshot_key bigint NOT NULL UNIQUE
        )
        """
    )
    await database.execute_ddl(
        f"""
        CREATE TABLE {schema}.ptg2_v3_snapshot_scope (
            snapshot_id varchar(96) PRIMARY KEY,
            plan_id varchar(64) NOT NULL,
            plan_market_type varchar(32) NOT NULL
        )
        """
    )
    await _install_model_tables(
        database,
        schema_name=schema_name,
        table_names=_ALLOWED_AMOUNT_TABLES + (_CANDIDATE_SOURCE_TABLES if include_candidate_sources else ()),
    )
    if include_candidate_sources:
        recorder = _DdlRecorder()
        install_frozen_source_file_binding(recorder, schema_name)
        for statement in recorder.statements:
            await database.execute_ddl(statement)


class _DdlRecorder:
    """Collect the current native frozen-binding DDL for one disposable schema."""

    def __init__(self) -> None:
        self.statements: list[str] = []

    def execute(self, statement: str) -> None:
        self.statements.append(statement)


def _frozen_params() -> tuple[dict[str, object], dict[str, object], list[dict[str, object]]]:
    descriptors: list[dict[str, object]] = []
    for ordinal, character in enumerate(("a", "b"), start=1):
        descriptors.append(
            {
                "source_type": "in_network",
                "canonical_url": f"https://example.test/rates-{ordinal}",
                "content_length": 100 + ordinal,
                "etag": f'"{character}"',
                "last_modified": "2026-09-01T00:00:00Z",
                "raw_sha256": character * 64,
                "logical_sha256": chr(ord(character) + 2) * 64,
                "logical_hash_deferred": False,
                "engine_source_identity_hash": chr(ord(character) + 4) * 64,
                "engine_source_file_version_id": str(ordinal) * 16,
                "ordinal": ordinal,
            }
        )
    file_set_digest = frozen_rate_file_set_sha256(descriptors)
    proof_rows = [
        {
            **descriptor,
            "contract": FROZEN_RATE_FILE_PROOF_CONTRACT,
            "raw_byte_count": descriptor["content_length"],
            "verification_mode": "downloaded",
        }
        for descriptor in descriptors
    ]
    parameter_map: dict[str, object] = {
        "import_id": "local-filing",
        "source_file_import_id": "local-filing",
        "source_key": "source_a",
        "import_month": "2026-09-01",
        "plan_ids": ["plan-a"],
        "plan_market_types": ["market-a"],
        "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
        "frozen_rate_files": descriptors,
        "frozen_rate_file_set_sha256": file_set_digest,
        "frozen_rate_file_count": len(descriptors),
    }
    return parameter_map, {**parameter_map, "frozen_rate_file_proof": proof_rows}, descriptors


def _candidate_manifest(
    *,
    frozen_params: dict[str, object],
    descriptors: list[dict[str, object]],
) -> dict[str, object]:
    binding = frozen_rate_binding_from_params(frozen_params)
    assert binding is not None
    proof_rows = frozen_params["frozen_rate_file_proof"]
    assert isinstance(proof_rows, list)
    return {
        "activation": {
            "source_key": "source_a",
            "plan_id": "plan-a",
            "plan_market_type": "market-a",
        },
        "source_file_import_id": binding["source_file_import_id"],
        "frozen_rate_file_binding": binding,
        "frozen_rate_file_set_contract": FROZEN_RATE_FILE_SET_CONTRACT,
        "frozen_rate_files": descriptors,
        "frozen_rate_file_set_sha256": frozen_params["frozen_rate_file_set_sha256"],
        "frozen_rate_file_count": len(descriptors),
        "frozen_rate_file_proof": proof_rows,
        "frozen_rate_file_proof_sha256": frozen_rate_file_proof_sha256(proof_rows),
        "source_file_versions": [
            {
                **descriptor,
                "raw_byte_count": descriptor["content_length"],
                "verification_mode": "downloaded",
            }
            for descriptor in descriptors
        ],
    }


async def _seed_source_versions(database: Database, schema: str, descriptor: dict) -> None:
    """Persist the source identity and immutable file version used by the candidate."""

    source_identity_hash = str(descriptor["engine_source_identity_hash"])
    source_version_id = str(descriptor["engine_source_file_version_id"])
    raw_sha256 = str(descriptor["raw_sha256"])
    logical_sha256 = str(descriptor["logical_sha256"])
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_source_identity
            (source_identity_hash, source_type, canonical_url)
        VALUES (:source_identity_hash, 'in_network', :canonical_url)
        """,
        source_identity_hash=source_identity_hash,
        canonical_url=descriptor["canonical_url"],
    )
    await database.status(
        f"""
        INSERT INTO {schema}.ptg2_source_file_version
            (source_file_version_id, source_identity_hash, raw_sha256,
             logical_sha256, content_length, etag, last_modified,
             verification_mode, payload)
        VALUES (:source_file_version_id, :source_identity_hash, :raw_sha256,
                :logical_sha256, :content_length, :etag, :last_modified,
                'downloaded', CAST(:payload AS jsonb))
        """,
        source_file_version_id=source_version_id,
        source_identity_hash=source_identity_hash,
        raw_sha256=raw_sha256,
        logical_sha256=logical_sha256,
        content_length=descriptor["content_length"],
        etag=descriptor["etag"],
        last_modified=descriptor["last_modified"],
        payload=json.dumps(
            {
                "raw_byte_count": descriptor["content_length"],
                "logical_hash_deferred": False,
            }
        ),
    )


async def _seed_candidate_sources(
    database: Database,
    *,
    schema_name: str,
    snapshot_id: str,
    descriptors: list[dict[str, object]],
) -> None:
    schema = _quoted(schema_name)
    for source_key, descriptor in enumerate(descriptors):
        source_version_id = str(descriptor["engine_source_file_version_id"])
        raw_sha256 = str(descriptor["raw_sha256"])
        logical_sha256 = str(descriptor["logical_sha256"])
        trace_hash = (str(source_key + 7) * 64)[:64]
        trace_set_hash = (str(source_key + 9) * 64)[:64]
        await _seed_source_versions(database, schema, descriptor)
        await database.status(
            f"""
            INSERT INTO {schema}.ptg2_source_trace
                (source_trace_hash, source_file_version_id)
            VALUES (:trace_hash, :source_file_version_id)
            """,
            trace_hash=trace_hash,
            source_file_version_id=source_version_id,
        )
        await database.status(
            f"""
            INSERT INTO {schema}.ptg2_source_trace_set
                (source_trace_set_hash, source_trace_hashes)
            VALUES (:trace_set_hash, ARRAY[:trace_hash]::varchar[])
            """,
            trace_set_hash=trace_set_hash,
            trace_hash=trace_hash,
        )
        await database.status(
            f"""
            INSERT INTO {schema}.ptg2_v3_snapshot_source
                (snapshot_id, source_key, source_type, identity_kind,
                 identity_sha256, raw_container_sha256, logical_json_sha256,
                 logical_hash_deferred, source_trace_set_hash)
            VALUES (:snapshot_id, :source_key, 'in_network',
                    'logical_json_sha256_v1', :logical_sha256, :raw_sha256,
                    :logical_sha256, false, :trace_set_hash)
            """,
            snapshot_id=snapshot_id,
            source_key=source_key,
            logical_sha256=logical_sha256,
            raw_sha256=raw_sha256,
            trace_set_hash=trace_set_hash,
        )


async def _seed_allowed_rows(
    database: Database,
    *,
    schema_name: str,
    snapshot_id: str,
    offset: int,
) -> None:
    schema = _quoted(schema_name)
    await database.status(
        f"INSERT INTO {schema}.ptg2_allowed_amount_plan (snapshot_id, plan_hash, file_id, plan_id) VALUES (:snapshot_id, :plan_hash, 1, 'plan-a')",
        snapshot_id=snapshot_id,
        plan_hash=100 + offset,
    )
    await database.status(
        f"INSERT INTO {schema}.ptg2_allowed_amount_item (snapshot_id, allowed_item_hash, file_id) VALUES (:snapshot_id, :item_hash, 1)",
        snapshot_id=snapshot_id,
        item_hash=200 + offset,
    )
    await database.status(
        f"INSERT INTO {schema}.ptg2_allowed_amount_payment (snapshot_id, payment_hash, allowed_item_hash) VALUES (:snapshot_id, :payment_hash, :item_hash)",
        snapshot_id=snapshot_id,
        payment_hash=300 + offset,
        item_hash=200 + offset,
    )
    await database.status(
        f"INSERT INTO {schema}.ptg2_allowed_amount_provider_payment (snapshot_id, provider_payment_hash, payment_hash, npi) VALUES (:snapshot_id, :provider_payment_hash, :payment_hash, ARRAY[1234567890]::bigint[])",
        snapshot_id=snapshot_id,
        provider_payment_hash=400 + offset,
        payment_hash=300 + offset,
    )


async def _seed_snapshot(
    database: Database,
    *,
    schema_name: str,
    snapshot_id: str,
    import_run_id: str,
    manifest: dict[str, object],
    snapshot_key: int | None = None,
) -> None:
    schema = _quoted(schema_name)
    await database.status(
        f"INSERT INTO {schema}.ptg2_snapshot (snapshot_id, import_run_id, manifest) VALUES (:snapshot_id, :import_run_id, CAST(:manifest AS jsonb))",
        snapshot_id=snapshot_id,
        import_run_id=import_run_id,
        manifest=json.dumps(manifest),
    )
    await database.status(
        f"INSERT INTO {schema}.ptg2_v3_snapshot_scope (snapshot_id, plan_id, plan_market_type) VALUES (:snapshot_id, 'plan-a', 'market-a')",
        snapshot_id=snapshot_id,
    )
    if snapshot_key is not None:
        await database.status(
            f"INSERT INTO {schema}.ptg2_v3_snapshot_binding (snapshot_id, snapshot_key) VALUES (:snapshot_id, :snapshot_key)",
            snapshot_id=snapshot_id,
            snapshot_key=snapshot_key,
        )


@dataclass(frozen=True)
class _CandidateFixture:
    """Own the isolated native catalogs and the destination input identity."""

    database: Database
    stage_name: str
    destination_name: str
    binding_params: dict


async def _seed_candidate_pair(fixture: _CandidateFixture, frozen_params: dict, descriptors: list) -> None:
    """Seed distinct local attempts with identical portable frozen input evidence."""

    stage_params_by_name = {
        **frozen_params,
        "import_id": "archive-filing",
        "source_file_import_id": "archive-filing",
    }
    await _seed_snapshot(
        fixture.database,
        schema_name=fixture.stage_name,
        snapshot_id="archive-snapshot",
        import_run_id="ptg2:archive-filing",
        manifest=_candidate_manifest(frozen_params=stage_params_by_name, descriptors=descriptors),
        snapshot_key=71,
    )
    await _seed_snapshot(
        fixture.database,
        schema_name=fixture.destination_name,
        snapshot_id="local-candidate",
        import_run_id="ptg2:local-filing",
        manifest=_candidate_manifest(frozen_params=frozen_params, descriptors=descriptors),
    )
    await _seed_candidate_sources(
        fixture.database,
        schema_name=fixture.destination_name,
        snapshot_id="local-candidate",
        descriptors=descriptors,
    )
    await _seed_allowed_rows(fixture.database, schema_name=fixture.stage_name, snapshot_id="archive-snapshot", offset=0)


@pytest_asyncio.fixture
async def native_candidate(monkeypatch):
    """Own and remove only this test's two UUID-named schemas."""

    _require_native_postgres()
    database = Database()
    await database.connect()
    frozen_input, frozen_params, descriptors = _frozen_params()
    fixture = _CandidateFixture(
        database,
        f"ptg_candidate_stage_{uuid.uuid4().hex[:16]}",
        f"ptg_candidate_destination_{uuid.uuid4().hex[:16]}",
        frozen_input,
    )
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", fixture.destination_name)
    try:
        await _install_schema(database, schema_name=fixture.stage_name, include_candidate_sources=False)
        await _install_schema(database, schema_name=fixture.destination_name, include_candidate_sources=True)
        await _seed_candidate_pair(fixture, frozen_params, descriptors)
        yield fixture
    finally:
        try:
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(fixture.destination_name)} CASCADE")
            await database.execute_ddl(f"DROP SCHEMA IF EXISTS {_quoted(fixture.stage_name)} CASCADE")
        finally:
            await database.disconnect()


async def _prepare_with_session(fixture: _CandidateFixture, session):
    """Prepare the fixture through an explicitly supplied caller session."""

    return await candidate_preparation.prepare_result_archive_candidate_evidence(
        session,
        schema_name=fixture.destination_name,
        staging_schema_name=fixture.stage_name,
        source_snapshot_key=71,
        destination_snapshot_id="local-candidate",
        frozen_binding_params=fixture.binding_params,
    )


@asynccontextmanager
async def _caller_transaction(database: Database):
    """Yield a raw caller transaction without a request-session context binding."""

    assert database.session_factory is not None
    session = database.session_factory()
    try:
        async with session.begin():
            yield session
    finally:
        await session.close()


async def _prepare_candidate(fixture: _CandidateFixture):
    """Run preparation inside an explicit caller-owned transaction."""

    async with _caller_transaction(fixture.database) as session:
        return await _prepare_with_session(fixture, session)


async def _assert_unwritten_candidate(fixture: _CandidateFixture) -> None:
    """Require complete rollback of destination evidence and frozen bindings."""

    destination = _quoted(fixture.destination_name)
    assert await fixture.database.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_frozen_source_file_binding") == 0
    for table_name in _ALLOWED_AMOUNT_TABLES:
        assert (
            await fixture.database.scalar(
                f"SELECT COUNT(*) FROM {destination}.{table_name} WHERE snapshot_id = 'local-candidate'"
            )
            == 0
        )


async def _assert_staging_mutation_is_blocked(
    fixture: _CandidateFixture,
    statement: str,
) -> None:
    """Require a competing stage mutation to lose to the prepared transaction lock."""

    async with fixture.database.session() as competing_session, competing_session.begin():
        await competing_session.execute(text("SET LOCAL lock_timeout = '100ms'"))
        with pytest.raises(DBAPIError) as raised:
            await competing_session.execute(text(statement))
        assert getattr(raised.value.orig, "sqlstate", None) == "55P03"


@pytest.mark.asyncio
async def test_native_candidate_rekeys_selected_allowed_evidence(native_candidate) -> None:
    """Rekey the selected closure, preserve unrelated rows, and permit exact replay."""

    fixture = native_candidate
    await _seed_allowed_rows(fixture.database, schema_name=fixture.stage_name, snapshot_id="unrelated-stage", offset=10)
    await _seed_allowed_rows(
        fixture.database,
        schema_name=fixture.destination_name,
        snapshot_id="unrelated-candidate",
        offset=20,
    )
    prepared = await _prepare_candidate(fixture)
    assert prepared.destination_snapshot_id == "local-candidate"
    assert prepared.source_snapshot_id == "archive-snapshot"
    assert set(prepared.allowed_amount_row_counts) == set(_ALLOWED_AMOUNT_TABLES)
    assert set(prepared.allowed_amount_row_counts.values()) == {1}
    assert (await _prepare_candidate(fixture)) == prepared
    destination = _quoted(fixture.destination_name)
    for table_name in _ALLOWED_AMOUNT_TABLES:
        for snapshot_id in ("local-candidate", "unrelated-candidate"):
            assert (
                await fixture.database.scalar(
                    f"SELECT COUNT(*) FROM {destination}.{table_name} WHERE snapshot_id = :snapshot_id",
                    snapshot_id=snapshot_id,
                )
                == 1
            )
    assert (
        await fixture.database.scalar(
            f"SELECT COUNT(*) FROM {destination}.ptg2_frozen_source_file_binding WHERE internal_run_id = 'ptg2:local-filing'"
        )
        == 1
    )


@pytest.mark.asyncio
async def test_native_candidate_rejects_source_scope_mismatch(native_candidate) -> None:
    """A local candidate with another source scope receives no staged evidence."""

    fixture = native_candidate
    await fixture.database.status(
        f"UPDATE {_quoted(fixture.destination_name)}.ptg2_snapshot SET manifest = "
        "jsonb_set(manifest, '{activation,source_key}', '\"other-source\"'::jsonb) "
        "WHERE snapshot_id = 'local-candidate'"
    )
    with pytest.raises(candidate_preparation.ResultArchiveCandidatePreparationError, match="source scope differs"):
        await _prepare_candidate(fixture)
    await _assert_unwritten_candidate(fixture)


@pytest.mark.asyncio
async def test_native_candidate_collision_rolls_back_frozen_binding(native_candidate) -> None:
    """A conflicting local allowed row rolls back the same caller transaction."""

    fixture = native_candidate
    destination = _quoted(fixture.destination_name)
    await fixture.database.status(
        f"INSERT INTO {destination}.ptg2_allowed_amount_plan "
        "(snapshot_id, plan_hash, file_id, plan_id) VALUES ('local-candidate', 100, 1, 'conflicting-plan')"
    )
    with pytest.raises(
        candidate_preparation.ResultArchiveCandidatePreparationError, match="ptg2_allowed_amount_plan conflicts"
    ):
        await _prepare_candidate(fixture)
    assert await fixture.database.scalar(f"SELECT COUNT(*) FROM {destination}.ptg2_frozen_source_file_binding") == 0
    assert (
        await fixture.database.scalar(
            f"SELECT COUNT(*) FROM {destination}.ptg2_allowed_amount_item WHERE snapshot_id = 'local-candidate'"
        )
        == 0
    )


@pytest.mark.asyncio
async def test_native_candidate_requires_open_caller_transaction_before_sql(
    native_candidate,
    monkeypatch,
) -> None:
    """Reject an unowned session without letting it begin an implicit transaction."""

    fixture = native_candidate

    async def no_sql(*_arguments, **_keywords):
        raise AssertionError("precondition must reject before SQL")

    async with fixture.database.session() as session:
        monkeypatch.setattr(session, "execute", no_sql)
        with pytest.raises(
            candidate_preparation.ResultArchiveCandidatePreparationError,
            match="already-open caller transaction",
        ):
            await _prepare_with_session(fixture, session)


@pytest.mark.asyncio
async def test_native_candidate_rejects_different_portable_frozen_month(native_candidate) -> None:
    """Do not attach staged rows from a different portable frozen generation."""

    fixture = native_candidate
    staging = _quoted(fixture.stage_name)
    await fixture.database.status(
        f"UPDATE {staging}.ptg2_snapshot SET manifest = "
        "jsonb_set(manifest, '{frozen_rate_file_binding,import_month}', CAST(:month AS jsonb)) "
        "WHERE snapshot_id = 'archive-snapshot'",
        month=json.dumps("2026-10-01"),
    )
    with pytest.raises(
        candidate_preparation.ResultArchiveCandidatePreparationError,
        match="portable frozen input differs",
    ):
        await _prepare_candidate(fixture)
    await _assert_unwritten_candidate(fixture)


@pytest.mark.asyncio
async def test_native_candidate_rejects_missing_portable_frozen_files(native_candidate) -> None:
    """Require the complete staged frozen file identity before destination writes."""

    fixture = native_candidate
    staging = _quoted(fixture.stage_name)
    await fixture.database.status(
        f"UPDATE {staging}.ptg2_snapshot SET manifest = manifest - 'frozen_rate_files' "
        "WHERE snapshot_id = 'archive-snapshot'"
    )
    with pytest.raises(
        candidate_preparation.ResultArchiveCandidatePreparationError,
        match="staging snapshot frozen input is invalid",
    ):
        await _prepare_candidate(fixture)
    await _assert_unwritten_candidate(fixture)


@pytest.mark.asyncio
async def test_native_candidate_pins_staged_generation_during_copy(native_candidate) -> None:
    """Block competing staged metadata and all admitted-family mutations until commit."""

    fixture = native_candidate
    staging = _quoted(fixture.stage_name)
    statements = (
        f"UPDATE {staging}.ptg2_snapshot SET manifest = manifest WHERE snapshot_id = 'archive-snapshot'",
        f"UPDATE {staging}.ptg2_v3_snapshot_binding SET snapshot_key = snapshot_key WHERE snapshot_key = 71",
        f"UPDATE {staging}.ptg2_v3_snapshot_scope SET plan_id = plan_id WHERE snapshot_id = 'archive-snapshot'",
        *(
            f"UPDATE {staging}.{table_name} SET {column_name} = {column_name} WHERE snapshot_id = 'archive-snapshot'"
            for table_name, column_name in _STAGED_AMOUNT_MUTATION_COLUMNS
        ),
    )
    async with _caller_transaction(fixture.database) as owner_session:
        await _prepare_with_session(fixture, owner_session)
        for statement in statements:
            await _assert_staging_mutation_is_blocked(fixture, statement)
