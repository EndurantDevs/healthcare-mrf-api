# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic helpers for the dormant NPI-enumeration PostgreSQL proof."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import datetime
import importlib.util
from pathlib import Path
from typing import Any, AsyncIterator, Mapping

import asyncpg
from sqlalchemy.ext.asyncio import AsyncEngine

from public_evidence import evidence_record_contract as record_contract
from public_evidence import evidence_record_primitives as record_primitives
from public_evidence import record_persistence_candidate_contract as candidate_contract
from public_evidence import (
    record_persistence_candidate_primitives as candidate_primitives,
)
from tests.public_evidence_record_support import enumeration_input, source_release
from tests.public_evidence_reference_roots_postgres_support import (
    EXPECTED_COLUMNS_BY_TABLE as REFERENCE_ROOT_COLUMNS,
    insert_reference_row,
    reference_roots_schema,
)
from tests.public_evidence_storage_postgres_support import (
    EXPECTED_COLUMNS_BY_TABLE as FOUNDATION_COLUMNS,
    insert_source_release,
    quoted,
    run_migration_action,
)


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260808170000_public_evidence_npi_enumeration_storage.py"
)
TABLE_NAMES = (
    "public_evidence_record",
    "public_evidence_record_source_link",
    "public_evidence_npi_enumeration",
)
EXPECTED_COLUMNS = {
    "public_evidence_record": (
        "evidence_ref",
        "record_contract",
        "record_contract_sha256",
        "foundation_scope",
        "source_release_ref",
        "source_release_contract_sha256",
        "source_kind",
        "observed_at",
        "effective_start_at",
        "effective_end_at",
        "record_type",
        "relationship_class",
        "source_record_count",
        "source_link_ordering_contract_id",
        "source_link_vector_sha256",
        "typed_row_sha256",
        "authority_state_sha256",
        "lifecycle_state",
        "positive_evidence_only",
        "serving_authority",
        "current_pointer_authority",
        "database_io_authority",
        "publication_enabled",
        "row_sha256",
        "created_at",
    ),
    "public_evidence_record_source_link": (
        "evidence_ref",
        "source_release_ref",
        "source_release_contract_sha256",
        "source_kind",
        "source_record_ordinal",
        "source_record_ref",
        "record_kind",
        "row_sha256",
        "created_at",
    ),
    "public_evidence_npi_enumeration": (
        "evidence_ref",
        "source_release_ref",
        "source_release_contract_sha256",
        "source_kind",
        "record_type",
        "relationship_class",
        "npi",
        "npi_entity_type",
        "enumeration_state",
        "row_sha256",
        "created_at",
    ),
}
EXPECTED_SCHEMA_TABLES = frozenset(
    (*FOUNDATION_COLUMNS, *REFERENCE_ROOT_COLUMNS, *TABLE_NAMES)
)
EXPECTED_COLUMN_TYPES = {
    "evidence_ref": "character varying(49)",
    "record_contract": "character varying(64)",
    "record_contract_sha256": "bytea",
    "foundation_scope": "character varying(64)",
    "source_release_ref": "character varying(50)",
    "source_release_contract_sha256": "bytea",
    "source_kind": "character varying(48)",
    "observed_at": "timestamp with time zone",
    "effective_start_at": "timestamp with time zone",
    "effective_end_at": "timestamp with time zone",
    "record_type": "character varying(64)",
    "relationship_class": "character varying(96)",
    "source_record_count": "smallint",
    "source_link_ordering_contract_id": "character varying(96)",
    "source_link_vector_sha256": "bytea",
    "typed_row_sha256": "bytea",
    "authority_state_sha256": "bytea",
    "lifecycle_state": "character varying(32)",
    "positive_evidence_only": "boolean",
    "serving_authority": "character varying(16)",
    "current_pointer_authority": "character varying(16)",
    "database_io_authority": "character varying(16)",
    "publication_enabled": "boolean",
    "row_sha256": "bytea",
    "created_at": "timestamp with time zone",
    "source_record_ordinal": "smallint",
    "source_record_ref": "character varying(49)",
    "record_kind": "character varying(64)",
    "npi": "character varying(10)",
    "npi_entity_type": "character varying(24)",
    "enumeration_state": "character varying(16)",
}


def _constraint_flags(
    table_name: str, constraint_type: str, *, is_deferred: bool = False
) -> tuple[str, str, bool, bool, bool, bool]:
    return (table_name, constraint_type, is_deferred, is_deferred, True, True)


EXPECTED_CONSTRAINT_FLAGS = {
    "public_evidence_record_pkey": _constraint_flags(TABLE_NAMES[0], "p"),
    "public_evidence_record_owner_key": _constraint_flags(TABLE_NAMES[0], "u"),
    "public_evidence_record_release_fkey": _constraint_flags(TABLE_NAMES[0], "f"),
    "public_evidence_record_shape_check": _constraint_flags(TABLE_NAMES[0], "c"),
    "public_evidence_record_source_link_pkey": _constraint_flags(TABLE_NAMES[1], "p"),
    "public_evidence_record_source_link_ref_key": _constraint_flags(
        TABLE_NAMES[1], "u"
    ),
    "public_evidence_record_source_link_record_fkey": _constraint_flags(
        TABLE_NAMES[1], "f", is_deferred=True
    ),
    "public_evidence_record_source_link_source_fkey": _constraint_flags(
        TABLE_NAMES[1], "f", is_deferred=True
    ),
    "public_evidence_record_source_link_shape_check": _constraint_flags(
        TABLE_NAMES[1], "c"
    ),
    "public_evidence_npi_enumeration_pkey": _constraint_flags(TABLE_NAMES[2], "p"),
    "public_evidence_npi_enumeration_record_fkey": _constraint_flags(
        TABLE_NAMES[2], "f", is_deferred=True
    ),
    "public_evidence_npi_enumeration_shape_check": _constraint_flags(
        TABLE_NAMES[2], "c"
    ),
}
EXPECTED_INDEX_NAMES = frozenset(
    {
        "public_evidence_record_pkey",
        "public_evidence_record_owner_key",
        "public_evidence_record_source_link_pkey",
        "public_evidence_record_source_link_ref_key",
        "public_evidence_npi_enumeration_pkey",
    }
)


def load_migration() -> Any:
    module_spec = importlib.util.spec_from_file_location(
        "public_evidence_npi_enumeration_postgres_proof",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


@asynccontextmanager
async def npi_enumeration_schema() -> AsyncIterator[tuple[AsyncEngine, Any, str, Any]]:
    async with reference_roots_schema() as (
        engine,
        database_url,
        schema_name,
        _reference_migration,
    ):
        migration = load_migration()
        migration._schema = lambda: schema_name
        await run_migration_action(engine, migration, "upgrade")
        yield engine, database_url, schema_name, migration


def _utc_datetime(value: str | None) -> datetime | None:
    if value is None:
        return None
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def npi_candidate(
    *,
    enumeration_state: str = "active",
    npi_entity_type: str = "individual_type_1",
    finite_active_interval: bool = False,
) -> candidate_primitives.PublicEvidenceRecordPersistenceCandidate:
    release = source_release("nppes_entity_address")
    raw = enumeration_input(
        release,
        enumeration_state=enumeration_state,
        npi_entity_type=npi_entity_type,
    )
    if finite_active_interval:
        raw["effective_interval"] = type(raw["effective_interval"])(
            "2026-07-01T00:00:00Z",
            "2026-07-01T18:00:00Z",
        )
    normalized = record_contract.build_public_evidence_record(release, raw)
    return candidate_contract.build_public_evidence_record_persistence_candidate(
        normalized
    )


def _digest_bytes(field_name: str, value: object) -> object:
    if field_name.endswith("sha256"):
        assert type(value) is str
        return bytes.fromhex(value)
    if field_name in {"observed_at", "effective_start_at", "effective_end_at"}:
        assert value is None or type(value) is str
        return _utc_datetime(value)
    return value


def candidate_rows(
    candidate: candidate_primitives.PublicEvidenceRecordPersistenceCandidate,
) -> dict[str, list[dict[str, object]]]:
    common_row_map = {
        field_name: _digest_bytes(field_name, value)
        for field_name, value in candidate.common_row._asdict().items()
    }
    source_link_parameters = [
        {
            field_name: _digest_bytes(field_name, value)
            for field_name, value in link._asdict().items()
        }
        for link in candidate.source_link_rows
    ]
    typed_row_map = {
        field_name: _digest_bytes(field_name, value)
        for field_name, value in candidate.typed_row._asdict().items()
    }
    return {
        TABLE_NAMES[0]: [common_row_map],
        TABLE_NAMES[1]: source_link_parameters,
        TABLE_NAMES[2]: [typed_row_map],
    }


async def insert_row(
    connection: asyncpg.Connection,
    schema_name: str,
    table_name: str,
    parameters: Mapping[str, object],
) -> None:
    column_names = tuple(parameters)
    placeholders = ", ".join(
        f"${ordinal}" for ordinal in range(1, len(column_names) + 1)
    )
    await connection.execute(
        f"INSERT INTO {quoted(schema_name)}.{quoted(table_name)} "
        f"({', '.join(column_names)}) VALUES ({placeholders})",
        *(parameters[column_name] for column_name in column_names),
    )


def source_root_parameters(
    persistence_candidate: (
        candidate_primitives.PublicEvidenceRecordPersistenceCandidate
    ),
    source_record: record_primitives.EvidenceSourceRecordReference,
) -> dict[str, object]:
    release = persistence_candidate.record.release
    return {
        "source_record_ref": source_record.source_record_ref,
        "source_release_ref": source_record.source_release_ref,
        "source_release_contract_sha256": bytes.fromhex(release.contract_sha256),
        "source_kind": release.source_kind,
        "record_kind": source_record.record_kind,
        "identity_contract_id": source_record.identity_contract_id,
        "record_hmac_sha256": bytes.fromhex(source_record.record_hmac_sha256),
        "payload_sha256": bytes.fromhex(source_record.payload_sha256),
    }


async def seed_owned_roots(
    connection: asyncpg.Connection,
    schema_name: str,
    candidate: candidate_primitives.PublicEvidenceRecordPersistenceCandidate,
) -> None:
    await insert_source_release(
        connection,
        schema_name,
        "nppes_entity_address",
    )
    for source_record in candidate.record.source_records:
        await insert_reference_row(
            connection,
            schema_name,
            "public_evidence_source_record",
            source_root_parameters(candidate, source_record),
        )


async def assert_owned_roots(
    connection: asyncpg.Connection,
    schema_name: str,
    persistence_candidate: (
        candidate_primitives.PublicEvidenceRecordPersistenceCandidate
    ),
) -> None:
    """Prove the exact release and source-record owner rows still exist."""

    release = persistence_candidate.record.release
    source_record = persistence_candidate.record.source_records[0]
    release_owner = await connection.fetchrow(
        f"SELECT source_release_ref, encode(contract_sha256,'hex') AS digest, "
        f"source_kind FROM {quoted(schema_name)}.public_evidence_source_release"
    )
    assert dict(release_owner) == {
        "source_release_ref": release.source_release_ref,
        "digest": release.contract_sha256,
        "source_kind": release.source_kind,
    }
    source_owner = await connection.fetchrow(
        f"SELECT source_record_ref, source_release_ref, "
        f"encode(source_release_contract_sha256,'hex') AS digest, source_kind "
        f"FROM {quoted(schema_name)}.public_evidence_source_record"
    )
    assert dict(source_owner) == {
        "source_record_ref": source_record.source_record_ref,
        "source_release_ref": release.source_release_ref,
        "digest": release.contract_sha256,
        "source_kind": release.source_kind,
    }


async def assert_stored_candidate(
    connection: asyncpg.Connection,
    schema_name: str,
    candidate: candidate_primitives.PublicEvidenceRecordPersistenceCandidate,
) -> None:
    schema = quoted(schema_name)
    common_digests = await connection.fetchrow(
        f"SELECT evidence_ref, encode(record_contract_sha256,'hex') AS record_digest, "
        f"encode(source_link_vector_sha256,'hex') AS vector_digest, "
        f"encode(typed_row_sha256,'hex') AS typed_digest, "
        f"encode(authority_state_sha256,'hex') AS authority_digest, "
        f"encode(row_sha256,'hex') AS common_digest FROM "
        f"{schema}.public_evidence_record"
    )
    assert dict(common_digests) == {
        "evidence_ref": candidate.record.evidence_ref,
        "record_digest": candidate.record.contract_sha256,
        "vector_digest": candidate.common_row.source_link_vector_sha256,
        "typed_digest": candidate.typed_row.row_sha256,
        "authority_digest": candidate.common_row.authority_state_sha256,
        "common_digest": candidate.common_row.row_sha256,
    }
    assert (
        await connection.fetchval(
            f"SELECT encode(row_sha256,'hex') FROM "
            f"{schema}.public_evidence_record_source_link"
        )
        == candidate.source_link_rows[0].row_sha256
    )
    assert (
        await connection.fetchval(
            f"SELECT encode(row_sha256,'hex') FROM "
            f"{schema}.public_evidence_npi_enumeration"
        )
        == candidate.typed_row.row_sha256
    )


async def insert_candidate(
    connection: asyncpg.Connection,
    schema_name: str,
    candidate: candidate_primitives.PublicEvidenceRecordPersistenceCandidate,
    *,
    table_order: tuple[str, ...] = TABLE_NAMES,
    rows: Mapping[str, list[dict[str, object]]] | None = None,
    force_immediate: bool = False,
    use_replica_role: bool = False,
) -> None:
    selected_rows = candidate_rows(candidate) if rows is None else rows
    async with connection.transaction():
        if use_replica_role:
            await connection.execute("SET LOCAL session_replication_role='replica'")
        for table_name in table_order:
            for parameters in selected_rows.get(table_name, []):
                await insert_row(connection, schema_name, table_name, parameters)
        if force_immediate:
            await connection.execute("SET CONSTRAINTS ALL IMMEDIATE")


async def wait_for_ungranted_advisory_lock(
    observer: asyncpg.Connection, backend_pid: int
) -> None:
    for _attempt in range(200):
        is_waiting = await observer.fetchval(
            "SELECT EXISTS (SELECT 1 FROM pg_locks "
            "WHERE pid=$1 AND locktype='advisory' AND NOT granted)",
            backend_pid,
        )
        if is_waiting:
            return
        await asyncio.sleep(0.01)
    raise AssertionError("backend did not wait on the record advisory lock")


def extra_source_link(
    candidate: candidate_primitives.PublicEvidenceRecordPersistenceCandidate,
) -> tuple[record_primitives.EvidenceSourceRecordReference, dict[str, object]]:
    release = candidate.record.release
    source_record = record_primitives.build_evidence_source_record_reference(
        release,
        {
            "record_kind": "nppes_registry_record",
            "identity_contract_id": "synthetic_record_hmac_v1",
            "record_hmac_sha256": "33" * 32,
            "payload_sha256": "cc" * 32,
        },
    )
    row = candidate_primitives.PublicEvidenceRecordSourceLinkRow(
        evidence_ref=candidate.common_row.evidence_ref,
        source_release_ref=release.source_release_ref,
        source_release_contract_sha256=release.contract_sha256,
        source_kind=release.source_kind,
        source_record_ordinal=1,
        source_record_ref=source_record.source_record_ref,
        record_kind=source_record.record_kind,
        row_sha256="",
    )
    finished = candidate_contract._finished_row(row)
    link_row_map = {
        field_name: _digest_bytes(field_name, value)
        for field_name, value in finished._asdict().items()
    }
    return source_record, link_row_map
