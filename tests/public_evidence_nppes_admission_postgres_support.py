# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic PostgreSQL 18 support for complete NPPES admission."""

from __future__ import annotations

from contextlib import asynccontextmanager
import importlib.util
from pathlib import Path
from typing import Any, AsyncIterator

import asyncpg
from sqlalchemy.ext.asyncio import AsyncEngine

from public_evidence import evidence_record_primitives as record_primitives
from process.nppes_public_evidence_chain import (
    NppesPublicEvidenceChainReceipt,
    _finished_chain_receipt,
)
from process.nppes_public_evidence_chain_rows import (
    CHAIN_ADMISSION_COLUMNS,
    CHAIN_ARCHIVE_COLUMNS,
)
from process.nppes_public_evidence_chain_writer import (
    admit_nppes_public_evidence_chain,
)
from process.nppes_public_evidence_import import (
    NPPES_RIGHTS_PROOF_SHA256,
    NppesEvidenceRuntimeConfig,
    _archive_receipt,
)
from process.nppes_public_evidence_replay import (
    PreparedNppesRegistryReplay,
    prepare_nppes_registry_replay,
)
from process.nppes_public_evidence_rows import (
    ADMISSION_COLUMNS,
    MEMBER_COLUMNS,
    source_record_values,
)
from process.nppes_public_evidence_writer import (
    admit_nppes_registry_archive,
)
from process.nppes_public_evidence_writer_contract import (
    NppesRegistryAdmissionReceipt,
)
from tests.nppes_public_evidence_process_support import prepared_archive
from tests.public_evidence_npi_enumeration_postgres_support import (
    npi_enumeration_schema,
)
from tests.public_evidence_nppes_registry_support import (
    active_type_1_row,
    sparse_deactivated_row,
)
from tests.public_evidence_storage_postgres_support import run_migration_action


ROOT = Path(__file__).resolve().parents[1]
MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260808220000_public_evidence_nppes_registry_admission.py"
)
LIFECYCLE_MIGRATION_PATH = (
    ROOT
    / "alembic"
    / "versions"
    / "20260809020000_nppes_lifecycle_date_tolerance.py"
)
ARCHIVE_NAME = "NPPES_Data_Dissemination_July_2026_V2.zip"
PRIMARY_SNAPSHOT = "20260712"
NEW_TABLES = (
    "public_evidence_nppes_registry_admission",
    "public_evidence_nppes_registry_admission_seal",
    "public_evidence_nppes_registry_member",
    "public_evidence_nppes_registry_chain_admission",
    "public_evidence_nppes_registry_chain_admission_seal",
    "public_evidence_nppes_registry_chain_archive",
)
EXPECTED_COLUMNS_BY_TABLE = {
    NEW_TABLES[0]: (*ADMISSION_COLUMNS, "created_at"),
    NEW_TABLES[1]: ("admission_ref", "sealed_at"),
    NEW_TABLES[2]: (*MEMBER_COLUMNS, "created_at"),
    NEW_TABLES[3]: (*CHAIN_ADMISSION_COLUMNS, "created_at"),
    NEW_TABLES[4]: ("chain_ref", "sealed_at"),
    NEW_TABLES[5]: (*CHAIN_ARCHIVE_COLUMNS, "created_at"),
}
EXPECTED_TYPE_BY_COLUMN = {
    "admission_ref": "character varying(50)",
    "admission_state": "character varying(32)",
    "archive_count": "integer",
    "archive_name": "text",
    "archive_ordinal": "integer",
    "artifact_byte_count": "bigint",
    "artifact_sha256": "bytea",
    "chain_ref": "character varying(50)",
    "contract": "character varying(64)",
    "contract_sha256": "bytea",
    "created_at": "timestamp with time zone",
    "effective_start_not_disclosed_count": "bigint",
    "entity_type_code": "character varying(1)",
    "entity_type_not_disclosed_count": "bigint",
    "evidence_ref": "character varying(49)",
    "evidence_root_sha256": "bytea",
    "excluded_record_count": "bigint",
    "exclusion_reason": "character varying(64)",
    "header_sha256": "bytea",
    "identity_contract_id": "character varying(96)",
    "last_update_date": "date",
    "leaf_sha256": "bytea",
    "listing_byte_count": "bigint",
    "listing_candidate_names": "text[]",
    "listing_sha256": "bytea",
    "manifest_contract": "character varying(64)",
    "manifest_sha256": "bytea",
    "minimum_effective_start_at": "timestamp with time zone",
    "npi": "character varying(10)",
    "npi_deactivation_date": "date",
    "npi_reactivation_date": "date",
    "payload_contract_id": "character varying(96)",
    "payload_sha256": "bytea",
    "primary_member_name": "text",
    "projected_record_count": "bigint",
    "projection_state": "character varying(16)",
    "provider_enumeration_date": "date",
    "publication_enabled": "boolean",
    "record_hmac_sha256": "bytea",
    "record_identity_contract_id": "character varying(96)",
    "record_kind": "character varying(64)",
    "rights_proof_sha256": "bytea",
    "row_sha256": "bytea",
    "sealed_at": "timestamp with time zone",
    "serving_authority": "character varying(16)",
    "snapshot_at": "timestamp with time zone",
    "source_kind": "character varying(48)",
    "source_record_count": "bigint",
    "source_record_ref": "character varying(49)",
    "source_release_contract_sha256": "bytea",
    "source_release_ref": "character varying(50)",
    "source_row_ordinal": "bigint",
    "source_url": "text",
    "tree_contract_id": "character varying(96)",
    "zip_member_census_sha256": "bytea",
    "zip_member_count": "integer",
}
DEFAULT_ROWS = (
    active_type_1_row(),
    sparse_deactivated_row(),
    ("1003000118", "2", "", "", "", ""),
)


class ConnectionDatabase:
    """Expose one exact asyncpg connection through the production DB seam."""

    def __init__(self, connection: asyncpg.Connection) -> None:
        self.connection = connection

    @asynccontextmanager
    async def acquire_driver(self) -> AsyncIterator[asyncpg.Connection]:
        yield self.connection


def load_admission_migration() -> Any:
    """Load the task migration without importing it as package state."""

    module_spec = importlib.util.spec_from_file_location(
        "public_evidence_nppes_admission_postgres_proof",
        MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


def load_lifecycle_migration() -> Any:
    """Load the lifecycle follow-on without importing package state."""

    module_spec = importlib.util.spec_from_file_location(
        "public_evidence_nppes_lifecycle_postgres_proof",
        LIFECYCLE_MIGRATION_PATH,
    )
    assert module_spec is not None and module_spec.loader is not None
    migration = importlib.util.module_from_spec(module_spec)
    module_spec.loader.exec_module(migration)
    return migration


class _NppesAdmissionMigrationStack:
    """Expose the base admission and lifecycle revisions as one test stack."""

    def __init__(self, admission: Any, lifecycle: Any) -> None:
        self.admission = admission
        self.lifecycle = lifecycle
        self.op: object | None = None

    def _bind_operation(self) -> None:
        if self.op is None:
            raise RuntimeError("migration operation is unavailable")
        self.admission.op = self.op
        self.lifecycle.op = self.op

    def upgrade(self) -> None:
        self._bind_operation()
        self.admission.upgrade()
        self.lifecycle.upgrade()

    def downgrade(self) -> None:
        self._bind_operation()
        self.lifecycle.downgrade()
        self.admission.downgrade()


@asynccontextmanager
async def nppes_admission_schema() -> (
    AsyncIterator[tuple[AsyncEngine, Any, str, Any]]
):
    """Install the NPI, admission, and lifecycle revisions."""

    async with npi_enumeration_schema() as (
        engine,
        database_url,
        schema_name,
        _npi_migration,
    ):
        admission = load_admission_migration()
        lifecycle = load_lifecycle_migration()
        admission._schema = lambda: schema_name
        lifecycle._schema = lambda: schema_name
        migration_stack = _NppesAdmissionMigrationStack(admission, lifecycle)
        await run_migration_action(engine, migration_stack, "upgrade")
        yield engine, database_url, schema_name, migration_stack


def required_config() -> NppesEvidenceRuntimeConfig:
    """Return the exact reviewed-rights required-mode configuration."""

    return NppesEvidenceRuntimeConfig("required", NPPES_RIGHTS_PROOF_SHA256)


async def prepared_replay(
    root: Path,
    rows: tuple[tuple[str, ...], ...] = DEFAULT_ROWS,
) -> PreparedNppesRegistryReplay:
    """Create and replay one sealed synthetic archive without database writes."""

    archive = prepared_archive(root, ARCHIVE_NAME, PRIMARY_SNAPSHOT, rows)
    return await prepare_nppes_registry_replay(archive, required_config())


async def admit_replay(
    connection: asyncpg.Connection,
    schema_name: str,
    replay: PreparedNppesRegistryReplay,
) -> NppesRegistryAdmissionReceipt:
    """Run the exact production archive writer against one disposable schema."""

    return await admit_nppes_registry_archive(
        replay,
        required_config(),
        schema=schema_name,
        database=ConnectionDatabase(connection),
    )


def finished_chain_receipt(
    replay: PreparedNppesRegistryReplay,
    admitted: NppesRegistryAdmissionReceipt,
) -> NppesPublicEvidenceChainReceipt:
    """Build the exact one-archive listing receipt produced by orchestration."""

    archive_receipt = _archive_receipt(replay, admitted)
    retained = replay.archive.retained
    return _finished_chain_receipt(
        retained.listing_sha256,
        123,
        (replay.archive.archive_name,),
        (archive_receipt,),
    )


async def admit_chain(
    connection: asyncpg.Connection,
    schema_name: str,
    chain: NppesPublicEvidenceChainReceipt,
) -> NppesPublicEvidenceChainReceipt:
    """Run the production listing-chain writer in the disposable schema."""

    return await admit_nppes_public_evidence_chain(
        chain,
        schema=schema_name,
        database=ConnectionDatabase(connection),
    )


def alternate_source_record_values(
    replay: PreparedNppesRegistryReplay,
) -> tuple[object, ...]:
    """Build a valid but unadmitted source row for append-seal testing."""

    release = replay.manifest.release
    source_record = record_primitives.build_evidence_source_record_reference(
        release,
        {
            "record_kind": "nppes_registry_record",
            "identity_contract_id": (
                replay.manifest.identity.record_identity_contract_id
            ),
            "record_hmac_sha256": "5a" * 32,
            "payload_sha256": "6b" * 32,
        },
    )
    return source_record_values(release, source_record, replay.admission_row)


async def _assert_altered_admission_columns(
    connection: asyncpg.Connection,
    schema: str,
    altered_tables: tuple[str, ...],
) -> None:
    """Require exact admission ownership columns on the altered NPPES tables."""

    altered_columns = await connection.fetch(
        "SELECT relation.relname, attribute.attnotnull, "
        "format_type(attribute.atttypid, attribute.atttypmod) AS data_type "
        "FROM pg_class AS relation JOIN pg_namespace AS namespace "
        "ON namespace.oid=relation.relnamespace JOIN pg_attribute AS attribute "
        "ON attribute.attrelid=relation.oid WHERE namespace.nspname=$1 "
        "AND relation.relname=ANY($2::text[]) "
        "AND attribute.attname='nppes_admission_ref'",
        schema,
        list(altered_tables),
    )
    assert {
        altered_column_row["relname"] for altered_column_row in altered_columns
    } == set(altered_tables)
    assert all(
        altered_column_row["data_type"] == "character varying(50)"
        for altered_column_row in altered_columns
    )
    assert {
        altered_column_row["relname"]
        for altered_column_row in altered_columns
        if not altered_column_row["attnotnull"]
    } == {altered_tables[0]}


async def assert_admission_catalog(
    connection: asyncpg.Connection,
    schema: str,
    altered_tables: tuple[str, ...],
) -> None:
    """Require exact NPPES admission and ownership columns."""

    column_rows = await connection.fetch(
        "SELECT relation.relname, attribute.attname, "
        "format_type(attribute.atttypid, attribute.atttypmod) AS data_type, "
        "attribute.attnotnull, "
        "pg_get_expr(default_value.adbin, default_value.adrelid) AS default_expr "
        "FROM pg_class AS relation JOIN pg_namespace AS namespace "
        "ON namespace.oid=relation.relnamespace JOIN pg_attribute AS attribute "
        "ON attribute.attrelid=relation.oid LEFT JOIN pg_attrdef AS default_value "
        "ON default_value.adrelid=relation.oid AND default_value.adnum=attribute.attnum "
        "WHERE namespace.nspname=$1 AND relation.relname=ANY($2::text[]) "
        "AND attribute.attnum>0 AND NOT attribute.attisdropped "
        "ORDER BY relation.relname, attribute.attnum",
        schema,
        list(NEW_TABLES),
    )
    assert {
        table_name: tuple(
            column_row["attname"]
            for column_row in column_rows
            if column_row["relname"] == table_name
        )
        for table_name in NEW_TABLES
    } == EXPECTED_COLUMNS_BY_TABLE
    nullable_member_columns = {
        "entity_type_code",
        "provider_enumeration_date",
        "last_update_date",
        "npi_deactivation_date",
        "npi_reactivation_date",
        "exclusion_reason",
        "evidence_ref",
    }
    for column_row in column_rows:
        assert column_row["data_type"] == EXPECTED_TYPE_BY_COLUMN[
            column_row["attname"]
        ]
        assert column_row["attnotnull"] is (
            column_row["relname"] != NEW_TABLES[2]
            or column_row["attname"] not in nullable_member_columns
        )
        expected_default = (
            "transaction_timestamp()"
            if column_row["attname"] in {"created_at", "sealed_at"}
            else None
        )
        assert column_row["default_expr"] == expected_default
    await _assert_altered_admission_columns(connection, schema, altered_tables)


def qualified(schema_name: str, table_name: str) -> str:
    """Quote one test-owned schema relation."""

    return f'"{schema_name}"."{table_name}"'


__all__ = (
    "DEFAULT_ROWS",
    "EXPECTED_COLUMNS_BY_TABLE",
    "EXPECTED_TYPE_BY_COLUMN",
    "NEW_TABLES",
    "ConnectionDatabase",
    "admit_chain",
    "admit_replay",
    "alternate_source_record_values",
    "assert_admission_catalog",
    "finished_chain_receipt",
    "load_admission_migration",
    "nppes_admission_schema",
    "prepared_replay",
    "qualified",
    "required_config",
)
