# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic PostgreSQL store for canonical-NPI publication receipts."""

from __future__ import annotations

import datetime as dt
import json
import re
from typing import NamedTuple

from process.npi_canonical_publication import (
    NPI_CANONICAL_PUBLICATION_TABLE,
    NPI_CANONICAL_TABLES,
    NpiCanonicalPublicationInput,
    NpiCanonicalPublicationReceipt,
    build_npi_canonical_publication_receipt,
    publication_error,
    receipt_insert_values,
)


_IDENTIFIER_RE = re.compile(r"[a-z_][a-z0-9_]{0,62}", flags=re.ASCII)
_RECEIPT_OID_COLUMNS = (
    "npi_table_oid",
    "npi_address_table_oid",
    "npi_taxonomy_table_oid",
    "npi_taxonomy_group_table_oid",
    "npi_other_identifier_table_oid",
    "npi_phone_staffing_table_oid",
)
_RECEIPT_COUNT_COLUMNS = tuple(
    column_name.replace("table_oid", "row_count")
    for column_name in _RECEIPT_OID_COLUMNS
)
_RECEIPT_INSERT_COLUMNS = (
    "publication_ref",
    "contract",
    "contract_sha256",
    "run_id",
    "attempt_id",
    "attempt_started_at",
    "chain_ref",
    "import_date",
    *_RECEIPT_OID_COLUMNS,
    *_RECEIPT_COUNT_COLUMNS,
    "publication_state",
    "evidence_serving_authority",
    "evidence_publication_enabled",
)


class NpiCanonicalPublicationCommit(NamedTuple):
    receipt: NpiCanonicalPublicationReceipt
    heartbeat_at: str
    finished_at: str


def _database_timestamp(value: object) -> str:
    if type(value) is not dt.datetime:
        raise publication_error()
    fixed = value.replace(tzinfo=dt.UTC) if value.tzinfo is None else value.astimezone(dt.UTC)
    return fixed.isoformat(timespec="microseconds")


def _commit_projection(
    row: object,
    receipt: NpiCanonicalPublicationReceipt,
) -> NpiCanonicalPublicationCommit:
    try:
        run_id = row["run_id"]
        snapshot_id = row["snapshot_id"]
        heartbeat_at = _database_timestamp(row["heartbeat_at"])
        finished_at = _database_timestamp(row["finished_at"])
    except (KeyError, TypeError):
        raise publication_error() from None
    if run_id != receipt.run_id or snapshot_id != receipt.publication_ref:
        raise publication_error()
    return NpiCanonicalPublicationCommit(receipt, heartbeat_at, finished_at)


def _identifier(value: object) -> str:
    if type(value) is not str or _IDENTIFIER_RE.fullmatch(value) is None:
        raise publication_error()
    return value


def _qualified(schema: str, table: str) -> str:
    return f'"{_identifier(schema)}"."{_identifier(table)}"'


async def lock_npi_publication_attempt(
    connection: object,
    *,
    schema: str,
    run_id: str,
    attempt_id: str,
    attempt_started_at: str,
) -> None:
    """Lock and verify the one control attempt authorized to publish."""

    try:
        relation_row = await connection.fetchrow(
            f"SELECT importer, status, progress->>'attempt_id' AS attempt_id, "
            f"progress->>'attempt_started_at' AS attempt_started_at "
            f"FROM {_qualified(schema, 'import_run')} WHERE run_id=$1 FOR UPDATE",
            run_id,
        )
    except Exception:
        raise publication_error() from None
    if (
        relation_row is None
        or relation_row["importer"] != "npi"
        or relation_row["status"] != "running"
        or relation_row["attempt_id"] != attempt_id
        or relation_row["attempt_started_at"] != attempt_started_at
    ):
        raise publication_error()


async def canonical_relation_oids(
    connection: object,
    *,
    schema: str,
) -> tuple[int, ...]:
    """Read the six exact live relation identities after table rotation."""

    fixed_schema = _identifier(schema)
    try:
        relation_row = await connection.fetchrow(
            "SELECT " + ", ".join(
                "(SELECT relation.oid::bigint "
                "FROM pg_catalog.pg_class AS relation "
                "JOIN pg_catalog.pg_namespace AS namespace "
                "ON namespace.oid=relation.relnamespace "
                f"WHERE namespace.nspname=$1 AND relation.relname=${ordinal + 1} "
                "AND relation.relkind IN ('r','p')) "
                f"AS relation_{ordinal}"
                for ordinal in range(1, len(NPI_CANONICAL_TABLES) + 1)
            ),
            fixed_schema,
            *NPI_CANONICAL_TABLES,
        )
    except Exception:
        raise publication_error() from None
    if relation_row is None:
        raise publication_error()
    relation_oids = tuple(
        relation_row[f"relation_{ordinal}"] for ordinal in range(1, 7)
    )
    if any(
        type(relation_oid) is not int or relation_oid < 1
        for relation_oid in relation_oids
    ):
        raise publication_error()
    return relation_oids


def _stored_claim_values(stored_receipt_row: object) -> tuple[object, ...]:
    """Decode the exact persisted claims compared with a rebuilt receipt."""

    try:
        return (
            stored_receipt_row["publication_ref"],
            stored_receipt_row["contract"],
            bytes(stored_receipt_row["contract_sha256"]).hex(),
            stored_receipt_row["run_id"],
            stored_receipt_row["attempt_id"],
            stored_receipt_row["attempt_started_at"].astimezone(dt.UTC).isoformat(
                timespec="microseconds"
            ),
            stored_receipt_row["chain_ref"],
            stored_receipt_row["import_date"],
            tuple(stored_receipt_row[column_name] for column_name in _RECEIPT_OID_COLUMNS),
            tuple(stored_receipt_row[column_name] for column_name in _RECEIPT_COUNT_COLUMNS),
            stored_receipt_row["publication_state"],
            stored_receipt_row["evidence_serving_authority"],
            stored_receipt_row["evidence_publication_enabled"],
        )
    except (AttributeError, KeyError, TypeError, ValueError):
        raise publication_error() from None


def _rebuilt_claim_values(
    receipt: NpiCanonicalPublicationReceipt,
) -> tuple[object, ...]:
    """Return the persisted claims expected for one rebuilt receipt."""

    return (
        receipt.publication_ref,
        receipt.contract,
        receipt.contract_sha256,
        receipt.run_id,
        receipt.attempt_id,
        receipt.attempt_started_at,
        receipt.chain_ref,
        dt.date.fromisoformat(receipt.import_date),
        receipt.relation_oids,
        receipt.row_counts,
        receipt.publication_state,
        receipt.evidence_serving_authority,
        receipt.evidence_publication_enabled,
    )


def _stored_receipt(
    stored_receipt_row: object,
    publication_input: NpiCanonicalPublicationInput,
) -> NpiCanonicalPublicationReceipt:
    """Rebuild and exact-compare one inserted or replayed receipt row."""

    try:
        publication_generation = stored_receipt_row["publication_generation"]
        created_at = stored_receipt_row["created_at"]
    except (KeyError, TypeError):
        raise publication_error() from None
    if type(publication_generation) is not int:
        raise publication_error()
    if type(created_at) is not dt.datetime or created_at.tzinfo is None:
        raise publication_error()
    rebuilt = build_npi_canonical_publication_receipt(
        publication_input,
        publication_generation=publication_generation,
        created_at=created_at.astimezone(dt.UTC).isoformat(timespec="microseconds"),
    )
    if _stored_claim_values(stored_receipt_row) != _rebuilt_claim_values(rebuilt):
        raise publication_error()
    return rebuilt


async def _insert_receipt_row(
    connection: object,
    *,
    schema: str,
    insert_values: tuple[object, ...],
) -> object:
    placeholders = ", ".join(
        f"${ordinal}" for ordinal in range(1, len(_RECEIPT_INSERT_COLUMNS) + 1)
    )
    try:
        return await connection.fetchrow(
            f"INSERT INTO {_qualified(schema, NPI_CANONICAL_PUBLICATION_TABLE)} "
            f"({', '.join(_RECEIPT_INSERT_COLUMNS)}) VALUES ({placeholders}) "
            "ON CONFLICT (run_id) DO NOTHING RETURNING *",
            *insert_values,
        )
    except Exception:
        raise publication_error() from None


async def _existing_sealed_receipt_row(
    connection: object,
    *,
    schema: str,
    run_id: str,
) -> object:
    try:
        return await connection.fetchrow(
            f"SELECT receipt.*, EXISTS (SELECT 1 FROM "
            f"{_qualified(schema, NPI_CANONICAL_PUBLICATION_TABLE + '_seal')} "
            "AS sealed WHERE sealed.publication_ref=receipt.publication_ref) "
            "AS is_sealed FROM "
            f"{_qualified(schema, NPI_CANONICAL_PUBLICATION_TABLE)} AS receipt "
            "WHERE receipt.run_id=$1",
            run_id,
        )
    except Exception:
        raise publication_error() from None


async def insert_npi_publication_receipt(
    connection: object,
    *,
    schema: str,
    publication_input: NpiCanonicalPublicationInput,
) -> NpiCanonicalPublicationReceipt:
    """Insert and rebuild one generated immutable publication receipt."""

    if type(publication_input) is not NpiCanonicalPublicationInput:
        raise publication_error()
    provisional_receipt = build_npi_canonical_publication_receipt(
        publication_input,
        publication_generation=1,
        created_at=publication_input.attempt_started_at,
    )
    stored_receipt_row = await _insert_receipt_row(
        connection,
        schema=schema,
        insert_values=receipt_insert_values(provisional_receipt),
    )
    is_inserted = stored_receipt_row is not None
    if stored_receipt_row is None:
        stored_receipt_row = await _existing_sealed_receipt_row(
            connection,
            schema=schema,
            run_id=publication_input.run_id,
        )
    if (
        stored_receipt_row is None
        or (not is_inserted and stored_receipt_row["is_sealed"] is not True)
    ):
        raise publication_error()
    return _stored_receipt(stored_receipt_row, publication_input)


async def mark_npi_publication_succeeded(
    connection: object,
    *,
    schema: str,
    receipt: NpiCanonicalPublicationReceipt,
    progress_by_name: dict[str, object],
    metrics_by_name: dict[str, object],
) -> NpiCanonicalPublicationCommit:
    """Atomically mark the exact control attempt terminal with its receipt."""

    committed_progress_by_name = {
        **progress_by_name,
        "attempt_id": receipt.attempt_id,
        "attempt_started_at": receipt.attempt_started_at,
    }
    try:
        updated_run = await connection.fetchrow(
            f"UPDATE {_qualified(schema, 'import_run')} "
            "SET status='succeeded', phase_detail='npi published', "
            "heartbeat_at=transaction_timestamp() AT TIME ZONE 'UTC', "
            "finished_at=transaction_timestamp() AT TIME ZONE 'UTC', "
            "progress=$4::json, metrics=$5::json, error=NULL, snapshot_id=$6 "
            "WHERE run_id=$1 AND importer='npi' AND status='running' "
            "AND progress->>'attempt_id'=$2 "
            "AND progress->>'attempt_started_at'=$3 "
            "RETURNING run_id, snapshot_id, heartbeat_at, finished_at",
            receipt.run_id,
            receipt.attempt_id,
            receipt.attempt_started_at,
            json.dumps(
                committed_progress_by_name,
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            json.dumps(
                metrics_by_name,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ),
            receipt.publication_ref,
        )
    except Exception:
        raise publication_error() from None
    if updated_run is None:
        raise publication_error()
    return _commit_projection(updated_run, receipt)


async def load_committed_npi_publication(
    connection: object,
    *,
    schema: str,
    receipt: NpiCanonicalPublicationReceipt,
    progress_by_name: dict[str, object],
    metrics_by_name: dict[str, object],
) -> NpiCanonicalPublicationCommit | None:
    """Load an exact sealed commit after an ambiguous transaction outcome."""

    expected_progress_by_name = {
        **progress_by_name,
        "attempt_id": receipt.attempt_id,
        "attempt_started_at": receipt.attempt_started_at,
    }
    try:
        stored_receipt_row = await connection.fetchrow(
            f"SELECT receipt.*, run.snapshot_id, run.heartbeat_at, run.finished_at "
            f"FROM {_qualified(schema, NPI_CANONICAL_PUBLICATION_TABLE)} AS receipt "
            f"JOIN {_qualified(schema, NPI_CANONICAL_PUBLICATION_TABLE + '_seal')} "
            "AS sealed USING (publication_ref) "
            f"JOIN {_qualified(schema, 'import_run')} AS run USING (run_id) "
            "WHERE receipt.run_id=$1 AND run.importer='npi' "
            "AND run.status='succeeded' AND run.phase_detail='npi published' "
            "AND run.progress::jsonb=$2::jsonb AND run.metrics::jsonb=$3::jsonb "
            "AND run.snapshot_id=receipt.publication_ref",
            receipt.run_id,
            json.dumps(
                expected_progress_by_name,
                ensure_ascii=True,
                separators=(",", ":"),
            ),
            json.dumps(
                metrics_by_name,
                ensure_ascii=True,
                separators=(",", ":"),
                sort_keys=True,
            ),
        )
    except Exception:
        raise publication_error() from None
    if stored_receipt_row is None:
        return None
    rebuilt_receipt = _stored_receipt(
        stored_receipt_row,
        NpiCanonicalPublicationInput(
            receipt.run_id,
            receipt.attempt_id,
            receipt.attempt_started_at,
            receipt.chain_ref,
            receipt.import_date,
            receipt.relation_oids,
            receipt.row_counts,
        ),
    )
    if rebuilt_receipt != receipt:
        raise publication_error()
    return _commit_projection(stored_receipt_row, rebuilt_receipt)


async def has_settled_npi_publication(
    connection: object,
    *,
    schema: str,
    run_id: str,
) -> bool:
    """Wait for and report the publication transaction's control-row settlement."""

    try:
        settled_run_id = await connection.fetchval(
            f"SELECT run_id FROM {_qualified(schema, 'import_run')} "
            "WHERE run_id=$1 FOR UPDATE",
            run_id,
        )
    except Exception:
        raise publication_error() from None
    if settled_run_id is None:
        return False
    if settled_run_id != run_id:
        raise publication_error()
    return True


__all__ = (
    "NpiCanonicalPublicationCommit",
    "canonical_relation_oids",
    "insert_npi_publication_receipt",
    "load_committed_npi_publication",
    "lock_npi_publication_attempt",
    "has_settled_npi_publication",
    "mark_npi_publication_succeeded",
)
