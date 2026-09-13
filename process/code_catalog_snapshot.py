# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native capture and replacement cutover for a completed code-catalog result.

The caller owns the repeatable-read transaction used for capture and the
transaction used for promotion.  A coordinator must run its normal import-idle
fence in the promotion transaction; this module does not infer importer state
or manufacture a historical import receipt.  The captured result is the
current, complete ``CodeCatalog`` table, including every code system.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import CodeCatalog

_receipt = importlib.import_module("process.entity_address_snapshot_receipt")
CONTRACT = "code_catalog.postgres.v1"
RECEIPT_VERSION = "code_catalog_result_receipt.v1"
_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_SHA256 = re.compile(r"^[0-9a-f]{64}$")


class CodeCatalogSnapshotError(ValueError):
    """A code-catalog result is not safe to capture or replace."""


@dataclass(frozen=True)
class CodeCatalogResultReceipt:
    """Schema and multiset identity for the complete materialized catalog."""

    table_name: str
    schema_sha256: str
    row_count: int
    row_sha256: str

    def as_dict(self) -> dict[str, Any]:
        """Return a schema-name-independent semantic receipt."""
        return {
            "contract": CONTRACT,
            "receipt_version": RECEIPT_VERSION,
            "table_name": self.table_name,
            "schema_sha256": self.schema_sha256,
            "row_count": self.row_count,
            "row_sha256": self.row_sha256,
        }


@dataclass(frozen=True)
class CodeCatalogCapture:
    """Receipt plus local relation identity for one caller-held source lock."""

    receipt: CodeCatalogResultReceipt
    relation_oid: int


def _identifier(value: object, *, field: str) -> str:
    if not isinstance(value, str) or _IDENTIFIER.fullmatch(value) is None:
        raise CodeCatalogSnapshotError(f"code-catalog {field} is invalid")
    return value


def _quoted(value: str) -> str:
    return f'"{value}"'


def _canonical_digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("ascii")
    ).hexdigest()


def _table_name() -> str:
    table_name = CodeCatalog.__tablename__
    return _identifier(table_name, field="model table")


async def _normalize_session(session: AsyncSession, schema_name: str) -> None:
    if not session.in_transaction():
        raise CodeCatalogSnapshotError("code-catalog snapshot requires a caller transaction")
    try:
        await _receipt._normalize_receipt_session(session, schema_name)
    except _receipt.EntityAddressArchiveReceiptError as exc:
        raise CodeCatalogSnapshotError("code-catalog snapshot session is invalid") from exc


async def _relation_oid(session: AsyncSession, schema_name: str, table_name: str) -> int:
    try:
        return await _receipt._relation_oid(session, schema_name, table_name)
    except _receipt.EntityAddressArchiveReceiptError as exc:
        raise CodeCatalogSnapshotError("code-catalog relation is unavailable") from exc


async def _table_receipt(
    session: AsyncSession,
    schema_name: str,
    relation_name: str,
    *,
    semantic_table_name: str | None = None,
) -> CodeCatalogCapture:
    """Read one physical relation under the stable model-table receipt name."""
    table_name = semantic_table_name or relation_name
    relation_oid = await _relation_oid(session, schema_name, relation_name)
    try:
        schema_sha256 = await _receipt._schema_identity(session, relation_oid, schema_name, table_name)
        row_count, row_sha256 = await _receipt._row_identity(session, schema_name, relation_name)
    except _receipt.EntityAddressArchiveReceiptError as exc:
        raise CodeCatalogSnapshotError("code-catalog relation receipt is invalid") from exc
    return CodeCatalogCapture(CodeCatalogResultReceipt(table_name, schema_sha256, row_count, row_sha256), relation_oid)


def validate_code_catalog_result_receipt(
    receipt: Mapping[str, Any] | CodeCatalogResultReceipt,
) -> CodeCatalogResultReceipt:
    """Validate persisted semantic metadata before it controls a local cutover."""
    receipt_value = receipt.as_dict() if isinstance(receipt, CodeCatalogResultReceipt) else receipt
    expected_table = _table_name()
    if (
        not isinstance(receipt_value, Mapping)
        or set(receipt_value)
        != {"contract", "receipt_version", "table_name", "schema_sha256", "row_count", "row_sha256"}
        or receipt_value["contract"] != CONTRACT
        or receipt_value["receipt_version"] != RECEIPT_VERSION
        or receipt_value["table_name"] != expected_table
        or type(receipt_value["row_count"]) is not int
        or receipt_value["row_count"] < 0
        or any(_SHA256.fullmatch(str(receipt_value[field])) is None for field in ("schema_sha256", "row_sha256"))
    ):
        raise CodeCatalogSnapshotError("code-catalog result receipt is invalid")
    return CodeCatalogResultReceipt(
        table_name=expected_table,
        schema_sha256=str(receipt_value["schema_sha256"]),
        row_count=receipt_value["row_count"],
        row_sha256=str(receipt_value["row_sha256"]),
    )


async def capture_code_catalog_result(
    session: AsyncSession,
    *,
    schema_name: str,
) -> CodeCatalogCapture:
    """Capture the entire model-owned catalog while holding a SHARE table lock.

    The caller must keep the transaction open through any native export that
    relies on this receipt.  This function does not create a source generation
    or assert any historical importer provenance.
    """
    schema = _identifier(schema_name, field="schema")
    table_name = _table_name()
    await _normalize_session(session, schema)
    await session.execute(text(f"LOCK TABLE {_quoted(schema)}.{_quoted(table_name)} IN SHARE MODE"))
    return await _table_receipt(session, schema, table_name)


async def validate_code_catalog_restored_stage(
    session: AsyncSession,
    *,
    schema_name: str,
    stage_table_name: str,
    expected_receipt: Mapping[str, Any] | CodeCatalogResultReceipt,
) -> CodeCatalogCapture:
    """Require a locally restored stage to match the reviewed source receipt."""
    schema = _identifier(schema_name, field="schema")
    stage_table = _identifier(stage_table_name, field="stage table")
    if stage_table == _table_name():
        raise CodeCatalogSnapshotError("code-catalog stage must not be the live table")
    expected = validate_code_catalog_result_receipt(expected_receipt)
    await _normalize_session(session, schema)
    await session.execute(text(f"LOCK TABLE {_quoted(schema)}.{_quoted(stage_table)} IN SHARE MODE"))
    observed = await _table_receipt(session, schema, stage_table, semantic_table_name=_table_name())
    await _require_current_role_owns_relation(session, observed.relation_oid)
    if observed.receipt != expected:
        raise CodeCatalogSnapshotError("code-catalog restored stage does not match its receipt")
    return observed


async def _foreign_key_dependents(session: AsyncSession, relation_oid: int) -> tuple[str, ...]:
    rows = (
        await session.execute(
            text(
                "SELECT pg_catalog.format('%I.%I', namespace_row.nspname, relation.relname) AS dependent_relation "
                "FROM pg_catalog.pg_constraint AS constraint_row "
                "JOIN pg_catalog.pg_class AS relation ON relation.oid=constraint_row.conrelid "
                "JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid=relation.relnamespace "
                "WHERE constraint_row.contype='f' AND constraint_row.confrelid=:relation_oid "
                "ORDER BY dependent_relation"
            ),
            {"relation_oid": relation_oid},
        )
    ).scalars()
    return tuple(str(row) for row in rows)


async def _require_no_foreign_key_dependents(session: AsyncSession, relation_oid: int) -> None:
    dependents = await _foreign_key_dependents(session, relation_oid)
    if dependents:
        raise CodeCatalogSnapshotError("code-catalog replacement has foreign-key dependents")


async def _require_no_dependent_views(session: AsyncSession, relation_oid: int) -> None:
    """Reject rewrite-rule consumers that PostgreSQL would bind to the predecessor."""
    dependent_views = tuple(
        str(row)
        for row in (
            await session.execute(
                text(
                    "SELECT DISTINCT pg_catalog.format('%I.%I', namespace_row.nspname, relation.relname) "
                    "FROM pg_catalog.pg_depend AS dependency "
                    "JOIN pg_catalog.pg_rewrite AS rewrite_rule ON rewrite_rule.oid=dependency.objid "
                    "JOIN pg_catalog.pg_class AS relation ON relation.oid=rewrite_rule.ev_class "
                    "JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid=relation.relnamespace "
                    "WHERE dependency.refclassid='pg_class'::regclass AND dependency.refobjid=:relation_oid "
                    "AND dependency.classid='pg_rewrite'::regclass AND relation.relkind IN ('v', 'm') "
                    "ORDER BY 1"
                ),
                {"relation_oid": relation_oid},
            )
        ).scalars()
    )
    if dependent_views:
        raise CodeCatalogSnapshotError("code-catalog replacement has dependent views")


async def _relation_absent(session: AsyncSession, schema_name: str, table_name: str) -> bool:
    return (
        await session.scalar(
            text(
                "SELECT NOT EXISTS (SELECT 1 FROM pg_catalog.pg_class AS relation "
                "JOIN pg_catalog.pg_namespace AS namespace_row ON namespace_row.oid=relation.relnamespace "
                "WHERE namespace_row.nspname=:schema_name AND relation.relname=:table_name)"
            ),
            {"schema_name": schema_name, "table_name": table_name},
        )
    ) is True


async def _require_current_role_owns_relation(session: AsyncSession, relation_oid: int) -> None:
    """Reject a peer-owned stage rather than treating restored SQL as trusted."""
    is_current_role_owner = await session.scalar(
        text(
            "SELECT relation.relowner = (SELECT role.oid FROM pg_catalog.pg_roles AS role "
            "WHERE role.rolname = current_user) FROM pg_catalog.pg_class AS relation "
            "WHERE relation.oid=:relation_oid"
        ),
        {"relation_oid": relation_oid},
    )
    if is_current_role_owner is not True:
        raise CodeCatalogSnapshotError("code-catalog restored stage is not owned by the local role")


async def promote_code_catalog_restored_stage(
    session: AsyncSession,
    *,
    schema_name: str,
    stage_table_name: str,
    retained_table_name: str,
    incumbent_capture: CodeCatalogCapture,
    require_import_idle: Callable[[AsyncSession], Awaitable[None]],
) -> CodeCatalogCapture:
    """Atomically replace the catalog after caller-owned idle and incumbent fences.

    ``require_import_idle`` is supplied by the coordinator and must verify its
    normal import-idle rule without committing.  The old live relation is kept
    at ``retained_table_name``; this primitive never drops a predecessor.
    """
    schema = _identifier(schema_name, field="schema")
    table_name = _table_name()
    stage_table = _identifier(stage_table_name, field="stage table")
    retained_table = _identifier(retained_table_name, field="retained table")
    if not isinstance(incumbent_capture, CodeCatalogCapture) or not callable(require_import_idle):
        raise CodeCatalogSnapshotError("code-catalog promotion contract is invalid")
    if len({table_name, stage_table, retained_table}) != 3:
        raise CodeCatalogSnapshotError("code-catalog promotion table names must be distinct")
    await _normalize_session(session, schema)
    await session.execute(
        text(
            f"LOCK TABLE {_quoted(schema)}.{_quoted(table_name)}, {_quoted(schema)}.{_quoted(stage_table)} "
            "IN ACCESS EXCLUSIVE MODE"
        )
    )
    await require_import_idle(session)
    if not await _relation_absent(session, schema, retained_table):
        raise CodeCatalogSnapshotError("code-catalog retained table already exists")
    live_capture = await _table_receipt(session, schema, table_name)
    if live_capture != incumbent_capture:
        raise CodeCatalogSnapshotError("code-catalog incumbent changed before promotion")
    await _require_no_foreign_key_dependents(session, live_capture.relation_oid)
    await _require_no_dependent_views(session, live_capture.relation_oid)
    stage_capture = await _table_receipt(session, schema, stage_table, semantic_table_name=table_name)
    await _require_current_role_owns_relation(session, stage_capture.relation_oid)
    await _require_no_foreign_key_dependents(session, stage_capture.relation_oid)
    await session.execute(
        text(f"ALTER TABLE {_quoted(schema)}.{_quoted(table_name)} RENAME TO {_quoted(retained_table)}")
    )
    await session.execute(text(f"ALTER TABLE {_quoted(schema)}.{_quoted(stage_table)} RENAME TO {_quoted(table_name)}"))
    promoted_capture = await _table_receipt(session, schema, table_name)
    if promoted_capture != stage_capture:
        raise CodeCatalogSnapshotError("code-catalog promoted relation receipt changed")
    return promoted_capture
