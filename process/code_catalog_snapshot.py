# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Native capture and replacement cutover for a completed code-catalog result.

The caller owns the repeatable-read transaction used for capture and the
transaction used for promotion.  A coordinator must run its normal import-idle
fence in the promotion transaction; this module does not infer importer state
or manufacture a historical import receipt.  The captured result is the
current, complete ``CodeCatalog`` table, including every code system.

This primitive is intentionally not registered by a source-scoped importer.
A full-table generation is valid only after every catalog writer participates
in the same admission and generation boundary.
"""

from __future__ import annotations

import hashlib
import importlib
import json
import re
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from sqlalchemy import Text as SQLText
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.schema import MetaData

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


@dataclass(frozen=True, order=True)
class _AccessGrant:
    column_name: str | None
    grantee_name: str | None
    privilege_type: str
    is_grantable: bool
    grantor_name: str


@dataclass(frozen=True)
class _RelationAccess:
    owner_name: str
    grants: tuple[_AccessGrant, ...]


_TABLE_PRIVILEGES = frozenset({"SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"})
_COLUMN_PRIVILEGES = frozenset({"SELECT", "INSERT", "UPDATE", "REFERENCES"})


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


async def _require_supported_relation_state(session: AsyncSession, relation_oid: int) -> None:
    """Reject relation state that an OID-changing cutover cannot preserve safely."""
    relation_state = (
        (
            await session.execute(
                text(
                    "SELECT relation.relreplident::text AS replica_identity, "
                    "EXISTS (SELECT 1 FROM pg_catalog.pg_trigger AS trigger_row "
                    "WHERE trigger_row.tgrelid=relation.oid AND NOT trigger_row.tgisinternal) AS has_triggers, "
                    "EXISTS (SELECT 1 FROM pg_catalog.pg_rewrite AS rewrite_row "
                    "WHERE rewrite_row.ev_class=relation.oid) AS has_rules, "
                    "EXISTS (SELECT 1 FROM pg_catalog.pg_policy AS policy_row "
                    "WHERE policy_row.polrelid=relation.oid) AS has_policies, "
                    "EXISTS (SELECT 1 FROM pg_catalog.pg_seclabel AS label_row "
                    "WHERE label_row.classoid='pg_class'::regclass AND label_row.objoid=relation.oid) AS has_security_labels, "
                    "(EXISTS (SELECT 1 FROM pg_catalog.pg_publication_rel AS publication_relation "
                    "WHERE publication_relation.prrelid=relation.oid) "
                    "OR EXISTS (SELECT 1 FROM pg_catalog.pg_publication AS publication WHERE publication.puballtables) "
                    "OR EXISTS (SELECT 1 FROM pg_catalog.pg_publication_namespace AS publication_namespace "
                    "WHERE publication_namespace.pnnspid=relation.relnamespace)) AS is_published "
                    "FROM pg_catalog.pg_class AS relation WHERE relation.oid=:relation_oid"
                ),
                {"relation_oid": relation_oid},
            )
        )
        .mappings()
        .one_or_none()
    )
    if relation_state is None or str(relation_state["replica_identity"]) != "d":
        raise CodeCatalogSnapshotError("code-catalog relation has unsupported replica identity")
    unsupported_by_kind = {
        "triggers": relation_state["has_triggers"],
        "rules": relation_state["has_rules"],
        "policies": relation_state["has_policies"],
        "security labels": relation_state["has_security_labels"],
        "publication membership": relation_state["is_published"],
    }
    present_kinds = tuple(name for name, is_present in unsupported_by_kind.items() if is_present)
    if present_kinds:
        raise CodeCatalogSnapshotError(f"code-catalog relation has unsupported {', '.join(present_kinds)}")


def _index_signature(index: Mapping[str, Any]) -> str:
    return json.dumps(dict(index), sort_keys=True, separators=(",", ":"), default=_receipt._json_scalar)


async def _is_matching_model_schema(
    session: AsyncSession,
    schema_name: str,
    capture: CodeCatalogCapture,
    *,
    has_normalized_descriptions: bool,
) -> bool:
    baseline_table = "ccmb_" + uuid4().hex
    metadata = MetaData()
    baseline = CodeCatalog.__table__.to_metadata(metadata, schema=None, name=baseline_table)
    baseline._prefixes.append("TEMPORARY")
    if has_normalized_descriptions:
        baseline.c.display_name.type = SQLText()
        baseline.c.short_description.type = SQLText()
    connection = await session.connection()
    await connection.run_sync(metadata.create_all)
    try:
        for ordinal, index_definition in enumerate(CodeCatalog.__my_additional_indexes__):
            elements = index_definition.get("index_elements")
            if not elements:
                raise CodeCatalogSnapshotError("code-catalog model index contract is invalid")
            index_name = f"ccmi_{ordinal}_{uuid4().hex}"
            await session.execute(
                text(f"CREATE INDEX {_quoted(index_name)} ON pg_temp.{_quoted(baseline_table)} ({', '.join(elements)})")
            )
        baseline_oid = await session.scalar(
            text(
                "SELECT relation.oid FROM pg_catalog.pg_class AS relation "
                "WHERE relation.relnamespace=pg_catalog.pg_my_temp_schema() AND relation.relname=:table_name"
            ),
            {"table_name": baseline_table},
        )
        if not isinstance(baseline_oid, int) or baseline_oid <= 0:
            raise CodeCatalogSnapshotError("code-catalog model baseline is unavailable")
        baseline_columns = await _receipt._catalog_columns(session, baseline_oid)
        observed_columns = await _receipt._catalog_columns(session, capture.relation_oid)
        baseline_constraints = await _receipt._catalog_constraints(session, baseline_oid, schema_name)
        observed_constraints = await _receipt._catalog_constraints(session, capture.relation_oid, schema_name)
        baseline_indexes = {_index_signature(index) for index in await _receipt._catalog_indexes(session, baseline_oid)}
        observed_indexes = {
            _index_signature(index) for index in await _receipt._catalog_indexes(session, capture.relation_oid)
        }
        return not (
            observed_columns != baseline_columns
            or observed_constraints != baseline_constraints
            or not baseline_indexes.issubset(observed_indexes)
        )
    finally:
        await session.execute(text(f"DROP TABLE pg_temp.{_quoted(baseline_table)}"))


async def _require_model_schema(session: AsyncSession, schema_name: str, capture: CodeCatalogCapture) -> None:
    """Require the model schema, including the importer-normalized text variant."""
    for has_normalized_descriptions in (False, True):
        if await _is_matching_model_schema(
            session,
            schema_name,
            capture,
            has_normalized_descriptions=has_normalized_descriptions,
        ):
            return
    raise CodeCatalogSnapshotError("code-catalog relation does not match the local model schema")


async def _catalog_grant_rows(session: AsyncSession, relation_oid: int):
    """Read the exact table and column ACL entries under the caller's relation lock."""
    return (
        (
            await session.execute(
                text(
                    "SELECT NULL::text AS column_name, grantee_role.rolname AS grantee_name, "
                    "acl.privilege_type, acl.is_grantable, grantor_role.rolname AS grantor_name, "
                    "acl.grantee=0 AS grantee_is_public "
                    "FROM pg_catalog.pg_class AS relation "
                    "CROSS JOIN LATERAL pg_catalog.aclexplode(COALESCE(relation.relacl, "
                    "pg_catalog.acldefault('r', relation.relowner))) AS acl "
                    "LEFT JOIN pg_catalog.pg_roles AS grantee_role ON grantee_role.oid=acl.grantee "
                    "JOIN pg_catalog.pg_roles AS grantor_role ON grantor_role.oid=acl.grantor "
                    "WHERE relation.oid=:relation_oid "
                    "UNION ALL "
                    "SELECT attribute.attname, grantee_role.rolname, acl.privilege_type, acl.is_grantable, "
                    "grantor_role.rolname, acl.grantee=0 "
                    "FROM pg_catalog.pg_class AS relation "
                    "JOIN pg_catalog.pg_attribute AS attribute ON attribute.attrelid=relation.oid "
                    "CROSS JOIN LATERAL pg_catalog.aclexplode(attribute.attacl) AS acl "
                    "LEFT JOIN pg_catalog.pg_roles AS grantee_role ON grantee_role.oid=acl.grantee "
                    "JOIN pg_catalog.pg_roles AS grantor_role ON grantor_role.oid=acl.grantor "
                    "WHERE relation.oid=:relation_oid AND attribute.attnum>0 AND NOT attribute.attisdropped"
                ),
                {"relation_oid": relation_oid},
            )
        )
        .mappings()
        .all()
    )


def _validated_access_grant(row: Mapping[str, Any], owner_name: str) -> _AccessGrant:
    """Accept only locally replayable privileges granted by the relation owner."""
    grantee_name = None if row["grantee_is_public"] else row["grantee_name"]
    privilege_type = str(row["privilege_type"])
    column_name = row["column_name"]
    allowed_privileges = _TABLE_PRIVILEGES if column_name is None else _COLUMN_PRIVILEGES
    if (
        (grantee_name is not None and not isinstance(grantee_name, str))
        or privilege_type not in allowed_privileges
        or not isinstance(row["grantor_name"], str)
        or row["grantor_name"] != owner_name
    ):
        raise CodeCatalogSnapshotError("code-catalog relation has unsupported grant state")
    return _AccessGrant(
        None if column_name is None else _identifier(column_name, field="grant column"),
        grantee_name,
        privilege_type,
        bool(row["is_grantable"]),
        row["grantor_name"],
    )


async def _relation_access(session: AsyncSession, relation_oid: int) -> _RelationAccess:
    """Capture the local owner and canonical ACL set for exact cutover preservation."""
    owner_name = await session.scalar(
        text(
            "SELECT owner_role.rolname FROM pg_catalog.pg_class AS relation "
            "JOIN pg_catalog.pg_roles AS owner_role ON owner_role.oid=relation.relowner "
            "WHERE relation.oid=:relation_oid"
        ),
        {"relation_oid": relation_oid},
    )
    if not isinstance(owner_name, str) or not owner_name:
        raise CodeCatalogSnapshotError("code-catalog relation owner is unavailable")
    grants = [_validated_access_grant(row, owner_name) for row in await _catalog_grant_rows(session, relation_oid)]
    return _RelationAccess(
        owner_name,
        tuple(
            sorted(
                grants,
                key=lambda grant: (
                    grant.column_name or "",
                    grant.grantee_name or "",
                    grant.privilege_type,
                    grant.is_grantable,
                    grant.grantor_name,
                ),
            )
        ),
    )


async def _require_stage_has_no_acl(session: AsyncSession, relation_oid: int) -> None:
    has_acl = await session.scalar(
        text(
            "SELECT relation.relacl IS NOT NULL OR EXISTS (SELECT 1 FROM pg_catalog.pg_attribute AS attribute "
            "WHERE attribute.attrelid=relation.oid AND attribute.attnum>0 AND NOT attribute.attisdropped "
            "AND attribute.attacl IS NOT NULL) FROM pg_catalog.pg_class AS relation WHERE relation.oid=:relation_oid"
        ),
        {"relation_oid": relation_oid},
    )
    if has_acl is not False:
        raise CodeCatalogSnapshotError("code-catalog restored stage has unsupported access grants")


async def _quoted_role(session: AsyncSession, role_name: str | None) -> str:
    if role_name is None:
        return "PUBLIC"
    quoted = await session.scalar(
        text("SELECT pg_catalog.format('%I', CAST(:role_name AS text))"),
        {"role_name": role_name},
    )
    if not isinstance(quoted, str) or not quoted:
        raise CodeCatalogSnapshotError("code-catalog grant role is unavailable")
    return quoted


async def _apply_relation_access(
    session: AsyncSession,
    schema_name: str,
    table_name: str,
    access: _RelationAccess,
) -> None:
    current_role = await session.scalar(text("SELECT current_user"))
    if current_role != access.owner_name:
        raise CodeCatalogSnapshotError("code-catalog incumbent is not owned by the local cutover role")
    owner = await _quoted_role(session, access.owner_name)
    await session.execute(
        text(f"REVOKE ALL PRIVILEGES ON TABLE {_quoted(schema_name)}.{_quoted(table_name)} FROM {owner}")
    )
    for grant in access.grants:
        grantee = await _quoted_role(session, grant.grantee_name)
        column = "" if grant.column_name is None else f" ({_quoted(grant.column_name)})"
        grant_option = " WITH GRANT OPTION" if grant.is_grantable else ""
        await session.execute(
            text(
                f"GRANT {grant.privilege_type}{column} ON TABLE "
                f"{_quoted(schema_name)}.{_quoted(table_name)} TO {grantee}{grant_option}"
            )
        )


async def _require_current_role_owns_access(session: AsyncSession, access: _RelationAccess) -> None:
    if await session.scalar(text("SELECT current_user")) != access.owner_name:
        raise CodeCatalogSnapshotError("code-catalog incumbent is not owned by the local cutover role")


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
    capture = await _table_receipt(session, schema, table_name)
    await _require_supported_relation_state(session, capture.relation_oid)
    await _require_model_schema(session, schema, capture)
    return capture


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
    await _require_stage_has_no_acl(session, observed.relation_oid)
    await _require_supported_relation_state(session, observed.relation_oid)
    await _require_model_schema(session, schema, observed)
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


async def _is_relation_absent(session: AsyncSession, schema_name: str, table_name: str) -> bool:
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


async def _validated_cutover_access(
    session: AsyncSession,
    schema: str,
    table_name: str,
    stage_table: str,
    incumbent_capture: CodeCatalogCapture,
    expected_stage_capture: CodeCatalogCapture,
) -> tuple[CodeCatalogCapture, _RelationAccess]:
    """Recheck both locked relations and obtain only replayable local access grants."""
    live_capture = await _table_receipt(session, schema, table_name)
    if live_capture != incumbent_capture:
        raise CodeCatalogSnapshotError("code-catalog incumbent changed before promotion")
    await _require_supported_relation_state(session, live_capture.relation_oid)
    await _require_no_foreign_key_dependents(session, live_capture.relation_oid)
    await _require_no_dependent_views(session, live_capture.relation_oid)
    stage_capture = await _table_receipt(session, schema, stage_table, semantic_table_name=table_name)
    if stage_capture != expected_stage_capture:
        raise CodeCatalogSnapshotError("code-catalog restored stage changed before promotion")
    await _require_current_role_owns_relation(session, stage_capture.relation_oid)
    await _require_stage_has_no_acl(session, stage_capture.relation_oid)
    await _require_supported_relation_state(session, stage_capture.relation_oid)
    await _require_model_schema(session, schema, stage_capture)
    await _require_no_foreign_key_dependents(session, stage_capture.relation_oid)
    incumbent_access = await _relation_access(session, live_capture.relation_oid)
    await _require_current_role_owns_access(session, incumbent_access)
    return stage_capture, incumbent_access


async def promote_code_catalog_restored_stage(
    session: AsyncSession,
    *,
    schema_name: str,
    stage_table_name: str,
    retained_table_name: str,
    incumbent_capture: CodeCatalogCapture,
    expected_stage_capture: CodeCatalogCapture,
    require_import_idle: Callable[[AsyncSession], Awaitable[None]],
) -> CodeCatalogCapture:
    """Atomically replace the catalog after caller-owned idle and incumbent fences.

    ``require_import_idle`` is supplied by the coordinator and must verify its
    normal import-idle rule without committing.  The old live relation is kept
    at ``retained_table_name``; this primitive never drops a predecessor.  The
    expected stage capture must be the exact OID and receipt returned by local
    validation, and both live and stage must be owned by the cutover role.

    The caller must use new stage and retained table names for every completed
    cycle.  PostgreSQL keeps index and constraint names when either table is
    renamed, so reusing a prior stage or retained name can make later local
    preparation collide even after the live table name is available again.
    """
    schema = _identifier(schema_name, field="schema")
    table_name = _table_name()
    stage_table = _identifier(stage_table_name, field="stage table")
    retained_table = _identifier(retained_table_name, field="retained table")
    if (
        not isinstance(incumbent_capture, CodeCatalogCapture)
        or not isinstance(expected_stage_capture, CodeCatalogCapture)
        or not callable(require_import_idle)
    ):
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
    if not await _is_relation_absent(session, schema, retained_table):
        raise CodeCatalogSnapshotError("code-catalog retained table already exists")
    stage_capture, incumbent_access = await _validated_cutover_access(
        session, schema, table_name, stage_table, incumbent_capture, expected_stage_capture
    )
    await session.execute(
        text(f"ALTER TABLE {_quoted(schema)}.{_quoted(table_name)} RENAME TO {_quoted(retained_table)}")
    )
    await session.execute(text(f"ALTER TABLE {_quoted(schema)}.{_quoted(stage_table)} RENAME TO {_quoted(table_name)}"))
    await _apply_relation_access(session, schema, table_name, incumbent_access)
    promoted_capture = await _table_receipt(session, schema, table_name)
    if promoted_capture != stage_capture:
        raise CodeCatalogSnapshotError("code-catalog promoted relation receipt changed")
    if await _relation_access(session, promoted_capture.relation_oid) != incumbent_access:
        raise CodeCatalogSnapshotError("code-catalog destination access grants changed")
    return promoted_capture
