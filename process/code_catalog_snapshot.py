# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Read a relation's owner and canonical ACL set so callers can replay existing SELECT access."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


class CodeCatalogSnapshotError(ValueError):
    """A code-catalog result is not safe to capture or replace."""


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
