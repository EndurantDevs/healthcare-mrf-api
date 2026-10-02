# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Permit immutable source bindings with explicit retained processing limits."""

from __future__ import annotations

import os

from alembic import op

revision = "20261002020000_custom_import_processing_policy"
down_revision = "20261002010000_custom_import_bounded_build"
branch_labels = None
depends_on = None

_TABLE = "custom_import_source_binding_revision"
_CHECK = "custom_import_source_binding_revision_shape_check"
_V1 = "custom-import/source-binding/v1"
_V2 = "custom-import/source-binding/v2"
_SHAPE_SQL = """binding_contract IN ({contracts})
            AND connector_kind = 'snowflake_bundle'
            AND revision_number > 0
            AND octet_length(definition_sha256) = 32
            AND octet_length(schema_sha256) = 32
            AND octet_length(source_object_fingerprint_sha256) = 32
            AND source_object_version ~ '^[A-Za-z0-9][A-Za-z0-9._-]{{0,127}}$'
            AND octet_length(canonical_binding) BETWEEN 2 AND 1048576
            AND octet_length(binding_sha256) = 32
            AND binding_sha256 = pg_catalog.sha256(
                convert_to(binding_contract || ':' || canonical_binding, 'UTF8'))"""


def _schema() -> str:
    runtime = os.getenv("HLTHPRT_DB_SCHEMA")
    legacy = os.getenv("DB_SCHEMA")
    if runtime and legacy and runtime != legacy:
        raise RuntimeError("DB_SCHEMA and HLTHPRT_DB_SCHEMA must match")
    return runtime or legacy or "mrf"


def _quote(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _replace_shape(table: str, contracts: tuple[str, ...]) -> None:
    shape = _SHAPE_SQL.format(contracts=", ".join(f"'{contract}'" for contract in contracts))
    op.execute(f"ALTER TABLE {table} DROP CONSTRAINT {_quote(_CHECK)}, ADD CONSTRAINT {_quote(_CHECK)} CHECK ({shape})")


def upgrade() -> None:
    """Expand only the binding version allowlist, retaining all identity checks."""

    _replace_shape(f"{_quote(_schema())}.{_quote(_TABLE)}", (_V1, _V2))


def downgrade() -> None:
    """Restore the legacy allowlist only when no retained v2 evidence exists."""

    table = f"{_quote(_schema())}.{_quote(_TABLE)}"
    op.execute(f"""DO $guard$ BEGIN
        IF EXISTS (SELECT 1 FROM {table} WHERE binding_contract = '{_V2}') THEN
            RAISE EXCEPTION 'custom_import_processing_policy_retention_required';
        END IF;
    END $guard$""")
    _replace_shape(table, (_V1,))
