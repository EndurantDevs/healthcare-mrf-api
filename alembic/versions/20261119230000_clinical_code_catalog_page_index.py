# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Index clinical code pages and public relationship counts."""

from __future__ import annotations

import os

from sqlalchemy import inspect, text

from alembic import op
from db.migration_index_adoption import IndexDefinition, _expected_index_shape, _shape_from_catalog
from db.migration_index_catalog import _index_catalog_record

revision = "20261119230000_clinical_code_catalog_page_index"
down_revision = "20261119090000_cms_npd_typed_release"
branch_labels = None
depends_on = None

INDEXES = (
    ("code_catalog", "code_catalog_type_page_idx", IndexDefinition(("code_type", "code_system", "code"))),
    (
        "code_relationship",
        "code_relationship_public_type_idx",
        IndexDefinition(
            ("relationship",),
            predicate="upper(from_system) <> 'SNOMEDCT_US' AND upper(to_system) <> 'SNOMEDCT_US'",
        ),
    ),
)


def _schema() -> str:
    return os.getenv("HLTHPRT_DB_SCHEMA") or os.getenv("DB_SCHEMA") or "mrf"


def _index_state(schema: str, table_name: str, index_name: str, definition: IndexDefinition) -> bool | None:
    """Identify an absent, usable, or interrupted build of this exact index."""

    index_record = _index_catalog_record(op, index_name, table_name, schema)
    if index_record is None:
        return None
    expected = _expected_index_shape(op, table_name, schema, definition, {})
    if _shape_from_catalog(index_record) != expected:
        raise RuntimeError(f"existing_schema_index_mismatch:{schema}.{index_name}")
    return all(index_record[field] for field in ("indisvalid", "indisready", "indislive"))


def _upgrade_index(schema: str, table_name: str, index_name: str, definition: IndexDefinition) -> None:
    context = op.get_context()
    if not context.as_sql and not inspect(op.get_bind()).has_table(table_name, schema=schema):
        return
    existing_state = None if context.as_sql else _index_state(schema, table_name, index_name, definition)
    if existing_state is True:
        return
    with context.autocommit_block():
        if existing_state is False:
            op.drop_index(
                index_name, table_name=table_name, schema=schema, if_exists=True, postgresql_concurrently=True
            )
        op.create_index(
            index_name,
            table_name,
            list(definition.key_columns),
            schema=schema,
            if_not_exists=True,
            postgresql_concurrently=True,
            postgresql_where=text(definition.predicate) if definition.predicate else None,
        )
    if not context.as_sql and _index_state(schema, table_name, index_name, definition) is not True:
        raise RuntimeError(f"required_index_missing:{schema}.{index_name}")


def upgrade() -> None:
    """Build the indexes concurrently and recover matching interrupted builds."""

    schema = _schema()
    for table_name, index_name, definition in INDEXES:
        _upgrade_index(schema, table_name, index_name, definition)


def downgrade() -> None:
    """Drop only these additive indexes without blocking catalog writes."""

    schema = _schema()
    context = op.get_context()
    for table_name, index_name, definition in reversed(INDEXES):
        if context.as_sql or _index_state(schema, table_name, index_name, definition) is not None:
            with context.autocommit_block():
                op.drop_index(
                    index_name, table_name=table_name, schema=schema, if_exists=True, postgresql_concurrently=True
                )
