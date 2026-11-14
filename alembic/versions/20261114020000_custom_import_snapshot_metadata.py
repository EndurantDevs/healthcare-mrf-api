# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Remove unused snapshot columns after the matching reader rollout."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261114020000_custom_import_snapshot_metadata"
down_revision = "20261114010000_custom_import_snapshot_names"
branch_labels = None
depends_on = None


def _names():
    path = Path(__file__).with_name("20261114010000_custom_import_snapshot_names.py")
    spec = importlib.util.spec_from_file_location("snapshot_metadata_names", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _schema():
    return _names()._schema()


def _drop_storage_metadata(schema):
    """Drop obsolete storage fields while preserving the logical slot constraint."""
    quoted = _names()._storage()._quote(schema)
    for table, constraints in (
        (
            "custom_import_snapshot_family",
            ("custom_import_snapshot_landing_shape", "custom_import_snapshot_origin_shape"),
        ),
        (
            "custom_import_snapshot_relation",
            (
                "custom_import_snapshot_relation_oid_key",
                "custom_import_snapshot_relation_shape",
                "custom_import_snapshot_layout_shape",
            ),
        ),
    ):
        for constraint in constraints:
            op.execute(f"ALTER TABLE {quoted}.{table} DROP CONSTRAINT IF EXISTS {constraint}")
    for table, columns in (
        (
            "custom_import_snapshot_family",
            tuple(
                f"{prefix}_{suffix}"
                for prefix in ("landing", "origin")
                for suffix in ("table_oid", "table_owner", "columns_sha256")
            ),
        ),
        ("custom_import_snapshot_relation", ("table_oid", "table_owner", "columns_sha256")),
    ):
        for column in columns:
            op.execute(f"ALTER TABLE {quoted}.{table} DROP COLUMN IF EXISTS {column}")
    op.create_check_constraint(
        "custom_import_snapshot_relation_shape",
        "custom_import_snapshot_relation",
        "relation_slot BETWEEN 1 AND 15",
        schema=schema,
    )
    op.execute(f"DROP FUNCTION {quoted}.custom_import_snapshot_columns_sha256(bigint)")


def _enable_family_creation(schema):
    """Reopen the existing creator after the matching reader rollout is complete."""
    names = _names()
    storage = names._storage()
    body = names._create_body(storage._CREATE_BODY, allow_new=True)
    rendered = " " + storage._sql(schema, storage._body(schema, body)) + " "
    names._refresh(
        f"{storage._quote(schema)}.create_custom_import_snapshot_family({names._AUTHORITY})",
        lambda _source: rendered,
    )


def upgrade():
    """The rollout applies this revision after all readers use qualified names."""
    names = _names()
    schema = _schema()
    quoted = names._storage()._quote(schema)
    op.execute(f"LOCK TABLE {quoted}.custom_import_snapshot_family IN ACCESS EXCLUSIVE MODE")
    names._check_remaining_references(schema)
    op.drop_column("source_bulk_authorization", "opened_by", schema=schema)
    _drop_storage_metadata(schema)
    for prefix in ("root", "child"):
        for suffix in ("table_oid", "table_owner", "columns_sha256"):
            op.execute(f"ALTER TABLE {quoted}.custom_import_query_projection DROP COLUMN IF EXISTS {prefix}_{suffix}")
    _enable_family_creation(schema)


def downgrade():
    """Discarded physical metadata cannot be restored by an automatic downgrade."""
    raise RuntimeError("custom_import_snapshot_metadata_requires_forward_migration")
