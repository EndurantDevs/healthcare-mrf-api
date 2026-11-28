# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare the root Boolean index for an opted-in frozen import candidate."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from alembic import op

revision = "20261123182000_custom_import_root_boolean_index"
down_revision = "20261121050000_custom_import_rooted_text"
branch_labels = None
depends_on = None

_INDEX = {
    "phase": "serving",
    "name": "custom_import_root_scalar_true_idx",
    "table": "custom_import_root_scalar",
    "keys": ["root_revision_id", "field_slot"],
    "predicate": "boolean_value",
    "ddl": (
        "CREATE INDEX custom_import_root_scalar_true_idx ON __CANDIDATE__.custom_import_root_scalar "
        "(root_revision_id, field_slot) WHERE boolean_value"
    ),
}
_LOOP = "    FOR item IN SELECT value FROM jsonb_array_elements(specifications) WHERE value->>'phase'=p_phase LOOP"


def _names():
    path = Path(__file__).with_name("20261114010000_custom_import_snapshot_names.py")
    spec = importlib.util.spec_from_file_location("root_boolean_snapshot_names", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _schema():
    return _names()._schema()


def _body(names, source, schema, *, enabled):
    index = json.dumps([_INDEX])
    schedule = f"""
    IF p_phase='serving' AND EXISTS (
        SELECT 1 FROM {schema}.custom_import_definition_revision d
        WHERE d.dataset_id=f.dataset_id AND d.definition_revision_id=f.definition_revision_id
          AND d.schema_revision_id=f.schema_revision_id
          AND d.canonical_definition::jsonb->>'storage_profile'='root_boolean_true_v1'
    ) THEN
        specifications := specifications || $root_boolean_index${index}$root_boolean_index$::jsonb;
    END IF;
"""
    expanded = schedule + _LOOP
    return names._replace(source, _LOOP, expanded) if enabled else names._replace(source, expanded, _LOOP)


def _install(*, enabled):
    names = _names()
    schema = names._storage()._quote(_schema())
    current = (lambda source: "'root_boolean_true_v1'" in source) if enabled else None
    for name in ("prepare_custom_import_snapshot_indexes", "verify_custom_import_snapshot_indexes"):
        names._refresh(
            f"{schema}.{name}(bigint,text)",
            lambda source: _body(names, source, schema, enabled=enabled),
            current=current,
        )


def upgrade():
    """Add the Boolean index to the serving phase for the explicit profile."""
    _install(enabled=True)


def downgrade():
    """Restore the previous schedule and preserve existing indexes."""
    _install(enabled=False)
