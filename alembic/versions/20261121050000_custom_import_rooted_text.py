# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Prepare a text covering index for an opted-in frozen import candidate."""

from __future__ import annotations

import importlib.util
import json
from pathlib import Path

from alembic import op

revision = "20261121050000_custom_import_rooted_text"
down_revision = "20261119230000_clinical_code_catalog_page_index"
branch_labels = None
depends_on = None

_INDEX = {
    "phase": "serving",
    "name": "custom_import_child_scalar_rooted_text_idx",
    "table": "custom_import_child_scalar",
    "keys": [
        "schema_revision_id",
        "collection_slot",
        "field_slot",
        "root_record_id",
        "string_value",
        "dataset_id",
        "child_revision_id",
    ],
    "key_count": 5,
    "predicate": "value_state='value'ANDstring_valueISNOTNULL",
    "ddl": (
        "CREATE INDEX custom_import_child_scalar_rooted_text_idx "
        "ON __CANDIDATE__.custom_import_child_scalar "
        "(schema_revision_id, collection_slot, field_slot, root_record_id, string_value) "
        "INCLUDE (dataset_id, child_revision_id) "
        "WHERE value_state = 'value' AND string_value IS NOT NULL"
    ),
}
_LOOP = "    FOR item IN SELECT value FROM jsonb_array_elements(specifications) WHERE value->>'phase'=p_phase LOOP"
_COLUMN_COUNT = "AND i.indnatts=cardinality(expected_keys) AND i.indexprs IS NULL"
_KEY_OPTIONS = "FROM jsonb_array_elements_text(item->'keys') WITH ORDINALITY AS keys(value,ordinal) ORDER BY ordinal)"


def _names():
    path = Path(__file__).with_name("20261114010000_custom_import_snapshot_names.py")
    spec = importlib.util.spec_from_file_location("rooted_text_snapshot_names", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _schema():
    return _names()._schema()


def _body(names, source, schema, *, enabled):
    # A space after each JSON key keeps numeric values from becoming SQLAlchemy bind names.
    specification = json.dumps([_INDEX])
    profile = f"""
    IF p_phase='serving' AND EXISTS (
        SELECT 1 FROM {schema}.custom_import_definition_revision d
        WHERE d.dataset_id=f.dataset_id AND d.definition_revision_id=f.definition_revision_id
          AND d.schema_revision_id=f.schema_revision_id
          AND d.canonical_definition::jsonb->>'storage_profile'='rooted_child_text_v1'
    ) THEN
        specifications := specifications || $rooted_text_index${specification}$rooted_text_index$::jsonb;
    END IF;
"""
    changes = (
        (_LOOP, profile + _LOOP),
        (
            _COLUMN_COUNT,
            _COLUMN_COUNT
            + "\n                  AND i.indnkeyatts=coalesce((item->>'key_count')::integer,cardinality(expected_keys))",
        ),
        (_KEY_OPTIONS, _KEY_OPTIONS.replace(" ORDER BY", " WHERE ordinal<=i.indnkeyatts ORDER BY")),
    )
    for before, after in changes:
        source = names._replace(source, before if enabled else after, after if enabled else before)
    return source


def _install(*, enabled):
    names = _names()
    schema = names._storage()._quote(_schema())
    current = (lambda source: "'rooted_child_text_v1'" in source) if enabled else None
    for name in ("prepare_custom_import_snapshot_indexes", "verify_custom_import_snapshot_indexes"):
        names._refresh(
            f"{schema}.{name}(bigint,text)",
            lambda source: _body(names, source, schema, enabled=enabled),
            current=current,
        )


def upgrade():
    """Update the existing fenced routines. They build indexes on demand."""
    _install(enabled=True)


def downgrade():
    """Restore the previous schedule while retaining any built indexes."""
    _install(enabled=False)
