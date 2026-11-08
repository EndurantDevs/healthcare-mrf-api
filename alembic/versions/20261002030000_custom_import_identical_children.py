# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Collapse explicitly opted-in identical children without dropping source evidence."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261002030000_custom_import_identical_children"
down_revision = "20261002020000_custom_import_processing_policy"
branch_labels = None
depends_on = None

_INDEX = "custom_import_build_final_child_idx"
_POLICY = "coalesce(definition_streams->(x.stream_slot-1)->>'duplicate_policy','reject')='collapse_identical'"
_FINAL_CHILD = """
            LEFT JOIN LATERAL (
                SELECT child.child_revision_id, child.canonical_payload
                FROM __SCHEMA__.custom_import_build_occurrence peer
                JOIN __SCHEMA__.custom_import_child_revision child ON child.child_revision_id=peer.child_revision_id
                WHERE __POLICY__ AND peer.build_id=x.build_id AND peer.origin='source'
                  AND peer.stream_slot=x.stream_slot AND peer.root_record_id=x.root_record_id
                  AND peer.collection_slot=x.collection_slot AND peer.raw_parent_key_sha256=x.raw_parent_key_sha256
                  AND peer.child_key_sha256=x.child_key_sha256 AND peer.child_revision_id IS NOT NULL
                ORDER BY peer.source_ordinal DESC LIMIT 1
            ) final_child ON true
""".replace("__POLICY__", _POLICY)

_DUPLICATE_CHECK = """ELSIF code IS NULL AND EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence x
                    WHERE x.build_id=b.build_id AND x.origin='source' AND x.record_kind='child'
                      AND x.root_record_id=o.root_record_id AND x.raw_parent_key_sha256=o.raw_parent_key_sha256 AND x.collection_slot=o.collection_slot
                      AND x.child_key_sha256=o.child_key_sha256 AND x.occurrence_id<>o.occurrence_id) THEN code:='duplicate_child_key';"""
_IDENTICAL_CHECK = (
    _DUPLICATE_CHECK.removesuffix(" code:='duplicate_child_key';")
    + """
                    IF coalesce(definition_streams->(o.stream_slot-1)->>'duplicate_policy','reject')<>'collapse_identical'
                       OR o.final_child_revision_id IS NULL OR EXISTS (
                        SELECT 1 FROM __SCHEMA__.custom_import_child_revision current_child
                        JOIN __SCHEMA__.custom_import_child_revision final_child
                          ON final_child.child_revision_id=o.final_child_revision_id
                        WHERE current_child.child_revision_id=o.child_revision_id
                          AND (current_child.payload_sha256 IS DISTINCT FROM final_child.payload_sha256
                            OR current_child.canonical_payload COLLATE "C" IS DISTINCT FROM final_child.canonical_payload COLLATE "C")
                    ) THEN code:='duplicate_child_key'; END IF;"""
)

_FINAL_OCCURRENCE = """
                AND (coalesce(definition_streams->(c.stream_slot-1)->>'duplicate_policy','reject')<>'collapse_identical'
                    OR NOT EXISTS (SELECT 1 FROM __SCHEMA__.custom_import_build_occurrence later
                        WHERE later.build_id=o.build_id AND later.origin='source' AND later.stream_slot=o.stream_slot
                          AND later.root_record_id=o.root_record_id AND later.collection_slot=o.collection_slot
                          AND later.raw_parent_key_sha256=o.raw_parent_key_sha256 AND later.child_key_sha256=o.child_key_sha256
                          AND later.child_revision_id IS NOT NULL AND later.source_ordinal>o.source_ordinal))
"""


def _legacy():
    path = Path(__file__).with_name("20261002010000_custom_import_bounded_build.py")
    spec = importlib.util.spec_from_file_location("identical_children_bounded_build", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _schema() -> str:
    return _legacy()._schema()


def _replace_once(body: str, old: str, new: str) -> str:
    if body.count(old) != 1:
        raise RuntimeError("custom import duplicate admission migration source differs")
    return body.replace(old, new, 1)


def _admission_body(legacy) -> str:
    body = _replace_once(
        legacy._ADMIT_CUSTOM_IMPORT_BUILD_PAGE_BODY,
        "root_hash bytea;",
        "root_hash bytea; definition_streams jsonb;",
    )
    body = _replace_once(
        body,
        "last_id:=b.admission_after_occurrence_id;",
        """SELECT canonical_definition::jsonb->'streams' INTO definition_streams
            FROM __SCHEMA__.custom_import_definition_revision WHERE definition_revision_id=b.definition_revision_id;
        last_id:=b.admission_after_occurrence_id;""",
    )
    body = _replace_once(
        body,
        "FOR o IN SELECT x.*,j.code initial_code,",
        "FOR o IN SELECT x.*,j.code initial_code,final_child.child_revision_id final_child_revision_id,",
    )
    body = _replace_once(
        body,
        "+coalesce(octet_length(child_key.canonical_child_key),0)*2 raw_bytes",
        "+coalesce(octet_length(child_key.canonical_child_key),0)*2"
        + "+coalesce(octet_length(final_child.canonical_payload),0)"
        + f"+CASE WHEN {_POLICY} THEN coalesce(octet_length(child_key.canonical_payload),0) ELSE 0 END raw_bytes",
    )
    body = _replace_once(
        body,
        "WHERE x.build_id=b.build_id AND x.origin='source' AND x.occurrence_id>last_id",
        _FINAL_CHILD + "            WHERE x.build_id=b.build_id AND x.origin='source' AND x.occurrence_id>last_id",
    )
    return _replace_once(body, _DUPLICATE_CHECK, _IDENTICAL_CHECK)


def _child_body(legacy) -> str:
    body = _replace_once(
        legacy._NEXT_CUSTOM_IMPORT_BUILD_CHILD_BODY,
        "c record; prior_name text;",
        "c record; prior_name text; definition_streams jsonb;",
    )
    body = _replace_once(
        body,
        "SELECT collection_name INTO prior_name",
        """SELECT canonical_definition::jsonb->'streams' INTO definition_streams
            FROM __SCHEMA__.custom_import_definition_revision WHERE definition_revision_id=b.definition_revision_id;
        SELECT collection_name INTO prior_name""",
    )
    body = _replace_once(
        body,
        "FOR c IN SELECT cc.collection_slot,cc.collection_name FROM __SCHEMA__.custom_import_child_collection cc",
        """FOR c IN SELECT cc.collection_slot,cc.collection_name,s.stream_slot
            FROM __SCHEMA__.custom_import_child_collection cc
            JOIN __SCHEMA__.custom_import_source_stream s ON s.definition_revision_id=b.definition_revision_id
              AND s.schema_revision_id=cc.schema_revision_id AND s.collection_slot=cc.collection_slot""",
    )
    return _replace_once(
        body,
        "ORDER BY o.child_key_sha256,o.child_revision_id LIMIT 1;",
        _FINAL_OCCURRENCE + "                ORDER BY o.child_key_sha256,o.child_revision_id LIMIT 1;",
    )


def _functions(schema: str, *, collapse: bool) -> None:
    legacy = _legacy()
    legacy._function(
        schema,
        "admit_custom_import_build_page",
        "p_build_id bigint, p_expected_after_id bigint",
        "TABLE(phase text, after_occurrence_id bigint, rows_processed integer, candidate_error_count bigint)",
        _admission_body(legacy) if collapse else legacy._ADMIT_CUSTOM_IMPORT_BUILD_PAGE_BODY,
    )
    legacy._function(
        schema,
        "next_custom_import_build_child",
        "p_build_id bigint, p_root_record_id bigint, p_slot smallint, p_key bytea, p_revision_id bigint",
        "TABLE(collection_slot smallint, child_key_sha256 bytea, child_revision_id bigint, pack_id bigint, raw_bytes bigint)",
        _child_body(legacy) if collapse else legacy._NEXT_CUSTOM_IMPORT_BUILD_CHILD_BODY,
    )


def upgrade() -> None:
    """Keep all source occurrences and select the final equal payload by source ordinal."""

    schema = _schema()
    quoted = _legacy()._q(schema)
    op.execute(f"""CREATE INDEX {_INDEX} ON {quoted}.custom_import_build_occurrence
        (build_id,stream_slot,root_record_id,collection_slot,raw_parent_key_sha256,child_key_sha256,source_ordinal DESC)
        WHERE origin='source' AND child_revision_id IS NOT NULL""")
    _functions(schema, collapse=True)


def downgrade() -> None:
    """Retain the policy while any immutable definition still declares it."""

    schema = _schema()
    quoted = _legacy()._q(schema)
    op.execute(f"""DO $guard$ BEGIN
        IF EXISTS (SELECT 1 FROM {quoted}.custom_import_definition_revision d,
            LATERAL jsonb_array_elements(d.canonical_definition::jsonb->'streams') stream
            WHERE stream->>'duplicate_policy'='collapse_identical') THEN
            RAISE EXCEPTION 'custom_import_identical_children_retention_required';
        END IF;
    END $guard$""")
    _functions(schema, collapse=False)
    op.execute(f"DROP INDEX {quoted}.{_INDEX}")
