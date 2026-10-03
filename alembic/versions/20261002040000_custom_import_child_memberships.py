# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate declared sibling membership before selecting source or retained families."""

from __future__ import annotations

import importlib.util
from pathlib import Path

from alembic import op

revision = "20261002040000_custom_import_child_memberships"
down_revision = "20261002030000_custom_import_identical_children"
branch_labels = None
depends_on = None

_INDEX = "custom_import_child_membership_key_idx"
_CURSORS = ("plan_membership_after_collection_slot", "plan_membership_after_child_revision_id")
_KEY_BODY = r"""
    DECLARE mapping jsonb; entry jsonb; parts text[]:=ARRAY[]::text[];
    BEGIN
        FOR mapping IN SELECT value FROM jsonb_array_elements(p_membership->'key_mapping') LOOP
            SELECT value->'value' INTO entry FROM jsonb_array_elements(p_child_key::jsonb->'fields')
                WHERE value->>'field'=mapping->>'inner_field';
            IF entry IS NULL OR entry->>'state'<>'value' OR entry->>'type' NOT IN ('string','integer') THEN
                RAISE EXCEPTION 'custom_import_build_structure_mismatch: membership key differs'; END IF;
            parts:=array_append(parts,'{"field":'||to_json(mapping->>'outer_field')::text
                ||',"value":{"state":"value","type":'||to_json(entry->>'type')::text
                ||',"value":'||(entry->'value')::text||'}}');
        END LOOP;
        RETURN '{"contract":"custom-import-key/v1","fields":['||array_to_string(parts,',')||']}';
    END;
"""
_MISSING_BODY = r"""
    DECLARE b __SCHEMA__.custom_import_build_attempt; membership jsonb; collection_name text;
        outer_slot smallint; expected_key text; expected_hash bytea; actual_key text;
    BEGIN
        SELECT * INTO b FROM __SCHEMA__.custom_import_build_attempt WHERE build_id=p_build_id;
        SELECT c.collection_name INTO collection_name FROM __SCHEMA__.custom_import_child_collection c
            WHERE c.schema_revision_id=b.schema_revision_id AND c.collection_slot=p_collection_slot;
        FOR membership IN SELECT value FROM jsonb_array_elements(p_memberships)
            WHERE value->>'inner_collection'=collection_name
        LOOP
            SELECT c.collection_slot INTO outer_slot FROM __SCHEMA__.custom_import_child_collection c
                WHERE c.schema_revision_id=b.schema_revision_id AND c.collection_name=membership->>'outer_collection';
            expected_key:=__SCHEMA__.custom_import_membership_key(membership,p_child_key);
            expected_hash:=sha256(decode('637573746f6d2d696d706f72742f76310063616e6469646174652d72756e6e65722f31006368696c642d6b657900','hex')
                ||convert_to(expected_key,'UTF8'));
            IF p_family_id IS NULL THEN
                SELECT c.canonical_child_key INTO actual_key FROM __SCHEMA__.custom_import_build_occurrence x
                    JOIN __SCHEMA__.custom_import_child_revision c ON c.child_revision_id=x.child_revision_id
                    WHERE x.build_id=b.build_id AND x.origin='source' AND x.root_record_id=p_root_record_id
                      AND x.collection_slot=outer_slot AND x.child_key_sha256=expected_hash
                      AND x.child_revision_id IS NOT NULL
                    ORDER BY x.child_revision_id LIMIT 1;
            ELSE
                SELECT c.canonical_child_key INTO actual_key FROM __SCHEMA__.custom_import_child_revision c
                    JOIN __SCHEMA__.custom_import_family_child fc ON fc.family_revision_id=p_family_id
                      AND fc.collection_slot=outer_slot AND fc.child_revision_id=c.child_revision_id
                    WHERE c.schema_revision_id=b.schema_revision_id AND c.root_record_id=p_root_record_id
                      AND c.collection_slot=outer_slot AND c.child_key_sha256=expected_hash
                    ORDER BY c.child_revision_id LIMIT 1;
            END IF;
            IF actual_key IS NULL THEN RETURN true; END IF;
            IF actual_key COLLATE "C" IS DISTINCT FROM expected_key COLLATE "C" THEN
                RAISE EXCEPTION 'custom_import_build_structure_mismatch: membership key digest collision'; END IF;
        END LOOP;
        RETURN false;
    END;
"""
_RETAINED_CHECK = r"""
                IF memberships<>'[]'::jsonb THEN
                    FOR member IN SELECT fc.collection_slot,fc.child_revision_id,
                        octet_length(c.canonical_child_key)*3+octet_length(memberships::text)*2 raw_bytes
                        FROM __SCHEMA__.custom_import_family_child fc
                        JOIN __SCHEMA__.custom_import_child_revision c ON c.child_revision_id=fc.child_revision_id
                        WHERE fc.family_revision_id=base_family
                          AND (fc.collection_slot,fc.child_revision_id)>
                            (b.plan_membership_after_collection_slot,b.plan_membership_after_child_revision_id)
                        ORDER BY fc.collection_slot,fc.child_revision_id LIMIT b.page_row_limit-n
                    LOOP
                        IF member.raw_bytes>b.page_byte_limit THEN RAISE EXCEPTION 'custom_import_build_page_too_large'; END IF;
                        EXIT WHEN n>0 AND bytes+member.raw_bytes>b.page_byte_limit;
                        n:=n+1; bytes:=bytes+member.raw_bytes;
                        SELECT canonical_child_key INTO child_key FROM __SCHEMA__.custom_import_child_revision
                            WHERE child_revision_id=member.child_revision_id;
                        IF __SCHEMA__.custom_import_child_membership_missing(b.build_id,base_family,r.root_record_id,
                            member.collection_slot,child_key,memberships) THEN
                            RAISE EXCEPTION 'custom_import_build_structure_mismatch: retained family violates child membership'; END IF;
                        b.plan_membership_after_collection_slot:=member.collection_slot;
                        b.plan_membership_after_child_revision_id:=member.child_revision_id;
                    END LOOP;
                    IF EXISTS(SELECT 1 FROM __SCHEMA__.custom_import_family_child fc WHERE fc.family_revision_id=base_family
                        AND (fc.collection_slot,fc.child_revision_id)>
                          (b.plan_membership_after_collection_slot,b.plan_membership_after_child_revision_id)) THEN
                        b.plan_after_base_root_record_id:=after_root;
                        EXIT;
                    END IF;
                    b.plan_membership_after_collection_slot:=0; b.plan_membership_after_child_revision_id:=0;
                END IF;
"""


def _previous():
    path = Path(__file__).with_name("20261002030000_custom_import_identical_children.py")
    spec = importlib.util.spec_from_file_location("child_memberships_identical_children", path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.op = op
    return module


def _schema() -> str:
    return _previous()._schema()


def _replace(body: str, old: str, new: str) -> str:
    if body.count(old) != 1:
        raise RuntimeError("custom import membership migration source differs")
    return body.replace(old, new, 1)


def _admission_body(previous, legacy) -> str:
    body = _replace(
        previous._admission_body(legacy), "definition_streams jsonb;", "definition_streams jsonb; memberships jsonb;"
    )
    body = _replace(
        body,
        "last_id:=b.admission_after_occurrence_id;",
        """SELECT coalesce(canonical_definition::jsonb->'child_memberships','[]'::jsonb) INTO memberships
            FROM __SCHEMA__.custom_import_definition_revision WHERE definition_revision_id=b.definition_revision_id;
        last_id:=b.admission_after_occurrence_id;""",
    )
    body = _replace(
        body,
        "FOR o IN SELECT x.*,j.code initial_code,",
        "FOR o IN SELECT x.*,j.code initial_code,child_key.canonical_child_key membership_key,",
    )
    body = _replace(
        body,
        "END raw_bytes",
        "END+CASE WHEN memberships<>'[]'::jsonb AND child_key.child_revision_id IS NOT NULL "
        "THEN octet_length(child_key.canonical_child_key)*3+octet_length(memberships::text)*2 ELSE 0 END raw_bytes",
    )
    return _replace(
        body,
        "IF o.child_revision_id IS NOT NULL THEN",
        """IF code IS NULL AND o.child_revision_id IS NOT NULL AND memberships<>'[]'::jsonb
                    AND __SCHEMA__.custom_import_child_membership_missing(b.build_id,NULL::bigint,o.root_record_id,
                        o.collection_slot,o.membership_key,memberships) THEN code:='child_membership_missing'; END IF;
                IF o.child_revision_id IS NOT NULL THEN""",
    )


def _planner_body(legacy) -> str:
    body = _replace(
        legacy._PLAN_CUSTOM_IMPORT_BUILD_FAMILY_PAGE_BODY,
        "base_schema bigint;",
        "base_schema bigint; memberships jsonb; member record; child_key text; after_root bigint; before_n integer; bytes bigint:=0;",
    )
    body = _replace(
        body,
        "WHILE n<b.page_row_limit LOOP",
        """SELECT coalesce(canonical_definition::jsonb->'child_memberships','[]'::jsonb) INTO memberships
            FROM __SCHEMA__.custom_import_definition_revision WHERE definition_revision_id=b.definition_revision_id;
        WHILE n<b.page_row_limit LOOP""",
    )
    body = _replace(body, "n:=n+1;", "before_n:=n; after_root:=b.plan_after_base_root_record_id;")
    body = _replace(
        body,
        "IF b.plan_stage='source' AND base_family IS NOT NULL THEN CONTINUE; END IF;",
        "IF b.plan_stage='source' AND base_family IS NOT NULL THEN n:=n+1; CONTINUE; END IF;",
    )
    body = _replace(
        body,
        "INSERT INTO __SCHEMA__.custom_import_build_family(build_id,root_record_id,root_key_sha256,selection_kind,base_family_revision_id)",
        _RETAINED_CHECK
        + "                INSERT INTO __SCHEMA__.custom_import_build_family(build_id,root_record_id,root_key_sha256,selection_kind,base_family_revision_id)",
    )
    body = _replace(
        body,
        "END LOOP;\n        IF n=0 THEN",
        "IF n=before_n THEN n:=n+1; END IF;\n        END LOOP;\n        IF n=0 THEN",
    )
    return _replace(
        body,
        "plan_after_base_root_record_id=b.plan_after_base_root_record_id,",
        "plan_membership_after_collection_slot=b.plan_membership_after_collection_slot,"
        "plan_membership_after_child_revision_id=b.plan_membership_after_child_revision_id,\n            "
        "plan_after_base_root_record_id=b.plan_after_base_root_record_id,",
    )


def _functions(schema: str, *, enabled: bool) -> None:
    previous = _previous()
    legacy = previous._legacy()
    legacy._function(
        schema,
        "admit_custom_import_build_page",
        "p_build_id bigint, p_expected_after_id bigint",
        "TABLE(phase text, after_occurrence_id bigint, rows_processed integer, candidate_error_count bigint)",
        _admission_body(previous, legacy) if enabled else previous._admission_body(legacy),
    )
    legacy._function(
        schema,
        "plan_custom_import_build_family_page",
        "p_build_id bigint, p_expected_page_sequence bigint",
        "TABLE(phase text, plan_stage text, page_sequence bigint, rows_processed integer, plan_complete boolean)",
        _planner_body(legacy) if enabled else legacy._PLAN_CUSTOM_IMPORT_BUILD_FAMILY_PAGE_BODY,
    )
    guard = legacy._GUARD_CUSTOM_IMPORT_BUILD_OWNED_BODY
    if enabled:
        guard = _replace(guard, "'plan_complete_at',", "'plan_complete_at','" + "','".join(_CURSORS) + "',")
    legacy._function(
        schema,
        "guard_custom_import_build_owned",
        "",
        "trigger",
        guard.replace("__SCHEMA_NAME__", schema.replace("'", "''")),
        invoker=True,
    )


def upgrade() -> None:
    """Keep membership admission and retained-family validation under the build fence."""

    schema = _schema()
    legacy = _previous()._legacy()
    quoted = legacy._q(schema)
    for name, kind in zip(_CURSORS, ("smallint", "bigint"), strict=True):
        op.execute(
            f"ALTER TABLE {quoted}.custom_import_build_attempt ADD COLUMN {name} {kind} NOT NULL DEFAULT 0 CHECK ({name}>=0)"
        )
    op.execute(f"""CREATE INDEX {_INDEX} ON {quoted}.custom_import_child_revision
        (schema_revision_id,root_record_id,collection_slot,child_key_sha256,child_revision_id)""")
    legacy._function(schema, "custom_import_membership_key", "p_membership jsonb, p_child_key text", "text", _KEY_BODY)
    legacy._function(
        schema,
        "custom_import_child_membership_missing",
        "p_build_id bigint, p_family_id bigint, p_root_record_id bigint, p_collection_slot smallint, p_child_key text, p_memberships jsonb",
        "boolean",
        _MISSING_BODY,
    )
    _functions(schema, enabled=True)


def downgrade() -> None:
    """Do not remove checks while any immutable definition requires membership."""

    schema = _schema()
    quoted = _previous()._legacy()._q(schema)
    op.execute(f"""DO $guard$ BEGIN
        IF EXISTS(SELECT 1 FROM {quoted}.custom_import_definition_revision
            WHERE canonical_definition::jsonb ? 'child_memberships') THEN
            RAISE EXCEPTION 'custom_import_child_memberships_retention_required'; END IF;
    END $guard$""")
    _functions(schema, enabled=False)
    op.execute(
        f"DROP FUNCTION {quoted}.custom_import_child_membership_missing(bigint,bigint,bigint,smallint,text,jsonb)"
    )
    op.execute(f"DROP FUNCTION {quoted}.custom_import_membership_key(jsonb,text)")
    op.execute(f"DROP INDEX {quoted}.{_INDEX}")
    for name in _CURSORS:
        op.execute(f"ALTER TABLE {quoted}.custom_import_build_attempt DROP COLUMN {name}")
