# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact native action hooks inherited from existing canonical foreign keys."""

import json

from db.plan_release_storage import storage_relation
from process.provider_directory_generic_source_storage import RELATIONS, _require

PARENTS = (*RELATIONS[:4], "provider_directory_rooted_graph_dataset_resource")
_TRIGGER_LOCAL = "ARRAY['oid','tgname','tgrelid','tgparentid','tgconstraint','tgconstrindid']::text[]"
_CONSTRAINT_LOCAL = (
    "ARRAY['oid','conname','conindid','confrelid','conparentid','conislocal','coninhcount','connoinherit']::text[]"
)


def parent_foreign_guard_snapshot_sql(schema):
    """Bind only existing incoming native FK constraints and their canonical action hooks."""
    names = ",".join("('" + root + "','" + storage_relation(schema, root).replace("'", "''") + "')" for root in PARENTS)
    return f"""WITH parents(root,qualified) AS (VALUES {names})
        SELECT coalesce(jsonb_agg(jsonb_build_object('root',parent.root,'parent_owner',c.relowner::bigint,
            'referencing_owner',referencer.relowner::bigint,'constraint',to_jsonb(k),
            'hooks',(SELECT jsonb_agg(jsonb_build_object('trigger',to_jsonb(t),'function',to_jsonb(p),
                'language',l.lanname) ORDER BY t.oid) FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
                JOIN pg_language l ON l.oid=p.prolang WHERE t.tgrelid=c.oid AND t.tgconstraint=k.oid))
            ORDER BY k.oid),'[]'::jsonb)
        FROM parents parent JOIN pg_class c ON c.oid=to_regclass(parent.qualified)
        JOIN pg_constraint k ON k.confrelid=c.oid AND k.contype='f' AND k.conparentid=0
        JOIN pg_class referencer ON referencer.oid=k.conrelid"""


async def capture_parent_foreign_guards(connection, schema):
    """Reuse native RI callable checks without admitting new foreign keys or custom row code."""
    from process.provider_directory_cms_control_custody import _require_native_foreign_functions

    encoded = await connection.fetchval(parent_foreign_guard_snapshot_sql(schema))
    guards = json.loads(encoded) if isinstance(encoded, str) else encoded
    function_oids = set()
    for guard in guards:
        constraint = guard["constraint"]
        _require(
            constraint["convalidated"]
            and constraint["connoinherit"]
            and constraint.get("conenforced", True)
            and guard["parent_owner"] == guard["referencing_owner"],
            "rooted_foreign_guard_changed",
        )
        hooks = guard["hooks"] or []
        _require(
            len(hooks) == 2 and {hook["trigger"]["tgtype"] for hook in hooks} == {9, 17}, "rooted_foreign_guard_changed"
        )
        for hook in hooks:
            trigger = hook["trigger"]
            _require(
                trigger["tgisinternal"]
                and trigger["tgenabled"] in {"O", "A"}
                and trigger["tgconstraint"] == constraint["oid"]
                and trigger["tgparentid"] == "0",
                "rooted_foreign_guard_changed",
            )
            function_oids.add(int(trigger["tgfoid"]))
    await _require_native_foreign_functions(connection, function_oids)
    return guards


def inherited_foreign_guards_valid_sql(relation_sql, root_sql, snapshot_sql):
    """Permit exactly the native action-hook clones of the protected canonical FK inventory."""
    return f"""WITH expected AS (
        SELECT guard_record.value->'constraint' AS constraint_json,guard_hook.value->'trigger' AS trigger_json
        FROM jsonb_array_elements({snapshot_sql}) guard_record
        CROSS JOIN LATERAL jsonb_array_elements(guard_record.value->'hooks') guard_hook
        WHERE guard_record.value->>'root'={root_sql}
    ) SELECT NOT EXISTS(SELECT FROM pg_trigger t LEFT JOIN pg_constraint k ON k.oid=t.tgconstraint
        WHERE t.tgrelid={relation_sql} AND NOT EXISTS(SELECT FROM expected e
            WHERE t.tgparentid=(e.trigger_json->>'oid')::oid AND t.tgisinternal
              AND k.contype='f' AND k.conparentid=(e.constraint_json->>'oid')::oid
              AND k.confrelid=t.tgrelid AND k.conindid=t.tgconstrindid
              AND NOT k.conislocal AND k.coninhcount=1 AND NOT k.connoinherit
              AND (to_jsonb(t)-{_TRIGGER_LOCAL})=(e.trigger_json-{_TRIGGER_LOCAL})
              AND (to_jsonb(k)-{_CONSTRAINT_LOCAL})=(e.constraint_json-{_CONSTRAINT_LOCAL})
              AND EXISTS(SELECT FROM pg_index child JOIN pg_inherits link ON link.inhrelid=child.indexrelid
                  WHERE child.indexrelid=k.conindid AND child.indrelid=t.tgrelid
                    AND link.inhparent=(e.constraint_json->>'conindid')::oid)))
        AND (SELECT count(*) FROM pg_trigger WHERE tgrelid={relation_sql})=(SELECT count(*) FROM expected)
        AND (SELECT count(DISTINCT tgparentid) FROM pg_trigger WHERE tgrelid={relation_sql})=(SELECT count(*) FROM expected)"""


async def require_inherited_foreign_guards(connection, document, root, relation_oid):
    """Normalize attached layout only after proving every native FK action hook's exact lineage."""
    query = inherited_foreign_guards_valid_sql("$1::oid", "$2::text", "$3::jsonb")
    _require(
        await connection.fetchval(query, relation_oid, root, json.dumps(document["snapshot"]["foreign_guards"]))
        is True,
        "rooted_foreign_guard_changed",
    )
