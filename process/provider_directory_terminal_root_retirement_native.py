# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Recognize the existing native replacement for legacy retirement row guards."""

from db.migration_provider_directory_dataset_candidates import TABLES
from process.provider_directory_cms_callable_custody import _decoded
from process.provider_directory_generic_source_release_checks import (
    _FUNCTIONS,
    _HOOKS,
    _LEGACY_FUNCTIONS,
    _payload_hooks,
    _require_hook,
    _require_native_function,
)
from process.provider_directory_terminal_root_retirement_contract import (
    TerminalRootRetirementError,
    quoted_relation,
    row_mapping,
    schema_name,
)

_ROOTS = TABLES[:5]
_NAMES = ("guard_pd_dataset_candidate_parent", "deny_pd_dataset_candidate_write")
_FAMILY = """WITH RECURSIVE family(oid,root) AS (
 SELECT to_regclass(relation)::oid,root FROM jsonb_to_recordset(CAST(:roots AS jsonb)) r(relation text,root text)
 UNION ALL SELECT i.inhrelid,f.root FROM pg_inherits i JOIN family f ON i.inhparent=f.oid)
 SELECT c.oid::bigint,c.relname,c.relkind::text,c.relispartition,n.nspname,c.relowner::bigint,f.root,
 c.relrowsecurity OR c.relforcerowsecurity AS row_security,c.relpersistence::text,
 ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=c.oid) AS parents,
 NOT EXISTS(SELECT FROM pg_roles actor WHERE NOT actor.rolsuper AND actor.oid<>c.relowner
   AND NOT pg_has_role(actor.oid,c.relowner,'SET')
   AND has_table_privilege(actor.oid,c.oid,'TRUNCATE,TRIGGER')) AS truncate_closed
 FROM family f JOIN pg_class c ON c.oid=f.oid JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY c.oid"""
_REPLICATION_CLOSED = """WITH reachable(actor,role) AS (
 SELECT actor.oid,selected.oid FROM pg_roles actor CROSS JOIN pg_roles selected
 WHERE actor.rolcanlogin AND NOT actor.rolsuper AND NOT actor.oid=ANY(CAST(:owners AS oid[]))
   AND (actor.oid=selected.oid OR pg_has_role(actor.oid,selected.oid,'SET')))
 SELECT NOT EXISTS(SELECT FROM reachable parameter JOIN reachable writer USING(actor)
 WHERE has_parameter_privilege(parameter.role,'session_replication_role','SET')
   AND EXISTS(SELECT FROM unnest(CAST(:relations AS oid[])) relation(oid)
     WHERE has_table_privilege(writer.role,relation.oid,'INSERT,UPDATE,DELETE,TRUNCATE')
       OR has_any_column_privilege(writer.role,relation.oid,'INSERT,UPDATE')))"""


def _require(condition):
    if not condition:
        raise TerminalRootRetirementError("state_invalid")


async def _native_functions(database, schema, owners):
    signatures = [quoted_relation(name) + "()" for name in _NAMES]
    query = _FUNCTIONS.replace("$1::text[]", "CAST(:signatures AS text[])")
    query = query.replace("$2", ":schema").replace("$3::text[]", "CAST(:names AS text[])")
    function_rows = await database.all(query, signatures=signatures, schema=schema, names=list(_NAMES))
    _require(len(function_rows) == len(signatures))
    namespace = await database.scalar("SELECT to_regnamespace(:schema)::oid::bigint", schema=schema)
    definition_by_oid = {}
    for raw in function_rows:
        row = row_mapping(raw)
        _require(row["signature"] in signatures)
        definition = _decoded(row["definition"])
        signature = definition["proname"] + "()"
        _require_native_function(definition, _LEGACY_FUNCTIONS[signature], row["language"], schema, namespace, owners)
        owner = int(definition["proowner"])
        _require(_decoded(row["acl"]) == [[owner, owner, "EXECUTE", False]])
        definition_by_oid[int(definition["oid"])] = definition
    return definition_by_oid


def _native_hooks(root, relation, original_oid):
    hooks = _payload_hooks(
        root,
        history=relation["relname"] != root,
        sealed_always=relation["oid"] != original_oid and relation["relname"] != root,
    )
    return {
        name: hook for name, hook in hooks.items() if name.startswith("a_pd_candidate_") or name == "pd_dataset_sealed"
    }


async def _verify_family(database, schema, plan, family, owners):
    root_owner_by_name = {
        relation["root"]: relation["relowner"] for relation in family if relation["relname"] == relation["root"]
    }
    definition_by_oid = await _native_functions(database, schema, owners | set(root_owner_by_name.values()))
    query = _HOOKS.replace("$1::oid[]", "CAST(:relations AS oid[])")
    hook_rows = await database.all(query, relations=[relation["oid"] for relation in family])
    by_relation = {}
    for raw in hook_rows:
        hook = row_mapping(raw)
        by_relation.setdefault(hook["tgrelid"], {})[hook["tgname"]] = hook
    for relation in family:
        root = relation["root"]
        identity = plan["tables"][root]
        is_parent = relation["relname"] == root
        _require(
            relation["nspname"] == schema
            and relation["relowner"] in owners | {root_owner_by_name[root]}
            and not relation["row_security"]
            and relation["relpersistence"] == "p"
        )
        _require(
            (relation["relkind"], relation["parents"], relation["relispartition"])
            == (("p", [], False) if is_parent else ("r", [identity["parent_oid"]], True))
        )
        _require(is_parent or relation["truncate_closed"] is True)
        actual = by_relation.get(relation["oid"], {})
        expected = _native_hooks(root, relation, identity["original_oid"])
        # Published detached candidates use the existing unconditional seal instead of mutable intake hooks.
        if not is_parent and "pd_dataset_sealed" in actual:
            expected = _payload_hooks(
                TABLES[4], history=True, sealed_always=relation["oid"] != identity["original_oid"]
            )
        selected_by_name = {
            name: hook
            for name, hook in actual.items()
            if name.startswith("a_pd_candidate_") or name == "pd_dataset_sealed"
        }
        _require(selected_by_name.keys() == expected.keys())
        for name, hook in selected_by_name.items():
            _require_hook(hook, expected[name])
            definition = _decoded(hook["native_function"])
            _require(definition_by_oid.get(int(definition["oid"])) == definition)


async def _native_migration_plan(database, schema, family_rows):
    plan = _decoded(
        await database.scalar(f"SELECT plan FROM {quoted_relation('pd_dataset_migration_plan')} WHERE singleton")
    )
    _require(type(plan) is dict and plan.get("complete") is True and set(plan.get("tables", {})) == set(TABLES))
    for root in _ROOTS:
        identity = plan["tables"][root]
        _require(
            type(identity) is dict
            and type(identity.get("original_oid")) is int
            and type(identity.get("parent_oid")) is int
        )
        parent = next(relation for relation in family_rows if relation["relname"] == root)
        _require(parent["oid"] == identity["parent_oid"])
        if not any(relation["oid"] == identity["original_oid"] for relation in family_rows):
            # An originally empty heap stays detached with a validated CHECK(false), never a writable bypass.
            _require(identity.get("keys") == [])
            closed = await database.scalar(
                """SELECT c.relkind='r' AND NOT c.relispartition
              AND n.nspname=:schema AND c.relname='pd_dataset_history_'||c.oid::text
              AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid)
              AND EXISTS(SELECT FROM pg_constraint WHERE conrelid=c.oid AND contype='c'
                  AND convalidated AND pg_get_expr(conbin,conrelid,true)='false')
              FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=:oid""",
                schema=schema,
                oid=identity["original_oid"],
            )
            _require(closed is True)
    return plan


async def require_native_retirement_controls(database):
    """Return replaced roots only after recognizing their complete native write boundary."""
    import json

    schema = schema_name()
    roots = [{"relation": quoted_relation(root), "root": root} for root in _ROOTS]
    family_rows = [row_mapping(native_record) for native_record in await database.all(_FAMILY, roots=json.dumps(roots))]
    native_roots = {
        relation["root"]
        for relation in family_rows
        if relation["relname"] == relation["root"] and relation["relkind"] == "p"
    }
    if not native_roots:
        return set()
    _require(native_roots == set(_ROOTS))
    _require(await database.scalar("SELECT current_setting('session_replication_role')") == "origin")
    plan = await _native_migration_plan(database, schema, family_rows)
    protected_owners = set(
        await database.scalar(
            """SELECT array_agg(oid::bigint) FROM pg_roles
      WHERE rolsuper OR oid=(SELECT nspowner FROM pg_namespace WHERE nspname=:schema)""",
            schema=schema,
        )
    )
    # The native migration preserves legacy owner authority; descendants cannot introduce another owner.
    legacy_owners = {relation["relowner"] for relation in family_rows if relation["relname"] == relation["root"]}
    _require(
        await database.scalar(
            _REPLICATION_CLOSED,
            owners=list(protected_owners | legacy_owners),
            relations=[relation["oid"] for relation in family_rows],
        )
        is True
    )
    try:
        await _verify_family(database, schema, plan, family_rows, protected_owners)
    except (ValueError, KeyError) as error:
        raise TerminalRootRetirementError("state_invalid") from error
    return native_roots
