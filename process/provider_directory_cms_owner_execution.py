# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Execute the protected common ownership plan in its caller's short Admin cut.

The operator must disposition incompatible consumers before calling, retain one
absolute deadline through COMMIT/ROLLBACK, and roll back on every refusal. This
performs no payload scan, commit, profile issuance or worker activation. The
separate canonical writer fence and full data finalizer remain mandatory.
"""

import asyncio
import json
from collections import defaultdict
from contextlib import asynccontextmanager
from copy import deepcopy

from db.plan_release_storage import storage_identifier, storage_relation
from process import provider_directory_cms_owner_permissions as permissions
from process import provider_directory_cms_owner_postimage as postimages
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_preparation_policy as profiles
from process import provider_directory_cms_reverse_hook_custody as reverse

_ACL_SQL = """WITH supplied AS (
 SELECT position,document,CASE pg_catalog.jsonb_typeof(document->'raw_acl')
 WHEN 'string' THEN (document->>'raw_acl')::pg_catalog.aclitem[]
 WHEN 'array' THEN ARRAY(SELECT value FROM
 pg_catalog.jsonb_array_elements_text(document->'raw_acl') raw_values(value))::pg_catalog.aclitem[]
 ELSE NULL END AS raw
 FROM pg_catalog.jsonb_array_elements($1::pg_catalog.jsonb)
 WITH ORDINALITY inputs(document,position)
)
SELECT supplied.position,COALESCE(pg_catalog.jsonb_agg(pg_catalog.jsonb_build_array(
 acl.grantee::bigint,acl.grantor::bigint,acl.privilege_type,acl.is_grantable)
 ORDER BY acl.grantee,acl.grantor,acl.privilege_type,acl.is_grantable)
 FILTER(WHERE acl.grantee IS NOT NULL),'[]'::pg_catalog.jsonb) AS edges
 FROM supplied LEFT JOIN LATERAL pg_catalog.aclexplode(COALESCE(raw,
 CASE document->>'kind' WHEN 'table' THEN
 pg_catalog.acldefault('r',(document->>'owner_oid')::pg_catalog.oid)
 WHEN 'routine' THEN pg_catalog.acldefault('f',(document->>'owner_oid')::pg_catalog.oid)
 ELSE NULL END)) acl ON true GROUP BY supplied.position ORDER BY supplied.position"""
_SIGNATURES_SQL = """SELECT p.oid::bigint,n.nspname,p.proname,
 pg_catalog.pg_get_function_identity_arguments(p.oid) AS arguments
 FROM pg_catalog.pg_proc p JOIN pg_catalog.pg_namespace n ON n.oid=p.pronamespace
 WHERE p.oid=ANY($1::pg_catalog.oid[]) ORDER BY p.oid"""
_DATABASE_SQL = """SELECT oid::bigint,datdba::bigint FROM pg_catalog.pg_database
 WHERE oid=$1::pg_catalog.oid FOR SHARE NOWAIT"""
_DDL_HOOKS_SQL = """SELECT NOT EXISTS(SELECT FROM pg_catalog.pg_event_trigger
 WHERE evtenabled OPERATOR(pg_catalog.<>) 'D'::pg_catalog."char")"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_owner_execution_" + code)


def _relations(inventory):
    canonical = inventory["canonical"]
    return [
        *canonical["relations"],
        *canonical["dependency_relations"],
        *canonical["dependency_histories"],
    ]


async def _expanded_acls(connection, inputs):
    payload = json.dumps(inputs, allow_nan=False, separators=(",", ":"))
    rows = await connection.fetch(_ACL_SQL, payload)
    _require([row["position"] for row in rows] == list(range(1, len(inputs) + 1)), "acl_expansion_changed")
    return [{**value, "edges": custody._decoded(row["edges"])} for value, row in zip(inputs, rows, strict=True)]


def _owner_edges(rows, previous, owner):
    """Apply native owner substitution, including duplicate grant merging."""
    grantable_by_edge = {}
    for row in rows:
        prefix, grantee, grantor, right, grantable = row[:-4], *row[-4:]
        edge = (
            *prefix,
            owner if grantee == previous else grantee,
            owner if grantor == previous else grantor,
            right,
        )
        grantable_by_edge[edge] = grantable_by_edge.get(edge, False) or grantable
    return {(*edge, grantable) for edge, grantable in grantable_by_edge.items()}


def _role_name(oid, roles):
    _require(type(oid) is int and (oid == 0 or oid in roles), "role_identity_changed")
    return "PUBLIC" if oid == 0 else storage_identifier(roles[oid])


def _acl_statements(before, desired, *, owner, resource, roles, columns=None):
    """Change only owner-issued edges; preserve independent reader grants."""
    statements = []
    old, new = set(map(tuple, before)), set(map(tuple, desired))
    for verb, edges in (("REVOKE", old - new), ("GRANT", new - old)):
        by_actor = defaultdict(list)
        for edge in sorted(edges):
            prefix, actor, grantor, right, grantable = edge[:-4], *edge[-4:]
            _require(grantor == owner and grantable is False, "grantor_transition_unsupported")
            if columns is None:
                _require(not prefix and right in permissions.controls._RIGHTS | {"EXECUTE"}, "privilege_changed")
                rendered = right
            else:
                _require(
                    len(prefix) == 1 and prefix[0] in columns and right in permissions._COLUMN_RIGHTS, "column_changed"
                )
                rendered = right + "(" + storage_identifier(columns[prefix[0]]) + ")"
            by_actor[actor].append(rendered)
        for actor, rights in sorted(by_actor.items()):
            direction = " FROM " if verb == "REVOKE" else " TO "
            statements.append(verb + " " + ",".join(rights) + " ON " + resource + direction + _role_name(actor, roles))
    return statements


def _table_statements(inventory, plan, roles):
    by_oid = {row["oid"]: row for row in _relations(inventory)}
    physical_by_oid = {row["oid"]: row["native"] for row in inventory["canonical"]["physical"]}
    statements = []
    for table in plan["relations"]:
        original = by_oid[table["oid"]]
        previous, owner = original["relowner"], table["owner_oid"]
        resource = storage_relation(table["nspname"], table["relname"])
        if previous != owner:
            statements.append("ALTER TABLE ONLY " + resource + " OWNER TO " + _role_name(owner, roles))
        column_names_by_number = {
            entry["attribute"]["attnum"]: entry["attribute"]["attname"]
            for entry in original["native"]["columns"]
            if not entry["attribute"]["attisdropped"]
        }
        for field, selected in (("table_acl", None), ("column_acl", column_names_by_number)):
            old = _owner_edges(physical_by_oid[table["oid"]][field], previous, owner)
            statements.extend(
                _acl_statements(
                    old,
                    table[field],
                    owner=owner,
                    resource="TABLE " + resource,
                    roles=roles,
                    columns=selected,
                )
            )
    return statements


async def _routine_statements(connection, inventory, plan, roles):
    originals_by_oid = {entry["oid"]: entry["native"] for entry in inventory["routines"]}
    selected_routines = [entry for entry in plan["routines"] if entry["nspname"] != "pg_catalog"]
    rows = await connection.fetch(_SIGNATURES_SQL, sorted(entry["oid"] for entry in selected_routines))
    _require(
        [row["oid"] for row in rows] == sorted(entry["oid"] for entry in selected_routines), "routine_identity_changed"
    )
    signature_by_oid = {}
    for row in rows:
        native = originals_by_oid[row["oid"]]
        _require(
            (row["nspname"], row["proname"]) == (native["namespace"][1], native["definition"]["proname"]),
            "routine_identity_changed",
        )
        signature_by_oid[row["oid"]] = storage_relation(row["nspname"], row["proname"]) + "(" + row["arguments"] + ")"
    statements = []
    for routine in selected_routines:
        native, owner = originals_by_oid[routine["oid"]], routine["owner_oid"]
        previous = int(native["definition"]["proowner"])
        signature = signature_by_oid[routine["oid"]]
        if previous != owner:
            statements.append("ALTER FUNCTION " + signature + " OWNER TO " + _role_name(owner, roles))
        old = _owner_edges(native["acl"], previous, owner)
        statements.extend(
            _acl_statements(old, routine["routine_acl"], owner=owner, resource="FUNCTION " + signature, roles=roles)
        )
    return statements


async def _family(connection, inventory, schema):
    return await custody._inventory(connection, schema, [row["relname"] for row in _relations(inventory)])


def _require_family(before, after, original, observed, plan):
    old_by_oid = {row["oid"]: row for row in before}
    new_by_oid = {row["oid"]: row for row in after}
    fixed_before_by_oid = {row["oid"]: row for row in _relations(original)}
    fixed_after_by_oid = {row["oid"]: row for row in _relations(observed)}
    plan_by_oid = {row["oid"]: row for row in plan["relations"]}
    _require(
        len(old_by_oid) == len(before) and len(new_by_oid) == len(after) and old_by_oid.keys() == new_by_oid.keys(),
        "family_identity_changed",
    )
    _require(plan_by_oid.keys() <= old_by_oid.keys(), "family_identity_changed")
    for oid, old in old_by_oid.items():
        new = new_by_oid[oid]
        if oid in plan_by_oid:
            for raw, fixed in ((old, fixed_before_by_oid[oid]), (new, fixed_after_by_oid[oid])):
                _require(
                    raw["native"]["relation"] == fixed["native"]["relation"]
                    and raw["native"]["columns"] == fixed["native"]["columns"],
                    "family_acl_binding_changed",
                )
            _require(
                postimages._same(postimages._relation_after(old, new, plan_by_oid[oid]), new),
                "family_structure_changed",
            )
        else:
            _require(postimages._same(old, new), "unplanned_descendant_changed")


async def _mutate_owner(connection, inventory, binding, policy, profile, plan, deadline, *, commit_clock=None):
    from process.provider_directory_cms_owner_role_rights import expected_cms_owner_roles

    await custody._lock_allocation_inventory(connection, _relations(inventory))
    before = await profiles._collect_profile(connection, binding, policy, profile)
    _require(postimages._same(before, inventory), "preimage_changed")
    family = await _family(connection, before, binding.catalog_schema)
    expected_roles = await expected_cms_owner_roles(connection, before, plan)
    expanded = await _expanded_acls(connection, postimages.cms_owner_acl_inputs(before, "before"))
    role_names_by_oid = {int(row["oid"]): row["rolname"] for row in before["roles"]["roles"]}
    commands = _table_statements(before, plan, role_names_by_oid) + await _routine_statements(
        connection, before, plan, role_names_by_oid
    )
    resources = sorted(storage_relation(row["nspname"], row["relname"]) for row in plan["relations"])
    reverse._deadline(deadline)
    if commit_clock is not None:
        commit_clock.reschedule(min(commit_clock.when(), asyncio.get_running_loop().time() + 3))
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + resource for resource in resources) + " IN ACCESS EXCLUSIVE MODE NOWAIT"
    )
    if commands:
        reverse._deadline(deadline)
        await connection.execute(";\n".join(commands) + ";")
    return dict(before=before, plan=plan, family=family, expected_roles=expected_roles, acl_expansions=expanded)


async def _verify_owner(connection, preparation, observed, binding, deadline):
    reverse._deadline(deadline)
    before, plan = preparation["before"], preparation["plan"]
    acl_expansions = [*preparation["acl_expansions"]]
    acl_expansions.extend(await _expanded_acls(connection, postimages.cms_owner_acl_inputs(observed, "after")))
    postimages.require_cms_owner_structure(before, observed, plan, acl_expansions=acl_expansions)
    _require(postimages._same(observed["roles"], preparation["expected_roles"]), "effective_rights_changed")
    _require_family(
        preparation["family"], await _family(connection, observed, binding.catalog_schema), before, observed, plan
    )


async def _execute(connection, inventory, binding, policy, profile, plan, deadline):
    preparation = await _mutate_owner(connection, inventory, binding, policy, profile, plan, deadline)
    after = await profiles._collect_profile(connection, binding, policy, profile)
    await _verify_owner(connection, preparation, after, binding, deadline)
    return after


@asynccontextmanager
async def cms_common_owner_custody(
    connection, *, inventory, storage_binding, policy, cms_profile, source_document, deadline
):
    """Retain original catalog/actor custody across explicit local data phases.

    This yields no authority or handle. All successful DB-backed exit checks
    precede the caller's outer commit; exceptional exit leaves rollback to it.
    """
    reverse._deadline(deadline)
    async with (
        asyncio.timeout_at(deadline),
        reverse.cms_reverse_hook_custody(
            connection,
            inventory=inventory,
            storage_binding=storage_binding,
            policy=policy,
            cms_profile=cms_profile,
            source_document=source_document,
            deadline=deadline,
        ) as recheck,
    ):
        database = await connection.fetchrow(_DATABASE_SQL, storage_binding.database_oid)
        _require(database is not None and database["oid"] == storage_binding.database_oid, "database_changed")
        _require(await connection.fetchval(_DDL_HOOKS_SQL) is True, "ddl_hooks_enabled")
        yield
        _require(await connection.fetchrow(_DATABASE_SQL, storage_binding.database_oid) == database, "database_changed")
        await recheck()


async def apply_cms_common_owner_permissions(
    connection, *, inventory, storage_binding, policy, cms_profile, source_document, deadline
):
    """Return verified native postimage data; caller owns issuance and final commit.

    Roll back the entire original transaction after every refusal/cancellation.
    This allocation-independent custody transaction must end before long history
    preparation, payload COPY or the separate fixed conversion/finalization cut.
    """
    reverse._deadline(deadline)
    inventory, cms_profile, source_document = deepcopy((inventory, cms_profile, source_document))
    plan = permissions.plan_cms_common_owner_permissions(
        inventory,
        storage_binding=storage_binding,
        policy=policy,
        cms_profile=cms_profile,
        source_document=source_document,
    )
    async with cms_common_owner_custody(
        connection,
        inventory=inventory,
        storage_binding=storage_binding,
        policy=policy,
        cms_profile=cms_profile,
        source_document=source_document,
        deadline=deadline,
    ):
        return await _execute(connection, inventory, storage_binding, policy, cms_profile, plan, deadline)


async def _owner_routines(connection, preparation):
    from process.provider_directory_cms_routine_settings import _current_routines

    original = preparation["before"]["routines"]
    current = await _current_routines(connection, [row["oid"] for row in original])
    _require([row["oid"] for row in current] == [row["oid"] for row in original], "routine_identity_changed")
    plans_by_oid = {row["oid"]: row for row in preparation["plan"]["routines"]}
    sites = [postimages._routine_site(row, "after") for row in current]
    expanded = await _expanded_acls(connection, sites)
    for before, after, acl in zip(original, current, expanded, strict=True):
        expected = postimages._routine_after(before, after, plans_by_oid[before["oid"]])
        _require(postimages._same(expected, after), "owner_routines_changed")
        _require(
            postimages._edges(acl["edges"], "routine") == postimages._edges(after["native"]["acl"], "routine"),
            "routine_acl_changed",
        )
    return current


async def _owner_roles(connection, before, routines, binding, policy):
    oids = sorted({row["oid"] for row in (*_relations(before), *before["private"])})
    return custody._decoded(
        await connection.fetchval(
            custody._ROLES_SQL,
            oids,
            [binding.catalog_schema, binding.storage_schema],
            [row["oid"] for row in routines],
            [policy.owner_role, policy.publisher_role, *policy.runtime_roles],
        )
    )


async def mutate_cms_common_owner_permissions(
    connection, *, inventory, storage_binding, policy, cms_profile, source_document, deadline, commit_clock=None
):
    """Return checked local owner-phase data, never an issued inventory or authority.

    The caller retains cms_common_owner_custody through terminal verification.
    Reader restoration is permitted only after this routine/role proof returns.
    """
    reverse._deadline(deadline)
    inventory, cms_profile, source_document = deepcopy((inventory, cms_profile, source_document))
    plan = permissions.plan_cms_common_owner_permissions(
        inventory,
        storage_binding=storage_binding,
        policy=policy,
        cms_profile=cms_profile,
        source_document=source_document,
    )
    async with asyncio.timeout_at(deadline):
        preparation = await _mutate_owner(
            connection, inventory, storage_binding, policy, cms_profile, plan, deadline, commit_clock=commit_clock
        )
        routines = await _owner_routines(connection, preparation)
        roles = await _owner_roles(connection, preparation["before"], routines, storage_binding, policy)
        _require(postimages._same(roles, preparation["expected_roles"]), "effective_rights_changed")
        preparation["reader_view"] = deepcopy(dict(routines=routines, roles=roles))
        return preparation


async def apply_cms_common_routine_settings(connection, *, inventory, storage_binding, policy, cms_profile, deadline):
    """Extend the verified owner postimage under the same retained catalog custody.

    The caller must retain the owner transaction's locks and deadline through
    successor issuance and commit. The owner-only primitive remains unchanged.
    """
    from process.provider_directory_cms_routine_settings import install_cms_routine_settings

    reverse._deadline(deadline)
    inventory = deepcopy(inventory)
    expected = deepcopy(inventory)
    async with asyncio.timeout_at(deadline):
        expected["routines"] = await install_cms_routine_settings(
            connection,
            expected_routines=inventory["routines"],
            catalog_schema=storage_binding.catalog_schema,
            catalog_schema_oid=storage_binding.catalog_schema_oid,
            preparation_schema="hp_snapshot_retention",
            deadline=deadline,
        )
        after = await profiles._collect_profile(connection, storage_binding, policy, cms_profile)
        _require(postimages._same(after, expected), "settings_postimage_changed")
        reverse._deadline(deadline)
        return after


async def _install_body_routines(connection, admin_connection, routines, storage_binding, deadline):
    from process import provider_directory_cms_routine_bodies as bodies
    from process.provider_directory_cms_routine_settings import _STABILITY_SQL, _current_routines

    reverse._deadline(deadline)
    _require(await bodies.cms_body_admin_driver(admin_connection) is connection, "admin_transport_changed")
    routines = deepcopy(routines)
    expected = bodies.cms_terminal_body_postimage(
        routines,
        catalog_schema=storage_binding.catalog_schema,
        catalog_schema_oid=storage_binding.catalog_schema_oid,
        preparation_schema="hp_snapshot_retention",
    )
    identities = [routine["oid"] for routine in routines]
    _require(await _current_routines(connection, identities) == routines, "body_preimage_changed")
    stability_rows = [dict(row) for row in await connection.fetch(_STABILITY_SQL, identities)]
    await admin_connection.run_sync(
        bodies.install_cms_terminal_bodies, storage_binding.catalog_schema, "hp_snapshot_retention", routines
    )
    return dict(routines=expected, identities=identities, stability=deepcopy(stability_rows))


async def _verify_terminal_hooks(connection, terminal, deadline):
    from process.provider_directory_cms_routine_settings import _STABILITY_SQL

    _require(
        [dict(row) for row in await connection.fetch(_STABILITY_SQL, terminal["identities"])] == terminal["stability"],
        "body_hooks_changed",
    )
    reverse._deadline(deadline)


async def apply_cms_common_routine_bodies(
    connection, admin_connection, *, inventory, storage_binding, policy, cms_profile, deadline
):
    """Apply exact source body successors under the same authenticated owner custody."""
    reverse._deadline(deadline)
    inventory = deepcopy(inventory)
    async with asyncio.timeout_at(deadline):
        terminal = await _install_body_routines(
            connection, admin_connection, inventory["routines"], storage_binding, deadline
        )
        expected = deepcopy(inventory)
        expected["routines"] = terminal["routines"]
        after = await profiles._collect_profile(connection, storage_binding, policy, cms_profile)
        _require(postimages._same(after, expected), "body_postimage_changed")
        await _verify_terminal_hooks(connection, terminal, deadline)
        return after


async def apply_cms_common_terminal_routines(
    connection, admin_connection, *, inventory, storage_binding, policy, cms_profile, deadline
):
    """Verify the cumulative settings/body postimage once; keep standalone APIs strict."""
    from process.provider_directory_cms_routine_bodies import cms_body_admin_driver
    from process.provider_directory_cms_routine_settings import install_cms_routine_settings

    reverse._deadline(deadline)
    _require(await cms_body_admin_driver(admin_connection) is connection, "admin_transport_changed")
    expected = deepcopy(inventory)
    expected["routines"] = await install_cms_routine_settings(
        connection,
        expected_routines=expected["routines"],
        catalog_schema=storage_binding.catalog_schema,
        catalog_schema_oid=storage_binding.catalog_schema_oid,
        preparation_schema="hp_snapshot_retention",
        deadline=deadline,
    )
    return await apply_cms_common_routine_bodies(
        connection,
        admin_connection,
        inventory=expected,
        storage_binding=storage_binding,
        policy=policy,
        cms_profile=cms_profile,
        deadline=deadline,
    )


async def install_cms_common_terminal_routines(connection, admin_connection, *, routines, storage_binding, deadline):
    """Install fixed terminal changes; the caller must verify the final raw-four image.

    These detached local observations issue no authority. Retain the same owner
    custody and deadline through finish_cms_common_owner_terminal and commit.
    """
    from process.provider_directory_cms_routine_bodies import cms_body_admin_driver
    from process.provider_directory_cms_routine_settings import install_cms_routine_settings

    reverse._deadline(deadline)
    _require(await cms_body_admin_driver(admin_connection) is connection, "admin_transport_changed")
    async with asyncio.timeout_at(deadline):
        settings_rows = await install_cms_routine_settings(
            connection,
            expected_routines=deepcopy(routines),
            catalog_schema=storage_binding.catalog_schema,
            catalog_schema_oid=storage_binding.catalog_schema_oid,
            preparation_schema="hp_snapshot_retention",
            deadline=deadline,
        )
        return await _install_body_routines(connection, admin_connection, settings_rows, storage_binding, deadline)


async def finish_cms_common_owner_terminal(
    connection, *, preparation, terminal, storage_binding, policy, cms_profile, deadline
):
    """Return only the actual final inventory after source-exact terminal and owner proofs."""
    from process import provider_directory_cms_routine_bodies as bodies
    from process import provider_directory_cms_routine_settings as settings

    reverse._deadline(deadline)
    preparation, terminal = deepcopy((preparation, terminal))
    permissions._authenticated(preparation["before"], storage_binding, policy, cms_profile)
    _require(preparation["plan"]["owner_oid"] == storage_binding.namespace_owner_oid, "plan_owner_changed")
    original = preparation["reader_view"]["routines"]
    targets_by_oid = settings._source_targets(
        original, storage_binding.catalog_schema, storage_binding.catalog_schema_oid, "hp_snapshot_retention"
    )
    expected = bodies.cms_terminal_body_postimage(
        settings._expected_postimage(original, targets_by_oid),
        catalog_schema=storage_binding.catalog_schema,
        catalog_schema_oid=storage_binding.catalog_schema_oid,
        preparation_schema="hp_snapshot_retention",
    )
    _require(postimages._same(terminal["routines"], expected), "terminal_projection_changed")
    _require(terminal["identities"] == [routine["oid"] for routine in original], "routine_identity_changed")
    async with asyncio.timeout_at(deadline):
        after = await profiles._collect_profile(connection, storage_binding, policy, cms_profile)
        _require(postimages._same(after["routines"], expected), "body_postimage_changed")
        await _verify_terminal_hooks(connection, terminal, deadline)
        owner_by_kind = {**after, "routines": original}
        await _verify_owner(connection, preparation, owner_by_kind, storage_binding, deadline)
        return after
