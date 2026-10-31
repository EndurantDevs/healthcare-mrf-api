# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare a stable CMS catalog policy; never issue custody or write authority.

Existing roots, histories, retained descendants and their rights remain exact.
Only independently checked generic allocator children and their native FK clones
stay outside that fixed fingerprint. The caller owns writer, callable and DDL custody.
"""

from dataclasses import replace

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_preparation_custody as custody
from process.provider_directory_generic_source_provision import _completed_legacy_dataset_plan
from process.provider_directory_source_acquisition_custody import _require_dataset_relationships

CONTRACT = "cms-preparation-custody-evidence-v2"
DEPENDENCY_CONTRACT = "cms-preparation-custody-evidence-v3"
PLAN = "pd_dataset_migration_plan"
ROOTS = (*custody.CANONICAL_ROOTS, *TABLES[4:], PLAN)
_HISTORY_SQL = """SELECT c.oid::bigint,n.nspname,c.relname,c.relkind::text,c.relispartition,
 ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent) AS parents,
 NOT EXISTS(SELECT FROM pg_inherits WHERE inhparent=c.oid) AS leaf,
 CASE WHEN cardinality($2::text[])=0 THEN
   EXISTS(SELECT FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='c'
     AND k.convalidated AND COALESCE((to_jsonb(k)->>'conenforced')::boolean,true)
     AND pg_get_expr(k.conbin,k.conrelid,true)='false')
 ELSE pg_get_expr(c.relpartbound,c.oid)=(SELECT 'FOR VALUES IN (' || string_agg(
   chr(39) || replace(CASE current_setting('standard_conforming_strings') WHEN 'on' THEN key
     ELSE replace(key,chr(92),chr(92)||chr(92)) END,chr(39),chr(39)||chr(39)) || chr(39),
   ', ' ORDER BY position) || ')' FROM unnest($2::text[]) WITH ORDINALITY bounds(key,position)) END AS bound_matches
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_preparation_policy_" + code)


def _profile(profile, storage_binding, policy):
    """Preserve the v1 fields and require an explicit exact retained-OID set."""
    _require(type(profile) is dict and profile.get("contract") in {CONTRACT, DEPENDENCY_CONTRACT}, "profile_required")
    _require("fixed_descendant_oids" in profile, "retained_descendants_required")
    retained = profile["fixed_descendant_oids"]
    _require(
        type(retained) is list
        and all(type(oid) is int and 0 < oid < 2**32 for oid in retained)
        and retained == sorted(set(retained)),
        "retained_descendants_required",
    )
    legacy_by_field = {key: value for key, value in profile.items() if key != "fixed_descendant_oids"}
    legacy_by_field["contract"] = custody.CONTRACT
    validated = custody._profile(legacy_by_field, storage_binding, policy)
    return {**validated, "contract": profile["contract"], "fixed_descendant_oids": list(retained)}


async def _histories(connection, schema, plan, roots_by_name):
    """Resolve all six legacy histories from completed migration identities."""
    histories = []
    for root in TABLES:
        identity = plan["tables"][root]
        _require(type(identity) is dict, "history_identity_changed")
        original, parent = identity.get("original_oid"), identity.get("parent_oid")
        keys = identity.get("keys", plan.get("keys"))
        _require(
            type(original) is int
            and 0 < original < 2**32
            and type(parent) is int
            and parent == roots_by_name[root]["oid"]
            and original != parent
            and type(keys) is list
            and all(type(key) is str for key in keys)
            and len(keys) == len(set(keys)),
            "history_identity_changed",
        )
        native = await connection.fetchrow(_HISTORY_SQL, original, keys)
        _require(
            native is not None
            and native["nspname"] == schema
            and native["relname"] == "pd_dataset_history_" + str(original)
            and native["relkind"] == "r"
            and native["leaf"] is True
            and native["parents"] == ([parent] if keys else [])
            and native["relispartition"] is bool(keys)
            and native["bound_matches"] is True,
            "history_identity_changed",
        )
        histories.append(dict(native))
    _require(len({entry["oid"] for entry in histories}) == len(TABLES), "history_identity_changed")
    return histories


def _family_oids(family, root_oids):
    selected_oids = set(root_oids)
    while True:
        expanded = selected_oids | {
            entry["oid"] for entry in family if selected_oids.intersection(entry["native"]["parents"])
        }
        if expanded == selected_oids:
            return selected_oids
        selected_oids = expanded


def _fixed_catalog_relations(family, fixed_oids, dynamic_oids):
    """Project only clones already authenticated by the dynamic allocation checker."""
    clones = {
        (hook["tgconstraint"], hook["tgconstrrelid"], str(relation["oid"]))
        for relation in family
        if relation["oid"] in dynamic_oids
        for hook in relation["native"]["triggers"]
        if hook["tgisinternal"]
    }
    removed, fixed = set(), []
    for relation in family:
        if relation["oid"] not in fixed_oids:
            continue
        constraints = []
        for native, definition in relation["native"]["constraints"]:
            identity = (native["oid"], native["conrelid"], native["confrelid"])
            if native["contype"] == "f" and identity in clones:
                removed.add(identity)
            else:
                constraints.append([native, definition])
        fixed.append(dict(relation, native=dict(relation["native"], constraints=constraints)))
    _require(removed == clones, "foreign_clones_changed")
    return fixed


async def _canonical(connection, storage_binding, policy, retained_oids):
    """Keep exact planned histories and fixed descendants; qualify every new child."""
    from process.provider_directory_cms_allocation_semantics import require_cms_dynamic_allocations

    schema = storage_binding.catalog_schema
    plan = await _completed_legacy_dataset_plan(connection, replace(policy, schema=schema))
    _require_dataset_relationships(plan, schema)
    initial = await custody._inventory(connection, schema, ROOTS)
    roots_by_name = {entry["relname"]: entry for entry in initial if entry["relname"] in ROOTS}
    _require(all(not entry["native"]["parents"] for entry in roots_by_name.values()), "root_changed")
    histories = await _histories(connection, schema, plan, roots_by_name)
    family = await custody._inventory(connection, schema, (*ROOTS, *(entry["relname"] for entry in histories)))
    root_oids = {entry["oid"] for entry in roots_by_name.values()}
    history_oids = {entry["oid"] for entry in histories}
    all_oids = {entry["oid"] for entry in family}
    _require(set(retained_oids) <= all_oids - root_oids - history_oids, "retained_descendants_changed")
    _require(
        all(entry["native"]["parents"] for entry in family if entry["oid"] in retained_oids),
        "retained_descendants_changed",
    )
    generic_oids = _family_oids(family, {roots_by_name[root]["oid"] for root in TABLES[:4]})
    dynamic = await require_cms_dynamic_allocations(
        connection,
        schema,
        family=[entry for entry in family if entry["oid"] in generic_oids],
        historical_oids=sorted(history_oids & generic_oids),
        retained_oids=sorted(set(retained_oids) & generic_oids),
    )
    fixed_oids = root_oids | history_oids | set(retained_oids)
    _require(set(dynamic) == all_oids - fixed_oids, "unsupported_descendant")
    fixed_relations = _fixed_catalog_relations(family, fixed_oids, set(dynamic))
    return {"relations": fixed_relations, "migration_plan": plan}, family


async def _coordinates(connection, storage_binding, policy):
    native = custody._decoded(
        await connection.fetchval(
            custody._COORDINATES_SQL,
            storage_binding.catalog_schema,
            storage_binding.storage_schema,
            policy.owner_role,
            policy.publisher_role,
        )
    )
    _require(
        type(native) is dict
        and all(
            native[field] == getattr(storage_binding, field)
            for field in ("database_oid", "catalog_schema_oid", "storage_schema_oid", "namespace_owner_oid")
        )
        and native["owner_oid"] != storage_binding.owner_oid
        and native["owner_login"] is False
        and native["publisher_login"] is True,
        "protected_coordinates_changed",
    )


async def _collect_objects(connection, storage_binding, policy, retained_oids):
    """Collect exact canonical/private/routine evidence before the version-specific role scan."""
    await _coordinates(connection, storage_binding, policy)
    canonical, family = await _canonical(connection, storage_binding, policy, retained_oids)
    private = await custody._inventory(connection, storage_binding.storage_schema, custody.ISOLATED_FHIR_ROOTS)
    # Parent read grants do not grant direct child LOCK rights. Do not borrow
    # owner reach or recursively pin unrelated children to observe this policy.
    roots = [entry for entry in family if entry["relname"] in ROOTS]
    relations = sorted(storage_relation(entry["nspname"], entry["relname"]) for entry in (*roots, *private))
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + relation for relation in relations) + " IN ACCESS SHARE MODE NOWAIT"
    )
    locked_canonical, _ = await _canonical(connection, storage_binding, policy, retained_oids)
    _require(locked_canonical == canonical, "catalog_changed")
    _require(
        await custody._inventory(connection, storage_binding.storage_schema, custody.ISOLATED_FHIR_ROOTS) == private,
        "catalog_changed",
    )
    fixed_relations = [*canonical["relations"], *private]
    oids = sorted(entry["oid"] for entry in fixed_relations)
    frontier = await custody._callee_frontier(connection, storage_binding.catalog_schema)
    routines = [
        {"oid": entry["oid"], "native": custody._decoded(entry["native"])}
        for entry in await connection.fetch(custody._ROUTINES_SQL, oids, [entry["oid"] for entry in frontier])
    ]
    _require(
        {entry["oid"] for entry in frontier} <= {entry["oid"] for entry in routines}
        and await custody._callee_frontier(connection, storage_binding.catalog_schema) == frontier,
        "callees_changed",
    )
    return dict(canonical=canonical, private=private, routines=routines)


async def _collect(connection, storage_binding, policy, retained_oids):
    """Collect the unchanged fixed v2 evidence, including retained descendant identities."""
    observed = await _collect_objects(connection, storage_binding, policy, retained_oids)
    oids = sorted(entry["oid"] for entry in (*observed["canonical"]["relations"], *observed["private"]))
    roles = custody._decoded(
        await connection.fetchval(
            custody._ROLES_SQL,
            oids,
            [storage_binding.catalog_schema, storage_binding.storage_schema],
            [entry["oid"] for entry in observed["routines"]],
            [policy.owner_role, policy.publisher_role, *policy.runtime_roles],
        )
    )
    return {**observed, "roles": roles}


async def _observe(connection, storage_binding, policy, retained_oids):
    """Observe stable evidence, pinning existing objects without fencing writers."""
    return custody._evidence_digests(await _collect(connection, storage_binding, policy, retained_oids))


async def _collect_profile(connection, storage_binding, policy, cms_profile):
    """Select the explicitly protected version without reinterpreting existing inventories."""
    profile = _profile(cms_profile, storage_binding, policy)
    if profile["contract"] == CONTRACT:
        return await _collect(connection, storage_binding, policy, profile["fixed_descendant_oids"])
    from process import provider_directory_cms_dependency_policy as dependencies

    return await dependencies._collect(connection, storage_binding, policy, profile["fixed_descendant_oids"])


async def _observe_profile(connection, storage_binding, policy, cms_profile):
    """Keep the v2 observation path exact; v3 hashes its complete dependency inventory."""
    profile = _profile(cms_profile, storage_binding, policy)
    if profile["contract"] == CONTRACT:
        return await _observe(connection, storage_binding, policy, profile["fixed_descendant_oids"])
    return custody._evidence_digests(await _collect_profile(connection, storage_binding, policy, profile))


async def compare_cms_preparation_policy(connection, *, storage_binding, policy, cms_profile):
    """Compare a protected caller's versioned pin without changing strict v1."""
    profile = _profile(cms_profile, storage_binding, policy)
    _require(connection.is_in_transaction() is True, "transaction_required")
    actor_by_field = dict(await connection.fetchrow(custody._ACTOR_SQL))
    _require(
        actor_by_field["session"] == policy.publisher_role
        and actor_by_field["current"] in {policy.owner_role, policy.publisher_role}
        and actor_by_field["backend"] == connection.get_server_pid()
        and actor_by_field["database_oid"] == storage_binding.database_oid
        and actor_by_field["replication"] == "origin"
        and bool(actor_by_field["transaction"]),
        "original_publisher_required",
    )
    observed = await _observe_profile(connection, storage_binding, policy, profile)
    _require(observed == {field: profile[field] for field in custody._DIGEST_FIELDS}, "native_evidence_changed")
    _require(
        connection.is_in_transaction() is True
        and dict(await connection.fetchrow(custody._ACTOR_SQL)) == actor_by_field,
        "original_publisher_changed",
    )
    return observed
