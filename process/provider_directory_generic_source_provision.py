# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source family checks, isolated generation provisioning and snapshot freeze."""

import re

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_storage import (
    CMS_IDENTITY_ROOTS,
    CMS_RESOURCE_IDENTITY_ROOT,
    HEADER,
    RELATIONS,
    RELATIONSHIP,
    WITNESS,
    _close_grants,
    _json,
    _require,
)
from process.provider_directory_postal_checkpoint import provision as provision_postal_checkpoint
from process.provider_directory_source_checkpoint_catalog import (
    CHECKPOINT_LAYOUTS,
    capture_checkpoint_controls,
)
from process.provider_directory_source_coverage_control import capture_source_coverage_control
from process.provider_directory_source_proof_control import PROOF_ROOT, verify_proof_control

PARTITIONED_ROOTS = (*TABLES, WITNESS, RELATIONSHIP)
CMS_DOCTORS_ROOTS = (
    "provider_directory_cms_doctors_group_binding",
    "provider_directory_cms_doctors_site_binding",
)
GLOBAL_ROOTS = (*CMS_IDENTITY_ROOTS, CMS_RESOURCE_IDENTITY_ROOT, *CMS_DOCTORS_ROOTS)
PAYLOAD_ROOTS = (*PARTITIONED_ROOTS, *GLOBAL_ROOTS)
FHIR_INPUT_ROOTS = tuple(
    sorted(
        (
            "provider_directory_dataset_affiliation_organization",
            "provider_directory_dataset_insurance_plan",
            "provider_directory_dataset_network_plan",
        )
    )
)
ISOLATED_FHIR_ROOTS = (HEADER, *RELATIONS, PROOF_ROOT, *CHECKPOINT_LAYOUTS)
ROOTED_SOURCE_INPUT_ROOTS = (
    "provider_directory_rooted_graph_acquisition",
    "provider_directory_rooted_graph_work",
    "provider_directory_rooted_graph_resource",
    "provider_directory_rooted_graph_edge",
    "provider_directory_rooted_graph_twin_attempt",
    "provider_directory_rooted_graph_twin_admission",
)

# Full pg_get_constraintdef output, with only the function namespace replaced.
# These two checks move to protected application validation, never disappear.
_ISOLATED_APPLICATION_CHECKS = {
    "pd_endpoint_dataset_completion_digest_check": (
        "provider_directory_subset_completion_proof_pair_valid(jsonb,text)",
        "4e22ef3ce7308145b26216e914c0ea2f8ec4c27df70fff6c5cc96144870a863f",
    ),
    "pd_endpoint_dataset_completion_shape_check": (
        "provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)",
        "a1a5f5744bae74014dd21d17f3b03c6905663f4a5785c52ad277b905fe6be7a3",
    ),
}


async def provision_isolated_fhir_source_storage(
    connection, policy, *, catalog_schema, namespace_owner, verify_role_profile=None, rooted=False
):
    """Create one empty generation with ordinary PostgreSQL grants and native constraints."""
    _require(type(policy) is GenericSourcePolicy, "explicit_policy_required")
    _require(connection.is_in_transaction(), "transaction_required")
    for name in (catalog_schema, policy.schema, namespace_owner):
        storage_identifier(name)
    _require(catalog_schema != policy.schema, "isolated_coordinates_required")
    async with connection.transaction():
        namespace = storage_identifier(policy.schema)
        await connection.execute(f"CREATE SCHEMA {namespace} AUTHORIZATION {storage_identifier(namespace_owner)}")
        await connection.execute(f"REVOKE ALL ON SCHEMA {namespace} FROM PUBLIC")
        await connection.execute(f"GRANT USAGE,CREATE ON SCHEMA {namespace} TO {storage_identifier(policy.owner_role)}")
        for role in (policy.publisher_role, *policy.runtime_roles):
            await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO {storage_identifier(role)}")
        roots = (*ISOLATED_FHIR_ROOTS, *(ROOTED_SOURCE_INPUT_ROOTS if rooted else ()))
        for root in roots:
            relation = storage_relation(policy.schema, root)
            template_root = root + "_legacy" if root in ROOTED_SOURCE_INPUT_ROOTS[1:4] else root
            template = storage_relation(catalog_schema, template_root)
            await connection.execute(
                f"CREATE TABLE {relation} (LIKE {template} INCLUDING DEFAULTS "
                "INCLUDING GENERATED INCLUDING STORAGE INCLUDING INDEXES INCLUDING CONSTRAINTS)"
            )
            checkpoint_columns = CHECKPOINT_LAYOUTS.get(root, {}).get("columns", ())
            if checkpoint_columns:
                json_columns = await connection.fetch(
                    "SELECT attname FROM pg_attribute WHERE attrelid=$1::regclass AND attname=ANY($2::text[]) "
                    "AND atttypid='pg_catalog.json'::regtype AND attnum>0 AND NOT attisdropped ORDER BY attnum",
                    relation,
                    [column[0] for column in checkpoint_columns if column[1] == "jsonb"],
                )
                if json_columns:
                    await connection.execute(
                        f"ALTER TABLE {relation} "
                        + ",".join(
                            f"ALTER COLUMN {storage_identifier(column['attname'])} TYPE jsonb "
                            f"USING {storage_identifier(column['attname'])}::jsonb"
                            for column in json_columns
                        )
                    )
            if root in ROOTED_SOURCE_INPUT_ROOTS[1:4]:
                checks = await connection.fetch(
                    "SELECT conname FROM pg_constraint WHERE conrelid=$1::regclass "
                    "AND conname LIKE 'pdrg_%_legacy_scope'",
                    relation,
                )
                for check in checks:
                    await connection.execute(
                        f"ALTER TABLE {relation} DROP CONSTRAINT {storage_identifier(check['conname'])}"
                    )
            if root == HEADER:
                for check in _ISOLATED_APPLICATION_CHECKS:
                    await connection.execute(f"ALTER TABLE {relation} DROP CONSTRAINT {storage_identifier(check)}")
            await connection.execute(f"ALTER TABLE {relation} OWNER TO {storage_identifier(policy.owner_role)}")
            await _close_grants(connection, policy, root)
        if not rooted:
            await provision_postal_checkpoint(connection, policy)
    return {"catalog_schema": catalog_schema, "storage_schema": policy.schema}


async def verify_generic_source_family(connection, policy, *, require_inert=True):
    """Whole attached family table shape and native-only proof; PostgreSQL enforces access."""
    roots = [storage_relation(policy.schema, name) for name in PAYLOAD_ROOTS]
    # The owner and reader arguments only feed CATALOG_SQL's ACL columns, which are not read here.
    relation_rows = await connection.fetch(CATALOG_SQL, roots, 0, [])
    _require(
        len(relation_rows) >= len(roots)
        and set(PAYLOAD_ROOTS) <= {relation_by_field["relname"] for relation_by_field in relation_rows},
        "roots_missing",
    )
    native_control_oids = (
        await _native_control_references(connection, policy.schema, relation_rows) if require_inert else set()
    )
    for relation_by_field in relation_rows:
        if relation_by_field["relname"] in PARTITIONED_ROOTS:
            is_legacy_heap = relation_by_field["relname"] in {WITNESS, RELATIONSHIP} and not require_inert
            _require(
                not relation_by_field["parents"]
                and (
                    relation_by_field["relkind"] == "p"
                    and relation_by_field["partition_key"] == "LIST (dataset_id)"
                    or is_legacy_heap
                    and relation_by_field["relkind"] == "r"
                ),
                "partitioned_roots_required",
            )
        elif relation_by_field["relname"] in GLOBAL_ROOTS:
            _require(relation_by_field["relkind"] == "r" and not relation_by_field["parents"], "global_root_changed")
        _require(
            relation_by_field["nspname"] == policy.schema
            and relation_by_field["relpersistence"] == "p"
            and relation_by_field["relkind"] in {"r", "p"}
            and not relation_by_field["relrowsecurity"]
            and not relation_by_field["relforcerowsecurity"],
            "family_custody_changed",
        )
        if require_inert:
            import json

            constraints = (
                json.loads(relation_by_field["constraints"])
                if isinstance(relation_by_field["constraints"], str)
                else relation_by_field["constraints"]
            )
            _require(
                (relation_by_field["inert"] or relation_by_field["oid"] in native_control_oids)
                and relation_by_field["native_checks"]
                and all(constraint["type"] in {"c", "p", "u"} and constraint["valid"] for constraint in constraints),
                "family_guard_or_foreign_key",
            )
    if require_inert:
        await verify_proof_control(connection, policy)
        await capture_source_coverage_control(connection, policy)
        await capture_checkpoint_controls(connection, policy)
    return relation_rows


# These low-volume manual review/binding constraints remain authoritative. Their
# target-side native hooks never inspect SOURCE INSERTs.
_NATIVE_CONTROL_REFERENCES = {
    "pd_entity_redirect_old_evidence_fkey": (
        "provider_directory_entity_redirect_decision",
        CMS_IDENTITY_ROOTS[1],
        ("source_id", "resource_type", "old_resource_id", "old_release_id"),
        ("source_id", "resource_type", "resource_id", "release_id"),
    ),
    "pd_entity_redirect_canonical_evidence_fkey": (
        "provider_directory_entity_redirect_decision",
        CMS_IDENTITY_ROOTS[1],
        ("source_id", "resource_type", "canonical_resource_id", "canonical_release_id"),
        ("source_id", "resource_type", "resource_id", "release_id"),
    ),
    "pd_mrf_payer_review_release_fkey": (
        "provider_directory_mrf_payer_review_decision",
        CMS_IDENTITY_ROOTS[1],
        ("source_id", "resource_type", "resource_id", "release_id"),
        ("source_id", "resource_type", "resource_id", "release_id"),
    ),
    "pd_mrf_payer_binding_organization_fkey": (
        "provider_directory_mrf_payer_binding",
        CMS_IDENTITY_ROOTS[0],
        ("source_id", "resource_type", "resource_id"),
        ("source_id", "resource_type", "resource_id"),
    ),
}


async def _native_control_references(connection, schema, family):
    """Retain exact native manual-control target UPDATE/DELETE hooks only."""
    non_inert_rows = [relation_by_field for relation_by_field in family if not relation_by_field["inert"]]
    if not non_inert_rows:
        return set()
    allowed_targets = {contract[1] for contract in _NATIVE_CONTROL_REFERENCES.values()}
    _require(
        {relation_by_field["relname"] for relation_by_field in non_inert_rows} == allowed_targets,
        "family_guard_or_foreign_key",
    )
    target_by_oid = {relation_by_field["oid"]: relation_by_field["relname"] for relation_by_field in non_inert_rows}
    triggers = await connection.fetch(
        "SELECT t.tgtype,t.tgenabled::text,t.tgisinternal,p.proname,pn.nspname AS function_schema,"
        "k.conname,k.contype::text,k.convalidated,k.condeferrable,k.confupdtype::text,k.confdeltype::text,"
        "k.confrelid::bigint,c.relname,n.nspname,"
        "ARRAY(SELECT a.attname::text FROM unnest(k.conkey) WITH ORDINALITY x(num,pos) JOIN pg_attribute a "
        "ON a.attrelid=k.conrelid AND a.attnum=x.num ORDER BY x.pos) AS source_columns,"
        "ARRAY(SELECT a.attname::text FROM unnest(k.confkey) WITH ORDINALITY x(num,pos) JOIN pg_attribute a "
        "ON a.attrelid=k.confrelid AND a.attnum=x.num ORDER BY x.pos) AS target_columns,"
        "EXISTS(SELECT FROM pg_rewrite WHERE ev_class=t.tgrelid) AS has_rules "
        "FROM pg_trigger t LEFT JOIN pg_constraint k ON k.oid=t.tgconstraint "
        "JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace pn ON pn.oid=p.pronamespace "
        "LEFT JOIN pg_class c ON c.oid=k.conrelid LEFT JOIN pg_namespace n ON n.oid=c.relnamespace "
        "WHERE t.tgrelid=ANY($1::oid[])",
        list(target_by_oid),
    )
    expected_hooks = {
        (name, event, function)
        for name in _NATIVE_CONTROL_REFERENCES
        for event, function in ((9, "RI_FKey_restrict_del"), (17, "RI_FKey_noaction_upd"))
    }
    _require(
        len(triggers) == len(expected_hooks)
        and {
            (trigger_by_field["conname"], trigger_by_field["tgtype"], trigger_by_field["proname"])
            for trigger_by_field in triggers
        }
        == expected_hooks
        and all(
            _native_control_reference_valid(trigger_by_field, schema, target_by_oid) for trigger_by_field in triggers
        ),
        "family_guard_or_foreign_key",
    )
    return set(target_by_oid)


def _native_control_reference_valid(trigger_by_field, schema, target_by_oid):
    contract = _NATIVE_CONTROL_REFERENCES.get(trigger_by_field["conname"])
    return bool(
        contract
        and trigger_by_field["tgisinternal"]
        and trigger_by_field["tgenabled"] == "O"
        and trigger_by_field["function_schema"] == "pg_catalog"
        and trigger_by_field["contype"] == "f"
        and trigger_by_field["convalidated"]
        and not trigger_by_field["condeferrable"]
        and trigger_by_field["confupdtype"] == "a"
        and trigger_by_field["confdeltype"] == "r"
        and trigger_by_field["nspname"] == schema
        and trigger_by_field["relname"] == contract[0]
        and target_by_oid.get(trigger_by_field["confrelid"]) == contract[1]
        and tuple(trigger_by_field["source_columns"]) == contract[2]
        and tuple(trigger_by_field["target_columns"]) == contract[3]
        and not trigger_by_field["has_rules"]
    )


async def capture_generic_source_global_roots(publisher, roots):
    """Return the shape-checked rows of fixed, nonpartitioned managed roots."""
    from process.provider_directory_generic_source_policy import bound_publisher

    _require(publisher is bound_publisher(), "publisher_connection_changed")
    _require(
        type(roots) is tuple and bool(roots) and len(set(roots)) == len(roots) and set(roots) <= set(GLOBAL_ROOTS),
        "global_roots_changed",
    )
    family = await verify_generic_source_family(publisher.connection, publisher.policy)
    selected_by_root = {row["relname"]: row for row in family if row["relname"] in roots}
    _require(set(selected_by_root) == set(roots), "global_roots_missing")
    return selected_by_root


def _require_fhir_inventory(proof):
    parents = [[proof["schema"], name] for name in FHIR_INPUT_ROOTS]
    _require(proof["parents"] == parents, "protected_fhir_roots_changed")
    inventory_rows = proof["inventory"]
    _require(type(inventory_rows) is list and bool(inventory_rows), "protected_fhir_inventory_changed")
    observed_names = set()
    for relation_by_field in inventory_rows:
        _require(type(relation_by_field) is dict, "protected_fhir_inventory_changed")
        name = (relation_by_field.get("nspname"), relation_by_field.get("relname"))
        _require(
            name[0] == proof["schema"] and type(name[1]) is str and name not in observed_names,
            "protected_fhir_inventory_changed",
        )
        storage_identifier(name[1])
        observed_names.add(name)
        _require(
            relation_by_field.get("relpersistence") == "p"
            and relation_by_field.get("relrowsecurity") is False
            and relation_by_field.get("relforcerowsecurity") is False
            and relation_by_field.get("inert") is True
            and relation_by_field.get("native_checks") is True
            and type(relation_by_field.get("columns")) is list
            and bool(relation_by_field["columns"])
            and type(relation_by_field.get("constraints")) is list
            and bool(relation_by_field["constraints"])
            and all(
                layout_by_field.get("valid") is True and layout_by_field.get("type") in {"c", "p", "u"}
                for layout_by_field in relation_by_field["constraints"]
            )
            and type(relation_by_field.get("indexes")) is list
            and bool(relation_by_field["indexes"])
            and all(layout_by_field.get("valid") is True for layout_by_field in relation_by_field["indexes"]),
            "protected_fhir_layout_changed",
        )
        if list(name) in parents:
            _require(
                relation_by_field.get("relkind") == "p"
                and relation_by_field.get("partition_key") == "LIST (dataset_id)"
                and relation_by_field.get("parents") == []
                and relation_by_field.get("relispartition") is False
                and relation_by_field.get("bound") is None,
                "protected_fhir_parent_changed",
            )
        else:
            edges = relation_by_field.get("parents")
            _require(
                relation_by_field.get("relkind") == "r"
                and relation_by_field.get("relispartition") is True
                and type(edges) is list
                and len(edges) == 1
                and edges[0] in parents
                and relation_by_field.get("partition_key") is None
                and type(relation_by_field.get("bound")) is str
                and re.fullmatch(r"FOR VALUES IN \('(?:[^']|'')+'(?:, '(?:[^']|'')+')*\)", relation_by_field["bound"]),
                "protected_fhir_leaf_changed",
            )
    _require(set(map(tuple, parents)) <= observed_names, "protected_fhir_roots_changed")


def require_protected_fhir_input(proof):
    """Validate named Source layouts; saved physical metadata is never authority."""
    _require(
        type(proof) is dict
        and set(proof) == {"version", "schema", "profile", "parents", "inventory"}
        and type(proof["version"]) is int
        and proof["version"] == 2,
        "protected_fhir_proof_changed",
    )
    profile = proof["profile"]
    _require(
        type(profile) is dict
        and set(profile) == {"owner_role", "publisher_role", "runtime_roles", "control_runtime_roles"}
        and type(profile["runtime_roles"]) is list
        and type(profile["control_runtime_roles"]) is list,
        "protected_fhir_profile_changed",
    )
    GenericSourcePolicy(
        proof["schema"],
        profile["owner_role"],
        profile["publisher_role"],
        tuple(profile["runtime_roles"]),
        tuple(profile["control_runtime_roles"]),
    )
    _require_fhir_inventory(proof)
    return proof


_FHIR_NAMED_INPUT_SQL = """WITH RECURSIVE family(oid) AS (
 SELECT to_regclass(name)::oid FROM unnest($1::text[]) requested(name)
 UNION SELECT edge.inhrelid FROM pg_inherits edge JOIN family parent ON parent.oid=edge.inhparent
)
SELECT n.nspname,c.relname,c.relkind::text,c.relpersistence::text,c.relispartition,
 c.relrowsecurity,c.relforcerowsecurity,pg_get_expr(c.relpartbound,c.oid) AS bound,
 pg_get_partkeydef(c.oid) AS partition_key,
 (SELECT coalesce(jsonb_agg(jsonb_build_array(pn.nspname,p.relname) ORDER BY pn.nspname,p.relname),'[]'::jsonb)
   FROM pg_inherits edge JOIN pg_class p ON p.oid=edge.inhparent
   JOIN pg_namespace pn ON pn.oid=p.relnamespace WHERE edge.inhrelid=c.oid) AS parents,
 (SELECT coalesce(jsonb_agg(jsonb_build_array(a.attname,format_type(a.atttypid,a.atttypmod),
   a.attnotnull,pg_get_expr(d.adbin,d.adrelid)) ORDER BY a.attnum),'[]'::jsonb)
   FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid=a.attrelid AND d.adnum=a.attnum
   WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped) AS columns,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('name',conname,'type',contype::text,
   'definition',pg_get_constraintdef(oid),'valid',convalidated) ORDER BY conname),'[]'::jsonb)
   FROM pg_constraint WHERE conrelid=c.oid AND contype<>'n') AS constraints,
 (SELECT coalesce(jsonb_agg(jsonb_build_object('name',idx.relname,'definition',pg_get_indexdef(i.indexrelid),
   'valid',i.indisvalid AND i.indisready AND i.indislive AND NOT i.indisexclusion) ORDER BY idx.relname),'[]'::jsonb)
   FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid WHERE i.indrelid=c.oid) AS indexes,
 NOT EXISTS(SELECT FROM pg_trigger WHERE tgrelid=c.oid)
   AND NOT EXISTS(SELECT FROM pg_rewrite WHERE ev_class=c.oid) AS inert,
 NOT EXISTS(SELECT FROM pg_constraint k JOIN pg_depend d ON d.classid='pg_constraint'::regclass AND d.objid=k.oid
   JOIN pg_proc p ON d.refclassid='pg_proc'::regclass AND d.refobjid=p.oid
   WHERE k.conrelid=c.oid AND p.pronamespace<>'pg_catalog'::regnamespace) AS native_checks
FROM family JOIN pg_class c ON c.oid=family.oid JOIN pg_namespace n ON n.oid=c.relnamespace
ORDER BY n.nspname,c.relname"""


async def _named_fhir_input_inventory(connection, roots):
    return [
        {
            key: _json(value) if key in {"parents", "columns", "constraints", "indexes"} else value
            for key, value in dict(row).items()
        }
        for row in await connection.fetch(_FHIR_NAMED_INPUT_SQL, roots)
    ]


async def capture_protected_fhir_input(connection, *, source_policy):
    """Observe fixed Source partition names and semantic layout in the caller transaction."""
    _require(type(source_policy) is GenericSourcePolicy, "explicit_policy_required")
    _require(connection.is_in_transaction() is True, "transaction_required")
    roots = [storage_relation(source_policy.schema, name) for name in FHIR_INPUT_ROOTS]
    root_kinds = await connection.fetch(
        "SELECT c.relkind::text AS kind FROM unnest($1::text[]) roots(name) "
        "LEFT JOIN pg_class c ON c.oid=to_regclass(name) ORDER BY name",
        roots,
    )
    kinds = [root_by_field["kind"] for root_by_field in root_kinds]
    if kinds in ([None] * 3, ["r"] * 3):
        return None
    _require(kinds == ["p"] * 3, "protected_fhir_roots_changed")
    await connection.execute("LOCK TABLE " + ", ".join(roots) + " IN ACCESS SHARE MODE NOWAIT")
    inventory = await _named_fhir_input_inventory(connection, roots)
    proof_by_field = dict(
        version=2,
        schema=source_policy.schema,
        profile=dict(
            owner_role=source_policy.owner_role,
            publisher_role=source_policy.publisher_role,
            runtime_roles=list(source_policy.runtime_roles),
            control_runtime_roles=list(source_policy.control_runtime_roles),
        ),
        parents=[[source_policy.schema, name] for name in FHIR_INPUT_ROOTS],
        inventory=inventory,
    )
    require_protected_fhir_input(proof_by_field)
    _require(await _named_fhir_input_inventory(connection, roots) == inventory, "protected_fhir_catalog_changed")
    _require(connection.is_in_transaction() is True, "transaction_required")
    return proof_by_field


async def verify_generic_source_read_family(connection, schema, owner_role, runtime_roles):
    """Check the read family's table shape; PostgreSQL enforces the readers' access."""
    storage_identifier(schema)
    storage_identifier(owner_role)
    _require(
        type(runtime_roles) is tuple
        and 1 <= len(runtime_roles) <= 64
        and len(set(runtime_roles)) == len(runtime_roles)
        and owner_role not in runtime_roles,
        "reader_profile_invalid",
    )
    for role in runtime_roles:
        storage_identifier(role)
    # The owner and reader arguments only feed CATALOG_SQL's ACL columns, which are not read here.
    relation_rows = await connection.fetch(
        CATALOG_SQL, [storage_relation(schema, name) for name in PAYLOAD_ROOTS], 0, []
    )
    native_control_oids = await _native_control_references(connection, schema, relation_rows)
    _require_reader_family(relation_rows, schema, native_control_oids)
    return relation_rows


def _require_reader_family(relation_rows, schema, native_control_oids=()):
    """Admit only native closed list roots and their direct persistent leaves."""
    root_oids_by_name = {
        relation_by_field["relname"]: relation_by_field["oid"]
        for relation_by_field in relation_rows
        if relation_by_field["relname"] in PAYLOAD_ROOTS
    }
    _require(set(root_oids_by_name) == set(PAYLOAD_ROOTS), "roots_missing")
    import json

    for relation_by_field in relation_rows:
        constraints = (
            json.loads(relation_by_field["constraints"])
            if isinstance(relation_by_field["constraints"], str)
            else relation_by_field["constraints"]
        )
        is_root = relation_by_field["relname"] in root_oids_by_name
        _require(
            relation_by_field["nspname"] == schema
            and relation_by_field["relpersistence"] == "p"
            and not relation_by_field["relrowsecurity"]
            and not relation_by_field["relforcerowsecurity"]
            and (relation_by_field["inert"] or relation_by_field["oid"] in native_control_oids)
            and relation_by_field["native_checks"]
            and all(
                constraint_by_field["type"] in {"c", "p", "u"} and constraint_by_field["valid"]
                for constraint_by_field in constraints
            )
            and (
                relation_by_field["relkind"] == ("p" if relation_by_field["relname"] in PARTITIONED_ROOTS else "r")
                and (
                    relation_by_field["partition_key"] == "LIST (dataset_id)"
                    if relation_by_field["relname"] in PARTITIONED_ROOTS
                    else relation_by_field["partition_key"] is None
                )
                and not relation_by_field["parents"]
                if is_root
                else relation_by_field["relkind"] == "r"
                and len(relation_by_field["parents"]) == 1
                and relation_by_field["parents"][0] in [root_oids_by_name[root] for root in PARTITIONED_ROOTS]
            ),
            "reader_family_changed",
        )


async def _snapshot_freeze_sequences(connection, storage_schema, relation_names):
    return await connection.fetch(
        "SELECT DISTINCT sequence.relname,'S'::text AS relkind FROM pg_class sequence "
        "JOIN pg_namespace namespace ON namespace.oid=sequence.relnamespace "
        "JOIN pg_depend dependency ON dependency.classid='pg_class'::regclass "
        "AND dependency.refclassid='pg_class'::regclass AND dependency.objid=sequence.oid "
        "AND dependency.objsubid=0 AND dependency.deptype IN ('a','i') "
        "JOIN pg_class owner_table ON owner_table.oid=dependency.refobjid "
        "WHERE namespace.nspname=$1 AND sequence.relkind='S' "
        "AND owner_table.relnamespace=namespace.oid AND owner_table.relname=ANY($2::text[]) ORDER BY sequence.relname",
        storage_schema,
        list(relation_names),
    )


async def _snapshot_freeze_relations(connection, storage_schema, relation_names):
    if relation_names is None:
        return await connection.fetch(
            "SELECT relname,relkind FROM pg_class c JOIN pg_namespace n "
            "ON n.oid=c.relnamespace WHERE n.nspname=$1 AND c.relkind IN ('r','p','S') ORDER BY relname",
            storage_schema,
        )
    relations = list(
        await connection.fetch(
            "SELECT relname,relkind FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE n.nspname=$1 AND c.relname=ANY($2::text[]) AND c.relkind IN ('r','S') ORDER BY relname",
            storage_schema,
            list(relation_names),
        )
    )
    _require(
        tuple(relation_by_field["relname"] for relation_by_field in relations) == tuple(sorted(relation_names))
        and all(relation_by_field["relkind"] in ("r", "S", b"r", b"S") for relation_by_field in relations),
        "snapshot_relation_selection_incomplete",
    )
    sequences = await _snapshot_freeze_sequences(connection, storage_schema, relation_names)
    relations.extend(
        sequence_by_field for sequence_by_field in sequences if sequence_by_field["relname"] not in relation_names
    )
    return relations


async def _freeze_snapshot_relation(connection, storage_schema, relation_by_field, owner, readers):
    name = storage_relation(storage_schema, relation_by_field["relname"])
    relation_kind = relation_by_field["relkind"]
    if isinstance(relation_kind, bytes):
        relation_kind = relation_kind.decode()
    kind = "SEQUENCE" if relation_kind == "S" else "TABLE"
    grants = await connection.fetch(
        "SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' "
        "ELSE pg_get_userbyid(a.grantee) END AS role, a.column_name FROM ("
        "SELECT acl.grantee,NULL::text AS column_name FROM pg_class c CROSS JOIN LATERAL "
        "aclexplode(coalesce(c.relacl,acldefault(CASE WHEN c.relkind='S' THEN 'S'::\"char\" ELSE 'r'::\"char\" END,c.relowner))) acl "
        "WHERE c.oid=to_regclass($1) UNION SELECT acl.grantee,attribute.attname FROM pg_attribute attribute "
        "CROSS JOIN LATERAL aclexplode(attribute.attacl) acl WHERE attribute.attrelid=to_regclass($1)) a",
        name,
    )
    await connection.execute(f"ALTER {kind} {name} OWNER TO {owner}")
    for grant in grants:
        principal = "PUBLIC" if grant["role"] == "PUBLIC" else storage_identifier(grant["role"])
        columns = " (" + storage_identifier(grant["column_name"]) + ")" if grant["column_name"] else ""
        await connection.execute(f"REVOKE ALL PRIVILEGES{columns} ON {kind} {name} FROM {principal} CASCADE")
    await connection.execute(f"REVOKE ALL ON {kind} {name} FROM {owner} CASCADE")
    for column in sorted({grant["column_name"] for grant in grants if grant["column_name"]}):
        await connection.execute(f"REVOKE ALL ({storage_identifier(column)}) ON {kind} {name} FROM {owner} CASCADE")
    await connection.execute(f"REVOKE ALL ON {kind} {name} FROM PUBLIC")
    if readers:
        await connection.execute(f"GRANT SELECT ON {kind} {name} TO " + ",".join(readers))


async def _freeze_snapshot_schema(connection, storage_schema, owner, readers):
    namespace = storage_identifier(storage_schema)
    grants = await connection.fetch(
        "SELECT DISTINCT CASE WHEN a.grantee=0 THEN 'PUBLIC' "
        "ELSE pg_get_userbyid(a.grantee) END AS role FROM pg_namespace n CROSS JOIN LATERAL "
        "aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a WHERE n.nspname=$1",
        storage_schema,
    )
    await connection.execute(f"ALTER SCHEMA {namespace} OWNER TO {owner}")
    for grant in grants:
        principal = "PUBLIC" if grant["role"] == "PUBLIC" else storage_identifier(grant["role"])
        await connection.execute(f"REVOKE ALL ON SCHEMA {namespace} FROM {principal} CASCADE")
    await connection.execute(f"REVOKE ALL ON SCHEMA {namespace} FROM {owner} CASCADE")
    await connection.execute(f"REVOKE ALL ON SCHEMA {namespace} FROM PUBLIC")
    if readers:
        await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO " + ",".join(readers))


async def freeze_fhir_source_snapshot(
    connection,
    storage_schema,
    *,
    namespace_owner,
    reader_roles=(),
    application_roles=None,
    relation_names=None,
    freeze_schema=True,
):
    """Close immutable generation writes, preserving optional shared-schema controls."""
    _require(connection.is_in_transaction(), "transaction_required")
    storage_identifier(storage_schema)
    _require(type(freeze_schema) is bool, "snapshot_relation_selection_invalid")
    if relation_names is not None:
        _require(
            isinstance(relation_names, (tuple, list))
            and bool(relation_names)
            and len(set(relation_names)) == len(relation_names),
            "snapshot_relation_selection_invalid",
        )
        relation_names = tuple(relation_names)
        for name in relation_names:
            storage_identifier(name)
    _require(freeze_schema or relation_names is not None, "snapshot_relation_selection_invalid")
    owner = storage_identifier(namespace_owner)
    readers = tuple(storage_identifier(role) for role in reader_roles)
    application_roles = reader_roles if application_roles is None else application_roles
    _require(not freeze_schema or namespace_owner not in application_roles, "snapshot_owner_overlap")
    applications = tuple(storage_identifier(role) for role in application_roles)
    if freeze_schema and applications:
        # Only an existing membership needs ADMIN on the owner to revoke; executors hold SET, not ADMIN.
        members = await connection.fetch(
            "SELECT member.rolname FROM pg_auth_members membership "
            "JOIN pg_roles granted ON granted.oid=membership.roleid JOIN pg_roles member ON member.oid=membership.member "
            "WHERE granted.rolname=$1 AND member.rolname=ANY($2::text[]) ORDER BY member.rolname",
            namespace_owner,
            list(application_roles),
        )
        if members:
            revoked = ",".join(storage_identifier(member["rolname"]) for member in members)
            await connection.execute(f"REVOKE {owner} FROM {revoked} CASCADE")
    if freeze_schema:
        # A new table owner needs CREATE on the schema; owning the schema first provides it.
        await connection.execute(f"ALTER SCHEMA {storage_identifier(storage_schema)} OWNER TO {owner}")
    for relation_by_field in await _snapshot_freeze_relations(connection, storage_schema, relation_names):
        await _freeze_snapshot_relation(connection, storage_schema, relation_by_field, owner, readers)
    if freeze_schema:
        await _freeze_snapshot_schema(connection, storage_schema, owner, readers)
