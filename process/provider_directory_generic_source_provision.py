# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Explicit native-admin cut after complete application writer and role closure."""

import asyncio
import hashlib
import json
import re

from db.migration_provider_dataset_online import _expected_bulk_relationships
from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_catalog import CATALOG_SQL, ROLE_SQL
from process.plan_release_storage_custody import storage_roles
from process.plan_release_storage_provision import _FAMILY_SQL, _close_relation_grants
from process.provider_directory_generic_source_policy import GenericSourcePolicy
from process.provider_directory_generic_source_storage import (
    CMS_IDENTITY_ROOTS,
    CMS_RESOURCE_IDENTITY_ROOT,
    HEADER,
    RELATIONS,
    RELATIONSHIP,
    WITNESS,
    _capture,
    _close_grants,
    _json,
    _require,
)
from process.provider_directory_source_checkpoint_catalog import (
    CHECKPOINT_LAYOUTS,
    capture_checkpoint_controls,
    retire_bulk_abandonment_hooks,
)
from process.provider_directory_source_control_privileges import grant_source_admission_controls
from process.provider_directory_source_coverage_control import COVERAGE_ROOT, capture_source_coverage_control
from process.provider_directory_source_proof_control import PROOF_ROOT, retire_proof_control_hooks, verify_proof_control

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
MINT_ROOTS = (
    "provider_directory_organization_identity",
    "provider_directory_site_identity",
    "provider_directory_insurance_network_identity",
)
CONTROL_ROOTS = (
    HEADER,
    PROOF_ROOT,
    COVERAGE_ROOT,
    *CHECKPOINT_LAYOUTS,
    *(name.removesuffix("_resource") for name in TABLES[4:]),
    "pd_dataset_migration_plan",
    *MINT_ROOTS,
)
ISOLATED_FHIR_ROOTS = (HEADER, *RELATIONS, PROOF_ROOT, *CHECKPOINT_LAYOUTS)
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
_ISOLATED_TEMPLATE_SQL = """SELECT c.oid::bigint,c.relname,
 c.relkind IN ('r','p') AND c.relpersistence='p' AND NOT c.relispartition
 AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AND c.reloftype=0
 AND NOT EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid)
 AND NOT EXISTS(SELECT FROM pg_attribute a JOIN pg_type t ON t.oid=a.atttypid
  WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped
  AND (a.attidentity<>'' OR t.typnamespace<>'pg_catalog'::regnamespace
   OR EXISTS(SELECT FROM pg_collation WHERE oid=a.attcollation AND collnamespace<>'pg_catalog'::regnamespace)))
 AND EXISTS(SELECT FROM pg_constraint WHERE conrelid=c.oid AND contype='p')
 AND NOT EXISTS(SELECT FROM pg_constraint WHERE conrelid=c.oid
  AND (contype NOT IN ('n','c','p','u','f','t') OR NOT convalidated))
 AND NOT EXISTS(SELECT FROM pg_index WHERE indrelid=c.oid
  AND NOT (indisvalid AND indisready AND indislive AND NOT indisexclusion)) AS supported,
 (SELECT jsonb_agg(jsonb_build_object('oid',k.oid::bigint,'name',k.conname,
  'definition',pg_get_constraintdef(k.oid),'functions',ARRAY(
   SELECT p.oid::regprocedure::text FROM pg_depend d JOIN pg_proc p ON p.oid=d.refobjid
   WHERE d.classid='pg_constraint'::regclass AND d.objid=k.oid
    AND d.refclassid='pg_proc'::regclass ORDER BY p.oid),'function_oids',ARRAY(
   SELECT d.refobjid::bigint FROM pg_depend d WHERE d.classid='pg_constraint'::regclass AND d.objid=k.oid
    AND d.refclassid='pg_proc'::regclass ORDER BY d.refobjid)) ORDER BY k.conname)
  FROM pg_constraint k WHERE k.conrelid=c.oid AND k.contype='c') AS checks
 FROM pg_class c WHERE c.relnamespace=$1::oid AND c.relname=ANY($2::text[]) ORDER BY c.relname"""
_ISOLATED_DEPENDENCIES_SQL = """WITH objects(classid,objid,relation) AS (
 SELECT 'pg_attrdef'::regclass,d.oid,d.adrelid FROM pg_attrdef d WHERE d.adrelid=ANY($1::oid[])
 UNION ALL SELECT 'pg_constraint'::regclass,k.oid,k.conrelid FROM pg_constraint k
  WHERE k.conrelid=ANY($1::oid[]) AND k.contype='c' AND k.oid<>ALL($2::oid[])
 UNION ALL SELECT 'pg_class'::regclass,i.indexrelid,i.indrelid FROM pg_index i WHERE i.indrelid=ANY($1::oid[])
 ) SELECT NOT EXISTS(SELECT FROM objects o JOIN pg_depend d ON d.classid=o.classid AND d.objid=o.objid
 WHERE ((pg_identify_object(d.refclassid,d.refobjid,d.refobjsubid)).schema='pg_catalog'
  OR d.refclassid='pg_class'::regclass AND d.refobjid=o.relation
  OR d.refclassid='pg_constraint'::regclass AND EXISTS(SELECT FROM pg_constraint k
   WHERE k.oid=d.refobjid AND k.conrelid=o.relation AND k.contype IN ('p','u'))) IS NOT TRUE)"""


async def provision_isolated_fhir_source_storage(
    connection, policy, *, catalog_schema, namespace_owner, verify_role_profile
):
    """Prepare empty private native storage; this evidence cannot activate a worker."""
    _require(type(policy) is GenericSourcePolicy and callable(verify_role_profile), "explicit_policy_required")
    _require(connection.is_in_transaction() is True, "transaction_required")
    role_names = (policy.owner_role, policy.publisher_role, *policy.runtime_roles)
    for schema in (catalog_schema, policy.schema):
        storage_identifier(schema)
        _require(schema not in {"public", "information_schema"} and not schema.startswith("pg_"), "schema_invalid")
    storage_identifier(namespace_owner)
    _require(catalog_schema != policy.schema and namespace_owner not in role_names, "isolated_coordinates_required")
    async with connection.transaction():
        await connection.execute("SET LOCAL search_path=pg_catalog,pg_temp")
        task = asyncio.current_task()
        identity, catalog_oid = await _isolated_admin_preimage(connection, policy, catalog_schema, namespace_owner)
        template_rows, checks_by_name, excluded_oids = await _isolated_template_preimage(
            connection, catalog_schema, catalog_oid
        )
        await _create_isolated_namespace(connection, policy, namespace_owner, verify_role_profile)
        await _clone_isolated_templates(connection, policy, catalog_schema, template_rows, excluded_oids)
        preparation = await _capture_isolated_preparation(
            connection, policy, catalog_schema, catalog_oid, namespace_owner, identity, checks_by_name
        )
        _require(
            connection.is_in_transaction() is True
            and asyncio.current_task() is task
            and await _fhir_input_identity(connection) == identity,
            "original_admin_changed",
        )
        return preparation


async def _isolated_admin_preimage(connection, policy, catalog_schema, namespace_owner):
    """Authenticate the independent issuer and untouched namespace coordinates."""
    identity = await _fhir_input_identity(connection)
    role_names = (policy.owner_role, policy.publisher_role, *policy.runtime_roles)
    _require(
        identity["session"] == identity["current"] and identity["current"] not in (*role_names, namespace_owner),
        "independent_admin_required",
    )
    _require(
        await connection.fetchval("SELECT rolsuper FROM pg_roles WHERE rolname=session_user") is True,
        "native_admin_required",
    )
    catalog_oid = await connection.fetchval("SELECT oid::bigint FROM pg_namespace WHERE nspname=$1", catalog_schema)
    _require(
        catalog_oid
        and not await connection.fetchval("SELECT EXISTS(SELECT FROM pg_namespace WHERE nspname=$1)", policy.schema),
        "namespace_preimage_changed",
    )
    _require(
        await connection.fetchval(
            "SELECT NOT (n.rolcanlogin OR n.rolsuper OR n.rolcreatedb OR n.rolcreaterole OR n.rolreplication OR n.rolbypassrls) "
            "AND NOT EXISTS(SELECT FROM pg_roles r WHERE "
            "(r.rolname=ANY($2::text[]) AND (pg_has_role(r.oid,n.oid,'SET') OR pg_has_role(r.oid,n.oid,'USAGE'))) "
            "OR ((r.rolsuper OR r.rolcreatedb OR r.rolcreaterole OR r.rolreplication OR r.rolbypassrls) "
            "AND (pg_has_role(n.oid,r.oid,'SET') OR pg_has_role(n.oid,r.oid,'USAGE')))) "
            "FROM pg_roles n WHERE n.rolname=$1",
            namespace_owner,
            list(role_names),
        )
        is True,
        "namespace_owner_unprotected",
    )
    return identity, catalog_oid


async def _isolated_template_preimage(connection, catalog_schema, catalog_oid):
    """Pin reader-compatible templates and authenticate every copied dependency."""
    templates = [storage_relation(catalog_schema, name) for name in ISOLATED_FHIR_ROOTS]
    await connection.execute(
        "LOCK TABLE " + ",".join("ONLY " + name for name in templates) + " IN ACCESS SHARE MODE NOWAIT"
    )
    template_rows = await connection.fetch(_ISOLATED_TEMPLATE_SQL, catalog_oid, list(ISOLATED_FHIR_ROOTS))
    _require(
        len(template_rows) == len(ISOLATED_FHIR_ROOTS) and all(template["supported"] for template in template_rows),
        "template_preimage_changed",
    )
    prefix = await connection.fetchval("SELECT quote_ident($1::text)||'.'", catalog_schema)
    checks_by_name, excluded_oids = {}, []
    for template in template_rows:
        for check in _json(template["checks"]) or []:
            if template["relname"] == HEADER and check["name"] in _ISOLATED_APPLICATION_CHECKS:
                function, digest = _ISOLATED_APPLICATION_CHECKS[check["name"]]
                normalized = check["definition"].replace(
                    prefix + function.split("(")[0] + "(", "__catalog__." + function.split("(")[0] + "("
                )
                _require(
                    check["functions"] == [prefix + function]
                    and hashlib.sha256(normalized.encode()).hexdigest() == digest,
                    "application_check_changed",
                )
                checks_by_name[check["name"]] = dict(check, normalized_definition_sha256=digest)
                excluded_oids.append(check["oid"])
    _require(set(checks_by_name) == set(_ISOLATED_APPLICATION_CHECKS), "application_check_missing")
    _require(
        await connection.fetchval(
            _ISOLATED_DEPENDENCIES_SQL, [template["oid"] for template in template_rows], excluded_oids
        )
        is True,
        "template_dependency_unsupported",
    )
    return template_rows, checks_by_name, excluded_oids


async def _create_isolated_namespace(connection, policy, namespace_owner, verify_role_profile):
    """Close the new namespace ACL before the existing native role admission."""
    namespace = storage_identifier(policy.schema)
    await connection.execute(f"CREATE SCHEMA {namespace} AUTHORIZATION {storage_identifier(namespace_owner)}")
    for grant in await connection.fetch(
        "SELECT DISTINCT a.grantee,r.rolname FROM pg_namespace n CROSS JOIN LATERAL "
        "aclexplode(coalesce(n.nspacl,acldefault('n',n.nspowner))) a LEFT JOIN pg_roles r ON r.oid=a.grantee "
        "WHERE n.nspname=$1 AND a.grantee<>n.nspowner",
        policy.schema,
    ):
        principal = "PUBLIC" if grant["grantee"] == 0 else storage_identifier(grant["rolname"])
        await connection.execute(f"REVOKE ALL ON SCHEMA {namespace} FROM {principal} CASCADE")
    role_names = (policy.owner_role, policy.publisher_role, *policy.runtime_roles)
    await connection.execute(f"GRANT USAGE ON SCHEMA {namespace} TO " + ",".join(map(storage_identifier, role_names)))
    await connection.execute(f"GRANT CREATE ON SCHEMA {namespace} TO {storage_identifier(policy.owner_role)}")
    await storage_roles(connection, policy.schema, **policy.role_arguments())
    await verify_role_profile(connection, policy.schema, **policy.role_arguments())


async def _clone_isolated_templates(connection, policy, catalog_schema, template_rows, excluded_oids):
    """Clone native heap structure and close each new table's inherited grants."""
    for template in template_rows:
        private_relation = storage_relation(policy.schema, template["relname"])
        template_relation = storage_relation(catalog_schema, template["relname"])
        await connection.execute(
            f"CREATE TABLE {private_relation} (LIKE {template_relation} INCLUDING DEFAULTS INCLUDING GENERATED "
            "INCLUDING STORAGE INCLUDING INDEXES INCLUDING CONSTRAINTS) USING heap"
        )
        checkpoint_columns = CHECKPOINT_LAYOUTS.get(template["relname"], {}).get("columns", ())
        json_columns = (
            await connection.fetch(
                "SELECT attname FROM pg_attribute WHERE attrelid=$1::regclass AND attname=ANY($2::text[]) "
                "AND atttypid='pg_catalog.json'::regtype AND attnum>0 AND NOT attisdropped ORDER BY attnum",
                private_relation,
                [column[0] for column in checkpoint_columns if column[1] == "jsonb"],
            )
            if checkpoint_columns
            else []
        )
        if json_columns:
            # Only the new empty heap changes; canonical readers keep their original layout.
            await connection.execute(
                f"ALTER TABLE {private_relation} "
                + ",".join(
                    f"ALTER COLUMN {storage_identifier(column['attname'])} TYPE jsonb "
                    f"USING {storage_identifier(column['attname'])}::jsonb"
                    for column in json_columns
                )
            )
        for check in _json(template["checks"]) or []:
            if check["oid"] in excluded_oids:
                await connection.execute(
                    f"ALTER TABLE {private_relation} DROP CONSTRAINT {storage_identifier(check['name'])}"
                )
        await connection.execute(f"ALTER TABLE {private_relation} OWNER TO {storage_identifier(policy.owner_role)}")
        await _close_grants(connection, policy, template["relname"])


async def _capture_isolated_preparation(
    connection, policy, catalog_schema, catalog_oid, namespace_owner, identity, checks_by_name
):
    """Bind preparation evidence to the complete physical inventory and native roles."""
    inventory_by_name = await _capture(connection, policy, ISOLATED_FHIR_ROOTS)
    inventory = await _fhir_physical_inventory(connection, list(inventory_by_name.values()))
    coordinates_by_name = dict(
        await connection.fetchrow(
            "SELECT n.oid::bigint AS storage_schema_oid,n.nspowner::bigint AS namespace_owner_oid,"
            "r.oid::bigint AS owner_oid,p.oid::bigint AS publisher_oid FROM pg_namespace n,pg_roles r,pg_roles p "
            "WHERE n.nspname=$1 AND r.rolname=$2 AND p.rolname=$3",
            policy.schema,
            policy.owner_role,
            policy.publisher_role,
        )
    )
    _require(
        await connection.fetchval(
            "SELECT array_agg(relname::text ORDER BY relname) FROM pg_class WHERE relnamespace=$1::oid AND relkind<>'i'",
            coordinates_by_name["storage_schema_oid"],
        )
        == sorted(ISOLATED_FHIR_ROOTS),
        "private_inventory_changed",
    )
    role_names = (policy.owner_role, policy.publisher_role, *policy.runtime_roles, namespace_owner)
    role_oids_by_name = {
        principal["rolname"]: principal["oid"]
        for principal in await connection.fetch(
            "SELECT rolname,oid::bigint FROM pg_roles WHERE rolname=ANY($1::text[])", list(role_names)
        )
    }
    return dict(
        version=1,
        preparation_only=True,
        catalog_schema=catalog_schema,
        storage_schema=policy.schema,
        catalog_schema_oid=catalog_oid,
        database_oid=identity["database_oid"],
        **coordinates_by_name,
        role_oids=role_oids_by_name,
        inventory=inventory,
        required_application_checks=checks_by_name,
    )


_PAYLOAD_GUARDS = {
    "guard_pd_dataset_candidate_parent",
    "deny_pd_dataset_candidate_write",
    "guard_provider_directory_terminal_root_retirement_child",
    "guard_pd_uhc_flex_practitioner_dataset_resource",
    "guard_provider_directory_rooted_graph_dataset_resource",
    "guard_provider_directory_rooted_graph_dependency",
    "guard_provider_directory_subset_abandonment_child",
    "guard_tin_npi_connector_dataset_resource",
    "pd_entity_redirect_parent_guard",
    "guard_cms_npd_published_resource",
    "guard_cms_npd_covered_truncate",
    "guard_cms_npd_resource_witness",
    "guard_cms_npd_resource_witness_insert",
    "guard_cms_npd_published_relationship",
    "guard_cms_npd_published_relationship_insert",
    "guard_cms_npd_relationship_truncate",
    "guard_cms_npd_serving_evidence",
    "guard_provider_directory_resource_identity",
    "guard_cms_npd_covered_network_witness",
}
_RETIRED_FUNCTIONS = (
    "prepare_pd_rooted_dataset_candidate(text)",
    "finish_pd_rooted_dataset_candidate(text)",
    "prepare_pd_practitioner_dataset_candidate(text)",
    "finish_pd_practitioner_dataset_candidate(text)",
    "prepare_pd_dataset_candidate(text,text)",
    "finish_pd_dataset_candidate(text,text)",
    "validate_pd_dataset_candidate(text,text)",
    "prepare_pd_generic_dataset_storage()",
    "guard_pd_generic_dataset_finalization()",
    "validate_pd_dataset_bulk_relationships(text,text)",
    "pd_dataset_candidate_tables(text)",
    "guard_pd_dataset_candidate_parent()",
    "deny_pd_dataset_candidate_write()",
)


async def verify_generic_source_family(connection, policy, *, require_inert=True):
    """Whole attached family ownership, inherited/column ACL and native-only proof."""
    owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    roots = [storage_relation(policy.schema, name) for name in PAYLOAD_ROOTS]
    relation_rows = await connection.fetch(CATALOG_SQL, roots, owner, readers)
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
            and relation_by_field["relowner"] == owner
            and relation_by_field["relpersistence"] == "p"
            and relation_by_field["relkind"] in {"r", "p"}
            and not relation_by_field["relrowsecurity"]
            and not relation_by_field["relforcerowsecurity"]
            and relation_by_field["direct_acl_closed"]
            and relation_by_field["readers_closed"],
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
    """Capture only fixed, nonpartitioned managed roots under genuine source custody."""
    from process.provider_directory_generic_source_policy import bound_publisher
    from process.provider_directory_generic_source_storage import _layout_proof

    _require(publisher is bound_publisher(), "publisher_connection_changed")
    _require(
        type(roots) is tuple and bool(roots) and len(set(roots)) == len(roots) and set(roots) <= set(GLOBAL_ROOTS),
        "global_roots_changed",
    )
    family = await verify_generic_source_family(publisher.connection, publisher.policy)
    selected_by_root = {row["relname"]: row for row in family if row["relname"] in roots}
    _require(set(selected_by_root) == set(roots), "global_roots_missing")
    return {root: _layout_proof(selected_by_root[root]) for root in roots}


def _fhir_catalog_row(row):
    catalog_by_field = dict(row)
    for key in ("columns", "constraints", "indexes", "column_acls"):
        if key in catalog_by_field and isinstance(catalog_by_field[key], str):
            catalog_by_field[key] = json.loads(catalog_by_field[key])
    return catalog_by_field


def _fhir_digest(value):
    return hashlib.sha256(
        json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _require_fhir_relation_custody(relation_by_field, proof):
    _require(
        all(
            type(relation_by_field[key]) is int and 0 < relation_by_field[key] < 2**32
            for key in ("oid", "schema_oid", "schema_owner_oid")
        )
        and type(relation_by_field["column_acls"]) is list
        and bool(relation_by_field["columns"])
        and all(
            relation_by_field[key] is None or type(relation_by_field[key]) is str
            for key in ("raw_acl", "raw_schema_acl")
        ),
        "protected_fhir_inventory_changed",
    )
    _require(
        relation_by_field["nspname"] == proof["schema"]
        and relation_by_field["relowner"] == proof["owner_oid"]
        and relation_by_field["relpersistence"] == "p"
        and relation_by_field["relrowsecurity"] is False
        and relation_by_field["relforcerowsecurity"] is False
        and relation_by_field["inert"] is True
        and relation_by_field["native_checks"] is True
        and relation_by_field["direct_acl_closed"] is True
        and relation_by_field["readers_closed"] is True
        and all(
            constraint_by_field["valid"] is True and constraint_by_field["type"] in {"c", "p", "u"}
            for constraint_by_field in relation_by_field["constraints"]
        )
        and all(constraint_by_field["valid"] is True for constraint_by_field in relation_by_field["indexes"]),
        "protected_fhir_custody_changed",
    )


def _require_fhir_inventory(proof):
    inventory_rows = proof["inventory"]
    _require(
        type(proof["parent_oids"]) is list
        and len(proof["parent_oids"]) == 3
        and all(
            type(pair) is list and len(pair) == 2 and type(pair[1]) is int and 0 < pair[1] < 2**32
            for pair in proof["parent_oids"]
        ),
        "protected_fhir_roots_changed",
    )
    parent_oids_by_name = dict(proof["parent_oids"])
    _require(
        list(parent_oids_by_name) == list(FHIR_INPUT_ROOTS) and len(parent_oids_by_name) == 3,
        "protected_fhir_roots_changed",
    )
    _require(
        type(inventory_rows) is list
        and len({relation_by_field["oid"] for relation_by_field in inventory_rows}) == len(inventory_rows),
        "protected_fhir_inventory_changed",
    )
    observed_parents_by_name = {}
    for relation_by_field in inventory_rows:
        _require_fhir_relation_custody(relation_by_field, proof)
        if relation_by_field["relname"] in parent_oids_by_name:
            _require(
                relation_by_field["oid"] == parent_oids_by_name[relation_by_field["relname"]]
                and relation_by_field["relkind"] == "p"
                and relation_by_field["partition_key"] == "LIST (dataset_id)"
                and not relation_by_field["parents"]
                and relation_by_field["relispartition"] is False
                and relation_by_field["bound"] is None
                and relation_by_field["relfilenode"] is None,
                "protected_fhir_parent_changed",
            )
            observed_parents_by_name[relation_by_field["relname"]] = relation_by_field["oid"]
        else:
            _require(
                relation_by_field["relkind"] == "r"
                and relation_by_field["relispartition"] is True
                and len(relation_by_field["parents"]) == 1
                and relation_by_field["parents"][0] in parent_oids_by_name.values()
                and relation_by_field["partition_key"] is None
                and type(relation_by_field["relfilenode"]) is int
                and relation_by_field["relfilenode"] > 0
                and type(relation_by_field["bound"]) is str
                and re.fullmatch(r"FOR VALUES IN \('(?:[^']|'')+'(?:, '(?:[^']|'')+')*\)", relation_by_field["bound"]),
                "protected_fhir_leaf_changed",
            )
    _require(observed_parents_by_name == parent_oids_by_name, "protected_fhir_roots_changed")


def require_protected_fhir_input(proof):
    """Validate serialized linkage; only a native recapture authorizes its use."""
    _require(
        type(proof) is dict
        and set(proof)
        == {"version", "database_oid", "schema", "owner_oid", "profile", "parent_oids", "inventory", "family_sha256"}
        and type(proof["version"]) is int
        and proof["version"] == 1,
        "protected_fhir_proof_changed",
    )
    _require(
        all(type(proof[key]) is int and 0 < proof[key] < 2**32 for key in ("database_oid", "owner_oid"))
        and type(proof["family_sha256"]) is str
        and re.fullmatch("[0-9a-f]{64}", proof["family_sha256"]),
        "protected_fhir_identity_changed",
    )
    profile = proof["profile"]
    _require(
        type(profile) is dict
        and set(profile) == {"owner_role", "publisher_role", "runtime_roles", "control_runtime_roles"},
        "protected_fhir_profile_changed",
    )
    _require(
        type(profile["runtime_roles"]) is list and type(profile["control_runtime_roles"]) is list,
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


async def _fhir_input_identity(connection):
    return dict(
        await connection.fetchrow(
            "SELECT (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,"
            "session_user::text AS session,current_user::text AS current,pg_backend_pid() AS backend_pid,"
            "(SELECT virtualtransaction FROM pg_catalog.pg_locks WHERE pid=pg_catalog.pg_backend_pid() "
            "AND locktype='virtualxid' AND granted) AS transaction_id"
        )
    )


async def _fhir_physical_inventory(connection, selected_rows):
    physical_rows = await connection.fetch(
        "SELECT c.oid::bigint,pg_relation_filenode(c.oid)::bigint AS relfilenode,"
        "n.oid::bigint AS schema_oid,n.nspowner::bigint AS schema_owner_oid,"
        "c.relacl::text AS raw_acl,n.nspacl::text AS raw_schema_acl,"
        "(SELECT coalesce(jsonb_agg(jsonb_build_array(attnum,attacl::text) ORDER BY attnum),'[]'::jsonb) "
        "FROM pg_attribute WHERE attrelid=c.oid AND attnum>0 AND NOT attisdropped AND attacl IS NOT NULL) AS column_acls "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=ANY($1::oid[]) ORDER BY c.oid",
        [relation_by_field["oid"] for relation_by_field in selected_rows],
    )
    by_oid = {relation_by_field["oid"]: _fhir_catalog_row(relation_by_field) for relation_by_field in physical_rows}
    _require(
        set(by_oid) == {relation_by_field["oid"] for relation_by_field in selected_rows},
        "protected_fhir_inventory_changed",
    )
    return [
        dict(
            relation_by_field,
            **{key: field_value for key, field_value in by_oid[relation_by_field["oid"]].items() if key != "oid"},
        )
        for relation_by_field in selected_rows
    ]


async def _capture_fhir_input_proof(connection, source_policy, identity, owner, family_rows):
    parent_oids_by_name = {
        relation_by_field["relname"]: relation_by_field["oid"]
        for relation_by_field in family_rows
        if relation_by_field["relname"] in FHIR_INPUT_ROOTS
    }
    selected_rows = [
        relation_by_field
        for relation_by_field in family_rows
        if relation_by_field["relname"] in parent_oids_by_name
        or set(relation_by_field["parents"]) & set(parent_oids_by_name.values())
    ]
    inventory_rows = await _fhir_physical_inventory(connection, selected_rows)
    return dict(
        version=1,
        database_oid=identity["database_oid"],
        schema=source_policy.schema,
        owner_oid=owner,
        profile=dict(
            owner_role=source_policy.owner_role,
            publisher_role=source_policy.publisher_role,
            runtime_roles=list(source_policy.runtime_roles),
            control_runtime_roles=list(source_policy.control_runtime_roles),
        ),
        parent_oids=[[name, parent_oids_by_name[name]] for name in FHIR_INPUT_ROOTS],
        inventory=inventory_rows,
        family_sha256=_fhir_digest(family_rows),
    )


async def capture_protected_fhir_input(connection, *, source_policy):
    """Observe fixed Source partitions; this never admits a dataset or registers a heap."""
    _require(type(source_policy) is GenericSourcePolicy, "explicit_policy_required")
    _require(connection.is_in_transaction() is True, "transaction_required")
    task = asyncio.current_task()
    identity = await _fhir_input_identity(connection)
    _require(type(identity["transaction_id"]) is str and bool(identity["transaction_id"]), "transaction_required")
    roots = [storage_relation(source_policy.schema, name) for name in FHIR_INPUT_ROOTS]
    kinds = await connection.fetch(
        "SELECT c.relkind::text AS kind FROM unnest($1::text[]) roots(name) "
        "LEFT JOIN pg_class c ON c.oid=to_regclass(name) ORDER BY name",
        roots,
    )
    kinds = [relation_by_field["kind"] for relation_by_field in kinds]
    if kinds in ([None] * 3, ["r"] * 3):
        return None
    _require(kinds == ["p"] * 3, "protected_fhir_roots_changed")
    await connection.execute("LOCK TABLE " + ", ".join(roots) + " IN ACCESS SHARE MODE NOWAIT")
    family_rows = [
        _fhir_catalog_row(relation_by_field)
        for relation_by_field in await verify_generic_source_family(connection, source_policy)
    ]
    owner = next(
        relation_by_field["relowner"]
        for relation_by_field in family_rows
        if relation_by_field["relname"] == FHIR_INPUT_ROOTS[0]
    )
    _require_reader_family(
        family_rows,
        source_policy.schema,
        owner,
        await _native_control_references(connection, source_policy.schema, family_rows),
    )
    proof_by_field = await _capture_fhir_input_proof(connection, source_policy, identity, owner, family_rows)
    require_protected_fhir_input(proof_by_field)
    after_family_rows = [
        _fhir_catalog_row(relation_by_field)
        for relation_by_field in await verify_generic_source_family(connection, source_policy)
    ]
    _require(after_family_rows == family_rows, "protected_fhir_catalog_changed")
    _require(
        asyncio.current_task() is task
        and connection.is_in_transaction() is True
        and await _fhir_input_identity(connection) == identity,
        "protected_fhir_transaction_changed",
    )
    return proof_by_field


async def _verify_control_closure(connection, policy):
    """Keep low-volume header/legacy-plan controls protected before retiring hooks."""
    owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    roots = [storage_relation(policy.schema, name) for name in CONTROL_ROOTS]
    control_rows = await connection.fetch(CATALOG_SQL, roots, owner, readers)
    _require(
        len(control_rows) == len(roots) and {row["relname"] for row in control_rows} == set(CONTROL_ROOTS),
        "control_roots_missing",
    )
    _require(
        all(
            row["relowner"] == owner
            and row["direct_acl_closed"]
            and row["readers_closed"]
            and row["nspname"] == policy.schema
            for row in control_rows
        ),
        "control_closure_failed",
    )


async def verify_generic_source_read_family(connection, schema, owner_role, runtime_roles):
    """Read custody uses its existing explicit owner/runtime tuple, never a new login."""
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
    names = (owner_role, *runtime_roles)
    roles = await connection.fetch(ROLE_SQL, list(names), owner_role, owner_role, schema)
    by_name = {role["rolname"]: role for role in roles}
    _require(set(by_name) == set(names), "reader_roles_missing")
    owner = by_name[owner_role]
    flags = ("rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
    _require(
        not owner["rolcanlogin"] and not owner["elevated"] and not any(owner[flag] for flag in flags),
        "owner_unprotected",
    )
    _require(
        all(
            not by_name[name]["elevated"]
            and not by_name[name]["owner_set"]
            and not by_name[name]["namespace_owner_reachable"]
            and not by_name[name]["namespace_create"]
            and by_name[name]["namespace_owner_protected"]
            and by_name[name]["namespace_usage"]
            for name in runtime_roles
        ),
        "reader_authority",
    )
    relation_rows = await connection.fetch(
        CATALOG_SQL,
        [storage_relation(schema, name) for name in PAYLOAD_ROOTS],
        owner["oid"],
        [by_name[name]["oid"] for name in runtime_roles],
    )
    native_control_oids = await _native_control_references(connection, schema, relation_rows)
    _require_reader_family(relation_rows, schema, owner["oid"], native_control_oids)
    return relation_rows


def _require_reader_family(relation_rows, schema, owner_oid, native_control_oids=()):
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
            and relation_by_field["relowner"] == owner_oid
            and relation_by_field["relpersistence"] == "p"
            and not relation_by_field["relrowsecurity"]
            and not relation_by_field["relforcerowsecurity"]
            and relation_by_field["direct_acl_closed"]
            and relation_by_field["readers_closed"]
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


def _expected_generic_relationships():
    relationships = {
        (source_root, target_root, left, right)
        for source_root, target_root, left, right, _ in _expected_bulk_relationships()
    }
    relationships.add(
        (
            WITNESS,
            TABLES[0],
            ("dataset_id", "resource_type", "resource_id"),
            ("dataset_id", "resource_type", "resource_id"),
        )
    )
    entity, evidence, network, plan = CMS_IDENTITY_ROOTS
    key = ("source_id", "resource_type", "resource_id")
    payload_key = ("dataset_id", "resource_type", "resource_id")
    relationships.update(
        {
            (RELATIONSHIP, TABLES[0], payload_key, payload_key),
            (RELATIONSHIP, WITNESS, payload_key, payload_key),
            (entity, MINT_ROOTS[0], ("organization_id",), ("organization_id",)),
            (entity, MINT_ROOTS[1], ("site_id",), ("site_id",)),
            (CMS_DOCTORS_ROOTS[0], MINT_ROOTS[0], ("organization_id",), ("organization_id",)),
            (CMS_DOCTORS_ROOTS[1], MINT_ROOTS[1], ("site_id",), ("site_id",)),
            (evidence, entity, key, key),
            (network, entity, key, key),
            (network, MINT_ROOTS[2], ("network_id",), ("network_id",)),
            (plan, network, ("source_id", "network_resource_id"), ("source_id", "resource_id")),
            (
                plan,
                evidence,
                ("source_id", "network_resource_type", "network_resource_id", "release_id"),
                (*key, "release_id"),
            ),
        }
    )
    return relationships


async def _retire_payload_hooks(connection, policy, family):
    """Retire only authenticated historical hooks and exact bulk FK contracts."""
    by_oid = {relation_by_field["oid"]: relation_by_field for relation_by_field in family}
    hooks = await connection.fetch(
        "SELECT t.tgrelid::bigint,t.tgname,p.proname,n.nspname FROM pg_trigger t "
        "JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace "
        "WHERE t.tgrelid=ANY($1::oid[]) AND NOT t.tgisinternal ORDER BY t.tgrelid,t.tgname",
        list(by_oid),
    )
    _require(
        all(
            relation_by_field["nspname"] == policy.schema and relation_by_field["proname"] in _PAYLOAD_GUARDS
            for relation_by_field in hooks
        ),
        "legacy_payload_hook_changed",
    )
    for relation_by_field in hooks:
        table = by_oid[relation_by_field["tgrelid"]]
        await connection.execute(
            f"DROP TRIGGER {storage_identifier(relation_by_field['tgname'])} ON "
            f"{storage_relation(table['nspname'], table['relname'])}"
        )
    expected_relationships = _expected_generic_relationships()
    foreign_keys = await connection.fetch(
        "SELECT k.conrelid::bigint,k.conname,target.relname AS target,ns.nspname AS target_schema,"
        "ARRAY(SELECT a.attname::text FROM unnest(k.conkey) WITH ORDINALITY x(num,pos) JOIN pg_attribute a "
        "ON a.attrelid=k.conrelid AND a.attnum=x.num ORDER BY x.pos) AS source_columns,"
        "ARRAY(SELECT a.attname::text FROM unnest(k.confkey) WITH ORDINALITY x(num,pos) JOIN pg_attribute a "
        "ON a.attrelid=k.confrelid AND a.attnum=x.num ORDER BY x.pos) AS target_columns "
        "FROM pg_constraint k JOIN pg_class target ON target.oid=k.confrelid "
        "JOIN pg_namespace ns ON ns.oid=target.relnamespace "
        "WHERE k.conrelid=ANY($1::oid[]) AND k.contype='f' AND k.conparentid=0 ORDER BY k.conrelid,k.conname",
        list(by_oid),
    )
    for relation_by_field in foreign_keys:
        table = by_oid[relation_by_field["conrelid"]]
        # History children inherit the same exact source contract as their root.
        source_root = table["source_root"]
        _require(
            relation_by_field["target_schema"] == policy.schema
            and (
                source_root,
                relation_by_field["target"],
                tuple(relation_by_field["source_columns"]),
                tuple(relation_by_field["target_columns"]),
            )
            in expected_relationships,
            "legacy_foreign_key_changed",
        )
        await connection.execute(
            f"ALTER TABLE {storage_relation(table['nspname'], table['relname'])} "
            f"DROP CONSTRAINT {storage_identifier(relation_by_field['conname'])}"
        )


async def _partition_dataset_payload(connection, policy, root_name, keys_by_column):
    """Reuse the old native heap as history; never copy its payload rows."""
    root = storage_relation(policy.schema, root_name)
    relation_by_field = await connection.fetchrow(
        "SELECT oid::bigint,relkind::text FROM pg_class WHERE oid=to_regclass($1)", root
    )
    _require(relation_by_field is not None and relation_by_field["relkind"] in {"r", "p"}, "dataset_root_changed")
    if relation_by_field["relkind"] == "p":
        return
    history_name = "pd_generic_source_history_" + str(relation_by_field["oid"])
    history = storage_relation(policy.schema, history_name)
    keys = await connection.fetchval(f"SELECT array_agg(DISTINCT dataset_id ORDER BY dataset_id) FROM {root}") or []
    await connection.execute(f"ALTER TABLE {root} RENAME TO {storage_identifier(history_name)}")
    indexes = await connection.fetch(
        "SELECT indexrelid::bigint,relname FROM pg_index JOIN pg_class ON oid=indexrelid WHERE indrelid=$1",
        relation_by_field["oid"],
    )
    for index in indexes:
        await connection.execute(
            f"ALTER INDEX {storage_relation(policy.schema, index['relname'])} RENAME TO "
            f"{storage_identifier('pd_generic_source_idx_' + str(index['indexrelid']))}"
        )
    await connection.execute(
        f"CREATE TABLE {root} (LIKE {history} INCLUDING DEFAULTS INCLUDING CONSTRAINTS "
        "INCLUDING STORAGE) PARTITION BY LIST (dataset_id)"
    )
    await connection.execute(f"ALTER TABLE {root} OWNER TO {storage_identifier(policy.owner_role)}")
    key_columns = ",".join(storage_identifier(column) for column in keys_by_column)
    await connection.execute(f"ALTER TABLE {root} ADD PRIMARY KEY ({key_columns})")
    if root_name == RELATIONSHIP:
        await connection.execute(
            f"CREATE INDEX {storage_identifier(root_name + '_target_idx')} ON {root} (dataset_id,target_type,target_resource_id)"
        )
    if keys:
        literals = ",".join("'" + dataset_id.replace("'", "''") + "'" for dataset_id in keys)
        await connection.execute(f"ALTER TABLE {root} ATTACH PARTITION {history} FOR VALUES IN ({literals})")
    else:
        # Preserve even an empty historical OID without introducing a default
        # partition that would force scans on every new snapshot attachment.
        await connection.execute(f"ALTER TABLE {history} ADD CHECK (false)")


async def _retire_header_entrypoints(connection, policy):
    header = storage_relation(policy.schema, HEADER)
    for name, function in (
        ("pd_dataset_storage", "prepare_pd_generic_dataset_storage"),
        ("pd_dataset_generic_finalization", "guard_pd_generic_dataset_finalization"),
    ):
        actual = await connection.fetchval(
            "SELECT p.proname FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid "
            "WHERE t.tgrelid=to_regclass($1) AND t.tgname=$2 AND NOT t.tgisinternal",
            header,
            name,
        )
        _require(actual in {None, function}, "legacy_header_hook_changed")
        if actual is not None:
            await connection.execute(f"DROP TRIGGER {storage_identifier(name)} ON {header}")
    # RESTRICT is intentional: any unexpected surviving dependency rolls back
    # the entire cut rather than dropping unrelated objects with CASCADE.
    for signature in _RETIRED_FUNCTIONS:
        function, arguments = signature.split("(", 1)
        await connection.execute(f"DROP FUNCTION IF EXISTS {storage_relation(policy.schema, function)}({arguments}")


async def _close_legacy_entrypoint_grants(connection, policy):
    """Close callable definer bypasses before the complete role gate or hook removal."""
    for signature in _RETIRED_FUNCTIONS:
        function_name, arguments = signature.split("(", 1)
        qualified = storage_relation(policy.schema, function_name) + "(" + arguments
        function_oid = await connection.fetchval("SELECT to_regprocedure($1)::oid::bigint", qualified)
        if function_oid is None:
            continue
        await connection.execute(f"ALTER FUNCTION {qualified} OWNER TO {storage_identifier(policy.owner_role)}")
        grantees = await connection.fetch(
            "SELECT DISTINCT a.grantee,r.rolname FROM pg_proc p CROSS JOIN LATERAL "
            "aclexplode(coalesce(p.proacl,acldefault('f',p.proowner))) a LEFT JOIN pg_roles r ON r.oid=a.grantee "
            "WHERE p.oid=$1 AND a.grantee<>p.proowner",
            function_oid,
        )
        for grantee in grantees:
            principal = "PUBLIC" if grantee["grantee"] == 0 else storage_identifier(grantee["rolname"])
            await connection.execute(f"REVOKE ALL PRIVILEGES ON FUNCTION {qualified} FROM {principal} CASCADE")


async def _completed_legacy_dataset_plan(connection, policy):
    """Read the locked original migration identities without rediscovering heaps by name."""
    from process.provider_directory_generic_source_storage import _json

    plan = _json(
        await connection.fetchval(
            f"SELECT plan FROM {storage_relation(policy.schema, 'pd_dataset_migration_plan')} WHERE singleton"
        )
    )
    _require(
        type(plan) is dict
        and plan.get("complete") is True
        and type(plan.get("tables")) is dict
        and set(plan["tables"]) == set(TABLES),
        "legacy_plan_changed",
    )
    return plan


async def _retained_empty_dataset_histories(connection, policy):
    """Authenticate only exact detached, permanently empty heaps in the completed old plan."""
    plan = await _completed_legacy_dataset_plan(connection, policy)
    retained_rows = []
    for root, identity in plan["tables"].items():
        _require(
            type(identity) is dict
            and type(identity.get("original_oid")) is int
            and type(identity.get("parent_oid")) is int,
            "legacy_history_identity_changed",
        )
        history_by_field = await connection.fetchrow(
            "SELECT c.oid::bigint,c.relname,n.nspname,c.relkind::text,c.relispartition,"
            "ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=c.oid) AS parents,"
            "to_regclass($2)::oid::bigint AS current_parent FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace "
            "WHERE c.oid=$1",
            identity["original_oid"],
            storage_relation(policy.schema, root),
        )
        _require(
            history_by_field is not None
            and history_by_field["nspname"] == policy.schema
            and history_by_field["current_parent"] == identity["parent_oid"],
            "legacy_history_identity_changed",
        )
        if history_by_field["parents"]:
            _require(
                history_by_field["parents"] == [identity["parent_oid"]] and history_by_field["relispartition"],
                "legacy_history_identity_changed",
            )
            continue
        _require(
            identity.get("keys") == []
            and history_by_field["relkind"] == "r"
            and not history_by_field["relispartition"]
            and history_by_field["relname"] == "pd_dataset_history_" + str(identity["original_oid"]),
            "legacy_history_unbound",
        )
        relation = storage_relation(policy.schema, history_by_field["relname"])
        await connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE")
        valid = await connection.fetchval(
            f"SELECT to_regclass($1)::oid=$2 AND NOT EXISTS(SELECT FROM {relation}) AND EXISTS(SELECT FROM pg_constraint "
            "WHERE conrelid=$2 AND contype='c' AND convalidated AND pg_get_expr(conbin,conrelid,true)='false')",
            relation,
            identity["original_oid"],
        )
        _require(valid is True, "legacy_history_not_empty")
        retained_rows.append({**dict(history_by_field), "source_root": root})
    return retained_rows


async def _verify_retained_empty_histories(connection, policy, retained):
    """Retain each authenticated old OID with native-only checks and closed read custody."""
    from process.provider_directory_generic_source_storage import _table_proof

    if not retained:
        return
    owner, readers = await storage_roles(connection, policy.schema, **policy.role_arguments())
    roots = [storage_relation(policy.schema, row["relname"]) for row in retained]
    rows = await connection.fetch(CATALOG_SQL, roots, owner, readers)
    _require({row["oid"] for row in rows} == {row["oid"] for row in retained}, "legacy_history_identity_changed")
    for row in rows:
        _require(row["relowner"] == owner and row["readers_closed"], "legacy_history_custody_changed")
        _table_proof(row, private=True)


async def provision_generic_source_storage(
    connection, policy, *, verify_role_profile, verify_writer_closure, verify_legacy_guards
):
    """Owner/ACL closure and known guard retirement in one independent-admin savepoint."""
    _require(type(policy) is GenericSourcePolicy and connection.is_in_transaction() is True, "explicit_admin_required")
    _require(
        all(callable(verifier) for verifier in (verify_role_profile, verify_writer_closure, verify_legacy_guards)),
        "complete_closure_verifiers_required",
    )
    actor = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    names = (policy.owner_role, policy.publisher_role, *policy.runtime_roles)
    _require(actor["session"] == actor["current"] and actor["current"] not in names, "independent_admin_required")
    async with connection.transaction():
        await storage_roles(connection, policy.schema, **policy.role_arguments())
        await connection.execute("SET LOCAL lock_timeout='5s'")
        roots = [storage_relation(policy.schema, name) for name in (*PAYLOAD_ROOTS, *CONTROL_ROOTS)]
        await connection.execute("LOCK TABLE " + ",".join(roots) + " IN ACCESS EXCLUSIVE MODE")
        family = await connection.fetch(_FAMILY_SQL, roots)
        _require(len(family) >= len(roots), "roots_missing")
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        coverage_before = await capture_source_coverage_control(connection, policy, legacy=True)
        checkpoint_before = await capture_checkpoint_controls(connection, policy, legacy=True)
        retained_empty = await _retained_empty_dataset_histories(connection, policy)
        # Preserve complete native family evidence for the trusted historical
        # guard verifier before any guard/FK is retired.
        await verify_legacy_guards(connection, policy, [*family, *retained_empty])
        for relation in (*family, *retained_empty):
            await _close_relation_grants(connection, relation, policy.owner_role, policy.runtime_roles)
        await grant_source_admission_controls(connection, policy)
        await _close_legacy_entrypoint_grants(connection, policy)
        await verify_generic_source_family(connection, policy, require_inert=False)
        await _verify_control_closure(connection, policy)
        await verify_writer_closure(connection, policy)
        await retire_bulk_abandonment_hooks(connection, policy, checkpoint_before)
        await retire_proof_control_hooks(connection, policy)
        payload_relations = await connection.fetch(
            "WITH RECURSIVE family(oid,source_root) AS (SELECT to_regclass(name)::oid,root "
            "FROM unnest($1::text[],$2::text[]) n(name,root) UNION ALL SELECT i.inhrelid,f.source_root "
            "FROM pg_inherits i JOIN family f ON f.oid=i.inhparent) "
            "SELECT c.oid::bigint,n.nspname,c.relname,f.source_root FROM family f JOIN pg_class c ON c.oid=f.oid "
            "JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY c.oid",
            [storage_relation(policy.schema, name) for name in PAYLOAD_ROOTS],
            list(PAYLOAD_ROOTS),
        )
        await _retire_payload_hooks(connection, policy, [*payload_relations, *retained_empty])
        from process.provider_directory_generic_source_completion import _KEYS

        for root_name in (WITNESS, RELATIONSHIP):
            await _partition_dataset_payload(connection, policy, root_name, _KEYS[root_name])
        # The newly created root must receive the same exact custody before
        # any old definer/storage-creation function is removed.
        for relation in await connection.fetch(_FAMILY_SQL, roots):
            await _close_relation_grants(connection, relation, policy.owner_role, policy.runtime_roles)
        await verify_generic_source_family(connection, policy)
        await _verify_retained_empty_histories(connection, policy, retained_empty)
        await _retire_header_entrypoints(connection, policy)
        await _require_coverage_identity(connection, policy, coverage_before)
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        return await verify_generic_source_family(connection, policy)


async def _require_coverage_identity(connection, policy, before):
    """Custody may change only owner and grants of the exact authenticated control."""
    after = await capture_source_coverage_control(connection, policy)
    _require(all(before[field] == after[field] for field in ("oid", "layout", "guards")), "coverage_identity_changed")
