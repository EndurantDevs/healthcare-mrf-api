# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed native legacy and write closure for the explicit SOURCE release cut."""

from db.migration_provider_directory_dataset_candidates import TABLES
from db.migration_provider_directory_terminal_root_retirement_guards import CHILD_TRIGGER_SUFFIXES
from db.plan_release_storage import storage_relation
from process import network_fhir_source_custody as retained
from process.provider_directory_cms_callable_custody import _body_digest, _decoded
from process.provider_directory_generic_source_provision import (
    _RETIRED_FUNCTIONS,
)
from process.provider_directory_generic_source_storage import _require

_IDENTITY = """SELECT session_user::text AS session,current_user::text AS current,
 pg_backend_pid() AS backend,pg_current_xact_id()::text AS xid,
 (SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS administrator"""
_FAMILY = """WITH RECURSIVE family(oid,root) AS (
 SELECT to_regclass(name)::oid,root FROM unnest($1::text[],$2::text[]) n(name,root)
 UNION ALL SELECT i.inhrelid,f.root FROM pg_inherits i JOIN family f ON i.inhparent=f.oid)
 SELECT c.oid::bigint,n.nspname,c.relname,f.root,c.relowner::bigint,c.relkind::text,
 ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=c.oid ORDER BY inhparent) AS parents
 FROM family f JOIN pg_class c ON c.oid=f.oid JOIN pg_namespace n ON n.oid=c.relnamespace ORDER BY c.oid"""
_HOOKS = """SELECT t.tgrelid::bigint,t.tgname,t.tgtype,t.tgenabled::text,t.tgnargs,
 t.tgqual IS NULL AS unconditional,t.tgoldtable,t.tgnewtable,t.tgattr::text,t.tgargs,
 t.tgdeferrable,t.tginitdeferred,t.tgconstraint::bigint,t.tgparentid::bigint,
 ARRAY(SELECT a.attname::text FROM unnest(t.tgattr::smallint[]) WITH ORDINALITY x(num,pos)
 JOIN pg_attribute a ON a.attrelid=t.tgrelid AND a.attnum=x.num ORDER BY x.pos) AS update_columns,
 p.proname,p.proowner::bigint,p.prosecdef,p.proconfig,p.prosrc,p.pronargs,p.provolatile::text,
 n.nspname,l.lanname,p.prorettype='trigger'::regtype AS returns_trigger,to_jsonb(t) AS native_trigger,
 to_jsonb(p) AS native_function,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,
 a.privilege_type,a.is_grantable) ORDER BY a.grantee,a.grantor,a.privilege_type)
 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a),'[]'::jsonb) AS function_acl
 FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid JOIN pg_namespace n ON n.oid=p.pronamespace
 JOIN pg_language l ON l.oid=p.prolang WHERE t.tgrelid=ANY($1::oid[]) AND NOT t.tgisinternal
 ORDER BY t.tgrelid,t.tgname"""
_FUNCTIONS = """SELECT signature,to_jsonb(p) AS definition,l.lanname AS language,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,
 a.privilege_type,a.is_grantable) ORDER BY a.grantee,a.grantor,a.privilege_type)
 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a),'[]'::jsonb) AS acl
 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
 LEFT JOIN unnest($1::text[]) selected(signature) ON p.oid=to_regprocedure(signature)
 WHERE p.pronamespace=to_regnamespace($2) AND p.proname=ANY($3::text[]) ORDER BY p.oid"""
_WRITE_REACH = """WITH RECURSIVE ordinary(oid) AS (
 SELECT oid FROM pg_roles WHERE rolname=ANY($2::text[]) UNION
 SELECT target.oid FROM pg_roles target WHERE EXISTS(SELECT FROM pg_roles actor
 WHERE actor.rolname=ANY($2::text[]) AND pg_has_role(actor.oid,target.oid,'SET')))
 SELECT EXISTS(SELECT FROM ordinary r,unnest($1::oid[]) c(oid)
 WHERE has_table_privilege(r.oid,c.oid,'INSERT,UPDATE,DELETE,TRUNCATE,REFERENCES,TRIGGER,MAINTAIN')
 OR has_any_column_privilege(r.oid,c.oid,'INSERT,UPDATE,REFERENCES'))"""
_GRAPH = """SELECT jsonb_build_object(
 'roles',(SELECT jsonb_agg(to_jsonb(r)-'rolpassword' ORDER BY oid) FROM pg_roles r),
 'memberships',(SELECT jsonb_agg(to_jsonb(m) ORDER BY roleid,member) FROM pg_auth_members m))"""
_SEQUENCE_REACH = """SELECT EXISTS(SELECT FROM pg_depend d JOIN pg_class s
 ON d.classid='pg_class'::regclass AND d.objid=s.oid AND s.relkind='S',pg_roles r
 WHERE d.refclassid='pg_class'::regclass AND d.refobjid=ANY($1::oid[]) AND d.deptype IN('a','i')
 AND EXISTS(SELECT FROM pg_roles ordinary WHERE ordinary.rolname=ANY($2::text[])
 AND pg_has_role(ordinary.oid,r.oid,'SET')) AND has_sequence_privilege(r.oid,s.oid,'USAGE,UPDATE'))"""

# Generated from the fixed candidate API SQL; focused tests bind every body.
_LEGACY_FUNCTIONS = {
    "deny_pd_dataset_candidate_write()": (
        (),
        "trigger",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        False,
        "70ff2e508ae8bbe96d48b9acf9713ca40597a7854d0841c5408c7127cb93a69c",
    ),
    "prepare_pd_generic_dataset_storage()": (
        (),
        "trigger",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "4078d67c51ad7277287e143282ab858e2335ba422d9fc24685d4c8abeb5590d2",
    ),
    "pd_dataset_candidate_tables(text)": (
        (("kind", "text"),),
        "text[]",
        (),
        "sql",
        "i",
        False,
        "u",
        False,
        "7c31768ad06c418a96015ca49b998403c7d8fabcccd6eece18092ed07e742215",
    ),
    "guard_pd_dataset_candidate_parent()": (
        (),
        "trigger",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "83da651b105792f6ece471f3edc5e8f179bf6ce8cd87087a2a78b28e634f3afd",
    ),
    "prepare_pd_dataset_candidate(text,text)": (
        (("requested_dataset", "text"), ("kind", "text")),
        "jsonb",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "2a3e8771d36cf0cb92c8f6682dcdbfc749d7f716cfc1d8db8d9a7de8b014d110",
    ),
    "validate_pd_dataset_bulk_relationships(text,text)": (
        (("requested_dataset", "text"), ("kind", "text")),
        "void",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        False,
        "51e3cdea671cf0edfa8d2ee0ba9ee5796ad4f4f865e5acf41e69b824ea2fab76",
    ),
    "guard_pd_generic_dataset_finalization()": (
        (),
        "trigger",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "e70dc40907f76e969540817665861336faa870016569373a7e8026aa9ac5dde0",
    ),
    "guard_pd_dataset_parent_key_update()": (
        (),
        "trigger",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "44c354bc8249e3ea1a28d31e05b2c960c467330bf95bc57325803e5ef4d2a324",
    ),
    "guard_pd_dataset_bulk_parent_delete()": (
        (),
        "trigger",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "855e7bda72ceca29b68c5edab8cd198d35c1cdeaaed423ec68920ee03ed2c582",
    ),
    "validate_pd_dataset_candidate(text,text)": (
        (("requested_dataset", "text"), ("kind", "text")),
        "void",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "fe32b7bff19a81444d93b8255cb41828718b3fdfe1ac5abedecd0dd580049ea2",
    ),
    "finish_pd_dataset_candidate(text,text)": (
        (("requested_dataset", "text"), ("kind", "text")),
        "void",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        True,
        "89541c4bab5025d73b58b6a3e9d5834ae51f11e6fc8777d951b085c6be924ff5",
    ),
    "prepare_pd_rooted_dataset_candidate(text)": (
        (("dataset", "text"),),
        "jsonb",
        (),
        "sql",
        "v",
        False,
        "u",
        True,
        "d6c57584bcdbe73fc602c0ab8d337593c2af044f009441cd0494ea3b90c6cd01",
    ),
    "finish_pd_rooted_dataset_candidate(text)": (
        (("dataset", "text"),),
        "void",
        (),
        "sql",
        "v",
        False,
        "u",
        True,
        "e1e880d9102e23badb273bda04e4039b174b49ef9e8ece93e2298d98c7f14476",
    ),
    "prepare_pd_practitioner_dataset_candidate(text)": (
        (("dataset", "text"),),
        "jsonb",
        (),
        "sql",
        "v",
        False,
        "u",
        True,
        "8a8d1fe85554bf10308fa57a8646195b3b5c844ac64dc4fe28f5c974d7ae4925",
    ),
    "finish_pd_practitioner_dataset_candidate(text)": (
        (("dataset", "text"),),
        "void",
        (),
        "sql",
        "v",
        False,
        "u",
        True,
        "07bb22841c7dc16aaa42866964e93fbdb023998e146d993cfac493814fd19da8",
    ),
    "guard_provider_directory_resource_identity()": (
        (),
        "trigger",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        False,
        "453f57c1a20237212ad3d786f8e77aea40844b39638947d94333fa6e70c515c1",
    ),
}
assert set(_RETIRED_FUNCTIONS) <= _LEGACY_FUNCTIONS.keys()


def _hook(function, event, *, mode="O", old=None, new=None, arguments=b"", columns=()):
    return function, event, mode, old, new, arguments, columns


def _rooted_hooks(root):
    hooks_by_name = {}
    rooted_suffix = dict(
        zip(TABLES[:4], ("resource", "insurance_plan", "network_plan", "affiliation_org"), strict=True)
    )[root]
    hooks_by_name["pd_rg_dep_" + rooted_suffix + "_row"] = _hook(
        "guard_provider_directory_rooted_graph_dependency", 28, mode="A"
    )
    hooks_by_name["pd_rg_dep_" + rooted_suffix + "_truncate"] = _hook(
        "guard_provider_directory_rooted_graph_dependency", 34, mode="A"
    )
    return hooks_by_name


def _resource_hooks():
    hook_by_name = {}
    for action, event, old, new in (
        ("insert", 4, None, "new_rows"),
        ("update", 16, "old_rows", "new_rows"),
        ("delete", 8, "old_rows", None),
        ("truncate", 34, None, None),
    ):
        hook_by_name["tin_npi_connector_dataset_resource_" + action + "_guard"] = _hook(
            "guard_tin_npi_connector_dataset_resource", event, mode="A", old=old, new=new
        )
    for action, event, old, new in (
        ("insert", 4, None, "new_rows"),
        ("update", 16, "old_rows", "new_rows"),
        ("delete", 8, "old_rows", None),
    ):
        hook_by_name["pd_subset_abandonment_resource_" + action + "_guard"] = _hook(
            "guard_provider_directory_subset_abandonment_child", event, mode="A", old=old, new=new
        )
    return hook_by_name


def _payload_hooks(root, *, history=False, sealed_always=False):
    """Known parent/leaf contracts come from the completed native migration plan."""
    if root in TABLES:
        if root in TABLES[4:]:
            return {
                "pd_dataset_sealed": _hook("deny_pd_dataset_candidate_write", 62, mode="A" if sealed_always else "O")
            }
        hook_by_name = {
            "a_pd_candidate_" + operation.lower() + "_" + image.lower(): _hook(
                "guard_pd_dataset_candidate_parent",
                event,
                old="affected_rows" if image == "OLD" else None,
                new="affected_rows" if image == "NEW" else None,
                arguments=(root + "\0" + image + "\0").encode(),
            )
            for operation, image, event in (
                ("INSERT", "NEW", 4),
                ("UPDATE", "NEW", 16),
                ("UPDATE", "OLD", 16),
                ("DELETE", "OLD", 8),
            )
        }
        if history:
            return hook_by_name
        hook_by_name.update(_rooted_hooks(root))
        suffix = CHILD_TRIGGER_SUFFIXES.get(root)
        if suffix is not None:
            hook_by_name["pd_trr_" + suffix + "_truncate"] = _hook(
                "guard_provider_directory_terminal_root_retirement_child", 34, mode="A"
            )
        if root == TABLES[0]:
            hook_by_name.update(_resource_hooks())
    else:
        hook_by_name = {}
    for name, event in retained._GUARDS.get(root, {}).items():
        old, new = retained._TRANSITION_TABLES.get(name, (None, None))
        hook_by_name[name] = _hook(retained._GUARD_FUNCTIONS[name], event, old=old, new=new)
    if root == "provider_directory_resource_identity":
        hook_by_name.update(
            pd_resource_identity_immutable=_hook("guard_provider_directory_resource_identity", 27),
            pd_resource_identity_no_truncate=_hook("guard_provider_directory_resource_identity", 34),
            cms_npd_covered_truncate_forbidden=_hook("guard_cms_npd_covered_truncate", 34),
        )
    if root in {"provider_directory_entity_source_binding", "provider_directory_entity_release_evidence"}:
        hook_by_name["pd_entity_redirect_parent_guard"] = _hook("pd_entity_redirect_parent_guard", 27)
    return hook_by_name


def _finalization_spec(spec, schema, body):
    from db.migration_cms_snapshot_allocator import generic_finalization_bodies

    if body == generic_finalization_bodies(schema)[1]:
        return (*spec[:-1], _body_digest(body, schema))
    return spec


def _require_native_function(definition, spec, language, schema, namespace, owners):
    from process.provider_directory_source_callable_custody import _require_definition

    _require(int(definition["proowner"]) in owners, "release_legacy_function_owner_changed")
    if (
        definition["proname"] == "guard_pd_dataset_candidate_parent"
        and spec == _LEGACY_FUNCTIONS["guard_pd_dataset_candidate_parent()"]
    ):
        from db.migration_generic_source_candidates import _canonical_copy_guard_bodies, _cms_canonical_copy_guard_body

        approved_bodies = (
            _canonical_copy_guard_bodies(schema, "hp_snapshot_retention")[1],
            _cms_canonical_copy_guard_body(schema, "hp_snapshot_retention"),
        )
        if definition["prosrc"] in approved_bodies:
            spec = (*spec[:-1], _body_digest(definition["prosrc"], schema))
    if (
        definition["proname"] == "guard_pd_generic_dataset_finalization"
        and spec == _LEGACY_FUNCTIONS["guard_pd_generic_dataset_finalization()"]
    ):
        spec = _finalization_spec(spec, schema, definition["prosrc"])
    _require_definition(
        dict(definition=definition, language=language),
        spec,
        schema,
        namespace,
        {"trigger": "2279", "void": "2278", "boolean": "16", "text[]": "1009", "text": "25", "jsonb": "3802"},
        '"' + schema.replace('"', '""') + '"',
    )


def _require_allocator_guard_acl(function_record, allocator, schema, namespace, owners, namespace_owner):
    """Admit one allocator edge after the caller verifies the guard's fixed definition."""
    owner = int(_decoded(function_record["definition"])["proowner"])
    expected_acl_edges = {(owner, owner, "EXECUTE", False)}
    actual_acl_edges = set(map(tuple, _decoded(function_record["acl"])))
    if actual_acl_edges == expected_acl_edges:
        return
    _require(allocator is not None, "release_legacy_callable_acl_changed")
    definition = _decoded(allocator["definition"])
    _require(
        definition["proname"] == "prepare_pd_generic_dataset_storage"
        and allocator["signature"] == storage_relation(schema, definition["proname"]) + "()"
        and 0 < int(definition["oid"]) < 2**32
        and int(definition["oid"]) != int(_decoded(function_record["definition"])["oid"]),
        "release_legacy_callable_acl_changed",
    )
    _require_native_function(
        definition,
        _LEGACY_FUNCTIONS["prepare_pd_generic_dataset_storage()"],
        allocator["language"],
        schema,
        namespace,
        owners,
    )
    allocator_owner = int(definition["proowner"])
    _require(
        allocator_owner == namespace_owner
        and owner in owners
        and owner != namespace_owner
        and set(map(tuple, _decoded(allocator["acl"]))) == {(allocator_owner, allocator_owner, "EXECUTE", False)}
        and actual_acl_edges == expected_acl_edges | {(namespace_owner, owner, "EXECUTE", False)},
        "release_legacy_callable_acl_changed",
    )


def _require_hook(row, expected):
    actual = (
        row["proname"],
        row["tgtype"],
        row["tgenabled"],
        row["tgoldtable"],
        row["tgnewtable"],
        bytes(row["tgargs"]),
        tuple(row["update_columns"]),
    )
    _require(
        actual == expected
        and row["unconditional"]
        and not row["tgdeferrable"]
        and not row["tginitdeferred"]
        and row["tgconstraint"] == 0
        and row["tgparentid"] == 0
        and _decoded(row["native_trigger"])["tgconstrrelid"] == "0"
        and _decoded(row["native_trigger"])["tgconstrindid"] == "0"
        and _decoded(row["native_trigger"])["tgisinternal"] is False
        and _decoded(row["native_trigger"])["tgfoid"] == _decoded(row["native_function"])["oid"]
        and row["tgnargs"] == expected[5].count(b"\0"),
        "release_legacy_invocation_changed",
    )


def _require_guard_function(guard, schema, namespace, owners):
    definition = _decoded(guard["native_function"])
    _require(int(guard["proowner"]) in owners, "release_legacy_guard_owner_changed")
    retained._require_function(guard, schema, guard["proowner"])
    expected_by_field = dict(
        prokind="f",
        prorettype="2279",
        pronargs=0,
        proargtypes=[],
        proargnames=None,
        proargmodes=None,
        proallargtypes=None,
        proargdefaults=None,
        pronargdefaults=0,
        provariadic="0",
        proleakproof=False,
        proisstrict=False,
        proretset=False,
        prosupport="-",
        probin=None,
        prosqlbody=None,
        protrftypes=None,
        procost=100,
        prorows=0,
        proparallel="u",
    )
    owner = int(guard["proowner"])
    acl_rows = [[owner, owner, "EXECUTE", False]]
    if not guard["prosecdef"]:
        acl_rows.append([0, owner, "EXECUTE", False])
    _require(
        guard["nspname"] == schema
        and int(definition["pronamespace"]) == namespace
        and all(definition[field] == field_value for field, field_value in expected_by_field.items())
        and sorted(_decoded(guard["function_acl"])) == sorted(acl_rows),
        "release_legacy_guard_definition_changed",
    )
