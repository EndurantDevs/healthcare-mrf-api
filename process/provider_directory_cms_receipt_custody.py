# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protect the existing common receipt while retaining its two-column legacy writers."""

import asyncio
import json
from copy import deepcopy

from db.plan_release_storage import storage_identifier, storage_relation
from process.provider_directory_cms_callable_custody import _admin_identity, capture_cms_callable_custody
from process.provider_directory_cms_control_privileges import (
    READ_COLUMN_GRANTS,
    cms_control_read_edges,
    cms_control_read_oids,
)
from process.provider_directory_generic_source_policy import GenericSourcePolicy

ROOT = "provider_directory_cms_serving_receipt"
_RIGHTS = {"INSERT", "SELECT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER", "MAINTAIN"}
_CATALOG_SQL = """WITH root AS (SELECT * FROM pg_class WHERE oid=to_regclass($1)),
 heaps AS (SELECT * FROM root UNION ALL SELECT t.* FROM root r JOIN pg_class t ON t.oid=r.reltoastrelid),
 objects AS (SELECT * FROM heaps UNION ALL SELECT c.* FROM heaps h JOIN pg_index i ON i.indrelid=h.oid
 JOIN pg_class c ON c.oid=i.indexrelid)
 SELECT jsonb_build_object(
 'classes',(SELECT jsonb_agg(to_jsonb(c)-ARRAY['relpages','reltuples','relallvisible','relallfrozen',
 'relfrozenxid','relminmxid'] ORDER BY c.oid) FROM objects c),
 'attributes',(SELECT jsonb_agg(to_jsonb(a)||jsonb_build_object('default_expression',
 COALESCE(pg_get_expr(d.adbin,d.adrelid,true),'')) ORDER BY a.attrelid,a.attnum)
 FROM heaps h JOIN pg_attribute a ON a.attrelid=h.oid LEFT JOIN pg_attrdef d
 ON (d.adrelid,d.adnum)=(a.attrelid,a.attnum) WHERE a.attnum>0),
 'indexes',(SELECT jsonb_agg(to_jsonb(i)||jsonb_build_object('index_name',c.relname,
 'method',m.amname,'index_expressions',COALESCE(pg_get_expr(i.indexprs,i.indrelid,true),''),
 'index_predicate',COALESCE(pg_get_expr(i.indpred,i.indrelid,true),''),
 'native',NOT EXISTS(SELECT FROM unnest(i.indclass) x(oid) JOIN pg_opclass o ON o.oid=x.oid
 JOIN pg_namespace n ON n.oid=o.opcnamespace WHERE n.nspname<>'pg_catalog' OR o.opcname NOT IN
 ('text_ops','int8_ops','uuid_ops','xid8_ops','bool_ops','timestamp_ops','timestamptz_ops','oid_ops','int4_ops')))
 ORDER BY i.indexrelid) FROM heaps h JOIN pg_index i ON i.indrelid=h.oid
 JOIN pg_class c ON c.oid=i.indexrelid JOIN pg_am m ON m.oid=c.relam),
 'constraints',(SELECT jsonb_agg(to_jsonb(k)||jsonb_build_object('constraint_definition',
 pg_get_constraintdef(k.oid,true)) ORDER BY k.contype,pg_get_constraintdef(k.oid,true),k.oid)
 FROM root r JOIN pg_constraint k ON k.conrelid=r.oid),
 'triggers',(SELECT jsonb_agg(to_jsonb(t) ORDER BY t.oid) FROM root r JOIN pg_trigger t ON t.tgrelid=r.oid),
 'rules',(SELECT jsonb_agg(to_jsonb(w) ORDER BY w.oid) FROM heaps h JOIN pg_rewrite w ON w.ev_class=h.oid),
 'policies',(SELECT jsonb_agg(to_jsonb(p) ORDER BY p.oid) FROM heaps h JOIN pg_policy p ON p.polrelid=h.oid),
 'inheritance',(SELECT jsonb_agg(to_jsonb(i) ORDER BY i.inhrelid,i.inhparent) FROM pg_inherits i
 WHERE i.inhrelid IN(SELECT oid FROM objects) OR i.inhparent IN(SELECT oid FROM objects)),
 'table_acl',(SELECT COALESCE(jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,
 a.privilege_type,a.is_grantable) ORDER BY a.grantee,a.grantor,a.privilege_type),'[]'::jsonb)
 FROM root r,LATERAL aclexplode(COALESCE(r.relacl,acldefault('r',r.relowner))) a),
 'column_acl',(SELECT COALESCE(jsonb_agg(jsonb_build_array(c.attnum,a.grantee::bigint,
 a.grantor::bigint,a.privilege_type,a.is_grantable) ORDER BY c.attnum,a.grantee,a.grantor,a.privilege_type),
 '[]'::jsonb) FROM root r JOIN pg_attribute c ON c.attrelid=r.oid,LATERAL aclexplode(c.attacl) a),
 'access',(SELECT jsonb_agg(jsonb_build_object('role',p.oid::bigint,'superuser',p.rolsuper,
 'builtin',p.oid<16384 AND NOT p.rolcanlogin,
 'owner_reach',pg_has_role(p.oid,$2::oid,'USAGE') OR pg_has_role(p.oid,$2::oid,'SET'),
 'owner_usage',pg_has_role(p.oid,$2::oid,'USAGE'),'owner_set',pg_has_role(p.oid,$2::oid,'SET'),
 'builder_reach',pg_has_role(p.oid,$3::oid,'USAGE') OR pg_has_role(p.oid,$3::oid,'SET'),
 'table',ARRAY(SELECT privilege FROM unnest(ARRAY['INSERT','SELECT','UPDATE','DELETE','TRUNCATE',
 'REFERENCES','TRIGGER','MAINTAIN']) privilege WHERE has_table_privilege(p.oid,r.oid,privilege)),
 'columns',(SELECT jsonb_agg(jsonb_build_array(c.attnum,
 ARRAY(SELECT privilege FROM unnest(ARRAY['INSERT','SELECT','UPDATE','REFERENCES']) privilege
 WHERE has_column_privilege(p.oid,r.oid,c.attnum,privilege))) ORDER BY c.attnum)
 FROM pg_attribute c WHERE c.attrelid=r.oid AND c.attnum>0 AND NOT c.attisdropped)) ORDER BY p.oid)
 FROM root r CROSS JOIN pg_roles p))"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_receipt_" + code)


def _decoded(part_by_field):
    return json.loads(part_by_field) if type(part_by_field) is str else part_by_field


def _receipt_heap(catalog, functions):
    """Require the authentic fixed heap, native columns and no hidden dependent ACL."""
    classes = catalog["classes"] or []
    roots = [entry for entry in classes if entry["relname"] == ROOT]
    _require(len(roots) == 1, "missing")
    root = roots[0]
    oid = int(root["oid"])
    _require(
        root["relkind"] == "r"
        and root["relpersistence"] == "p"
        and root["relam"] == "2"
        and root["relnamespace"] == str(functions["schema_oid"])
        and not any(root[key] for key in ("relrowsecurity", "relforcerowsecurity", "relispartition"))
        and root["relreplident"] == "d"
        and root["reloptions"] is None
        and all(catalog[key] is None for key in ("rules", "policies", "inheritance")),
        "layout_changed",
    )
    attributes = [entry for entry in catalog["attributes"] if int(entry["attrelid"]) == oid]
    _require(
        len(attributes) == 9
        and [entry["attnum"] for entry in attributes] == list(range(1, 10))
        and all(not entry["attisdropped"] and entry["attidentity"] == "" for entry in attributes)
        and [entry["attnotnull"] for entry in attributes] == [True, True, True, False, True, True, True, True, False]
        and [entry["attgenerated"] for entry in attributes] == ["", "", "", "s", "s", "s", "s", "", ""]
        and [int(entry["attcollation"]) for entry in attributes] == [100, 0, 0, 100, 0, 0, 100, 0, 0],
        "columns_changed",
    )
    _require(
        all(entry["relacl"] is None for entry in classes if entry is not root)
        and all(entry["attacl"] is None for entry in catalog["attributes"] if int(entry["attrelid"]) != oid),
        "dependent_acl_changed",
    )
    return root, attributes


def _require_indexes(indexes, attributes, oid):
    _require(
        all(
            entry["native"]
            and entry["method"] == "btree"
            and entry["indisvalid"]
            and entry["indisready"]
            and entry["indislive"]
            for entry in indexes
        ),
        "indexes_changed",
    )
    for index in indexes:
        is_main = int(index["indrelid"]) == oid
        keys = index["indkey"]
        collations = [
            int(attributes[key - 1]["attcollation"])
            if key
            else (100 if index["index_name"] == "cms_serving_registry_capture_unique" else 0)
            for key in keys
        ]
        _require(
            index["indnatts"] == index["indnkeyatts"] == len(keys)
            and index["indoption"] == [0] * len(keys)
            and (not is_main or list(map(int, index["indcollation"])) == collations)
            and not any(index[key] for key in ("indisclustered", "indisreplident", "indnullsnotdistinct")),
            "indexes_changed",
        )


def _require_constraints(catalog, schema, oid, attributes):
    from process.provider_directory_profile_serving_receipt import _assert_receipt_catalog

    indexes = catalog["indexes"]
    _assert_receipt_catalog(
        oid,
        schema,
        [dict(entry, relation_oid=int(entry["attrelid"]), atttypid=int(entry["atttypid"])) for entry in attributes],
        [
            dict(entry, relation_oid=int(entry["indrelid"]), indkey=" ".join(map(str, entry["indkey"])))
            for entry in indexes
            if int(entry["indrelid"]) == oid
        ],
        [dict(entry, constraint_type=entry["contype"]) for entry in catalog["constraints"]],
    )
    foreign_keys = [entry for entry in catalog["constraints"] if entry["contype"] == "f"]
    _require(
        len(foreign_keys) == 1
        and int(foreign_keys[0]["confrelid"]) == oid
        and foreign_keys[0]["conkey"] == [4]
        and foreign_keys[0]["confkey"] == [1]
        and foreign_keys[0]["confupdtype"] == foreign_keys[0]["confdeltype"] == "a"
        and foreign_keys[0]["confmatchtype"] == "s"
        and not foreign_keys[0]["condeferrable"],
        "foreign_key_changed",
    )
    keys = {tuple(entry["conkey"]) for entry in catalog["constraints"] if entry["contype"] in {"p", "u"}}
    _require(
        keys == {(1,), (3,), (4,), (5, 6, 7)}
        and sum(entry["contype"] in {"p", "u"} for entry in catalog["constraints"]) == 4,
        "constraints_changed",
    )
    _require(
        len(catalog["constraints"]) == 15
        and sum(entry["contype"] == "t" for entry in catalog["constraints"]) == 1
        and sorted(entry["conkey"] for entry in catalog["constraints"] if entry["contype"] == "n")
        == [[1], [2], [3], [5], [6], [7], [8]],
        "constraints_changed",
    )


def _require_hooks(catalog, functions):
    hooks = [entry for entry in catalog["triggers"] if not entry["tgisinternal"]]
    _require(len(hooks) == 2, "hooks_changed")
    for hook in hooks:
        name = hook["tgname"]
        is_deferred = name == "cms_serving_receipt_insert"
        _require(
            name in {"cms_serving_receipt_insert", "cms_serving_receipt_immutable"}
            and hook["tgtype"] == (5 if is_deferred else 58)
            and hook["tgdeferrable"] is is_deferred
            and hook["tginitdeferred"] is is_deferred
            and hook["tgenabled"] in {"O", "A"}
            and hook["tgqual"] is None
            and hook["tgnargs"] == 0
            and hook["tgattr"] == []
            and hook["tgargs"] == "\\x"
            and hook["tgparentid"] == "0"
            and hook["tgnewtable"] is hook["tgoldtable"] is None
            and hook["tgfoid"] == functions["functions"][name]["definition"]["oid"],
            "hooks_changed",
        )


def _require_layout(catalog, schema, functions):
    root, attributes = _receipt_heap(catalog, functions)
    oid = int(root["oid"])
    _require_indexes(catalog["indexes"], attributes, oid)
    _require_constraints(catalog, schema, oid, attributes)
    _require_hooks(catalog, functions)
    return root


def _read_acl(catalog, old_owner, owner):
    table_reads = [
        [grantee, owner if grantor == old_owner else grantor, right, grantable]
        for grantee, grantor, right, grantable in catalog["table_acl"]
        if right == "SELECT" and grantee != old_owner
    ]
    columns = [
        [column, grantee, owner if grantor == old_owner else grantor, right, grantable]
        for column, grantee, grantor, right, grantable in catalog["column_acl"]
        if right == "SELECT"
    ]
    return table_reads, columns


def _require_acl(catalog, root, principal_oids, readers, *, legacy):
    owner, publisher, builder, migration = principal_oids
    old_owner = int(root["relowner"])
    _require(old_owner in {owner, builder} if legacy else old_owner == owner, "owner_changed")
    for grantee, grantor, right, grantable in catalog["table_acl"]:
        _require(
            not grantable
            and grantor in {owner, old_owner}
            and (
                (grantee == old_owner and right in _RIGHTS)
                or (grantee in readers | {owner} and right == "SELECT")
                or (old_owner == owner and grantee == builder and right == "MAINTAIN")
            ),
            "table_acl_changed",
        )
    read_columns = {
        entry["attnum"]
        for entry in catalog["attributes"]
        if entry["attrelid"] == root["oid"] and entry["attname"] in READ_COLUMN_GRANTS[ROOT]
    }
    for column, grantee, grantor, right, grantable in catalog["column_acl"]:
        _require(
            not grantable
            and grantor in {owner, old_owner}
            and (
                (grantee in readers | {owner} and right == "SELECT")
                or (grantee == publisher and column in read_columns and right == "SELECT")
                or (old_owner == owner and grantee == builder and column in {1, 2} and right == "INSERT")
            ),
            "column_acl_changed",
        )
    if old_owner == owner:
        expected_grants = {(owner, owner, right, False) for right in _RIGHTS}
        expected_grants |= {(builder, owner, right, False) for right in ("SELECT", "MAINTAIN")}
        _require(
            expected_grants <= {tuple(entry) for entry in catalog["table_acl"]}
            and {(column, builder, owner, "INSERT", False) for column in (1, 2)}
            <= {tuple(entry) for entry in catalog["column_acl"]},
            "legacy_writer_missing",
        )
    _require_effective_access(catalog["access"], old_owner, owner, publisher, builder, migration)


def _require_effective_access(access_entries, old_owner, owner, publisher, builder, migration):
    migration_access = [entry for entry in access_entries if entry["role"] == migration]
    _require(
        len(migration_access) == 1
        and all(migration_access[0][key] is True for key in ("owner_reach", "owner_usage", "owner_set")),
        "migration_authority_changed",
    )
    for access in access_entries:
        if access["superuser"] or access["role"] in {owner, publisher, migration}:
            continue
        _require(not access["owner_reach"], "owner_authority")
        # Predefined NOLOGIN capability roles are not application principals. A
        # login or task role inheriting them is checked through its effective ACL.
        if access["builtin"]:
            continue
        if access["role"] == builder:
            if old_owner == builder:
                _require(
                    "SELECT" in access["table"]
                    and bool(set(access["table"]) & {"MAINTAIN", "UPDATE", "DELETE", "TRUNCATE"})
                    and all("INSERT" in rights for column, rights in access["columns"] if column in {1, 2}),
                    "legacy_writer_missing",
                )
            if old_owner == owner:
                _require(
                    set(access["table"]) == {"SELECT", "MAINTAIN"}
                    and all(
                        set(rights) == ({"SELECT", "INSERT"} if column in {1, 2} else {"SELECT"})
                        for column, rights in access["columns"]
                    ),
                    "legacy_writer_changed",
                )
        else:
            _require(
                not access["builder_reach"]
                and set(access["table"]) <= {"SELECT"}
                and all(set(rights) <= {"SELECT"} for _, rights in access["columns"]),
                "inherited_writer",
            )


def _require_migration_identity(functions, policy, migration_role):
    """Use only the explicit enrolled namespace migrator, never an observed grantee."""
    _require(
        type(migration_role) is str
        and migration_role
        not in {policy.owner_role, policy.publisher_role, *policy.runtime_roles, *policy.control_runtime_roles},
        "migration_identity_required",
    )
    storage_identifier(migration_role)
    roles = [entry for entry in functions["role_graph"]["roles"] if entry["rolname"] == migration_role]
    _require(
        len(roles) == 1
        and roles[0]["rolcanlogin"] is True
        and roles[0]["rolinherit"] is True
        and not any(
            roles[0][key] for key in ("rolsuper", "rolcreaterole", "rolcreatedb", "rolreplication", "rolbypassrls")
        ),
        "migration_identity_changed",
    )
    migration = int(roles[0]["oid"])
    owner = next(
        int(entry["oid"]) for entry in functions["role_graph"]["roles"] if entry["rolname"] == policy.owner_role
    )
    memberships = [
        entry
        for entry in functions["role_graph"]["memberships"] or ()
        if int(entry["member"]) == migration and int(entry["roleid"]) == owner
    ]
    _require(
        len(memberships) == 1
        and memberships[0]["admin_option"] is False
        and memberships[0]["inherit_option"] is True
        and memberships[0]["set_option"] is True,
        "migration_membership_changed",
    )
    return migration


async def capture_cms_serving_receipt_custody(connection, policy, *, migration_role, legacy=False):
    """Attest the fixed heap and closed native privileges, without inferring a writer from ACLs."""
    _require(
        type(policy) is GenericSourcePolicy and type(legacy) is bool and bool(policy.control_runtime_roles),
        "policy_required",
    )
    functions = await capture_cms_callable_custody(connection, policy)
    migration = _require_migration_identity(functions, policy, migration_role)
    role_oid_by_name = {entry["rolname"]: int(entry["oid"]) for entry in functions["role_graph"]["roles"]}
    owner, publisher = role_oid_by_name[policy.owner_role], role_oid_by_name[policy.publisher_role]
    # This is the existing configured Builder identity used by the original control callers.
    builder = role_oid_by_name[policy.control_runtime_roles[0]]
    catalog = _decoded(await connection.fetchval(_CATALOG_SQL, storage_relation(policy.schema, ROOT), owner, builder))
    root = _require_layout(catalog, policy.schema, functions)
    _require_acl(
        catalog,
        root,
        (owner, publisher, builder, migration),
        {role_oid_by_name[name] for name in policy.runtime_roles},
        legacy=legacy,
    )
    return dict(
        schema=policy.schema, owner=owner, builder=builder, migration=migration, functions=functions, catalog=catalog
    )


def require_cms_serving_receipt_adoption_delta(before, after):
    """Permit only ownership rewrites and the fixed ACL closure, never a new catalog baseline."""
    _require(
        {key: part_by_field for key, part_by_field in before.items() if key != "catalog"}
        == {key: part_by_field for key, part_by_field in after.items() if key != "catalog"},
        "adoption_scope_changed",
    )
    expected = deepcopy(before["catalog"])
    root = next(entry for entry in expected["classes"] if entry["relname"] == ROOT)
    old_owner, owner, builder = int(root["relowner"]), after["owner"], after["builder"]
    table_reads, column_reads = _read_acl(expected, old_owner, owner)
    table_acl = {tuple(entry) for entry in table_reads} | {(owner, owner, right, False) for right in _RIGHTS}
    table_acl |= {(builder, owner, right, False) for right in ("SELECT", "MAINTAIN")}
    column_acl = {tuple(entry) for entry in column_reads} | {
        (column, builder, owner, "INSERT", False) for column in (1, 2)
    }
    _require(
        set(map(tuple, after["catalog"]["table_acl"])) == table_acl
        and set(map(tuple, after["catalog"]["column_acl"])) == column_acl,
        "adoption_acl_changed",
    )
    for entry in expected["classes"]:
        _require(int(entry["relowner"]) == old_owner, "dependent_owner_changed")
        entry["relowner"] = str(owner)
        entry["relacl"] = next(
            current["relacl"] for current in after["catalog"]["classes"] if current["oid"] == entry["oid"]
        )
    for entry in expected["attributes"]:
        entry["attacl"] = next(
            current["attacl"]
            for current in after["catalog"]["attributes"]
            if (current["attrelid"], current["attnum"]) == (entry["attrelid"], entry["attnum"])
        )
    expected["table_acl"], expected["column_acl"], expected["access"] = (
        after["catalog"]["table_acl"],
        after["catalog"]["column_acl"],
        after["catalog"]["access"],
    )
    _require(expected == after["catalog"], "adoption_catalog_changed")


async def provision_cms_serving_receipt_custody(connection, policy, *, migration_role, verify_role_profile):
    """Use one supplied genuine admin transaction; no receipt, profile, role or namespace is created."""
    _require(
        type(policy) is GenericSourcePolicy
        and connection.is_in_transaction() is True
        and callable(verify_role_profile),
        "explicit_admin_required",
    )
    task, actor = asyncio.current_task(), await _admin_identity(connection, policy)
    async with connection.transaction():
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        table = storage_relation(policy.schema, ROOT)
        await connection.execute(f"LOCK TABLE ONLY {table} IN ACCESS EXCLUSIVE MODE NOWAIT")
        before = await capture_cms_serving_receipt_custody(
            connection, policy, migration_role=migration_role, legacy=True
        )
        if next(entry for entry in before["catalog"]["classes"] if entry["relname"] == ROOT)["relowner"] != str(
            before["owner"]
        ):
            await connection.execute(f"ALTER TABLE {table} OWNER TO {storage_identifier(policy.owner_role)}")
            builder = storage_identifier(policy.control_runtime_roles[0])
            await connection.execute(f"GRANT SELECT,MAINTAIN ON TABLE {table} TO {builder}")
            await connection.execute(f"GRANT INSERT(receipt_id,payload) ON TABLE {table} TO {builder}")
        after = await capture_cms_serving_receipt_custody(connection, policy, migration_role=migration_role)
        require_cms_serving_receipt_adoption_delta(before, after)
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        _require(
            asyncio.current_task() is task and await _admin_identity(connection, policy) == actor,
            "admin_transaction_changed",
        )
        return after


def require_cms_serving_receipt_read_delta(before, after, policy):
    """Admit only this policy's exact metadata SELECT edges, preserving all other custody."""
    _require(policy.schema == before["schema"], "read_scope_changed")
    _require(
        {key: value for key, value in before.items() if key != "catalog"}
        == {key: value for key, value in after.items() if key != "catalog"},
        "read_scope_changed",
    )
    grantees = cms_control_read_oids(policy, before["functions"]["role_graph"])
    expected = deepcopy(before["catalog"])
    root = next(entry for entry in expected["classes"] if entry["relname"] == ROOT)
    attributes = [entry for entry in expected["attributes"] if entry["attrelid"] == root["oid"]]
    additions = cms_control_read_edges(ROOT, attributes, grantees, int(root["relowner"]))
    _require(
        {tuple(entry) for entry in after["catalog"]["column_acl"]}
        == {tuple(entry) for entry in expected["column_acl"]} | additions,
        "read_acl_changed",
    )
    for attribute in attributes:
        if any(edge[0] == attribute["attnum"] for edge in additions):
            attribute["attacl"] = next(
                entry["attacl"]
                for entry in after["catalog"]["attributes"]
                if (entry["attrelid"], entry["attnum"]) == (attribute["attrelid"], attribute["attnum"])
            )
    expected["column_acl"] = after["catalog"]["column_acl"]
    expected["access"] = after["catalog"]["access"]
    _require(expected == after["catalog"], "read_catalog_changed")
