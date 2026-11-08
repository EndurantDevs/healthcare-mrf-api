# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Authenticate existing generic allocation shapes; never issue CMS authority.

The caller pins root/history/retained metadata, callables and their role graph.
Only the existing generic allocator class is supported for new descendants;
other allocation classes require their own reviewed contract.
"""

import json

from db.migration_provider_directory_dataset_candidates import TABLES
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_preparation_custody as custody

_ROOTS = TABLES[:4]
_EXTRA_SQL = """SELECT c.oid::bigint,
 c.relam=(SELECT oid FROM pg_am WHERE amname='heap' AND amtype='t') AS heap,
 c.reltablespace=0 AND c.relreplident='d' AND c.reloptions IS NULL AS default_storage,
 NOT (owner.rolcanlogin OR owner.rolsuper OR owner.rolcreatedb OR owner.rolcreaterole
      OR owner.rolreplication OR owner.rolbypassrls)
 AND NOT EXISTS(SELECT FROM pg_roles elevated WHERE
   (elevated.rolsuper OR elevated.rolcreatedb OR elevated.rolcreaterole
    OR elevated.rolreplication OR elevated.rolbypassrls)
   AND pg_has_role(owner.oid,elevated.oid,'MEMBER')) AS owner_protected,
 NOT EXISTS(SELECT FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) acl
   WHERE acl.grantee<>c.relowner OR acl.grantor<>c.relowner OR acl.is_grantable)
 AND NOT EXISTS(SELECT FROM pg_attribute a WHERE a.attrelid=c.oid AND a.attacl IS NOT NULL) AS acl_closed,
 NOT EXISTS(SELECT FROM pg_policy WHERE polrelid=c.oid) AS no_policies,
 (SELECT COALESCE(jsonb_agg(jsonb_build_array(i.indexrelid::bigint,
   ARRAY(SELECT inhparent::bigint FROM pg_inherits WHERE inhrelid=i.indexrelid ORDER BY inhparent),
   idx.relam::bigint,idx.relpersistence::text,idx.reltablespace::bigint) ORDER BY i.indexrelid),'[]'::jsonb)
  FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid WHERE i.indrelid=c.oid) AS index_topology
 FROM pg_class c JOIN pg_roles owner ON owner.oid=c.relowner WHERE c.oid=ANY($1::oid[])"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_allocation_semantics_" + code)


def _clone_columns(parent):
    """Account only for native attachment fields and freshly allocated default OIDs."""
    columns = json.loads(json.dumps(parent["native"]["columns"]))
    for column in columns:
        attribute = column["attribute"]
        _require(
            not attribute["attisdropped"]
            and not attribute["attidentity"]
            and not attribute["atthasmissing"]
            and attribute["attmissingval"] is None,
            "unsupported_parent_columns",
        )
        for field in ("attrelid", "attislocal", "attinhcount", "attacl"):
            attribute.pop(field)
        default = column["default"]
        if default is not None:
            default.pop("oid")
            default.pop("adrelid")
    return columns


def _require_columns(parent, child):
    expected = _clone_columns(parent)
    actual = json.loads(json.dumps(child["native"]["columns"]))
    for column in actual:
        attribute = column["attribute"]
        _require(
            attribute.pop("attrelid") == str(child["oid"])
            and attribute.pop("attislocal") is False
            and attribute.pop("attinhcount") == 1
            and attribute.pop("attacl") is None,
            "columns_changed",
        )
        default = column["default"]
        if default is not None:
            _require(default.pop("adrelid") == str(child["oid"]), "defaults_changed")
            default.pop("oid")
    _require(actual == expected, "columns_changed")


def _index_signature(index):
    """Retain complete native key/opclass/collation semantics, excluding instance IDs."""
    signature = json.loads(json.dumps(index))
    native = signature["index"]
    _require(
        all(native[field] is True for field in ("indisvalid", "indisready", "indislive", "indimmediate"))
        and not native["indisexclusion"],
        "indexes_changed",
    )
    for field in ("indexrelid", "indrelid"):
        native.pop(field)
    signature["class"].pop(0)
    _, separator, semantics = signature["definition"].partition(" USING ")
    _require(bool(separator), "indexes_changed")
    signature["definition"] = semantics
    return signature


def _require_indexes(parent, child, extra):
    """Require one actual attached child index for every pinned parent index."""
    expected_by_oid = {index["class"][0]: _index_signature(index) for index in parent["native"]["indexes"]}
    topology_by_oid = {entry[0]: entry[1:] for entry in custody._decoded(extra["index_topology"])}
    parent_by_index = {}
    _require(len(topology_by_oid) == len(expected_by_oid) == len(child["native"]["indexes"]), "indexes_changed")
    for index in child["native"]["indexes"]:
        oid = index["class"][0]
        parents, method, persistence, tablespace = topology_by_oid[oid]
        _require(len(parents) == 1 and parents[0] in expected_by_oid, "index_parent_changed")
        parent_oid = parents[0]
        _require(
            str(parent_oid) not in parent_by_index.values() and persistence == "p" and tablespace == 0,
            "indexes_changed",
        )
        _require(_index_signature(index) == expected_by_oid[parent_oid], "indexes_changed")
        _require(all(int(opclass["opcmethod"]) == method for opclass in index["opclasses"]), "indexes_changed")
        parent_by_index[str(oid)] = str(parent_oid)
    return parent_by_index


def _require_constraints(parent, child, index_parents):
    """Match native inherited constraints and the one separately checked dataset bound."""
    expected_by_oid = {}
    for native, definition in parent["native"]["constraints"]:
        _require(native["contype"] in {"n", "p", "u"}, "unsupported_parent_constraints")
        expected_by_oid[native["oid"]] = (native, definition)
    matched, bounds = set(), []
    for native, definition in child["native"]["constraints"]:
        if native["conname"] == "pd_dataset_bound":
            bounds.append(native)
            continue
        candidates = [
            (oid, pin, text)
            for oid, (pin, text) in expected_by_oid.items()
            if text == definition and pin["contype"] == native["contype"]
        ]
        _require(len(candidates) == 1, "constraints_changed")
        oid, pin, _ = candidates[0]
        _require(oid not in matched, "constraints_changed")
        wanted_by_field = dict(pin, oid=native["oid"], conrelid=str(child["oid"]), conislocal=False, coninhcount=1)
        if pin["contype"] in {"p", "u"}:
            _require(index_parents.get(native["conindid"]) == pin["conindid"], "constraint_index_changed")
            wanted_by_field.update(conname=native["conname"], conindid=native["conindid"], conparentid=oid)
        _require(native == wanted_by_field, "constraints_changed")
        matched.add(oid)
    _require(matched == set(expected_by_oid) and len(bounds) == 1, "constraints_changed")
    bound = bounds[0]
    _require(
        bound["contype"] == "c"
        and bound["conkey"] == [1]
        and bound["convalidated"]
        and bound.get("conenforced", True)
        and not bound["condeferrable"]
        and not bound["condeferred"]
        and not bound["connoinherit"]
        and bound["conislocal"]
        and bound["coninhcount"] == 0
        and bound["conparentid"] == "0",
        "bound_changed",
    )


def _require_guards(parent, child):
    """Require exactly the four allocator transition-table hooks from pinned roots."""
    expected_hooks = [
        trigger for trigger in parent["native"]["triggers"] if trigger["tgname"].startswith("a_pd_candidate_")
    ]
    actual_hooks = [trigger for trigger in child["native"]["triggers"] if not trigger["tgisinternal"]]
    _require(len(expected_hooks) == len(actual_hooks) == 4, "guards_changed")
    expected_by_name = {trigger["tgname"]: trigger for trigger in expected_hooks}
    _require({trigger["tgname"] for trigger in actual_hooks} == set(expected_by_name), "guards_changed")
    for trigger in actual_hooks:
        pin = expected_by_name[trigger["tgname"]]
        _require(pin["tgenabled"] in {"O", "A"}, "guards_changed")
        _require(trigger == dict(pin, oid=trigger["oid"], tgrelid=str(child["oid"]), tgenabled="O"), "guards_changed")


def _require_inherited_guards(parent, child, index_parents, clones):
    """Accept only native action hooks cloned from every authenticated parent FK hook."""
    expected_by_oid = {hook["oid"]: hook for hook in parent["native"]["triggers"] if hook["tgisinternal"]}
    actual_hooks = [hook for hook in child["native"]["triggers"] if hook["tgisinternal"]]
    _require(
        len(actual_hooks) == len(expected_by_oid)
        and {hook["tgparentid"] for hook in actual_hooks} == set(expected_by_oid),
        "foreign_guards_changed",
    )
    for hook in actual_hooks:
        pin = expected_by_oid[hook["tgparentid"]]
        clone = clones.get(int(hook["tgconstraint"]))
        _require(
            parent["relname"] == _ROOTS[0]
            and pin["tgparentid"] == "0"
            and pin["tgenabled"] in {"O", "A"}
            and clone is not None
            and clone["valid"] is True
            and str(clone["parent"]) == pin["tgconstraint"]
            and str(clone["root"]) == pin["tgconstrrelid"]
            and index_parents.get(hook["tgconstrindid"]) == pin["tgconstrindid"],
            "foreign_guards_changed",
        )
        wanted_by_field = dict(
            pin,
            oid=hook["oid"],
            tgname="RI_ConstraintTrigger_a_" + hook["oid"],
            tgrelid=str(child["oid"]),
            tgparentid=pin["oid"],
            tgconstraint=hook["tgconstraint"],
            tgconstrindid=hook["tgconstrindid"],
            tgenabled="O",
        )
        _require(hook == wanted_by_field, "foreign_guards_changed")


async def _foreign_clones(connection, schema, roots, children):
    """Reuse fixed native FK ancestry and function checks; never skip unknown internal hooks."""
    from process.provider_directory_cms_canonical_layout import _CLONES_SQL, _RESOURCE_FKS
    from process.provider_directory_cms_control_custody import _require_native_foreign_functions

    hooks = [hook for child in children for hook in child["native"]["triggers"] if hook["tgisinternal"]]
    if not hooks:
        return {}
    parent_hooks = [hook for root in roots for hook in root["native"]["triggers"] if hook["tgisinternal"]]
    rows = await connection.fetch(
        "SELECT * FROM (" + _CLONES_SQL + ") clones WHERE clones.oid=ANY($4::oid[])",
        sorted({int(hook["tgconstrrelid"]) for hook in parent_hooks}),
        list(_RESOURCE_FKS),
        storage_relation(schema, _ROOTS[0]),
        sorted({int(hook["tgconstraint"]) for hook in hooks}),
    )
    await _require_native_foreign_functions(connection, {int(hook["tgfoid"]) for hook in hooks})
    return {row["oid"]: dict(row) for row in rows}


async def _header_matches(connection, schema, dynamic, roots):
    """Resolve exact existing header/name/bound triples without parsing SQL literals."""
    header = storage_relation(schema, "provider_directory_endpoint_dataset")
    rows = await connection.fetch(
        f"SELECT c.oid::bigint,h.dataset_id FROM pg_class c CROSS JOIN {header} h "
        "JOIN unnest($2::oid[],$3::text[]) parent(oid,name) ON true "
        "WHERE c.oid=ANY($1::oid[]) AND EXISTS(SELECT FROM pg_inherits WHERE inhrelid=c.oid AND inhparent=parent.oid) "
        "AND c.relname='pd_generic_'||md5(parent.name||':'||h.dataset_id) "
        "AND pg_get_expr(c.relpartbound,c.oid)=format('FOR VALUES IN (%L)',h.dataset_id) "
        "AND COALESCE(h.publication_metadata_json->>'publication_contract_id','')<>ALL($4::text[]) "
        "AND EXISTS(SELECT FROM pg_constraint k WHERE k.conrelid=c.oid AND k.conname='pd_dataset_bound' "
        "AND pg_get_constraintdef(k.oid)=format('CHECK (((dataset_id)::text = %L::text))',h.dataset_id))",
        [child["oid"] for child in dynamic],
        [root["oid"] for root in roots],
        [root["relname"] for root in roots],
        [
            "healthporta.provider-directory.uhc-flex-practitioner-dataset-publication.v1",
            "healthporta.provider-directory.rooted-graph-publication.v1",
            "healthporta.provider-directory.rooted-graph-publication.v2",
        ],
    )
    _require(
        len(rows) == len(dynamic) and {row["oid"] for row in rows} == {child["oid"] for child in dynamic},
        "header_changed",
    )


async def require_cms_dynamic_allocations(connection, catalog_schema, *, family, historical_oids, retained_oids=()):
    """Validate new generic children; the caller authenticates fixed roots and omissions.

    Every retained OID must identify an existing nonroot descendant. Historical
    identities come from the separately validated migration plan, including its
    detached histories. This read-only check supplies no writer or DDL fence.
    """
    _require(connection.is_in_transaction() is True, "transaction_required")
    _require(type(family) is list, "family_changed")
    family = json.loads(json.dumps(family, allow_nan=False))
    by_oid = {relation["oid"]: relation for relation in family}
    roots = [relation for relation in family if relation["relname"] in _ROOTS]
    _require(len(by_oid) == len(family) and {root["relname"] for root in roots} == set(_ROOTS), "family_changed")
    root_by_oid = {root["oid"]: root for root in roots}
    historical, retained = set(historical_oids), set(retained_oids)
    _require(all(type(oid) is int and 0 < oid < 2**32 for oid in historical | retained), "omissions_changed")
    _require(not historical & root_by_oid.keys() and not historical & retained, "omissions_changed")
    _require(retained <= by_oid.keys() and not retained & root_by_oid.keys(), "retained_changed")
    _require(all(by_oid[oid]["native"]["parents"] for oid in retained), "retained_changed")
    dynamic_relations = [
        relation for relation in family if relation["oid"] not in root_by_oid.keys() | historical | retained
    ]
    for child in dynamic_relations:
        _require(
            child["nspname"] == catalog_schema and child["native"]["parents"] in [[oid] for oid in root_by_oid],
            "topology_changed",
        )
    _require(await custody._inventory(connection, catalog_schema, _ROOTS) == family, "family_changed")
    if not dynamic_relations:
        return []
    extra_by_oid = {
        entry["oid"]: entry
        for entry in await connection.fetch(_EXTRA_SQL, [child["oid"] for child in dynamic_relations])
    }
    _require(set(extra_by_oid) == {child["oid"] for child in dynamic_relations}, "family_changed")
    await _header_matches(connection, catalog_schema, dynamic_relations, roots)
    foreign_clones = await _foreign_clones(connection, catalog_schema, roots, dynamic_relations)
    for child in dynamic_relations:
        parent = root_by_oid[child["native"]["parents"][0]]
        native = child["native"]
        _require(
            child["relkind"] == "r"
            and child["relpersistence"] == "p"
            and child["relowner"] == parent["relowner"]
            and native["relation"][5:10] == [True, False, False, 0, None]
            and not native["rules"]
            and not any(child["oid"] in member["native"]["parents"] for member in family)
            and all(
                extra_by_oid[child["oid"]][field] is True
                for field in ("heap", "default_storage", "owner_protected", "acl_closed", "no_policies")
            ),
            "storage_changed",
        )
        _require_columns(parent, child)
        index_parents = _require_indexes(parent, child, extra_by_oid[child["oid"]])
        _require_constraints(parent, child, index_parents)
        _require_guards(parent, child)
        _require_inherited_guards(parent, child, index_parents, foreign_clones)
    return sorted(child["oid"] for child in dynamic_relations)
