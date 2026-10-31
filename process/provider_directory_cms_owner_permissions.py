# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Plan fixed common ownership/ACL data from genuine protected v3/Source records.

This executes no SQL and issues no authority. The issuer must load genuine
records and retain role/DDL/writer/reverse-hook custody, retained-data proof,
ordinary ATTACH compatibility, old-consumer disposition and commit/recovery
gates. Exact expected ACLs do not establish any of those execution contracts.
"""

from dataclasses import asdict

from db.migration_provider_directory_dataset_candidates import TABLES
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_canonical_layout as layouts
from process import provider_directory_cms_control_custody as controls
from process import provider_directory_cms_dependency_policy as dependencies
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_cms_preparation_policy as profiles
from process import provider_directory_cms_transition_routines as routines
from process import provider_directory_source_acquisition_custody as acquisition
from process import provider_directory_source_callable_custody as source_callables
from process.provider_directory_cms_proof_contract import CMS_INVOKER_PROOF, CMS_PROOF_DEFINERS, cms_proof_actor_oids
from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS

_HEADER = "provider_directory_endpoint_dataset"
_PUBLICATION_UPDATE = ("status", "validated_at", "is_current", "published_at", "superseded_at")
_ORDINARY_UPDATE = (
    "import_run_id",
    "previous_dataset_id",
    "completion_proof_required_version",
    "status",
    "publication_metadata_json",
    "dataset_hash",
    "is_current",
    "resource_count",
    "validated_at",
    "published_at",
    "superseded_at",
    "publication_metadata_summary_json",
    "publication_metadata_sha256",
    "content_proof_admission_version",
    "content_proof_admission_kind",
    "content_proof_admission_sha256",
    "content_proof_resource_types",
    "artifact_selection_receipt_json",
    "completion_proof_json",
    "completion_proof_sha256",
    "candidate_storage_json",
)
_IDENTITIES = custody.CANONICAL_ROOTS[5:13]
_READ_ONLY = (*custody.CANONICAL_ROOTS[13:15], *TABLES[4:], profiles.PLAN)
_REVIEWED = ("provider_directory_entity_source_binding", "provider_directory_entity_release_evidence")
_KEYS = {"provider_directory_source": "source_id", "provider_directory_api_endpoint": "endpoint_id"}
_COLUMN_RIGHTS = {"SELECT", "INSERT", "UPDATE", "REFERENCES"}


def _require(condition, code):
    if not condition:
        raise ValueError("cms_common_owner_permissions_" + code)


def _oid(value):
    _require(type(value) is int and 0 < value < 2**32, "oid_required")
    return value


def _authenticated(inventory, binding, policy, cms_profile):
    profile = profiles._profile(cms_profile, binding, policy)
    _require(profile["contract"] == dependencies.CONTRACT, "v3_profile_required")
    _require(
        type(inventory) is dict and set(inventory) == {"canonical", "private", "routines", "roles"},
        "inventory_required",
    )
    _require(
        custody._evidence_digests(inventory) == {field: profile[field] for field in custody._DIGEST_FIELDS},
        "inventory_changed",
    )
    routines.require_cms_transition_routine_definitions(
        inventory["routines"],
        catalog_schema=binding.catalog_schema,
        catalog_schema_oid=binding.catalog_schema_oid,
        preparation_schema="hp_snapshot_retention",
    )
    return profile


def _roles(inventory):
    # pg_roles JSON stores OIDs as decimal strings; keep the authenticated raw rows intact.
    rows = [
        {
            **role,
            "oid": _oid(int(role["oid"]))
            if type(role["oid"]) is str and role["oid"].isascii() and role["oid"].isdecimal()
            else _oid(role["oid"]),
        }
        for role in inventory["roles"]["roles"]
    ]
    by_name = {row["rolname"]: row for row in rows}
    _require(len(by_name) == len(rows) and len({_oid(row["oid"]) for row in rows}) == len(rows), "roles_changed")
    return by_name


def _source_roles(document, binding, policy, by_name):
    _require(type(document) is dict and custody._digest(document) == binding.enrollment_sha256, "source_changed")
    source_custody = document["storage_custody"]
    expected_by_field = {key: coordinate for key, coordinate in asdict(binding).items() if key != "enrollment_sha256"}
    _require(
        document["version"] == 2 and source_custody["coordinates"] == expected_by_field, "source_coordinates_changed"
    )
    declaration, publication = document["policy"], document["publication"]
    _require(
        declaration["schema"] == binding.storage_schema
        and publication == {"owner_role": policy.owner_role, "publisher_role": policy.publisher_role},
        "source_policy_changed",
    )
    names = (
        declaration["namespace_owner_role"],
        declaration["owner_role"],
        publication["publisher_role"],
        publication["owner_role"],
    )
    _require(
        all(name in by_name and source_custody["roles"][name]["oid"] == by_name[name]["oid"] for name in names),
        "source_role_changed",
    )
    target_role = by_name[names[0]]
    owner = by_name[names[1]]
    publisher = by_name[names[2]]
    _require(
        target_role["oid"] == binding.namespace_owner_oid and owner["oid"] == binding.owner_oid, "source_role_changed"
    )
    _require(
        not any(
            target_role[field]
            for field in ("rolcanlogin", "rolsuper", "rolcreatedb", "rolcreaterole", "rolreplication", "rolbypassrls")
        )
        and owner["rolcanlogin"] is False
        and publisher["rolcanlogin"] is True,
        "protected_roles_required",
    )
    _require(len({target_role["oid"], owner["oid"], publisher["oid"]}) == 3, "source_role_changed")
    publication_owner = by_name[names[3]]
    _require(
        publication_owner["rolcanlogin"] is False
        and len({target_role["oid"], owner["oid"], publisher["oid"], publication_owner["oid"]}) == 4,
        "publication_owner_required",
    )
    return target_role["oid"], owner["oid"], publisher["oid"], publication_owner["oid"]


def _legacy_writer(inventory, document, target_oid, by_name):
    metadata = document["storage_custody"]["canonical_authority"]["metadata"]
    required_signatures = {signature.replace("character varying", "varchar") for signature in METADATA_FUNCTIONS}
    _require(type(metadata) is dict and set(metadata) == required_signatures, "source_metadata_required")
    native_by_oid = {routine["oid"]: routine["native"] for routine in inventory["routines"]}
    owners, callers = set(), []
    for signature, entry in metadata.items():
        _require(
            type(entry) is dict
            and type(entry.get("definition")) is dict
            and {"oid", "proname", "proowner"} <= entry["definition"].keys(),
            "source_metadata_changed",
        )
        native = native_by_oid.get(int(entry["definition"]["oid"]))
        _require(
            native is not None
            and entry["definition"]["proname"] == signature.split("(", 1)[0]
            and native["definition"] == entry["definition"]
            and native["language"]["lanname"] == entry["language"]
            and native["acl"] == entry["acl"],
            "source_metadata_changed",
        )
        owner = int(entry["definition"]["proowner"])
        owners.add(owner)
        callers.append({edge[0] for edge in entry["acl"] if edge[0] not in {0, target_oid}})
    _require(len(owners) == 1, "ordinary_writer_unbound")
    writer = owners.pop()
    if writer == target_oid:
        _require(len(callers[0]) == 1 and all(caller == callers[0] for caller in callers), "ordinary_writer_unbound")
        writer = next(iter(callers[0]))
    role_by_oid = {routine["oid"]: routine for routine in by_name.values()}
    _require(
        writer in role_by_oid and role_by_oid[writer]["rolcanlogin"] is True and not role_by_oid[writer]["rolsuper"],
        "ordinary_writer_unbound",
    )
    return writer


def _source_readonly_oids(canonical, by_name):
    source_plan = canonical["dependency_migration_plan"]
    _require(type(source_plan) is dict and set(source_plan) == {dependencies.PROVENANCE_ROOT}, "source_plan_changed")
    acquisition._require_practitioner_migration_plan(source_plan[dependencies.PROVENANCE_ROOT], by_name)
    for history_name, parent_name in zip(dependencies.HISTORY_ROOTS, acquisition.BULK_ROOTS[:2], strict=True):
        history = by_name[history_name]
        _require(
            history["relkind"] == "r" and history["native"]["parents"] in ([], [by_name[parent_name]["oid"]]),
            "source_history_parent_changed",
        )
    return {
        by_name[name]["oid"]
        for name in (*dependencies.HISTORY_ROOTS, *dependencies.CONTROL_ROOTS, dependencies.PROVENANCE_ROOT)
    }


def _dataset_history_oids(canonical, by_name, by_oid):
    histories = set()
    for root in TABLES:
        identity = canonical["migration_plan"]["tables"][root]
        oid = _oid(identity["original_oid"])
        _require(identity["parent_oid"] == by_name[root]["oid"] and oid in by_oid, "history_changed")
        history = by_oid[oid]
        keys = identity.get("keys", canonical["migration_plan"].get("keys"))
        _require(
            type(keys) is list
            and history["relname"] == "pd_dataset_history_" + str(oid)
            and history["relkind"] == "r"
            and history["native"]["parents"] == ([identity["parent_oid"]] if keys else []),
            "history_changed",
        )
        histories.add(oid)
    _require(len(histories) == 6, "history_changed")
    return histories


def _fixed_relations(inventory, binding, profile):
    canonical = inventory["canonical"]
    primary, dependency = canonical["relations"], canonical["dependency_relations"]
    source_histories = canonical["dependency_histories"]
    _require(
        type(source_histories) is list
        and len(source_histories) == 2
        and {relation["relname"] for relation in source_histories} == set(dependencies.HISTORY_ROOTS),
        "source_histories_changed",
    )
    relations = [*primary, *dependency, *source_histories]
    by_oid = {_oid(relation["oid"]): relation for relation in relations}
    by_name = {relation["relname"]: relation for relation in relations}
    _require(len(by_oid) == len(relations) == len(by_name), "relations_changed")
    roots = {*profiles.ROOTS, *dependencies.ROOTS}
    _require(
        set(relation["relname"] for relation in dependency) == set(dependencies.ROOTS)
        and all(name in by_name and not by_name[name]["native"]["parents"] for name in roots),
        "fixed_roots_changed",
    )
    source_readonly = _source_readonly_oids(canonical, by_name)
    histories = _dataset_history_oids(canonical, by_name, by_oid)
    retained_oids = set(profile["fixed_descendant_oids"])
    root_oids = {by_name[root]["oid"] for root in roots}
    _require(not histories.intersection(root_oids | retained_oids), "history_changed")
    _require(set(by_oid) == root_oids | histories | retained_oids | source_readonly, "descendants_changed")
    _retained_ancestry(by_oid, retained_oids, {by_name[root]["oid"] for root in TABLES})
    _require(
        all(
            relation["nspname"] == binding.catalog_schema
            and relation["relkind"] in {"r", "p"}
            and relation["relpersistence"] == "p"
            and relation["native"]["namespace"][:3]
            == [binding.catalog_schema_oid, binding.catalog_schema, binding.namespace_owner_oid]
            for relation in relations
        ),
        "relation_namespace_changed",
    )
    physical_by_oid = {relation["oid"]: relation["native"] for relation in canonical["physical"]}
    _require(
        set(physical_by_oid) == set(by_oid) and len(physical_by_oid) == len(canonical["physical"]), "physical_changed"
    )
    return (
        sorted(relations, key=lambda relation: relation["oid"]),
        physical_by_oid,
        histories | retained_oids | source_readonly,
    )


def _retained_ancestry(by_oid, retained, roots):
    def _reaches_root(oid, visiting):
        if oid in roots:
            return True
        _require(oid in by_oid and oid not in visiting, "retained_ancestry_changed")
        parents = by_oid[oid]["native"]["parents"]
        _require(type(parents) is list and len(parents) == 1 and type(parents[0]) is int, "retained_ancestry_changed")
        return _reaches_root(parents[0], visiting | {oid})

    for oid in retained:
        _require(_reaches_root(oid, set()), "retained_ancestry_changed")


def _columns(row):
    attributes = [entry["attribute"] for entry in row["native"]["columns"] if not entry["attribute"]["attisdropped"]]
    by_name = {entry["attname"]: entry["attnum"] for entry in attributes}
    _require(
        len(by_name) == len(attributes) == len(set(by_name.values()))
        and all(type(number) is int and number > 0 for number in by_name.values()),
        "columns_changed",
    )
    if row["relname"] in layouts._LAYOUTS:
        _require(
            set(by_name) == {column[0] for column in layouts._LAYOUTS[row["relname"]]["columns"]},
            "source_columns_changed",
        )
    return by_name


def _ordinary_rights(name):
    models = dependencies._models()
    if name in controls.CONTROL_ROOTS[:2]:
        return controls._writer_rights(name, (column.name for column in models[name].__table__.columns))
    if name in acquisition.ACQUISITION_ROOTS:
        return acquisition._writer_rights(name, (column.name for column in models[name].__table__.columns))
    if name in dependencies.ROOTS:
        columns = tuple(column.name for column in models[name].__table__.columns)
        return {"SELECT"}, {
            "INSERT": columns,
            "UPDATE": ("status", "dataset_hash", "validated_at", "is_current", "published_at", "superseded_at"),
        }
    if name == _HEADER:
        return {"SELECT", "INSERT"}, {"UPDATE": _ORDINARY_UPDATE}
    if name in _IDENTITIES:
        return {"SELECT", "INSERT"}, {"UPDATE": ("source_id",)} if name in _REVIEWED else {}
    if name in TABLES[:4]:
        return {"SELECT", "INSERT"}, {}
    if name in _READ_ONLY:
        return {"SELECT"}, {}
    column = {
        "provider_directory_cms_npd_relationship_receipt": "relationship_count",
        "provider_directory_cms_candidate_coverage": "created_at",
    }.get(name)
    _require(column is not None, "rights_contract_missing")
    return {"SELECT", "INSERT"}, {"UPDATE": (column,)}


def _actor_rights(row, readonly, actors):
    target, writer, source_owner, publisher, publication_owner = actors
    name = row["relname"]
    ordinary = ({"SELECT"}, {}) if readonly else _ordinary_rights(name)
    rights_by_actor = {writer: ordinary, publication_owner: ordinary}
    if not readonly and name in _KEYS:
        rights_by_actor[source_owner] = ({"SELECT"}, {"UPDATE": (_KEYS[name],)})
    if not readonly and (name == _HEADER or name in TABLES[:4]):
        rights_by_actor[publisher] = ({"SELECT", "INSERT"}, {"UPDATE": _PUBLICATION_UPDATE} if name == _HEADER else {})
    return rights_by_actor


def project_cms_relation_permissions(effective_rights, maximum, columns):
    """Intersect native effective rights with fixed caps; callers bind actor and relation."""
    _require(
        type(columns) is dict
        and all(type(name) is str and name and type(number) is int and number > 0 for name, number in columns.items())
        and len(set(columns.values())) == len(columns),
        "permission_columns_changed",
    )
    _require(
        type(effective_rights) is dict
        and set(effective_rights) == {"table", "columns"}
        and type(effective_rights["table"]) is list
        and type(effective_rights["columns"]) is list,
        "effective_rights_required",
    )
    tables, column_rows = effective_rights["table"], effective_rights["columns"]
    _require(
        all(type(right) is str and right in controls._RIGHTS for right in tables)
        and len(tables) == len(set(tables))
        and all(
            type(edge) is list
            and len(edge) == 2
            and type(edge[0]) is int
            and edge[0] in columns.values()
            and type(edge[1]) is str
            and edge[1] in _COLUMN_RIGHTS
            for edge in column_rows
        ),
        "effective_rights_changed",
    )
    existing_columns = set(map(tuple, column_rows))
    _require(len(existing_columns) == len(column_rows), "effective_rights_changed")
    _require(type(maximum) is tuple and len(maximum) == 2, "permission_cap_required")
    table_cap, column_cap = maximum
    _require(
        type(table_cap) is set
        and table_cap <= controls._RIGHTS
        and type(column_cap) is dict
        and all(
            right in _COLUMN_RIGHTS
            and type(names) in {tuple, list}
            and all(type(name) is str and name in columns for name in names)
            and len(names) == len(set(names))
            for right, names in column_cap.items()
        ),
        "permission_cap_changed",
    )
    missing_tables = table_cap.intersection(_COLUMN_RIGHTS).difference(tables)
    allowed_by_right = {
        right: tuple(columns) if right in missing_tables else column_cap[right]
        for right in missing_tables | column_cap.keys()
    }
    selected_by_right = {
        right: tuple(sorted(name for name in names if right in tables or (columns[name], right) in existing_columns))
        for right, names in allowed_by_right.items()
    }
    return table_cap.intersection(tables), {right: names for right, names in selected_by_right.items() if names}


def _effective_relations(inventory, rows, by_name):
    relations = [*rows, *inventory["private"]]
    columns_by_oid = {row["oid"]: _columns(row) for row in relations}
    _require(len(columns_by_oid) == len(relations), "effective_relations_changed")
    role_oids = {role["oid"] for role in by_name.values()}
    supplied = inventory["roles"].get("relation_rights")
    _require(type(supplied) is list, "effective_rights_required")
    by_identity = {}
    for row in supplied:
        _require(
            type(row) is list
            and len(row) == 4
            and type(row[0]) is int
            and row[0] in role_oids
            and type(row[1]) is int
            and row[1] in columns_by_oid,
            "effective_identity_changed",
        )
        identity_oids = tuple(row[:2])
        _require(identity_oids not in by_identity, "effective_identity_changed")
        rights_by_kind = dict(table=row[2], columns=row[3])
        project_cms_relation_permissions(rights_by_kind, (set(), {}), columns_by_oid[row[1]])
        by_identity[identity_oids] = rights_by_kind
    _require(
        set(by_identity) == {(role, relation) for role in role_oids for relation in columns_by_oid},
        "effective_inventory_incomplete",
    )
    return by_identity


def _project_actor_rights(relation, readonly, actors, effective):
    columns = _columns(relation)
    rights_by_actor = {
        actor: project_cms_relation_permissions(effective[actor, relation["oid"]], maximum, columns)
        for actor, maximum in _actor_rights(relation, readonly, actors).items()
    }
    if relation["relname"] in _KEYS and not readonly:
        source_owner = actors[2]
        tables, selected = rights_by_actor[source_owner]
        _require(
            ("SELECT" in tables or set(selected.get("SELECT", ())) == set(columns))
            and _KEYS[relation["relname"]] in selected.get("UPDATE", ()),
            "source_owner_admission_rights_missing",
        )
    return rights_by_actor


def _acl_edges(rows, size):
    _require(
        type(rows) is list and all(type(edge) is list and len(edge) == size and edge[-1] is False for edge in rows),
        "grant_option_or_acl_changed",
    )
    edges = set(map(tuple, rows))
    _require(len(edges) == len(rows), "duplicate_acl_edge")
    _require(
        all(
            all(type(value) is int and value >= 0 for value in edge[:-2])
            and edge[-3] > 0
            and (size == 4 or edge[0] > 0)
            and type(edge[-2]) is str
            for edge in edges
        ),
        "acl_identity_changed",
    )
    return edges


def _relation_plan(relation, physical, readonly, actors, administrative_owners, effective):
    target_oid, writer, *_rest = actors
    previous = relation["relowner"]
    _require(previous in {target_oid, writer, actors[4], *administrative_owners}, "relation_owner_unbound")
    columns = _columns(relation)
    allowed = _project_actor_rights(relation, readonly, actors, effective)
    table_edges = {(target_oid, target_oid, right, False) for right in controls._RIGHTS}
    column_edges = set()
    for actor, (rights, by_right) in allowed.items():
        table_edges.update((actor, target_oid, right, False) for right in rights)
        for right, names in by_right.items():
            _require(set(names) <= columns.keys(), "writer_columns_changed")
            column_edges.update((columns[name], actor, target_oid, right, False) for name in names)
    for grantee, grantor, right, grantable in _acl_edges(physical["table_acl"], 4):
        if grantee in {previous, writer, actors[4]}:
            continue
        if right != "SELECT":
            _require(
                grantee in allowed and right in allowed[grantee][0],
                "external_table_writer_unbound:" + relation["relname"],
            )
        else:
            table_edges.add((grantee, target_oid if grantor == previous else grantor, right, grantable))
    for number, grantee, grantor, right, grantable in _acl_edges(physical["column_acl"], 5):
        _require(number in columns.values(), "column_acl_changed")
        if grantee in {previous, writer, actors[4]}:
            continue
        if right != "SELECT":
            _require(
                (number, grantee, target_oid, right, False) in column_edges
                or grantee in allowed
                and right in allowed[grantee][0],
                "external_column_writer_unbound:" + relation["relname"],
            )
        else:
            column_edges.add((number, grantee, target_oid if grantor == previous else grantor, right, grantable))
    return dict(
        oid=relation["oid"],
        nspname=relation["nspname"],
        relname=relation["relname"],
        owner_oid=target_oid,
        table_acl=[list(edge) for edge in sorted(table_edges)],
        column_acl=[list(edge) for edge in sorted(column_edges)],
    )


def _preserved_routine_acl(native, edges, actors, administrative_owners):
    target_oid, writer, *_rest = actors
    definition = native["definition"]
    name, previous = definition["proname"], int(definition["proowner"])
    if name in callables._SERVING_CALLEES:
        callables._require_definition(
            dict(definition=definition, language=native["language"]["lanname"], acl=[list(edge) for edge in edges]),
            native["namespace"][1],
            int(definition["pronamespace"]),
        )
    elif previous in administrative_owners:
        _require(
            name
            in {
                "provider_directory_rooted_graph_dataset_intrinsic_valid",
                "provider_directory_uhc_flex_practitioner_dataset_valid",
            },
            "source_administrative_owner_unbound",
        )
        required_edges = {(previous, previous, "EXECUTE", False)}
        if (
            name == "provider_directory_uhc_flex_practitioner_dataset_valid"
            and (0, previous, "EXECUTE", False) in edges
        ):
            required_edges.add((0, previous, "EXECUTE", False))
        _require(edges == required_edges, "source_administrative_acl_changed")
    elif (
        name == "provider_directory_uhc_flex_practitioner_dataset_valid"
        and previous == target_oid
        and edges == {(previous, previous, "EXECUTE", False)}
    ):
        return edges
    else:
        public = {0} if name == "provider_directory_uhc_flex_practitioner_dataset_valid" else set()
        _require(
            previous in {target_oid, writer}
            and all(
                grantee in {previous, writer, *public} and grantor == previous and right == "EXECUTE"
                for grantee, grantor, right, _ in edges
            ),
            "source_routine_caller_unbound:" + name,
        )
        _require(not public or (0, previous, "EXECUTE", False) in edges, "source_public_contract_required")
    after_edges = {
        (
            target_oid if grantee == previous else grantee,
            target_oid if grantor == previous else grantor,
            right,
            grantable,
        )
        for grantee, grantor, right, grantable in edges
    }
    if name not in callables._SERVING_CALLEES and previous == writer:
        after_edges.add((writer, target_oid, "EXECUTE", False))
    return after_edges


def _effective_routines(inventory, by_name):
    role_oids = {role["oid"] for role in by_name.values()}
    routine_oids = {routine["oid"] for routine in inventory["routines"]}
    supplied = inventory["roles"].get("routine_rights")
    _require(type(supplied) is list, "routine_rights_required")
    by_identity = {}
    for row in supplied:
        _require(
            type(row) is list
            and len(row) == 3
            and type(row[0]) is int
            and row[0] in role_oids
            and type(row[1]) is int
            and row[1] in routine_oids
            and type(row[2]) is bool,
            "routine_rights_changed",
        )
        identity_oids = tuple(row[:2])
        _require(identity_oids not in by_identity, "routine_rights_changed")
        by_identity[identity_oids] = row[2]
    _require(
        set(by_identity) == {(role, routine) for role in role_oids for routine in routine_oids},
        "routine_rights_incomplete",
    )
    return by_identity


def _proof_routine_acl(routine, edges, target, proof_actors, effective):
    definition = routine["native"]["definition"]
    name, previous = definition["proname"], int(definition["proowner"])
    is_invoker = name == CMS_INVOKER_PROOF
    _require(definition["prosecdef"] is (not is_invoker), "proof_security_changed")
    after_edges = {(target, target, "EXECUTE", False)}
    if is_invoker:
        _require(
            all(grantee == 0 or effective.get((grantee, routine["oid"])) is True for grantee, *_ in edges),
            "invoker_rights_changed",
        )
        after_edges = {
            (target if grantee == previous else grantee, target if grantor == previous else grantor, right, grantable)
            for grantee, grantor, right, grantable in edges
        }
    if not is_invoker or not any(grantee == 0 for grantee, *_ in edges):
        after_edges.update(
            (actor, target, "EXECUTE", False) for actor in proof_actors if effective[actor, routine["oid"]] is True
        )
    return after_edges


def _outside_routine_acl(routine, edges, target_oid, source_actors, effective, administrative_owners):
    definition = routine["native"]["definition"]
    name, previous = definition["proname"], int(definition["proowner"])
    _require(
        definition["proargtypes"]
        == [source_callables._TYPE_OIDS[kind] for kind in dependencies._CALLBACK_CALLEES[name]],
        "outside_signature_changed:" + name,
    )
    _require(previous in {target_oid, *source_actors, *administrative_owners}, "outside_owner_unbound:" + name)
    has_public = any(grantee == 0 for grantee, *_ in edges)
    _require(not has_public or name in source_callables._PUBLIC_NAMES, "outside_public_unbound:" + name)
    _require(
        all(
            grantee in {previous, target_oid, *source_actors, *((0,) if has_public else ())}
            and grantor in {previous, target_oid}
            and right == "EXECUTE"
            for grantee, grantor, right, _ in edges
        ),
        "outside_caller_unbound:" + name,
    )
    _require(
        all(grantee == 0 or effective.get((grantee, routine["oid"])) is True for grantee, *_ in edges),
        "outside_rights_changed:" + name,
    )
    after_edges = {(target_oid, target_oid, "EXECUTE", False)}
    if has_public:
        _require(
            all(effective[actor, routine["oid"]] is True for actor in source_actors), "outside_rights_changed:" + name
        )
        after_edges.add((0, target_oid, "EXECUTE", False))
        after_edges.update(
            (actor, target_oid, "EXECUTE", False)
            for actor in source_actors
            if any(grantee == actor for grantee, *_ in edges)
        )
    else:
        after_edges.update(
            (actor, target_oid, "EXECUTE", False) for actor in source_actors if effective[actor, routine["oid"]] is True
        )
    return after_edges


def _routine_plans(inventory, actors, administrative_owners, proof_actors, effective):
    target_oid, writer, *_rest = actors
    preserved = {signature.split("(")[0] for signature in callables._SOURCE_CALLEES} | set(callables._SERVING_CALLEES)
    planned_routines = []
    for routine in inventory["routines"]:
        native = routine["native"]
        definition, namespace = native["definition"], native["namespace"]
        previous = int(definition["proowner"])
        edges = _acl_edges(native["acl"], 4)
        owner, after = previous, edges
        if namespace[1] != "pg_catalog":
            _require(
                (previous, previous, "EXECUTE", False) in edges,
                "routine_owner_execute_missing:" + definition["proname"],
            )
            owner, after = target_oid, {(target_oid, target_oid, "EXECUTE", False)}
            if definition["proname"] in preserved:
                after = _preserved_routine_acl(native, edges, actors, administrative_owners)
            elif definition["proname"] in CMS_PROOF_DEFINERS or definition["proname"] == CMS_INVOKER_PROOF:
                after = _proof_routine_acl(routine, edges, target_oid, proof_actors, effective)
            if definition["proname"] in dependencies._CALLBACK_CALLEES:
                after = _outside_routine_acl(routine, edges, target_oid, proof_actors, effective, administrative_owners)
        planned_routines.append(
            dict(
                oid=routine["oid"],
                nspname=namespace[1],
                proname=definition["proname"],
                owner_oid=owner,
                routine_acl=[list(edge) for edge in sorted(after)],
            )
        )
    return sorted(planned_routines, key=lambda routine: routine["oid"])


def plan_cms_common_owner_permissions(inventory, *, storage_binding, policy, cms_profile, source_document):
    """Derive expected data only; genuine record loading and every execution fence remain caller-owned."""
    profile = _authenticated(inventory, storage_binding, policy, cms_profile)
    by_name = _roles(inventory)
    administrative_owners = {role["oid"] for role in by_name.values() if role["rolsuper"] is True}
    target, source_owner, publisher, publication_owner = _source_roles(
        source_document, storage_binding, policy, by_name
    )
    writer = _legacy_writer(inventory, source_document, target, by_name)
    _require(writer not in {target, source_owner, publisher}, "ordinary_writer_unbound")
    actors = target, writer, source_owner, publisher, publication_owner
    rows, physical, readonly = _fixed_relations(inventory, storage_binding, profile)
    effective = _effective_relations(inventory, rows, by_name)
    proof_actors = cms_proof_actor_oids(source_document, inventory["roles"]["roles"], namespace_owner_oid=target)
    routine_rights = _effective_routines(inventory, by_name)
    return dict(
        owner_oid=target,
        ordinary_writer_oid=writer,
        relations=[
            _relation_plan(row, physical[row["oid"]], row["oid"] in readonly, actors, administrative_owners, effective)
            for row in rows
        ],
        routines=_routine_plans(inventory, actors, administrative_owners, proof_actors, routine_rights),
    )
