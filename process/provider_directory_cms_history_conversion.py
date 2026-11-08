# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Plan the two legacy CMS heap replacements; execute nothing and issue no authority.

The caller authenticates every supplied native identity, complete dataset bounds,
fixed layout and continuous writer/callable custody. This deterministic SQL/data
plan is not a preflight, lock receipt, protected issuer or conversion executor.
"""

from copy import deepcopy

from sqlalchemy.dialects.postgresql import dialect

from db.plan_release_storage import storage_identifier, storage_relation
from process import network_fhir_source_custody as legacy
from process.provider_directory_cms_canonical_layout import _LAYOUTS

ROOTS = (
    "provider_directory_cms_npd_resource_witness",
    "provider_directory_cms_npd_relationship",
)
_REQUEST_FIELDS = {
    "database_oid",
    "catalog_schema",
    "catalog_schema_oid",
    "future_schema",
    "future_schema_oid",
    "owner_oid",
    "heaps",
}
_IDENTITY_FIELDS = {"oid", "name", "schema_oid", "owner_oid", "kind", "indexes", "foreign_keys", "hooks"}
_BOUND = "cms_history_bound"


def _require(condition, code):
    if not condition:
        raise ValueError("cms_history_plan_" + code)


def _oid(value):
    _require(type(value) is int and 0 < value < 2**32, "oid_required")
    return value


def cms_history_dependencies(catalog_schema, root):
    """Return only source-fixed legacy dependency semantics, without native OIDs."""
    _require(root in ROOTS, "root_required")
    storage_identifier(catalog_schema)
    schema = dialect().identifier_preparer.quote(catalog_schema)
    return {
        "foreign_keys": {
            name: definition.replace("__schema__", schema)
            for name, kind, definition, _columns in _LAYOUTS[root]["constraints"]
            if kind == "f"
        },
        "hooks": {
            name: {
                "function": legacy._GUARD_FUNCTIONS[name],
                "tgtype": kind,
                "enabled": "O",
                "old_table": legacy._TRANSITION_TABLES.get(name, (None, None))[0],
                "new_table": legacy._TRANSITION_TABLES.get(name, (None, None))[1],
            }
            for name, kind in legacy._GUARDS[root].items()
        },
    }


def _dependencies(identity, schema, root, *, parent):
    expected = {"foreign_keys": {}, "hooks": {}} if parent else cms_history_dependencies(schema, root)
    for field, entries in expected.items():
        actual = identity[field]
        _require(type(actual) is dict and set(actual) == set(entries), "dependencies_changed")
        seen_oids = set()
        for name, semantics in entries.items():
            observed = actual[name]
            fixed = {"definition": semantics} if field == "foreign_keys" else semantics
            extra = {"oid"} if field == "foreign_keys" else {"oid", "function_oid"}
            _require(type(observed) is dict and set(observed) == set(fixed) | extra, "dependencies_changed")
            _require({key: observed[key] for key in fixed} == fixed, "dependencies_changed")
            _require(_oid(observed["oid"]) not in seen_oids, "dependencies_changed")
            seen_oids.add(observed["oid"])
            if field == "hooks":
                _oid(observed["function_oid"])


def _identity(identity, request, root, *, parent):
    _require(type(identity) is dict and set(identity) == _IDENTITY_FIELDS, "identity_required")
    name = identity["name"]
    storage_identifier(name)
    _oid(identity["oid"])
    _require(
        identity["owner_oid"] == request["owner_oid"]
        and type(identity["owner_oid"]) is int
        and identity["schema_oid"] == request["future_schema_oid" if parent else "catalog_schema_oid"]
        and type(identity["schema_oid"]) is int
        and identity["kind"] == ("p" if parent else "r")
        and (parent or name == root),
        "identity_changed",
    )
    indexes = identity["indexes"]
    expected = (
        {
            name + "_pkey" if primary else name + "_i" + str(position)
            for position, (_key, _definition, *_semantics, primary) in enumerate(_LAYOUTS[root]["indexes"])
        }
        if parent
        else {index[0] for index in _LAYOUTS[root]["indexes"]}
    )
    _require(type(indexes) is dict and set(indexes) == expected, "indexes_changed")
    for index_name, oid in indexes.items():
        storage_identifier(index_name)
        _oid(oid)
    _dependencies(identity, request["catalog_schema"], root, parent=parent)


def _bounds(dataset_ids):
    _require(
        type(dataset_ids) is list
        and all(type(key) is str and 0 < len(key) <= 96 and "\x00" not in key for key in dataset_ids)
        and dataset_ids == sorted(set(dataset_ids)),
        "bounds_required",
    )
    # E literals escape backslashes independently of standard_conforming_strings.
    return ",".join("E'" + key.replace("\\", "\\\\").replace("'", "''") + "'" for key in dataset_ids)


def cms_history_bound_statements(catalog_schema, history_dataset_ids):
    """Render only the two fixed heap bounds, without an allocation or caller SQL."""
    _require(type(history_dataset_ids) is dict and set(history_dataset_ids) == set(ROOTS), "roots_required")
    additions, validations = [], []
    for root in ROOTS:
        relation = storage_relation(catalog_schema, root)
        literals = _bounds(history_dataset_ids[root])
        expression = "dataset_id IN (" + literals + ")" if literals else "false"
        additions.append(f"ALTER TABLE {relation} ADD CONSTRAINT {_BOUND} CHECK ({expression}) NOT VALID")
        validations.append(f"ALTER TABLE {relation} VALIDATE CONSTRAINT {_BOUND}")
    return {"add": tuple(additions), "validate": tuple(validations)}


def _request(request):
    _require(type(request) is dict and set(request) == _REQUEST_FIELDS, "request_required")
    request = deepcopy(request)
    for key in ("database_oid", "catalog_schema_oid", "future_schema_oid", "owner_oid"):
        _oid(request[key])
    for key in ("catalog_schema", "future_schema"):
        storage_identifier(request[key])
    _require(
        (request["catalog_schema"] == request["future_schema"])
        == (request["catalog_schema_oid"] == request["future_schema_oid"]),
        "namespace_identity_changed",
    )
    _require(type(request["heaps"]) is dict and set(request["heaps"]) == set(ROOTS), "roots_required")
    class_oids, catalog_names = [], set(ROOTS)
    for root in ROOTS:
        entry = request["heaps"][root]
        _require(type(entry) is dict and set(entry) == {"old", "parent", "dataset_ids"}, "heap_required")
        _bounds(entry["dataset_ids"])
        for parent in (False, True):
            identity = entry["parent" if parent else "old"]
            _identity(identity, request, root, parent=parent)
            class_oids.extend((identity["oid"], *identity["indexes"].values()))
            names = {identity["name"], *identity["indexes"]} if parent else set(identity["indexes"])
            _require(not catalog_names.intersection(names), "name_collision")
            catalog_names.update(names)
        history_name = "cms_history_" + str(entry["old"]["oid"])
        _require(history_name not in catalog_names, "name_collision")
        catalog_names.add(history_name)
    _require(len(class_oids) == len(set(class_oids)), "oid_collision")
    return request


def _heap_plan(request, root):
    entry = request["heaps"][root]
    old, parent = entry["old"], entry["parent"]
    schema, future = request["catalog_schema"], request["future_schema"]
    old_relation = storage_relation(schema, root)
    history_name = "cms_history_" + str(old["oid"])
    history = storage_relation(schema, history_name)
    literals = _bounds(entry["dataset_ids"])
    bound = "dataset_id IN (" + literals + ")" if literals else "false"
    prework = cms_history_bound_statements(schema, {name: request["heaps"][name]["dataset_ids"] for name in ROOTS})
    position = ROOTS.index(root)
    return {
        "prework": (prework["add"][position], prework["validate"][position]),
        "foreign_keys": tuple(
            f"ALTER TABLE {old_relation} DROP CONSTRAINT {storage_identifier(name)}"
            for name in sorted(old["foreign_keys"])
        ),
        "hooks": tuple(f"DROP TRIGGER {storage_identifier(name)} ON {old_relation}" for name in sorted(old["hooks"])),
        "rename": (f"ALTER TABLE {old_relation} RENAME TO {storage_identifier(history_name)}",),
        "parents": (
            ()
            if future == schema
            else (f"ALTER TABLE {storage_relation(future, parent['name'])} SET SCHEMA {storage_identifier(schema)}",)
        )
        + (f"ALTER TABLE {storage_relation(schema, parent['name'])} RENAME TO {storage_identifier(root)}",),
        "attach": (
            (f"ALTER TABLE {old_relation} ATTACH PARTITION {history} FOR VALUES IN ({literals})",) if literals else ()
        ),
        "expected": {
            "old": {
                **old,
                "name": history_name,
                "foreign_keys": {},
                "hooks": {},
                "parents": [parent["oid"]] if literals else [],
            },
            "parent": {**parent, "name": root, "schema_oid": request["catalog_schema_oid"], "parents": []},
            "dataset_ids": entry["dataset_ids"],
            "bound_constraint": {"name": _BOUND, "expression": bound, "validated": True},
            "acl_change": False,
            "callable_change": False,
        },
    }


def plan_cms_history_conversion(request):
    """Render an exact data plan; callers still prove layout, custody and held locks.

    Complete CHECK validation belongs to prework. The critical phases contain
    only dependency retirement, identity-preserving moves and bounded ATTACH DDL;
    an empty history remains detached with its prevalidated false CHECK.
    """
    request = _request(request)
    plans_by_root = {root: _heap_plan(request, root) for root in ROOTS}
    phases = ("foreign_keys", "hooks", "rename", "parents", "attach")
    return {
        "request": request,
        "prework": tuple(statement for root in ROOTS for statement in plans_by_root[root]["prework"]),
        "critical": {
            phase: tuple(statement for root in ROOTS for statement in plans_by_root[root][phase]) for phase in phases
        },
        "expected": {root: plans_by_root[root]["expected"] for root in ROOTS},
    }
