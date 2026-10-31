# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite source-bound read definers, separate from the fixed callable frontier.

The caller authenticates the complete raw routines digest and genuine Source
record before this projection. These checks neither establish inventory
completeness nor replace owner reachability, writers or unknown-definer closure.
"""

from process import provider_directory_cms_callee_inventory as callees
from process import provider_directory_cms_dependency_policy as dependencies
from process import provider_directory_source_callable_custody as source
from process.provider_directory_cms_proof_contract import cms_proof_actor_oids
from process.provider_directory_cms_routine_settings import require_cms_terminal_routine_settings
from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS


def _require(condition, code):
    if not condition:
        raise ValueError("cms_read_definers_" + code)


def _read_names():
    names = set()
    for name, arguments in dependencies._CALLBACK_CALLEES.items():
        spec = source._CONTRACTS[name + "(" + ",".join(arguments) + ")"]
        if not spec[7]:
            continue
        # Exact source bodies and their reviewed transitive calls justify this finite set.
        _require(
            spec[1] in {"text", "boolean", "jsonb"}
            and not spec[2]
            and spec[3] in {"sql", "plpgsql"}
            and spec[4] in {"s", "i"},
            "source_contract_changed",
        )
        names.add(name)
    _require(len(names) == 11, "source_contract_changed")
    return names


def _source_actors(functions_by_name, document, namespace_owner):
    _require(type(document) is dict and document.get("version") == 2, "source_required")
    metadata_names = {signature.split("(")[0] for signature in METADATA_FUNCTIONS}
    _require(metadata_names <= functions_by_name.keys(), "source_metadata_required")
    try:
        custody = document["storage_custody"]
        callees._require_source_metadata(functions_by_name, custody["canonical_authority"]["metadata"])
        owner_name = document["policy"]["namespace_owner_role"]
        _require(custody["roles"][owner_name]["rolcanlogin"] is False, "owner_changed")
        return cms_proof_actor_oids(document, document["role_graph"]["roles"], namespace_owner_oid=namespace_owner)
    except (KeyError, TypeError, IndexError) as error:
        raise ValueError("cms_read_definers_source_required") from error


def _require_acl(entry, namespace_owner, actors):
    definition = entry["definition"]
    _require(definition["proowner"] == str(namespace_owner), "owner_changed")
    edges = entry["acl"]
    _require(type(edges) is list and bool(edges), "acl_changed")
    grantees = set()
    allowed_grantees = actors | {namespace_owner}
    if definition["proname"] in source._PUBLIC_NAMES:
        allowed_grantees |= {0}
    for edge in edges:
        _require(
            type(edge) is list
            and len(edge) == 4
            and type(edge[0]) is int
            and type(edge[1]) is int
            and edge[0] in allowed_grantees
            and edge[1:] == [namespace_owner, "EXECUTE", False]
            and edge[3] is False
            and edge[0] not in grantees,
            "acl_changed",
        )
        grantees.add(edge[0])
    _require(namespace_owner in grantees, "acl_changed")


def require_cms_read_definers(
    routines, *, catalog_schema, catalog_schema_oid, preparation_schema, source_document, namespace_owner_oid
):
    """Return exactly eleven qualified OIDs from caller-authenticated raw custody.

    Existing PUBLIC execution is accepted only for the source-approved UHC
    ready function. This projection never creates or infers an execution grant.
    """
    _require(type(namespace_owner_oid) is int and 0 < namespace_owner_oid < 2**32, "owner_required")
    require_cms_terminal_routine_settings(
        routines,
        catalog_schema=catalog_schema,
        catalog_schema_oid=catalog_schema_oid,
        preparation_schema=preparation_schema,
    )
    functions_by_name = {
        routine["native"]["definition"]["proname"]: dict(
            definition=routine["native"]["definition"],
            language=routine["native"]["language"]["lanname"],
            acl=routine["native"].get("acl"),
        )
        for routine in routines
    }
    names = _read_names()
    _require(names <= functions_by_name.keys(), "function_set_changed")
    actors = _source_actors(functions_by_name, source_document, namespace_owner_oid)
    for routine in routines:
        if routine["native"]["definition"]["proname"] in names:
            namespace = routine["native"]["namespace"]
            _require(
                type(namespace) is list and len(namespace) == 4 and namespace[2] == namespace_owner_oid,
                "owner_changed",
            )
    for name in names:
        _require_acl(functions_by_name[name], namespace_owner_oid, actors)
    return sorted(int(functions_by_name[name]["definition"]["oid"]) for name in names)
