# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Finite proof callers from genuine Source records; authentication stays with the caller."""

from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS

CMS_PROOF_DEFINERS = frozenset(
    {
        "provider_directory_subset_abandonment_valid",
        "provider_directory_subset_terminal_disposition_valid",
        "provider_directory_terminal_root_retirement_evidence",
        "provider_directory_terminal_root_retirement_v2_evidence",
        "provider_directory_terminal_root_retirement_valid",
        "provider_directory_terminal_root_retirement_v2_valid",
    }
)
CMS_INVOKER_PROOF = "provider_directory_fhir_request_failure_coverage_valid"


def _require(condition, code):
    if not condition:
        raise ValueError("cms_proof_contract_" + code)


def _oid(value):
    _require(type(value) is int or type(value) is str and value.isascii() and value.isdecimal(), "oid_required")
    result = int(value)
    _require(0 < result < 2**32, "oid_required")
    return result


def _metadata_caller(metadata, namespace_owner_oid):
    signatures = {signature.replace("character varying", "varchar") for signature in METADATA_FUNCTIONS}
    _require(type(metadata) is dict and set(metadata) == signatures, "metadata_required")
    owners = {_oid(entry["definition"]["proowner"]) for entry in metadata.values()}
    _require(len(owners) == 1, "metadata_caller_unbound")
    owner = owners.pop()
    if owner != namespace_owner_oid:
        return owner
    callers = [
        {edge[0] for edge in entry["acl"] if edge[0] not in {0, namespace_owner_oid}} for entry in metadata.values()
    ]
    _require(len(callers[0]) == 1 and all(caller == callers[0] for caller in callers), "metadata_caller_unbound")
    return _oid(next(iter(callers[0])))


def cms_proof_actor_oids(source_document, role_rows, *, namespace_owner_oid):
    """Resolve only source-bound callers after Source/profile and exact Source5 authentication."""
    _require(type(role_rows) is list, "roles_required")
    by_name = {role_row["rolname"]: {**role_row, "oid": _oid(role_row["oid"])} for role_row in role_rows}
    _require(len(by_name) == len(role_rows) == len({role_row["oid"] for role_row in by_name.values()}), "roles_changed")
    custody = source_document["storage_custody"]
    declaration, publication = source_document["policy"], source_document["publication"]
    names = (
        declaration["namespace_owner_role"],
        declaration["owner_role"],
        publication["owner_role"],
        publication["publisher_role"],
    )
    _require(
        len(set(names)) == 4
        and all(
            name in by_name and name in custody["roles"] and _oid(custody["roles"][name]["oid"]) == by_name[name]["oid"]
            for name in names
        ),
        "source_role_changed",
    )
    target_by_field = by_name[names[0]]
    private_owner_by_field = by_name[names[1]]
    publication_owner_by_field = by_name[names[2]]
    publisher_by_field = by_name[names[3]]
    _require(
        _oid(namespace_owner_oid) == _oid(custody["coordinates"]["namespace_owner_oid"]) == target_by_field["oid"]
        and private_owner_by_field["oid"] == _oid(custody["coordinates"]["owner_oid"])
        and all(
            role["rolcanlogin"] is False
            for role in (target_by_field, private_owner_by_field, publication_owner_by_field)
        )
        and publisher_by_field["rolcanlogin"] is True,
        "source_role_changed",
    )
    caller = _metadata_caller(custody["canonical_authority"]["metadata"], target_by_field["oid"])
    by_oid = {role["oid"]: role for role in by_name.values()}
    _require(
        caller in by_oid
        and by_oid[caller]["rolcanlogin"] is True
        and by_oid[caller]["rolsuper"] is False
        and caller
        not in {
            target_by_field["oid"],
            private_owner_by_field["oid"],
            publication_owner_by_field["oid"],
            publisher_by_field["oid"],
        },
        "metadata_caller_unbound",
    )
    return frozenset(
        {caller, private_owner_by_field["oid"], publication_owner_by_field["oid"], publisher_by_field["oid"]}
    )
