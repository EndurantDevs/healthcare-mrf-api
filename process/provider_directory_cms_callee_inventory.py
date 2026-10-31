# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Compare the fixed CMS callees with authenticated SourceV2/profile inventories.

The caller authenticates all four existing raw CMS digests before projecting
these fingerprints. They are expected data, never custody or write authority.
No SourceV1 role labels are invented for the isolated SourceV2 contract.
"""

from copy import deepcopy

from db.plan_release_storage import storage_identifier
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_preparation_custody as custody


def callee_fingerprint(entry):
    """Fingerprint the complete native routine row and its deterministic ACL expansion."""
    return custody._digest(
        dict(
            definition=callables._decoded(entry["definition"]),
            language=entry["language"],
            acl=callables._decoded(entry["acl"]),
        )
    )


def _has_owner_only_acl(entry):
    owner = int(entry["definition"]["proowner"])
    return entry["acl"] == [[owner, owner, "EXECUTE", False]]


def _require_source_metadata(functions, source_metadata):
    """Match all five genuine Source records to complete verified native rows."""
    from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS

    signatures = {signature.replace("character varying", "varchar") for signature in METADATA_FUNCTIONS}
    callables._require(type(source_metadata) is dict and set(source_metadata) == signatures, "source_metadata_required")
    for signature, metadata in source_metadata.items():
        name = signature.split("(")[0]
        callables._require(
            type(metadata) is dict
            and {"definition", "language", "acl"} <= metadata.keys()
            and all(metadata[field] == functions[name][field] for field in ("definition", "language", "acl")),
            "source_metadata_changed",
        )


def _source_metadata_caller(functions, source_metadata, namespace_owner):
    """Bind the preserved caller to the five genuine current Source records."""
    _require_source_metadata(functions, source_metadata)
    callers = {
        grantee
        for metadata in source_metadata.values()
        for grantee, *_ in metadata["acl"]
        if grantee not in {0, namespace_owner}
    }
    callables._require(len(callers) == 1, "source_caller_changed")
    return callers.pop()


def _source_acl_shape(functions, namespace_owner, source_metadata):
    names = {signature.split("(")[0] for signature in callables._SOURCE_CALLEES}
    # Isolated SourceV2 need not grant canonical metadata execution to any caller.
    if all(_has_owner_only_acl(functions[name]) for name in names):
        return
    public = "provider_directory_uhc_flex_practitioner_dataset_valid"
    owners, callers = set(), set()
    optional_callers = set()
    for name in names:
        entry = functions[name]
        owner = int(entry["definition"]["proowner"])
        owners.add(owner)
        edges = set(map(tuple, entry["acl"]))
        callables._require(
            len(edges) == len(entry["acl"])
            and (owner, owner, "EXECUTE", False) in edges
            and all(
                grantor == owner and right == "EXECUTE" and grantable is False for _, grantor, right, grantable in edges
            ),
            "acl_changed",
        )
        extra_grantees = {grantee for grantee, *_ in edges if grantee not in {0, owner}}
        if (
            name in {"provider_directory_rooted_graph_dataset_intrinsic_valid", public}
            and owner == namespace_owner
            and edges == {(owner, owner, "EXECUTE", False)}
        ):
            continue
        if name == public:
            callables._require((0, owner, "EXECUTE", False) in edges and len(extra_grantees) <= 1, "acl_changed")
            optional_callers.update(extra_grantees)
        else:
            callables._require(not any(edge[0] == 0 for edge in edges) and len(extra_grantees) == 1, "acl_changed")
            callers.update(extra_grantees)
    if owners == {namespace_owner}:
        preserved_caller = _source_metadata_caller(functions, source_metadata, namespace_owner)
        callables._require(callers == {preserved_caller} and optional_callers <= callers, "source_acl_changed")
        return
    callables._require(
        len(owners) == len(callers) == 1
        and not owners.intersection(callers | {namespace_owner})
        and namespace_owner not in callers
        and optional_callers <= callers,
        "source_acl_changed",
    )


def _entry(entry, spec, schema, namespace, namespace_owner, preparation_schema, allocator, proof_actors):
    from process.provider_directory_generic_source_release_checks import _require_allocator_guard_acl

    name = entry["definition"]["proname"]
    if name == "guard_pd_dataset_candidate_parent":
        spec = callables._canonical_guard_spec(spec, schema, preparation_schema, entry["definition"]["prosrc"])
    callables._require_cms_callee_body(entry, spec, schema, namespace)
    if name in callables._SERVING_CALLEES or name in {key.split("(")[0] for key in callables._SOURCE_CALLEES}:
        return
    owner = int(entry["definition"]["proowner"])
    if name == "guard_pd_dataset_candidate_parent":
        _require_allocator_guard_acl(entry, allocator, schema, namespace, {owner, namespace_owner}, namespace_owner)
    elif entry["acl"] != [[owner, owner, "EXECUTE", False]] and proof_actors is not None:
        callables._require_cms_proof_acl(entry, namespace_owner, proof_actors)
    else:
        callables._require(entry["acl"] == [[owner, owner, "EXECUTE", False]], "acl_changed")


def _require_fingerprint(entry, pin):
    callables._require(
        type(pin) is dict
        and set(pin) == {"oid", "sha256"}
        and type(pin["oid"]) is int
        and pin["oid"] == int(entry["definition"]["oid"])
        and pin["sha256"] == callee_fingerprint(entry),
        "callee_inventory_changed",
    )


def _verified_callees(
    function_rows,
    *,
    catalog_schema,
    catalog_schema_oid,
    expected_functions,
    namespace_owner_oid,
    preparation_schema,
    allocator,
    proof_actors,
):
    storage_identifier(preparation_schema)
    specs = callables._cms_callee_contracts()
    callables._require(
        type(expected_functions) is dict
        and set(expected_functions) == set(specs)
        and type(catalog_schema_oid) is int
        and catalog_schema_oid > 0
        and type(namespace_owner_oid) is int
        and namespace_owner_oid > 0,
        "callee_inventory_required",
    )
    signatures_by_name = dict(zip(sorted(specs), callables.cms_callee_signatures(catalog_schema), strict=True))
    verified_by_name = {}
    oids = set()
    for function_row in function_rows:
        entry_by_field = {
            **dict(function_row),
            "definition": callables._decoded(function_row["definition"]),
            "acl": callables._decoded(function_row["acl"]),
        }
        name, oid = entry_by_field["definition"]["proname"], int(entry_by_field["definition"]["oid"])
        callables._require(
            name in specs and name not in verified_by_name and oid > 0 and oid not in oids, "function_set_changed"
        )
        _require_fingerprint(entry_by_field, expected_functions[name])
        callables._require(entry_by_field["signature"] == signatures_by_name[name], "signature_changed")
        _entry(
            entry_by_field,
            specs[name],
            catalog_schema,
            catalog_schema_oid,
            namespace_owner_oid,
            preparation_schema,
            allocator,
            proof_actors,
        )
        verified_by_name[name] = entry_by_field
        oids.add(oid)
    callables._require(set(verified_by_name) == set(specs), "function_set_changed")
    return verified_by_name


def require_cms_callee_inventory(
    function_rows,
    *,
    catalog_schema,
    catalog_schema_oid,
    expected_functions,
    namespace_owner_oid,
    preparation_schema,
    allocator=None,
    source_metadata=None,
):
    """Keep exact fixed32 bodies, fingerprints and existing metadata-only ACL checks."""
    verified = _verified_callees(
        function_rows,
        catalog_schema=catalog_schema,
        catalog_schema_oid=catalog_schema_oid,
        expected_functions=expected_functions,
        namespace_owner_oid=namespace_owner_oid,
        preparation_schema=preparation_schema,
        allocator=allocator,
        proof_actors=None,
    )
    _source_acl_shape(verified, namespace_owner_oid, source_metadata)
    return deepcopy(verified)


def require_cms_source_callee_inventory(
    function_rows,
    *,
    catalog_schema,
    catalog_schema_oid,
    expected_functions,
    namespace_owner_oid,
    preparation_schema,
    source_document,
    allocator=None,
):
    """Recognize finite proof caller ACLs bound to the genuine current SourceV2.

    Caller authentication of the complete Source and profile remains mandatory,
    together with native owner, role-reach, dependency and transaction custody.
    """
    from process.provider_directory_cms_proof_contract import cms_proof_actor_oids

    callables._require(
        type(source_document) is dict
        and source_document.get("version") == 2
        and type(source_document.get("role_graph")) is dict
        and type(source_document["role_graph"].get("roles")) is list,
        "source_document_required",
    )
    proof_actors = cms_proof_actor_oids(
        source_document, source_document["role_graph"]["roles"], namespace_owner_oid=namespace_owner_oid
    )
    verified = _verified_callees(
        function_rows,
        catalog_schema=catalog_schema,
        catalog_schema_oid=catalog_schema_oid,
        expected_functions=expected_functions,
        namespace_owner_oid=namespace_owner_oid,
        preparation_schema=preparation_schema,
        allocator=allocator,
        proof_actors=proof_actors,
    )
    metadata = source_document["storage_custody"]["canonical_authority"]["metadata"]
    _require_source_metadata(verified, metadata)
    _source_acl_shape(verified, namespace_owner_oid, metadata)
    return deepcopy(verified)
