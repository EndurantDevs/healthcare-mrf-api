# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Protect existing CMS callable bodies; table-hook lifetime remains a separate cut gate."""

import asyncio
import json
from copy import deepcopy
from hashlib import sha256

from db.plan_release_storage import storage_identifier, storage_relation
from process.plan_release_storage_custody import storage_roles
from process.provider_directory_cms_routine_settings import approved_cms_routine_settings
from process.provider_directory_generic_source_policy import GenericSourcePolicy

# Exact prosrc contracts from the receipt migration, shared builder and forward argument repair.
_CONTRACTS = {
    "cms_serving_address_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "afb90c15e4a45c3022403891afaaf85b4cbc81bd759a98cbdb1d227bba8ebef7",
    ),
    "cms_serving_archive_result_matches": (
        (("p", "jsonb"),),
        "boolean",
        "plpgsql",
        "s",
        "38426c90255c28e871039fe2aa9e844758cdb67de6ab8e2199ef04912ef06b66",
    ),
    "cms_serving_current_receipt_matches": (
        (("p", "jsonb"),),
        "boolean",
        "sql",
        "s",
        "dc6a70bf64fd378e2a83a7caf180e856490c5a66676335be6fe8a1df1860f756",
    ),
    "cms_serving_dataset_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "64da00faca333a3e97821a2bceeaaf6900618f22a8119de54b08f78109367522",
    ),
    "cms_serving_doctors_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "ee37d843ebe88cf5433990eabb8f742200eaf3cdf2333cceaf22672f7d4f6fd0",
    ),
    "cms_serving_native_matches": (
        (("p", "jsonb"),),
        "boolean",
        "sql",
        "s",
        "059b4c093c0c089ce9d5b43bdfa67111586f33d24072517fe57b37f2ea1fb287",
    ),
    "cms_serving_native_snapshot": (
        (),
        "jsonb",
        "sql",
        "s",
        "91306c115e0835db8ece5ed05b21f48d021735d464d64a70f4587d6c9fc77f4e",
    ),
    "cms_serving_no_truncate": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "f4f8f7df3cb932032a88345290d71f39cbe9720324b60d7699cec8ccd32612e3",
    ),
    "cms_serving_oids": (
        (("names", "text[]"),),
        "jsonb",
        "sql",
        "s",
        "90a3b2c46612c899a8695c8196de49fa4ca90e3ac4806e243302a167dc2532d7",
    ),
    "cms_serving_profile_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "a8af12e1bb98c589ae88fd3e622c69b5b3b4e853875fb16ff0b57020f8fb6539",
    ),
    "cms_serving_receipt_immutable": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "6eac5c7ed6443bf8f45d3414170ce849bf51d6726f64039bc5f31ef972e63727",
    ),
    "cms_serving_receipt_insert": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "4ce374dcc2f22282e5996e3693248e74b4c9c6a4c740448b91cc6296ce00beb3",
    ),
    "cms_serving_receipt_matches": (
        (("p", "jsonb"),),
        "boolean",
        "plpgsql",
        "s",
        "a7e3da06af9af8c047bb262f27cb94e6cd776f40ff05087b24167ddb35b20ec9",
    ),
    "cms_serving_require_fresh": (
        (("kind", "text"), ("previous", "jsonb")),
        "void",
        "plpgsql",
        "v",
        "7a1769065c5da4ac2fbd9c04e8f846e3093be25baa078f5c3e2da56ed60b8469",
    ),
    "cms_serving_snapshot_receipt_matches": (
        (("p", "jsonb"),),
        "boolean",
        "sql",
        "s",
        "b2f575c615b7e076fb8628e3f9ba7cab0411ac7ca6c14fb310a6724441860d3c",
    ),
    "cms_serving_source_transition": (
        (),
        "trigger",
        "plpgsql",
        "v",
        "a54cf5a5ba64a12d78832923b8482bd2fc266058f2b80d2438b861ad96b43de6",
    ),
}
CMS_CALLABLES = tuple(_CONTRACTS)
# This post-transition frontier is separate from the sixteen-function adoption scope.
_SERVING_CALLEES = (
    "cms_serving_archive_result_matches",
    "cms_serving_native_matches",
    "cms_serving_native_snapshot",
    "cms_serving_oids",
    "cms_serving_receipt_matches",
    "cms_serving_require_fresh",
)
_SOURCE_CALLEES = (
    "provider_directory_endpoint_dataset_admission_metadata_sha256(jsonb,smallint,text,text,varchar[])",
    "provider_directory_rooted_graph_dataset_intrinsic_valid(text)",
    "provider_directory_subset_canonical_sha256(jsonb)",
    "provider_directory_subset_completion_canonical_json(jsonb)",
    "provider_directory_subset_completion_proof_pair_valid(jsonb,text)",
    "provider_directory_subset_completion_proof_shape_valid(jsonb,text,bigint)",
    "provider_directory_subset_content_proof_valid(jsonb,text,text,text,jsonb,jsonb,text,bigint,jsonb,jsonb)",
    "provider_directory_subset_payload_canonical_json(jsonb)",
    "provider_directory_subset_payload_sha256(jsonb)",
    "provider_directory_uhc_flex_practitioner_dataset_valid(text)",
)
_ALLOCATOR_CALLEES = (
    "guard_pd_dataset_candidate_parent()",
    "pd_dataset_candidate_tables(text)",
    "validate_pd_dataset_bulk_relationships(text,text)",
)
# Existing allocation callbacks are optional; the permanent 32-callee frontier stays fixed.
_ORIGINAL_ALLOCATION_GUARDS = {
    "guard_cms_npd_covered_truncate": (
        "05cf75e46f4f3a2931f72ac491da47061d435afc1566fa5387fbd10fa37c5148",
        False,
    ),
    "guard_cms_npd_published_resource": (
        "3c869160a7ef6cbc0342e0fbb1d048e5fe1aaa1e954e844d877950737697475c",
        False,
    ),
    "guard_pd_endpoint_dataset_subset_replay_evidence": (
        "1bad9b653fb789345f534559c7818b5e251b5ba4e5b6c1042e5f82629c7126c4",
        True,
    ),
    "guard_provider_directory_endpoint_dataset_admission_truncate": (
        "78ad5a392bbfda88bae9e32a7b1d07de52ceaa4e94bd8ed4ec5ea37ab00944ea",
        True,
    ),
    "guard_provider_directory_reviewed_subset_activation_dataset": (
        "827798b2be6b5ccf264d614e0e64d5d49b59d5f0f5b3341413c9798e1b6eb407",
        True,
    ),
    "guard_provider_directory_subset_abandonment_child": (
        "11939dd6dba2a4dc875d8d0a5fd06ab67d0a0696ab3fbc391fe39b334df9c902",
        True,
    ),
    "guard_provider_directory_subset_abandonment_dataset": (
        "d416653fb3e364905e30b0a9b4c5db61cba103c924e40424bdcec187fc226d58",
        True,
    ),
    "guard_provider_directory_terminal_root_retirement_parent": (
        "bc73e0e7e9a1dd6c1e7900d27fef68f68d73ecb8c347616f37208d47158d3968",
        True,
    ),
    "guard_tin_npi_connector_dataset_resource": (
        "461bce234d4178442a64465a3c90604da16c67938c0e323a79e93d26e2bcdd89",
        True,
    ),
}
# Exact current subset/disposition and terminal-retirement constructors; tests bind the bodies.
_CALLEE_CONTRACTS = {
    "provider_directory_subset_abandonment_valid": (
        (("candidate_dataset_id", "text"),),
        "boolean",
        (),
        "sql",
        "s",
        False,
        "u",
        True,
        "24fe2518237a778b6d0e083d52e638cb160a65a7c6409961fd0a3a46307a1823",
    ),
    "provider_directory_subset_coverage_shape_valid": (
        (
            ("coverage_candidate", "jsonb"),
            ("completion_candidate", "jsonb"),
            ("completion_sha256", "text"),
            ("expected_twin_state", "text"),
        ),
        "boolean",
        (),
        "plpgsql",
        "i",
        True,
        "s",
        True,
        "d24502e18298579a1169cf748f456830265e1ada3c6d916256d442f0706bc779",
    ),
    "provider_directory_subset_replay_evidence_shape_valid": (
        (
            ("replay_candidate", "jsonb"),
            ("replay_sha256", "text"),
            ("completion_candidate", "jsonb"),
            ("completion_sha256", "text"),
        ),
        "boolean",
        (),
        "plpgsql",
        "i",
        True,
        "s",
        True,
        "6293d6acf2d5cde949fc58f0444575073a1d90258441ff9ab429fefcec4cd1d9",
    ),
    "provider_directory_subset_terminal_disposition_v4_valid": (
        (("candidate_dataset_id", "text"),),
        "boolean",
        (),
        "plpgsql",
        "s",
        False,
        "u",
        True,
        "1117af08726cca2c3b59d179da570dbd8563109dd87cda4e5c094ebbedebc420",
    ),
    "provider_directory_subset_terminal_disposition_v5_valid": (
        (("candidate_dataset_id", "text"),),
        "boolean",
        (),
        "plpgsql",
        "s",
        False,
        "u",
        True,
        "3212ec66ec12015222ea8658f26180f61cc4f11401f914fbdc675b0a827a6e91",
    ),
    "provider_directory_subset_terminal_disposition_valid": (
        (("candidate_dataset_id", "text"),),
        "boolean",
        (),
        "plpgsql",
        "s",
        False,
        "u",
        True,
        "2b2dddb7809c6d4900246f8dc72a6a9fa006ca59fc9fbc1a7ed5643dd2f53c2d",
    ),
    "provider_directory_terminal_root_retirement_eligible": (
        (("candidate_dataset_id", "text"), ("minimum_age", "integer")),
        "boolean",
        (),
        "sql",
        "s",
        False,
        "u",
        True,
        "43e6ef5a58573e0e92c47fd8a0dac3790ed41b91557f4bebe6474a114eea53cf",
    ),
    "provider_directory_terminal_root_retirement_evidence": (
        (("candidate_dataset_id", "text"),),
        "jsonb",
        (),
        "sql",
        "s",
        False,
        "u",
        True,
        "ba101807efd88ec7618eb5b68b11cd6129c33e30f3d7a5d8d2c2ec5ce2c4c7ea",
    ),
    "provider_directory_terminal_root_retirement_marker_valid": (
        (("candidate_dataset_id", "text"), ("marker", "jsonb")),
        "boolean",
        (),
        "sql",
        "s",
        False,
        "u",
        True,
        "2c70683e2a5ead7e2646dd134d465d83fcb0d6e2421f59117b0d51828be4e32a",
    ),
    "provider_directory_terminal_root_retirement_relation_evidence": (
        (("candidate_dataset_id", "text"), ("candidate_relation", "text")),
        "jsonb",
        (),
        "plpgsql",
        "s",
        False,
        "u",
        True,
        "4d463fd2d4f2a4944398c9ed943a53b46edcb292a7617f91107c2960641fd7e9",
    ),
    "provider_directory_terminal_root_retirement_v2_eligible": (
        (("candidate_dataset_id", "text"), ("minimum_age", "integer")),
        "boolean",
        (),
        "sql",
        "s",
        False,
        "u",
        True,
        "c0dbf6976b64b769b98fc7aaddb6302c5a988cb0b3f60da8aa32a0d55a2eeb58",
    ),
    "provider_directory_terminal_root_retirement_v2_evidence": (
        (("candidate_dataset_id", "text"),),
        "jsonb",
        (),
        "sql",
        "s",
        False,
        "u",
        True,
        "87c8b3756173597edd37b86bfdc02efaf57440c854867e176f3bf4e76cb5042c",
    ),
    "provider_directory_terminal_root_retirement_v2_marker_valid": (
        (("candidate_dataset_id", "text"), ("marker", "jsonb")),
        "boolean",
        (),
        "sql",
        "s",
        False,
        "u",
        True,
        "6e0d9b9de9c582fcf3c19738af8452d882cb60fda5e9c5b99ba1ff1087ec8afb",
    ),
}
_CALLEE_SETTINGS = {
    name: ["search_path=pg_catalog", "TimeZone=UTC"]
    for name in (
        "provider_directory_terminal_root_retirement_evidence",
        "provider_directory_terminal_root_retirement_relation_evidence",
        "provider_directory_terminal_root_retirement_v2_evidence",
    )
}
_TYPE_OIDS = {"text[]": 1009, "text": 25, "jsonb": 3802, "boolean": 16, "trigger": 2279, "void": 2278}
_FUNCTION_SQL = """SELECT signature,to_jsonb(p) AS definition,l.lanname AS language,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,
 a.privilege_type,a.is_grantable) ORDER BY a.grantee,a.grantor,a.privilege_type,a.is_grantable)
 FROM aclexplode(COALESCE(p.proacl,acldefault('f',p.proowner))) a),'[]'::jsonb) AS acl,
 (SELECT jsonb_agg(jsonb_build_array(r.oid::bigint,r.rolsuper,
 EXISTS(SELECT FROM pg_roles ordinary WHERE ordinary.rolname=ANY($4::text[])
 AND pg_has_role(r.oid,ordinary.oid,'MEMBER')),pg_has_role(r.oid,p.proowner,'MEMBER'),pg_has_role(r.oid,p.proowner,'SET'),
 has_function_privilege(r.oid,p.oid,'EXECUTE')) ORDER BY r.oid) FROM pg_roles r) AS access
 FROM pg_proc p JOIN pg_language l ON l.oid=p.prolang
 LEFT JOIN unnest($1::text[]) f(signature) ON p.oid=to_regprocedure(signature)
 WHERE p.pronamespace=to_regnamespace($2) AND p.proname=ANY($3::text[]) ORDER BY p.oid"""
_ROLE_GRAPH_SQL = """SELECT jsonb_build_object(
 'roles',(SELECT jsonb_agg(to_jsonb(r)-'rolpassword' ORDER BY oid) FROM pg_roles r),
 'memberships',(SELECT jsonb_agg(to_jsonb(m) ORDER BY roleid,member) FROM pg_auth_members m))"""


def _require(condition, code):
    if not condition:
        raise ValueError("cms_callable_" + code)


def _decoded(value):
    return json.loads(value) if type(value) is str else value


def _body_digest(body, schema):
    canonical = body.replace('"' + schema + '"', '"__schema__"').replace("'" + schema + "'", "'__schema__'")
    return sha256(canonical.encode("utf-8")).hexdigest()


def _signatures(schema):
    return tuple(
        storage_relation(schema, name) + "(" + ",".join(kind for _, kind in spec[0]) + ")"
        for name, spec in _CONTRACTS.items()
    )


def _cms_callee_contracts():
    from process.provider_directory_generic_source_release_checks import _LEGACY_FUNCTIONS
    from process.provider_directory_source_callable_custody import _CONTRACTS as source_contracts

    contract_by_name = dict(_CALLEE_CONTRACTS)
    contract_by_name.update({key.split("(")[0]: source_contracts[key] for key in _SOURCE_CALLEES})
    contract_by_name.update({key.split("(")[0]: _LEGACY_FUNCTIONS[key] for key in _ALLOCATOR_CALLEES})
    for name in _SERVING_CALLEES:
        arguments, returns, language, volatility, digest = _CONTRACTS[name]
        contract_by_name[name] = (arguments, returns, (), language, volatility, False, "u", False, digest)
    return contract_by_name


def cms_callee_signatures(catalog_schema):
    """Return the fixed post-transition CMS callee frontier, including exact overloads."""
    alias_by_type = {"smallint": "int2", "integer": "int4", "bigint": "int8"}
    return tuple(
        storage_relation(catalog_schema, name)
        + "("
        + ",".join(alias_by_type.get(kind, kind) for _, kind in spec[0])
        + ")"
        for name, spec in sorted(_cms_callee_contracts().items())
    )


def _shared_callee_definition_entry(entry_by_field):
    """Check exact source settings before adapting to the shared attribute checker."""
    definition_by_field = entry_by_field["definition"]
    expected_settings = _CALLEE_SETTINGS.get(definition_by_field["proname"])
    if expected_settings is None:
        return entry_by_field
    _require(definition_by_field["proconfig"] in approved_cms_routine_settings(expected_settings), "definition_changed")
    return {
        **entry_by_field,
        "definition": {**definition_by_field, "proconfig": ["search_path=pg_catalog"]},
    }


def _require_cms_callee_body(entry_by_field, spec, schema, namespace):
    from process.provider_directory_source_callable_custody import _TYPE_OIDS as source_types
    from process.provider_directory_source_callable_custody import _require_definition as require_source_definition

    name = entry_by_field["definition"]["proname"]
    if name in _SERVING_CALLEES:
        _require_definition(entry_by_field, schema, namespace)
        return
    require_source_definition(
        _shared_callee_definition_entry(entry_by_field),
        spec,
        schema,
        namespace,
        {**source_types, "text[]": "1009"},
        storage_identifier(schema),
    )


def _require_cms_proof_acl(entry_by_field, namespace_owner, source_actors):
    """Check finite, source-verified proof caller ACLs after exact body validation."""
    from process.provider_directory_cms_proof_contract import CMS_PROOF_DEFINERS

    definition = entry_by_field["definition"]
    _require(
        definition["proname"] in CMS_PROOF_DEFINERS
        and int(definition["proowner"]) == namespace_owner
        and type(source_actors) is frozenset
        and all(type(oid) is int and oid > 0 and oid != namespace_owner for oid in source_actors),
        "proof_source_required",
    )
    edges = set(map(tuple, entry_by_field["acl"]))
    _require(
        len(edges) == len(entry_by_field["acl"])
        and (namespace_owner, namespace_owner, "EXECUTE", False) in edges
        and all(
            grantee in source_actors | {namespace_owner}
            and grantor == namespace_owner
            and right == "EXECUTE"
            and grantable is False
            for grantee, grantor, right, grantable in edges
        ),
        "acl_changed",
    )


def _require_cms_callee_entry(entry_by_field, spec, schema, namespace, source_roles, allocator):
    from process.provider_directory_generic_source_release_checks import _require_allocator_guard_acl
    from process.provider_directory_source_callable_custody import _require_acl as require_source_acl

    _require_cms_callee_body(entry_by_field, spec, schema, namespace)
    name = entry_by_field["definition"]["proname"]
    if name in _SERVING_CALLEES:
        return
    if name in {key.split("(")[0] for key in _SOURCE_CALLEES}:
        require_source_acl(entry_by_field, source_roles, legacy=False)
    elif name == "guard_pd_dataset_candidate_parent":
        _require_allocator_guard_acl(
            entry_by_field,
            allocator,
            schema,
            namespace,
            {int(entry_by_field["definition"]["proowner"]), source_roles["namespace_owner"]},
            source_roles["namespace_owner"],
        )
    else:
        owner = int(entry_by_field["definition"]["proowner"])
        _require(entry_by_field["acl"] == [[owner, owner, "EXECUTE", False]], "acl_changed")


def _canonical_guard_spec(spec, schema, preparation_schema, body):
    from db.migration_generic_source_candidates import _canonical_copy_guard_bodies, _cms_canonical_copy_guard_body

    approved_bodies = (
        _canonical_copy_guard_bodies(schema, preparation_schema)[1],
        _cms_canonical_copy_guard_body(schema, preparation_schema),
    )
    _require(body in approved_bodies, "definition_changed")
    return (*spec[:-1], _body_digest(body, schema))


def require_cms_callee_definitions(
    function_rows,
    *,
    catalog_schema,
    catalog_schema_oid,
    expected_owner_oids,
    source_roles,
    preparation_schema,
    allocator=None,
):
    """Check fixed source contracts against catalog rows without granting authority.

    The caller supplies independently authenticated owner/Source-role OIDs and the
    installed preparation schema, and retains dependency, role-reach and fencing checks.
    Only exact generic/CMS guard bodies and the checked allocator EXECUTE edge
    are recognized here; recognition does not install or activate CMS.
    """
    storage_identifier(preparation_schema)
    contract_by_name = _cms_callee_contracts()
    _require(
        set(expected_owner_oids) == contract_by_name.keys()
        and all(type(oid) is int and oid > 0 for oid in expected_owner_oids.values())
        and type(catalog_schema_oid) is int
        and catalog_schema_oid > 0,
        "expected_owners_required",
    )
    _require(
        set(source_roles) == {"owner", "publisher", "builder", "namespace_owner", "migration"}
        and all(type(oid) is int and oid > 0 for oid in source_roles.values())
        and len(set(source_roles.values())) == 5,
        "source_roles_required",
    )
    guard = "guard_pd_dataset_candidate_parent"
    signature_by_name = dict(zip(sorted(contract_by_name), cms_callee_signatures(catalog_schema), strict=True))
    function_by_name = {}
    oids = set()
    for function_row in function_rows:
        entry_by_field = {
            **dict(function_row),
            "definition": _decoded(function_row["definition"]),
            "acl": _decoded(function_row["acl"]),
        }
        definition_by_field = entry_by_field["definition"]
        name, oid = definition_by_field["proname"], int(definition_by_field["oid"])
        _require(
            name in contract_by_name and name not in function_by_name and oid > 0 and oid not in oids,
            "function_set_changed",
        )
        _require(entry_by_field["signature"] == signature_by_name[name], "signature_changed")
        _require(int(definition_by_field["proowner"]) == expected_owner_oids[name], "owner_changed")
        spec = contract_by_name[name]
        if name == guard:
            spec = _canonical_guard_spec(spec, catalog_schema, preparation_schema, definition_by_field["prosrc"])
        _require_cms_callee_entry(entry_by_field, spec, catalog_schema, catalog_schema_oid, source_roles, allocator)
        function_by_name[name] = entry_by_field
        oids.add(oid)
    _require(function_by_name.keys() == contract_by_name.keys(), "function_set_changed")
    return function_by_name


def _original_callee_contracts(catalog_schema, preparation_schema):
    from db.migration_generic_source_candidates import _canonical_copy_guard_bodies
    from process.provider_directory_generic_source_release_checks import _LEGACY_FUNCTIONS
    from process.provider_directory_source_callable_custody import _CONTRACTS as source_contracts

    contract_by_name = {key.split("(")[0]: spec for key, spec in {**source_contracts, **_LEGACY_FUNCTIONS}.items()}
    contract_by_name.update(
        {
            name: ((), "trigger", (), "plpgsql", "v", False, "u", is_definer, digest)
            for name, (digest, is_definer) in _ORIGINAL_ALLOCATION_GUARDS.items()
        }
    )
    for name, (arguments, returns, language, volatility, digest) in _CONTRACTS.items():
        contract_by_name[name] = (arguments, returns, (), language, volatility, False, "u", False, digest)
    contract_by_name.update(_cms_callee_contracts())
    guard = "guard_pd_dataset_candidate_parent"
    contract_by_name[guard] = (
        *contract_by_name[guard][:-1],
        _body_digest(_canonical_copy_guard_bodies(catalog_schema, preparation_schema)[1], catalog_schema),
    )
    return contract_by_name


def _replay_resolution_spec(spec, schema, body):
    from db.migration_cms_replay_resolution import replay_guard_bodies

    _require(body in replay_guard_bodies(schema), "definition_changed")
    return (*spec[:-1], _body_digest(body, schema))


def require_cms_terminal_guard_resolution(routines, *, catalog_schema, catalog_schema_oid, preparation_schema):
    """CMS terminal execution needs exact dispatch and receipt closure, beyond compatibility.

    The caller authenticates this complete routine inventory and retains owner,
    writer and creator closure. This requirement neither installs the variants
    nor permits schema CREATE; general allocation/readback compatibility is unchanged.
    """
    from db.migration_cms_replay_resolution import GUARD, replay_guard_bodies
    from db.migration_cms_retirement_resolution import retirement_resolution_bodies
    from db.migration_cms_snapshot_allocator import generic_finalization_bodies
    from db.migration_cms_terminal_resolution import GUARD as terminal_guard
    from db.migration_cms_terminal_resolution import terminal_guard_bodies
    from db.migration_generic_source_candidates import _cms_canonical_copy_guard_body
    from process.provider_directory_cms_transition_routines import _contracts as transition_contracts

    bodies_by_name = {
        "guard_pd_generic_dataset_finalization": generic_finalization_bodies(catalog_schema)[1],
        GUARD: replay_guard_bodies(catalog_schema)[1],
        terminal_guard: terminal_guard_bodies(catalog_schema)[1],
        "guard_pd_dataset_candidate_parent": _cms_canonical_copy_guard_body(catalog_schema, preparation_schema),
    }
    contracts = transition_contracts(catalog_schema, preparation_schema)
    retirement = retirement_resolution_bodies(catalog_schema)
    bodies_by_name.update({name: typed for name, (_old, typed) in retirement.items()})
    verified_names = set()
    for routine in routines:
        native = _decoded(routine["native"])
        definition = _decoded(native["definition"])
        name = definition["proname"]
        if name not in bodies_by_name:
            continue
        _require(
            name not in verified_names and definition["prosrc"] == bodies_by_name[name],
            "terminal_guard_resolution_required",
        )
        spec = (*contracts[name][:-1], _body_digest(bodies_by_name[name], catalog_schema))
        _require_cms_callee_body(
            {"definition": definition, "language": native["language"]["lanname"]},
            spec,
            catalog_schema,
            catalog_schema_oid,
        )
        verified_names.add(name)
    _require(verified_names == bodies_by_name.keys(), "terminal_guard_resolution_required")


def require_original_cms_allocation_callees(
    function_rows, *, catalog_schema, catalog_schema_oid, preparation_schema, expected_functions, source_metadata
):
    """Compare exact source bodies with protected CMS pins and SourceV2 metadata.

    Fresh observations provide no authority; allocation freeze retains mutable owners.
    """
    from process.provider_directory_generic_source_release_checks import _finalization_spec
    from process.provider_directory_source_callable_custody import _TYPE_OIDS as source_types
    from process.provider_directory_source_callable_custody import _require_definition as require_source_definition
    from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS

    fields = ("definition", "language", "acl")
    contract_by_name = _original_callee_contracts(catalog_schema, preparation_schema)
    _require(
        type(expected_functions) is dict
        and _cms_callee_contracts().keys() <= expected_functions.keys() <= contract_by_name.keys()
        and type(source_metadata) is dict
        and set(source_metadata)
        == {signature.replace("character varying", "varchar") for signature in METADATA_FUNCTIONS},
        "original_allocation_pins_required",
    )
    metadata_by_name = {signature.split("(")[0]: entry for signature, entry in source_metadata.items()}
    verified_by_name = {}
    for function_row in function_rows:
        entry_by_field = {
            field: function_row[field] if field == "language" else _decoded(function_row[field]) for field in fields
        }
        definition = entry_by_field["definition"]
        name = definition["proname"]
        _require(name in expected_functions and name not in verified_by_name, "function_set_changed")
        expected_by_field = {field: expected_functions[name][field] for field in fields}
        _require(entry_by_field == expected_by_field, "original_allocation_pin_changed")
        if name in metadata_by_name:
            _require(
                entry_by_field == {field: metadata_by_name[name][field] for field in fields},
                "original_source_metadata_changed",
            )
        spec = contract_by_name[name]
        if name == "guard_pd_dataset_candidate_parent":
            spec = _canonical_guard_spec(spec, catalog_schema, preparation_schema, definition["prosrc"])
        if name == "guard_pd_generic_dataset_finalization":
            spec = _finalization_spec(spec, catalog_schema, definition["prosrc"])
        if name == "guard_pd_endpoint_dataset_subset_replay_evidence":
            spec = _replay_resolution_spec(spec, catalog_schema, definition["prosrc"])
        require_source_definition(
            _shared_callee_definition_entry(entry_by_field),
            spec,
            catalog_schema,
            catalog_schema_oid,
            {**source_types, "text[]": "1009"},
            storage_identifier(catalog_schema),
        )
        verified_by_name[name] = entry_by_field
    _require(verified_by_name.keys() == expected_functions.keys(), "function_set_changed")
    _require(
        len({entry["definition"]["oid"] for entry in verified_by_name.values()}) == len(verified_by_name),
        "function_set_changed",
    )
    return verified_by_name


def _require_definition(entry, schema, namespace):
    definition = entry["definition"]
    spec = _CONTRACTS.get(definition["proname"])
    _require(spec is not None, "function_set_changed")
    arguments, returns, language, volatility, body_hash = spec
    expected_arguments = [str(_TYPE_OIDS[kind]) for _, kind in arguments]
    _require(
        int(definition["pronamespace"]) == namespace
        and definition["prokind"] == "f"
        and entry["language"] == language
        and int(definition["prorettype"]) == _TYPE_OIDS[returns]
        and int(definition["pronargs"]) == len(arguments)
        and definition["proargtypes"] == expected_arguments
        and definition["proargnames"] == ([name for name, _ in arguments] or None)
        and definition["proargmodes"] is None
        and definition["proallargtypes"] is None
        and definition["proargdefaults"] is None
        and int(definition["pronargdefaults"]) == int(definition["provariadic"]) == 0
        and definition["prosecdef"] is False
        and definition["proleakproof"] is False
        and definition["proisstrict"] is False
        and definition["proretset"] is False
        and definition["prosupport"] == "-"
        and definition["probin"] is None
        and definition["prosqlbody"] is None
        and definition["protrftypes"] is None
        and definition["procost"] == 100
        and definition["prorows"] == 0
        and definition["provolatile"] == volatility
        and definition["proparallel"] == "u"
        and definition["proconfig"] in approved_cms_routine_settings(["search_path=pg_catalog"])
        and _body_digest(definition["prosrc"], schema) == body_hash,
        "definition_changed",
    )
    owner = int(definition["proowner"])
    _require(
        sorted(entry["acl"]) == sorted([[0, owner, "EXECUTE", False], [owner, owner, "EXECUTE", False]]),
        "acl_changed",
    )


async def capture_cms_callable_custody(connection, policy, *, legacy=False):
    """Attest only the fixed existing callables, never infer authority from their names."""
    _require(type(policy) is GenericSourcePolicy and type(legacy) is bool, "policy_required")
    owner, _ = await storage_roles(connection, policy.schema, **policy.role_arguments())
    graph = _decoded(await connection.fetchval(_ROLE_GRAPH_SQL))
    role_oid_by_name = {role["rolname"]: int(role["oid"]) for role in graph["roles"]}
    publisher = role_oid_by_name[policy.publisher_role]
    allowed_owners = {owner, *(role_oid_by_name[name] for name in policy.control_runtime_roles)} if legacy else {owner}
    namespace = await connection.fetchval("SELECT to_regnamespace($1)::oid::bigint", policy.schema)
    signatures = _signatures(policy.schema)
    function_rows = await connection.fetch(
        _FUNCTION_SQL, list(signatures), policy.schema, list(CMS_CALLABLES), list(policy.runtime_roles)
    )
    _require(
        len(function_rows) == len(signatures)
        and {function_row["signature"] for function_row in function_rows} == set(signatures),
        "function_set_changed",
    )
    function_by_name = {}
    for native_row in function_rows:
        function_by_field = {
            key: _decoded(native_row[key]) if key != "language" and key != "signature" else native_row[key]
            for key in native_row.keys()
        }
        _require_definition(function_by_field, policy.schema, namespace)
        _require(int(function_by_field["definition"]["proowner"]) in allowed_owners, "owner_changed")
        if not legacy:
            _require(
                all(
                    superuser or not ordinary or role in {owner, publisher} or not (member or can_set)
                    for role, superuser, ordinary, member, can_set, _ in function_by_field["access"]
                ),
                "replacement_authority",
            )
        function_by_name[function_by_field["definition"]["proname"]] = function_by_field
    return {"schema_oid": namespace, "owner_oid": owner, "role_graph": graph, "functions": function_by_name}


def require_cms_callable_adoption_delta(before, after):
    """Allow only owner and its native default-ACL rewrite, retaining every OID/body/right."""
    _require(
        before["schema_oid"] == after["schema_oid"]
        and before["owner_oid"] == after["owner_oid"]
        and before["role_graph"] == after["role_graph"]
        and before["functions"].keys() == after["functions"].keys() == _CONTRACTS.keys(),
        "adoption_scope_changed",
    )
    owner = after["owner_oid"]
    for name, previous in before["functions"].items():
        current = after["functions"][name]
        expected = deepcopy(previous["definition"])
        expected["proowner"] = str(owner) if type(expected["proowner"]) is str else owner
        # ALTER OWNER rewrites explicit owner ACL entries; NULL stays NULL.
        if expected["proacl"] is None:
            _require(current["definition"]["proacl"] is None, "adoption_acl_changed")
        else:
            expected["proacl"] = current["definition"]["proacl"]
        _require(
            expected == current["definition"] and previous["signature"] == current["signature"], "adoption_changed"
        )
        prior_acl_owner = int(previous["definition"]["proowner"])
        expected_acl_rows = [
            [
                owner if grantee == prior_acl_owner else grantee,
                owner if grantor == prior_acl_owner else grantor,
                privilege,
                grantable,
            ]
            for grantee, grantor, privilege, grantable in previous["acl"]
        ]
        _require(sorted(expected_acl_rows) == sorted(current["acl"]), "adoption_acl_changed")
        previous_execute_by_role = {access[0]: access[-1] for access in previous["access"] if access[0] != owner}
        current_execute_by_role = {access[0]: access[-1] for access in current["access"] if access[0] != owner}
        _require(previous_execute_by_role == current_execute_by_role, "ordinary_execute_changed")


async def _admin_identity(connection, policy):
    actor_by_field = dict(
        await connection.fetchrow(
            "SELECT session_user::text AS session,current_user::text AS current,pg_backend_pid() AS backend,"
            "pg_current_xact_id()::text AS xid,(SELECT rolsuper FROM pg_roles WHERE rolname=current_user) AS native_admin"
        )
    )
    _require(
        actor_by_field["native_admin"] is True
        and actor_by_field["session"] == actor_by_field["current"]
        and actor_by_field["current"] not in (policy.owner_role, policy.publisher_role, *policy.runtime_roles),
        "independent_admin_required",
    )
    return actor_by_field


async def provision_cms_callable_custody(connection, policy, *, verify_role_profile):
    """Use the supplied admin transaction/savepoint; this never activates final publication."""
    _require(
        type(policy) is GenericSourcePolicy
        and connection.is_in_transaction() is True
        and callable(verify_role_profile),
        "explicit_admin_required",
    )
    task, actor = asyncio.current_task(), await _admin_identity(connection, policy)
    async with connection.transaction():
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        before = await capture_cms_callable_custody(connection, policy, legacy=True)
        for name, entry in sorted(before["functions"].items(), key=lambda item: int(item[1]["definition"]["oid"])):
            if int(entry["definition"]["proowner"]) != before["owner_oid"]:
                await connection.execute(
                    f"ALTER FUNCTION {entry['signature']} OWNER TO {storage_identifier(policy.owner_role)}"
                )
        after = await capture_cms_callable_custody(connection, policy)
        require_cms_callable_adoption_delta(before, after)
        await verify_role_profile(connection, policy.schema, **policy.role_arguments())
        _require(
            asyncio.current_task() is task and await _admin_identity(connection, policy) == actor,
            "admin_transaction_changed",
        )
        return after
