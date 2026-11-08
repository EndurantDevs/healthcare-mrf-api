# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Check source definitions in an already authenticated common-transition inventory.

The caller supplies complete protected routine custody and retains owner, ACL,
writer, namespace and transaction fences. This comparison grants no authority,
performs no SQL and cannot establish that the supplied inventory is complete.
"""

from db.migration_cms_retirement_resolution import ORIGINAL_SHA256
from db.plan_release_storage import storage_identifier
from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_preparation_custody as custody
from process import provider_directory_source_callable_custody as source

# Exact raw prosrc from the latest migration constructors; tests regenerate each.
_INVOKER_BODIES = {
    "guard_cms_npd_serving_evidence": "d334317c76f8240305b26dcdbe01b5bf2069f5eb16b4b3d67e23a1950b32b704",
    "guard_cms_npd_covered_network_witness": "7b28324d02a7f1969a772d4116fe9dea4b5b38ba37442e906f69e66796718259",
    "guard_cms_npd_serving_coverage": "fabe0d29c071f95352ae7560ac0fedb3627d7bf61c44f4c8d56f8ef26b4de009",
    "guard_cms_npd_resource_witness_insert": "b41710e3f7d5a0f3d147d2da6a4235220e6746fe2e320d60d45dfe5f587d4efa",
    "guard_cms_npd_resource_witness": "54132ea34cd56486ccc670fb5408c2c5103ce3f95ba0b49b8661698b2c0b98d9",
    "guard_cms_npd_published_relationship_insert": "b69e8929a575a65783e772e8a65fe244f2cc20464738979937f182d96fda0b02",
    "guard_cms_npd_published_relationship": "81fd940f1f3cb54f047db2c478fa80c83759bb77e1627be46d2c0b73b5052d1e",
    "guard_cms_npd_relationship_truncate": "04b2d4635d8d173e2408ca68a5629f52d8bfbcf1dc9d426ae8945513931a07be",
    "guard_cms_npd_relationship_receipt_insert": "770126350044d7affdb9ac2af9e061cdf90544185bcc24a01091759fd7425826",
    "guard_cms_npd_relationship_receipt_mutation": "e689c76cb666b98404188671b465486c1b0488bc80c1a86aeb693c83c32475d2",
    "pd_entity_redirect_parent_guard": "9cee31bd2dad0119880f387bf8700d83c920757a7be27de2a3f893d7cbc3deaa",
    "guard_cms_npd_candidate_coverage_insert": "01c9f84a6118b591fc6b897cdab53f4d9459793e0cd2d6efd12491bef02f98d3",
}
_RUN_RETIRED_BODY = "66098f64097f25435634fe2944b8e7fa36d617c0eda93dae2683cf9057cfc75c"
# The deferred checkpoint dispatcher is retained by both later disposition migrations.
_CHECKPOINT_GUARD_BODY = "4765a340dd3708d705171a282751c2d537d7c09dcbd17c6fbbe11187b3e1997c"
# The same twelve internal RI functions accepted by the existing native checker.
_NATIVE_FOREIGN_OIDS = {
    "RI_FKey_check_ins": 1644,
    "RI_FKey_check_upd": 1645,
    "RI_FKey_cascade_del": 1646,
    "RI_FKey_cascade_upd": 1647,
    "RI_FKey_restrict_del": 1648,
    "RI_FKey_restrict_upd": 1649,
    "RI_FKey_setnull_del": 1650,
    "RI_FKey_setnull_upd": 1651,
    "RI_FKey_setdefault_del": 1652,
    "RI_FKey_setdefault_upd": 1653,
    "RI_FKey_noaction_del": 1654,
    "RI_FKey_noaction_upd": 1655,
}


def _require(condition, code):
    if not condition:
        raise ValueError("cms_transition_routine_" + code)


def _contracts(schema, preparation_schema):
    specs = callables._original_callee_contracts(schema, preparation_schema)
    specs.update(
        {name: ((), "trigger", (), "plpgsql", "v", False, "u", False, body) for name, body in _INVOKER_BODIES.items()}
    )
    for name in (
        "provider_directory_terminal_root_retirement_valid",
        "provider_directory_terminal_root_retirement_v2_valid",
    ):
        specs[name] = (
            (("candidate_dataset_id", "text"),),
            "boolean",
            (),
            "sql",
            "s",
            False,
            "u",
            True,
            ORIGINAL_SHA256[name],
        )
    name = "guard_provider_directory_terminal_root_retirement_run"
    specs[name] = ((), "trigger", (), "plpgsql", "v", False, "u", True, ORIGINAL_SHA256[name])
    specs["provider_directory_terminal_root_run_retired"] = (
        (("candidate_run_id", "text"),),
        "boolean",
        (),
        "sql",
        "s",
        False,
        "u",
        True,
        _RUN_RETIRED_BODY,
    )
    specs["guard_provider_directory_subset_abandonment_checkpoint"] = (
        (),
        "trigger",
        (),
        "plpgsql",
        "v",
        False,
        "u",
        True,
        _CHECKPOINT_GUARD_BODY,
    )
    return specs


def _require_native_foreign(definition, language):
    name = definition["proname"]
    expected_by_field = dict(
        oid=str(_NATIVE_FOREIGN_OIDS.get(name)),
        pronamespace="11",
        proowner="10",
        prolang="12",
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
        prosecdef=False,
        proleakproof=False,
        proisstrict=True,
        proretset=False,
        prosupport="-",
        probin=None,
        prosqlbody=None,
        protrftypes=None,
        procost=1,
        prorows=0,
        provolatile="v",
        proparallel="s",
        proconfig=None,
        prosrc=name,
    )
    _require(
        name in _NATIVE_FOREIGN_OIDS
        and language == "internal"
        and all(definition.get(field) == expected_value for field, expected_value in expected_by_field.items()),
        "native_foreign_function_changed",
    )


def _variant_spec(spec, definition, schema, preparation_schema):
    from process.provider_directory_generic_source_release_checks import _finalization_spec

    name, body = definition["proname"], definition["prosrc"]
    if name == "guard_pd_dataset_candidate_parent":
        return callables._canonical_guard_spec(spec, schema, preparation_schema, body)
    if name == "guard_pd_generic_dataset_finalization":
        return _finalization_spec(spec, schema, body)
    if name == "guard_pd_endpoint_dataset_subset_replay_evidence":
        return callables._replay_resolution_spec(spec, schema, body)
    return spec


def _require_source_definition(entry, spec, schema, namespace, preparation_schema, *, namespace_identifier=None):
    definition = entry["definition"]
    spec = _variant_spec(spec, definition, schema, preparation_schema)
    checked_by_field = callables._shared_callee_definition_entry(entry)
    if definition["proname"] == "guard_cms_npd_relationship_truncate":
        _require(definition["proconfig"] is None, "definition_changed")
        checked_by_field = {**entry, "definition": {**definition, "proconfig": ["search_path=pg_catalog"]}}
    type_oid_by_kind = {**source._TYPE_OIDS, "text[]": "1009"}
    _require(
        all(kind in type_oid_by_kind for _, kind in (*spec[0], *spec[2])) and spec[1] in type_oid_by_kind,
        "type_contract_required",
    )
    quoted_schema = storage_identifier(schema)
    if definition["proname"] in {"ptg_wave_canonical_json_ascii_v1", "ptg_wave_json_ascii_text_v1"}:
        _require(
            namespace_identifier == quoted_schema or (namespace_identifier == schema and schema == schema.lower()),
            "namespace_identifier_changed",
        )
        quoted_schema = namespace_identifier
    source._require_definition(checked_by_field, spec, schema, namespace, type_oid_by_kind, quoted_schema)


def require_cms_transition_routine_definitions(routines, *, catalog_schema, catalog_schema_oid, preparation_schema):
    """Verify every supplied definition; protected inventory/rights remain caller-owned.

    Unknown signatures and overloads refuse. The optional composite-argument
    witness validator requires its separate existing type contract and refuses
    here; no argument OID is promoted from a fresh observation into policy.
    """
    storage_identifier(catalog_schema)
    storage_identifier(preparation_schema)
    _require(type(catalog_schema_oid) is int and 0 < catalog_schema_oid < 2**32, "namespace_required")
    _require(type(routines) is list and bool(routines), "inventory_required")
    specs = _contracts(catalog_schema, preparation_schema)
    names, oids = set(), set()
    for routine in routines:
        native = custody._decoded(routine["native"])
        definition = custody._decoded(native["definition"])
        language = custody._decoded(native["language"])
        namespace = custody._decoded(native["namespace"])
        name, oid = definition["proname"], routine["oid"]
        _require(
            type(oid) is int
            and 0 < oid < 2**32
            and definition["oid"] == str(oid)
            and oid not in oids
            and name not in names,
            "identity_changed",
        )
        _require(definition["prolang"] == language["oid"], "language_changed")
        if namespace[:2] == [11, "pg_catalog"]:
            _require_native_foreign(definition, language["lanname"])
        else:
            _require(namespace[:2] == [catalog_schema_oid, catalog_schema] and name in specs, "function_set_changed")
            _require_source_definition(
                dict(definition=definition, language=language["lanname"]),
                specs[name],
                catalog_schema,
                catalog_schema_oid,
                preparation_schema,
                namespace_identifier=native.get("namespace_identifier"),
            )
        names.add(name)
        oids.add(oid)
