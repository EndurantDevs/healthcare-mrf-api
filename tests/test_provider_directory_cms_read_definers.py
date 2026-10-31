# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Real source constructors and protected Source-shaped read-definer boundaries."""

from copy import deepcopy
from itertools import combinations
from types import SimpleNamespace

import pytest

from process import provider_directory_cms_dependency_policy as dependencies
from process import provider_directory_cms_read_definers as readers
from process import provider_directory_source_callable_custody as source
from process.provider_directory_cms_routine_settings import approved_cms_routine_settings
from tests.test_acquisition_validator_argument_types_migration import _current_functions as acquisition_functions
from tests.test_acquisition_validator_argument_types_migration import _migration as acquisition_migration
from tests.test_provider_directory_cms_callee_inventory import _source_metadata
from tests.test_provider_directory_cms_proof_callees import source_case as source_case
from tests.test_provider_directory_cms_transition_routines import _native_foreign, _raw
from tests.test_provider_directory_source_callable_custody import _entry
from tests.test_source_validator_argument_types_migration import _authentic_functions as source_functions
from tests.test_source_validator_argument_types_migration import _migration as source_migration

_SCHEMA = "snapshot_schema"
_OWNER = 104
_ACTORS = (103, 201, 301, 302)
_NAMES = tuple(
    name
    for name, arguments in dependencies._CALLBACK_CALLEES.items()
    if source._CONTRACTS[name + "(" + ",".join(arguments) + ")"][7]
)
_PUBLIC = "provider_directory_uhc_flex_practitioner_dataset_ready"


def _callback_rows(schema):
    acquisition_by_identity = acquisition_functions()
    source_by_name = source_functions()
    callbacks = []
    for oid, name in enumerate(_NAMES, 21000):
        signature = name + "(text)"
        if (name, "text") in acquisition_by_identity:
            body = acquisition_by_identity[name, "text"][-1]
            repairs = acquisition_migration()._calls(_SCHEMA)
        else:
            body = source_by_name[name][-1]
            repairs = source_migration()._calls(_SCHEMA)
        for caller, old, new, count in repairs:
            if caller == name:
                assert body.count(old) == count
                body = body.replace(old, new)
        entry = _entry(signature, owner=_OWNER)
        entry["definition"].update(oid=str(oid), prosrc=body.replace(_SCHEMA, schema))
        entry["acl"].extend([actor, _OWNER, "EXECUTE", False] for actor in _ACTORS)
        callbacks.append(entry)
    return callbacks


def _read_case(source_case, schema=_SCHEMA):
    """Reusable offline real-body fixture; no database observations or authority."""
    case = deepcopy(source_case)
    entries = [*case.rows, *_callback_rows(schema)]
    routines = []
    for entry in entries:
        entry["definition"]["prosrc"] = entry["definition"]["prosrc"].replace(_SCHEMA, schema)
        legacy = entry["definition"]["proconfig"]
        entry["definition"]["proconfig"] = approved_cms_routine_settings(legacy)[1]
        routine = _raw(entry, schema)
        routine["native"].update(acl=deepcopy(entry["acl"]), namespace_identifier=schema, dependencies=[])
        routines.append(routine)
    functions = [
        dict(
            definition=routine["native"]["definition"],
            language=routine["native"]["language"]["lanname"],
            acl=routine["native"]["acl"],
        )
        for routine in routines
    ]
    case.document["storage_custody"]["canonical_authority"]["metadata"] = _source_metadata(functions)
    return SimpleNamespace(
        routines=routines,
        document=case.document,
        catalog_schema=schema,
        catalog_schema_oid=42,
        preparation_schema="preparation_schema",
        namespace_owner_oid=_OWNER,
    )


@pytest.fixture
def read_case(source_case):
    return _read_case(source_case)


def _verify(case):
    return readers.require_cms_read_definers(
        case.routines,
        catalog_schema=case.catalog_schema,
        catalog_schema_oid=case.catalog_schema_oid,
        preparation_schema=case.preparation_schema,
        source_document=case.document,
        namespace_owner_oid=case.namespace_owner_oid,
    )


def _read_row(case, name=_NAMES[0]):
    return next(routine for routine in case.routines if routine["native"]["definition"]["proname"] == name)


@pytest.mark.parametrize("schema", [_SCHEMA, "catalog", "Mixed_Catalog"])
def test_exact_source_definers_are_separate_and_immutable(source_case, schema):
    case = _read_case(source_case, schema)
    before = deepcopy(case)
    assert _verify(case) == list(range(21000, 21011))
    assert case == before
    assert readers._read_names() == set(_NAMES)
    assert readers._read_names() & source._PUBLIC_NAMES == {_PUBLIC}


@pytest.mark.parametrize("name", _NAMES)
def test_every_source_definer_is_required(read_case, name):
    read_case.routines.remove(_read_row(read_case, name))
    with pytest.raises(ValueError, match="function_set_changed"):
        _verify(read_case)


@pytest.mark.parametrize("count", range(5))
def test_existing_bound_actor_subsets_are_optional(read_case, count):
    for actors in combinations(_ACTORS, count):
        routine = _read_row(read_case)
        routine["native"]["acl"] = [[actor, _OWNER, "EXECUTE", False] for actor in (_OWNER, *actors)]
        assert len(_verify(read_case)) == 11


@pytest.mark.parametrize("name", _NAMES)
def test_public_is_limited_to_source_approved_existing_edge(read_case, name):
    routine = _read_row(read_case, name)
    routine["native"]["acl"] = [[_OWNER, _OWNER, "EXECUTE", False], [0, _OWNER, "EXECUTE", False]]
    if name == _PUBLIC:
        assert len(_verify(read_case)) == 11
        routine["native"]["acl"].pop()
        assert len(_verify(read_case)) == 11
    else:
        with pytest.raises(ValueError, match="acl_changed"):
            _verify(read_case)


@pytest.mark.parametrize("fault", ["missing", "duplicate", "unknown", "grantor", "grantable", "right", "bool"])
def test_acl_changes_refuse_even_with_current_source_bodies(read_case, fault):
    acl = _read_row(read_case)["native"]["acl"]
    match fault:
        case "missing":
            acl.pop(0)
        case "duplicate":
            acl.append(deepcopy(acl[0]))
        case "unknown":
            acl[1][0] = 999
        case "grantor":
            acl[1][1] = 103
        case "grantable":
            acl[1][3] = True
        case "right":
            acl[1][2] = "USAGE"
        case "bool":
            acl[1][0] = True
    with pytest.raises(ValueError, match="acl_changed"):
        _verify(read_case)


@pytest.mark.parametrize(
    "field,value",
    [
        ("proowner", "103"),
        ("prosrc", "SELECT true"),
        ("proargtypes", ["1043"]),
        ("prosecdef", False),
        ("provolatile", "v"),
        ("proconfig", ["search_path=pg_catalog"]),
    ],
)
def test_owner_body_abi_and_target_settings_remain_strict(read_case, field, value):
    _read_row(read_case)["native"]["definition"][field] = value
    with pytest.raises(ValueError, match="owner_changed|definition_changed|terminal_settings_required"):
        _verify(read_case)


@pytest.mark.parametrize(
    "fault", ["namespace", "namespace_owner", "short_namespace", "name", "duplicate", "oid", "language"]
)
def test_native_identity_and_namespace_remain_strict(read_case, fault):
    routine = _read_row(read_case)
    match fault:
        case "namespace":
            routine["native"]["namespace"][0] = 999
        case "namespace_owner":
            routine["native"]["namespace"][2] = 999
        case "short_namespace":
            routine["native"]["namespace"] = routine["native"]["namespace"][:2]
        case "name":
            routine["native"]["definition"]["proname"] = "unrecognized"
        case "duplicate":
            read_case.routines.append(deepcopy(routine))
        case "oid":
            routine["oid"] += 1
        case "language":
            routine["native"]["language"]["lanname"] = "plpgsql"
    with pytest.raises(ValueError):
        _verify(read_case)


@pytest.mark.parametrize("field", ["prosrc", "oid", "language", "acl"])
def test_genuine_source5_metadata_must_equal_native(read_case, field):
    metadata = next(iter(read_case.document["storage_custody"]["canonical_authority"]["metadata"].values()))
    if field == "language":
        metadata[field] = "unbound"
    elif field == "acl":
        metadata[field].append([999, _OWNER, "EXECUTE", False])
    else:
        metadata["definition"][field] = "unbound"
    with pytest.raises(ValueError, match="source_metadata_changed"):
        _verify(read_case)


@pytest.mark.parametrize("fault", ["version", "missing", "login", "storage_login", "role_oid", "caller", "coordinates"])
def test_source_actor_and_nologin_owner_binding_is_required(read_case, fault):
    document = read_case.document
    match fault:
        case "version":
            document["version"] = 1
        case "missing":
            document.pop("role_graph")
        case "login":
            document["role_graph"]["roles"][0]["rolcanlogin"] = True
        case "storage_login":
            document["storage_custody"]["roles"]["namespace_owner"]["rolcanlogin"] = True
        case "role_oid":
            document["storage_custody"]["roles"]["publication_owner"]["oid"] = 999
        case "caller":
            document["role_graph"]["roles"][1]["rolcanlogin"] = False
        case "coordinates":
            document["storage_custody"]["coordinates"]["namespace_owner_oid"] = 999
    with pytest.raises(ValueError):
        _verify(read_case)


def test_missing_native_source5_never_binds_metadata_caller(read_case):
    signature = next(iter(read_case.document["storage_custody"]["canonical_authority"]["metadata"]))
    name = signature.split("(")[0]
    read_case.routines = [
        routine for routine in read_case.routines if routine["native"]["definition"]["proname"] != name
    ]
    with pytest.raises(ValueError, match="source_metadata_required"):
        _verify(read_case)


@pytest.mark.parametrize("field,value", [(1, "trigger"), (2, (("extra", "text"),)), (3, "internal"), (4, "v")])
def test_source_frontier_drift_requires_review(monkeypatch, field, value):
    signature = _NAMES[0] + "(text)"
    specification_fields = list(source._CONTRACTS[signature])
    specification_fields[field] = value
    monkeypatch.setitem(source._CONTRACTS, signature, tuple(specification_fields))
    with pytest.raises(ValueError, match="source_contract_changed"):
        readers._read_names()


def test_unrelated_fixed_definers_and_native_ri_are_not_exempt(read_case):
    native = _native_foreign("RI_FKey_check_ins")
    read_case.routines.append(native)
    assert _verify(read_case) == list(range(21000, 21011))
    assert len(read_case.routines) == 44
