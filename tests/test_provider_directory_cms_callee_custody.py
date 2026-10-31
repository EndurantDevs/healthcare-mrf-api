# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-bound CMS callee contracts; catalog transport is synthetic and read-only."""

import json
import re
from copy import deepcopy
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.dialects.postgresql.asyncpg import dialect

from db import migration_generic_source_candidates as candidates
from db import migration_provider_directory_first_acquisition_retirement as first_retirement
from db import migration_provider_directory_first_retirement_owner_lineage as owner_lineage
from db import migration_provider_directory_terminal_root_retirement_evidence as evidence
from db import migration_provider_directory_terminal_root_retirement_guards as retirement
from db import migration_provider_directory_terminal_root_retirement_v2 as retirement_v2
from db.migration_provider_directory_dataset_candidates import candidate_functions
from db.plan_release_storage import storage_relation
from process import provider_directory_cms_callable_custody as custody
from process import provider_directory_source_callable_custody as source_custody
from tests.test_provider_directory_cms_callable_custody import _migration_functions
from tests.test_source_validator_argument_types_migration import _load

_SCHEMA = "snapshot_schema"
_PREPARATION_SCHEMA = "preparation_schema"
_SOURCE_ROLES = dict(owner=101, publisher=102, builder=103, namespace_owner=104, migration=105)
_EXPECTED_FRONTIER = """
cms_serving_archive_result_matches(jsonb)
cms_serving_native_matches(jsonb)
cms_serving_native_snapshot()
cms_serving_oids(text[])
cms_serving_receipt_matches(jsonb)
cms_serving_require_fresh(text,jsonb)
guard_pd_dataset_candidate_parent()
pd_dataset_candidate_tables(text)
provider_directory_endpoint_dataset_admission_metadata_sha256(jsonb,int2,text,text,varchar[])
provider_directory_rooted_graph_dataset_intrinsic_valid(text)
provider_directory_subset_abandonment_valid(text)
provider_directory_subset_canonical_sha256(jsonb)
provider_directory_subset_completion_canonical_json(jsonb)
provider_directory_subset_completion_proof_pair_valid(jsonb,text)
provider_directory_subset_completion_proof_shape_valid(jsonb,text,int8)
provider_directory_subset_content_proof_valid(jsonb,text,text,text,jsonb,jsonb,text,int8,jsonb,jsonb)
provider_directory_subset_coverage_shape_valid(jsonb,jsonb,text,text)
provider_directory_subset_payload_canonical_json(jsonb)
provider_directory_subset_payload_sha256(jsonb)
provider_directory_subset_replay_evidence_shape_valid(jsonb,text,jsonb,text)
provider_directory_subset_terminal_disposition_v4_valid(text)
provider_directory_subset_terminal_disposition_v5_valid(text)
provider_directory_subset_terminal_disposition_valid(text)
provider_directory_terminal_root_retirement_eligible(text,int4)
provider_directory_terminal_root_retirement_evidence(text)
provider_directory_terminal_root_retirement_marker_valid(text,jsonb)
provider_directory_terminal_root_retirement_relation_evidence(text,text)
provider_directory_terminal_root_retirement_v2_eligible(text,int4)
provider_directory_terminal_root_retirement_v2_evidence(text)
provider_directory_terminal_root_retirement_v2_marker_valid(text,jsonb)
provider_directory_uhc_flex_practitioner_dataset_valid(text)
validate_pd_dataset_bulk_relationships(text,text)
""".split()


def _source_statements(schema, subset):
    seal = _load("20260812020000_provider_directory_endpoint_dataset_admission_seal.py")
    rooted = _load("20260909120000_fhir_request_failure_budget.py")
    flex = _load("20260830090000_uhc_flex_retry_exhaustion.py")
    latest = _load("20260814000000_provider_directory_reviewed_subset_terminal_tail_tolerance.py")
    return (
        subset._canonical_sha256_function_sql(schema),
        subset._canonical_json_function_sql(schema),
        subset._proof_pair_valid_function_sql(schema),
        latest._proof_shape_valid_function_sql(schema, tail_tolerant=True),
        subset._content_proof_valid_function_sql(schema, replace_existing=True),
        subset._payload_canonical_json_function_sql(schema),
        subset._payload_sha256_function_sql(schema),
        seal._digest_function_sql(schema),
        rooted._intrinsic_valid_sql(schema),
        flex._publication_valid_sql(schema, partial=True),
    )


def _remaining_statements(schema, subset):
    abandonment = _load("20260809000000_provider_directory_subset_abandonment.py")
    direct = _load("20260810110000_provider_directory_reviewed_subset_direct_v4_disposition.py")
    latest = _load("20260811120000_provider_directory_reviewed_subset_v5_http410_disposition.py")
    return (
        subset._replay_shape_valid_function_sql(schema),
        subset._coverage_shape_valid_function_sql(schema, replace_existing=True, reviewed_root_policy_aware=True),
        abandonment._valid_function_sql(schema),
        direct._direct_valid_sql(schema),
        latest._v5_valid_sql(schema),
        latest._shared_valid_sql(schema),
        retirement.marker_function_sql(schema),
        *(current for _previous, current in owner_lineage.replacements(schema)),
        evidence.relation_evidence_function_sql(schema),
        retirement_v2.eligible_function_sql(schema),
        retirement_v2.marker_function_sql(schema),
        retirement_v2.evidence_function_sql(schema),
    )


def _parse_statements(statements, schema):
    function_by_name = {}
    pattern = re.compile(
        rf'CREATE(?: OR REPLACE)? FUNCTION "{schema}"\."?(\w+)"?\((.*?)\)\s+'
        r"RETURNS\s+(\w+(?:\[\])?)\s+LANGUAGE\s+(\w+)(.*?)AS (\$\w*\$)(.*?)\6",
        re.S,
    )
    for statement in statements:
        compiled = text(statement).compile(dialect=dialect())
        assert not compiled.params
        matches = pattern.findall(str(compiled))
        assert matches
        for name, arguments, returns, language, options, _tag, body in matches:
            args = tuple(tuple(arg.strip().split()) for arg in arguments.split(",")) if arguments.strip() else ()
            settings = re.findall(r"\bSET\s+(\w+)\s*=\s*('[^']*'|[a-z_]+)(?=\s|$)", options)
            assert len(settings) == len(re.findall(r"\bSET\b", options))
            function_by_name[name] = (
                args,
                returns,
                (),
                language,
                "i" if "IMMUTABLE" in options else "s" if "STABLE" in options else "v",
                "STRICT" in options,
                "s" if "PARALLEL SAFE" in options else "u",
                "SECURITY DEFINER" in options,
                [setting + "=" + value.strip("'") for setting, value in settings] or None,
                body,
            )
    return function_by_name


def _authentic_functions(schema):
    subset = _load("20260808190000_provider_directory_subset_completion_proof.py")
    function_by_name = _parse_statements(
        (*_source_statements(schema, subset), *_remaining_statements(schema, subset), candidate_functions(schema)),
        schema,
    )
    repair = _load("20261016040000_source_validator_argument_types.py")
    for name, old, new, count in repair._calls(schema):
        if name in function_by_name:
            *attributes, body = function_by_name[name]
            assert body.count(old) == count
            function_by_name[name] = (*attributes, body.replace(old, new))
    for name, (arguments, returns, language, volatility, body) in _migration_functions().items():
        function_by_name[name] = (
            arguments,
            returns,
            (),
            language,
            volatility,
            False,
            "u",
            False,
            ["search_path=pg_catalog"],
            body.replace(_SCHEMA, schema),
        )
    guard = "guard_pd_dataset_candidate_parent"
    function_by_name[guard] = (
        *function_by_name[guard][:-1],
        candidates._canonical_copy_guard_bodies(schema, _PREPARATION_SCHEMA)[1],
    )
    names = {signature.split("(")[0] for signature in _EXPECTED_FRONTIER}
    return {name: spec for name, spec in function_by_name.items() if name in names}


def _catalog_definition(name, spec, owner, oid):
    arguments, returns, _outputs, _language, volatility, strict, parallel, security, settings, body = spec
    oid_by_type = {**source_custody._TYPE_OIDS, "text[]": "1009"}
    return dict(
        oid=str(oid),
        pronamespace="42",
        proname=name,
        proowner=str(owner),
        proacl=None,
        prokind="f",
        prorettype=oid_by_type[returns],
        pronargs=len(arguments),
        proargtypes=[oid_by_type[kind] for _, kind in arguments],
        proargnames=[field for field, _ in arguments] or None,
        proargmodes=None,
        proallargtypes=None,
        proargdefaults=None,
        pronargdefaults=0,
        provariadic="0",
        prosecdef=security,
        proleakproof=False,
        proisstrict=strict,
        proretset=False,
        prosupport="-",
        probin=None,
        prosqlbody=None,
        protrftypes=None,
        procost=100,
        prorows=0,
        provolatile=volatility,
        proparallel=parallel,
        proconfig=settings,
        prosrc=body,
    )


def _catalog_rows(schema):
    function_by_name = _authentic_functions(schema)
    rows = []
    alias_by_type = {"smallint": "int2", "integer": "int4", "bigint": "int8"}
    for oid, (name, spec) in enumerate(sorted(function_by_name.items()), 1000):
        key = name + "(" + ",".join(kind for _, kind in spec[0]) + ")"
        is_source = key in source_custody._CONTRACTS
        owner = 101 if is_source else 107 if name.startswith("cms_serving_") else 105
        acl_rows = [[owner, owner, "EXECUTE", False]]
        if name.startswith("cms_serving_") or name == "provider_directory_uhc_flex_practitioner_dataset_valid":
            acl_rows.append([0, owner, "EXECUTE", False])
        elif is_source:
            acl_rows.append([103, owner, "EXECUTE", False])
        signature = (
            storage_relation(schema, name) + "(" + ",".join(alias_by_type.get(kind, kind) for _, kind in spec[0]) + ")"
        )
        rows.append(
            dict(
                signature=signature,
                definition=_catalog_definition(name, spec, owner, oid),
                language=spec[3],
                acl=acl_rows,
            )
        )
    return rows


def _allocator_row(schema):
    name = "prepare_pd_generic_dataset_storage"
    spec = _parse_statements([candidate_functions(schema)], schema)[name]
    owner = _SOURCE_ROLES["namespace_owner"]
    return dict(
        signature=storage_relation(schema, name) + "()",
        definition=_catalog_definition(name, spec, owner, 2000),
        language=spec[3],
        acl=[[owner, owner, "EXECUTE", False]],
    )


@pytest.fixture(scope="module")
def callee_rows():
    return _catalog_rows(_SCHEMA)


def _verify(rows, **overrides):
    argument_by_name = dict(
        catalog_schema=_SCHEMA,
        catalog_schema_oid=42,
        expected_owner_oids={row["definition"]["proname"]: int(row["definition"]["proowner"]) for row in rows},
        source_roles=dict(_SOURCE_ROLES),
        preparation_schema=_PREPARATION_SCHEMA,
    )
    argument_by_name.update(overrides)
    return custody.require_cms_callee_definitions(rows, **argument_by_name)


def test_frontier_is_exact_and_legacy_adoption_scope_is_unchanged():
    expected_signatures = tuple(
        storage_relation(_SCHEMA, key.split("(")[0]) + "(" + key.split("(")[1] for key in _EXPECTED_FRONTIER
    )
    assert custody.cms_callee_signatures(_SCHEMA) == expected_signatures
    assert len(expected_signatures) == 32
    assert len(custody.CMS_CALLABLES) == 16
    assert set(custody.CMS_CALLABLES) == _migration_functions().keys()


@pytest.mark.parametrize("schema", [_SCHEMA, "alternate_catalog"])
def test_complete_authentic_current_frontier_is_accepted_without_mutation(schema, callee_rows):
    rows = callee_rows if schema == _SCHEMA else _catalog_rows(schema)
    before = deepcopy(rows)
    captured = _verify(rows, catalog_schema=schema)
    assert len(captured) == 32
    assert list(captured) == sorted(captured)
    assert rows == before


@pytest.mark.parametrize("guard_version", ["v1", "v2"])
@pytest.mark.parametrize("additional_execute", [False, True])
def test_exact_guard_and_allocator_compatibility_preserves_frontier(callee_rows, guard_version, additional_execute):
    rows, allocator = deepcopy(callee_rows), _allocator_row(_SCHEMA)
    guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    if guard_version == "v2":
        guard["definition"]["prosrc"] = candidates._cms_canonical_copy_guard_body(_SCHEMA, _PREPARATION_SCHEMA)
    if additional_execute:
        guard["acl"].append([_SOURCE_ROLES["namespace_owner"], int(guard["definition"]["proowner"]), "EXECUTE", False])
    before = deepcopy((rows, allocator))
    verified = _verify(rows, allocator=allocator if additional_execute else None)
    assert len(verified) == 32 and "prepare_pd_generic_dataset_storage" not in verified
    assert (rows, allocator) == before


@pytest.mark.parametrize(
    "damage",
    ["missing", "name", "signature", "oid", "same_oid", "namespace", "owner", "body", "language", "settings", "acl"],
)
def test_guard_execute_requires_exact_original_allocator(callee_rows, damage):
    rows, allocator = deepcopy(callee_rows), _allocator_row(_SCHEMA)
    guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    guard["acl"].append([_SOURCE_ROLES["namespace_owner"], int(guard["definition"]["proowner"]), "EXECUTE", False])
    if damage == "missing":
        allocator = None
    elif damage in {"signature", "language", "acl"}:
        allocator[damage] = {"signature": "other()", "language": "sql", "acl": [[0, 104, "EXECUTE", False]]}[damage]
    else:
        field, value = {
            "name": ("proname", "other"),
            "oid": ("oid", "0"),
            "same_oid": ("oid", int(guard["definition"]["oid"])),
            "namespace": ("pronamespace", "43"),
            "owner": ("proowner", "105"),
            "body": ("prosrc", allocator["definition"]["prosrc"] + " RETURN NULL;"),
            "settings": ("proconfig", ["search_path=public"]),
        }[damage]
        allocator["definition"][field] = value
    with pytest.raises(ValueError):
        _verify(rows, allocator=allocator)


@pytest.mark.parametrize("field,value", [(0, 0), (1, 104), (2, "SELECT"), (3, True)])
def test_guard_execute_edge_remains_exact(callee_rows, field, value):
    rows = deepcopy(callee_rows)
    guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    guard["acl"].append([_SOURCE_ROLES["namespace_owner"], int(guard["definition"]["proowner"]), "EXECUTE", False])
    guard["acl"][-1][field] = value
    with pytest.raises(ValueError, match="callable_acl_changed"):
        _verify(rows, allocator=_allocator_row(_SCHEMA))


def test_allocator_compatibility_decodes_transport_json(callee_rows):
    rows, allocator = deepcopy(callee_rows), _allocator_row(_SCHEMA)
    guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    guard["acl"].append([_SOURCE_ROLES["namespace_owner"], int(guard["definition"]["proowner"]), "EXECUTE", False])
    for field in ("definition", "acl"):
        allocator[field] = json.dumps(allocator[field])
    assert len(_verify(rows, allocator=allocator)) == 32


def test_retirement_contracts_follow_the_complete_forward_migration_chain(monkeypatch, callee_rows):
    statements = []
    for filename in (
        "20261022120000_provider_directory_first_retirement.py",
        "20261024050000_provider_directory_first_retirement_owner_lineage.py",
    ):
        migration = _load(filename)
        monkeypatch.setattr(migration, "_schema", lambda: _SCHEMA)
        monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
        migration.upgrade()
    replacements = [statement for statement in statements if re.match(r"\s*CREATE OR REPLACE FUNCTION", statement)]
    assert len(replacements) == 6
    function_by_name = _parse_statements(replacements, _SCHEMA)
    for name in (
        "provider_directory_terminal_root_retirement_eligible",
        "provider_directory_terminal_root_retirement_evidence",
    ):
        entry = next(row for row in callee_rows if row["definition"]["proname"] == name)
        assert entry["definition"]["prosrc"] == function_by_name[name][-1]
        assert entry["definition"]["proconfig"] == function_by_name[name][-2]
        assert custody._CALLEE_CONTRACTS[name][-1] == custody._body_digest(function_by_name[name][-1], _SCHEMA)


@pytest.mark.parametrize("version", ["base", "first_acquisition"])
@pytest.mark.parametrize("kind", ["eligible", "evidence"])
def test_superseded_retirement_bodies_are_refused(callee_rows, version, kind):
    constructors_by_version = {
        "base": {"eligible": retirement.eligible_function_sql, "evidence": evidence.evidence_function_sql},
        "first_acquisition": {"eligible": first_retirement.eligible_sql, "evidence": first_retirement.evidence_sql},
    }
    statement = constructors_by_version[version][kind](_SCHEMA)
    name = "provider_directory_terminal_root_retirement_" + kind
    rows = deepcopy(callee_rows)
    entry = next(row for row in rows if row["definition"]["proname"] == name)
    entry["definition"]["prosrc"] = _parse_statements([statement], _SCHEMA)[name][-1]
    with pytest.raises(ValueError, match="definition_changed"):
        _verify(rows)


@pytest.mark.parametrize(
    "name",
    [
        "provider_directory_terminal_root_retirement_evidence",
        "provider_directory_terminal_root_retirement_relation_evidence",
        "provider_directory_terminal_root_retirement_v2_evidence",
    ],
)
@pytest.mark.parametrize(
    "settings",
    [
        None,
        ["search_path=pg_catalog"],
        ["search_path=pg_catalog", "TimeZone=Etc/GMT"],
        ["search_path=pg_catalog", "TimeZone=UTC", "statement_timeout=1s"],
        ["TimeZone=UTC", "search_path=pg_catalog"],
    ],
)
def test_evidence_helpers_require_exact_source_declared_settings(callee_rows, name, settings):
    rows = deepcopy(callee_rows)
    entry = next(row for row in rows if row["definition"]["proname"] == name)
    assert entry["definition"]["proconfig"] == ["search_path=pg_catalog", "TimeZone=UTC"]
    entry["definition"]["proconfig"] = settings
    with pytest.raises(ValueError, match="definition_changed"):
        _verify(rows)


def test_transport_json_is_decoded(callee_rows):
    rows = deepcopy(callee_rows)
    owner_oid_by_name = {row["definition"]["proname"]: int(row["definition"]["proowner"]) for row in rows}
    for row in rows:
        row["definition"] = json.dumps(row["definition"])
        row["acl"] = json.dumps(row["acl"])
    captured = custody.require_cms_callee_definitions(
        rows,
        catalog_schema=_SCHEMA,
        catalog_schema_oid=42,
        expected_owner_oids=owner_oid_by_name,
        source_roles=dict(_SOURCE_ROLES),
        preparation_schema=_PREPARATION_SCHEMA,
    )
    assert len(captured) == 32


@pytest.mark.parametrize("name", [key.split("(")[0] for key in _EXPECTED_FRONTIER])
def test_every_callee_body_and_execute_acl_are_independently_bound(callee_rows, name):
    for field in ("body", "acl"):
        rows = deepcopy(callee_rows)
        entry = next(row for row in rows if row["definition"]["proname"] == name)
        if field == "body":
            entry["definition"]["prosrc"] += "\n-- changed"
        else:
            entry["acl"].append([999, int(entry["definition"]["proowner"]), "EXECUTE", False])
        with pytest.raises(ValueError, match="definition_changed|acl_changed"):
            _verify(rows)


@pytest.mark.parametrize(
    "field,value",
    [
        ("pronamespace", "43"),
        ("prokind", "p"),
        ("prorettype", "25"),
        ("pronargs", 2),
        ("proargtypes", ["1043"]),
        ("proargnames", ["renamed"]),
        ("proargmodes", ["i"]),
        ("proallargtypes", ["25"]),
        ("proargdefaults", "forged"),
        ("pronargdefaults", 1),
        ("provariadic", "25"),
        ("prosecdef", False),
        ("proleakproof", True),
        ("proisstrict", True),
        ("proretset", True),
        ("prosupport", "forged"),
        ("probin", "forged"),
        ("prosqlbody", "forged"),
        ("protrftypes", ["25"]),
        ("procost", 1),
        ("prorows", 1),
        ("provolatile", "v"),
        ("proparallel", "s"),
        ("proconfig", ["search_path=public, pg_catalog"]),
    ],
)
def test_new_helper_native_attributes_are_bound(callee_rows, field, value):
    rows = deepcopy(callee_rows)
    entry = next(row for row in rows if row["definition"]["proname"] == "provider_directory_subset_abandonment_valid")
    entry["definition"][field] = value
    with pytest.raises(ValueError, match="definition_changed"):
        _verify(rows)


def test_original_guard_without_canonical_copy_closure_is_refused(callee_rows):
    rows = deepcopy(callee_rows)
    guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    guard["definition"]["prosrc"] = candidates._canonical_copy_guard_bodies(_SCHEMA, _PREPARATION_SCHEMA)[0]
    with pytest.raises(ValueError, match="definition_changed"):
        _verify(rows)


@pytest.mark.parametrize("guard_version", ["v1", "v2"])
def test_guard_uses_independent_installed_preparation_schema(callee_rows, guard_version):
    rows = deepcopy(callee_rows)
    guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    if guard_version == "v2":
        guard["definition"]["prosrc"] = candidates._cms_canonical_copy_guard_body(_SCHEMA, _PREPARATION_SCHEMA)
    with pytest.raises(ValueError, match="definition_changed"):
        _verify(rows, preparation_schema="other_preparation")


def test_cms_guard_body_tampering_is_refused(callee_rows):
    rows = deepcopy(callee_rows)
    guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    guard["definition"]["prosrc"] = (
        candidates._cms_canonical_copy_guard_body(_SCHEMA, _PREPARATION_SCHEMA) + " RETURN NULL;"
    )
    with pytest.raises(ValueError, match="definition_changed"):
        _verify(rows)


@pytest.mark.parametrize("damage", ["missing", "duplicate"])
def test_catalog_function_set_drift_is_refused(callee_rows, damage):
    rows = deepcopy(callee_rows)
    owner_oid_by_name = {row["definition"]["proname"]: int(row["definition"]["proowner"]) for row in rows}
    if damage == "missing":
        rows.pop()
    else:
        rows.append(deepcopy(rows[0]))
    with pytest.raises(ValueError):
        _verify(rows, expected_owner_oids=owner_oid_by_name)


@pytest.mark.parametrize(
    "area,field,value",
    [
        ("definition", "oid", "1001"),
        ("entry", "signature", "wrong(jsonb)"),
        ("entry", "signature", None),
        ("definition", "proowner", "999"),
        ("entry", "language", "plpython3u"),
    ],
)
def test_catalog_identity_drift_is_refused(callee_rows, area, field, value):
    rows = deepcopy(callee_rows)
    owner_oid_by_name = {row["definition"]["proname"]: int(row["definition"]["proowner"]) for row in rows}
    entry = rows[0] if area == "entry" else rows[0]["definition"]
    entry[field] = value
    with pytest.raises(ValueError):
        _verify(rows, expected_owner_oids=owner_oid_by_name)


@pytest.mark.parametrize("name", sorted(source_custody._SOURCE_EXEC_NAMES))
def test_source_pins_retain_source_owner_and_original_caller_acl(callee_rows, name):
    rows = deepcopy(callee_rows)
    entry = next(row for row in rows if row["definition"]["proname"] == name)
    assert entry["definition"]["proowner"] == "101"
    assert [103, 101, "EXECUTE", False] in entry["acl"]
    _verify(rows)
    entry["definition"]["proowner"] = "105"
    entry["acl"] = [[105, 105, "EXECUTE", False], [103, 105, "EXECUTE", False]]
    with pytest.raises(ValueError, match="owner_changed"):
        _verify(rows)


@pytest.mark.parametrize("damage", ["owners", "owner type", "roles", "role type", "role alias"])
def test_independent_role_contract_is_required(callee_rows, damage):
    owner_oid_by_name = {row["definition"]["proname"]: int(row["definition"]["proowner"]) for row in callee_rows}
    role_oid_by_kind = dict(_SOURCE_ROLES)
    if damage == "owners":
        owner_oid_by_name.pop(next(iter(owner_oid_by_name)))
    elif damage == "owner type":
        owner_oid_by_name[next(iter(owner_oid_by_name))] = True
    elif damage == "roles":
        role_oid_by_kind.pop("builder")
    elif damage == "role type":
        role_oid_by_kind["owner"] = "101"
    else:
        role_oid_by_kind["builder"] = role_oid_by_kind["owner"]
    with pytest.raises(ValueError, match="required"):
        _verify(callee_rows, expected_owner_oids=owner_oid_by_name, source_roles=role_oid_by_kind)


def _original_allocation_pins(rows):
    from process.provider_directory_source_control_privileges import METADATA_FUNCTIONS

    fields = ("definition", "language", "acl")
    function_by_name = {row["definition"]["proname"]: {field: deepcopy(row[field]) for field in fields} for row in rows}
    metadata = {
        signature.replace("character varying", "varchar"): deepcopy(function_by_name[signature.split("(")[0]])
        for signature in METADATA_FUNCTIONS
    }
    return dict(
        catalog_schema=_SCHEMA,
        catalog_schema_oid=42,
        preparation_schema=_PREPARATION_SCHEMA,
        expected_functions=function_by_name,
        source_metadata=metadata,
    )


@pytest.mark.parametrize("guard_version", ["v1", "v2"])
def test_original_allocation_keeps_manifest_metadata_without_spoofing_source_roles(callee_rows, guard_version):
    rows = deepcopy(callee_rows)
    for row in rows:
        if row["definition"]["proname"] in source_custody._SOURCE_EXEC_NAMES:
            row["definition"]["proowner"] = "109"
            row["acl"] = [[109, 109, "EXECUTE", False]]
    if guard_version == "v2":
        guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
        guard["definition"]["prosrc"] = candidates._cms_canonical_copy_guard_body(_SCHEMA, _PREPARATION_SCHEMA)
    original = deepcopy(rows)
    verified = custody.require_original_cms_allocation_callees(rows, **_original_allocation_pins(rows))
    assert len(verified) == 32 and rows == original
    with pytest.raises(ValueError, match="owner_changed"):
        _verify(rows)


def test_original_allocation_rejects_actual_v2_against_expected_v1(callee_rows):
    rows = deepcopy(callee_rows)
    pins = _original_allocation_pins(rows)
    guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    guard["definition"]["prosrc"] = candidates._cms_canonical_copy_guard_body(_SCHEMA, _PREPARATION_SCHEMA)
    with pytest.raises(ValueError, match="original_allocation_pin_changed"):
        custody.require_original_cms_allocation_callees(rows, **pins)


@pytest.mark.parametrize("guard_version", ["v1", "v2"])
@pytest.mark.parametrize("damage", ["tampered", "preparation_schema", "v0"])
def test_original_allocation_rejects_unapproved_guard_even_with_matching_pin(callee_rows, guard_version, damage):
    rows = deepcopy(callee_rows)
    guard = next(row for row in rows if row["definition"]["proname"] == "guard_pd_dataset_candidate_parent")
    body = (
        candidates._canonical_copy_guard_bodies(_SCHEMA, _PREPARATION_SCHEMA)[1]
        if guard_version == "v1"
        else candidates._cms_canonical_copy_guard_body(_SCHEMA, _PREPARATION_SCHEMA)
    )
    if damage == "tampered":
        body += " RETURN NULL;"
    elif damage == "preparation_schema":
        body = body.replace(_PREPARATION_SCHEMA, "other_preparation")
    else:
        body = candidates._canonical_copy_guard_bodies(_SCHEMA, _PREPARATION_SCHEMA)[0]
    guard["definition"]["prosrc"] = body
    with pytest.raises(ValueError, match="definition_changed"):
        custody.require_original_cms_allocation_callees(rows, **_original_allocation_pins(rows))


@pytest.mark.parametrize("damage", ["body", "acl", "owner", "manifest", "overload", "unknown", "missing"])
def test_original_allocation_refuses_changed_or_unaccounted_callables(callee_rows, damage):
    rows = deepcopy(callee_rows)
    pins = _original_allocation_pins(rows)
    metadata_name = "provider_directory_subset_canonical_sha256"
    row = next(row for row in rows if row["definition"]["proname"] == metadata_name)
    if damage == "body":
        row["definition"]["prosrc"] += " -- changed"
        pins = _original_allocation_pins(rows)
    if damage == "acl":
        row["acl"].append([999, 101, "EXECUTE", False])
    if damage == "owner":
        row["definition"]["proowner"] = "999"
    if damage == "manifest":
        pins["source_metadata"][metadata_name + "(jsonb)"]["definition"]["proowner"] = "999"
    if damage == "overload":
        rows.append(deepcopy(row))
    if damage == "unknown":
        row["definition"]["proname"] = "unknown_helper"
        pins = _original_allocation_pins(callee_rows)
        pins["expected_functions"]["unknown_helper"] = deepcopy(row)
    if damage == "missing":
        rows.remove(row)
    with pytest.raises(ValueError):
        custody.require_original_cms_allocation_callees(rows, **pins)
