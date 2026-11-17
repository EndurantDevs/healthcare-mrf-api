# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source call sites select exact text arguments without changing proof semantics."""

import importlib
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from db import migration_provider_directory_terminal_root_retirement_guards as retirement
from process import provider_directory_source_callable_custody as custody

_RETIREMENT_KEY = retirement.CHILD_GUARD + "()"


def test_cohort_guards_are_inside_the_complete_source_capability():
    names = {
        "guard_provider_directory_uhc_flex_npi_cohort",
        "guard_provider_directory_uhc_flex_npi_member",
        "validate_provider_directory_uhc_flex_npi_members",
        retirement.CHILD_GUARD,
    }
    assert names <= set(custody.SOURCE_DEFINER_NAMES)
    for name in names:
        assert custody._CONTRACTS[name + "()"][-2] is True
        assert custody._CONTRACTS[name + "()"][-1] == custody._GUARD_BODIES[name + "()"][0]


fhir = importlib.import_module("process.provider_directory_fhir")


def test_seal_predicate_selects_the_exact_digest_signature():
    sql = fhir._artifact_admission_seal_shape_valid_sql("candidate")
    assert "candidate.content_proof_admission_kind::text," in sql
    assert "candidate.content_proof_admission_sha256::text," in sql
    assert "candidate.content_proof_admission_version," in sql
    assert "candidate.content_proof_resource_types" in sql


def test_sealed_metadata_update_keeps_digest_and_predicate_typed():
    sql = fhir._sealed_publication_proof_sql("candidate", "digest", "proof", "identity")
    assert sql.count("dataset.content_proof_admission_kind::text,") == 2
    assert sql.count("dataset.content_proof_admission_sha256::text,") == 2
    assert "UPDATE candidate AS dataset" in sql
    assert "WHERE identity" in sql


@pytest.mark.parametrize("marker", [None, "admitted"])
def test_stored_proof_selects_exact_identity_arguments_in_every_branch(marker):
    sql = fhir._artifact_stored_content_proof_sql("candidate", "metadata", "proof", marker)
    for column in ("dataset_id", "endpoint_id", "acquisition_root_run_id", "dataset_hash"):
        assert f"candidate.{column}::text," in sql
    assert "candidate.resource_count," in sql
    if marker is not None:
        assert "ELSE (" in sql


def test_legacy_proof_casts_the_complete_root_run_expression():
    expression = "COALESCE(dataset.acquisition_root_run_id,dataset.import_run_id)"
    sql = fhir._artifact_legacy_content_proof_valid_sql("metadata", "proof", expression)
    assert f"({expression})::text," in sql
    for column in ("dataset_id", "endpoint_id", "dataset_hash"):
        assert f"dataset.{column}::text," in sql
    assert "dataset.resource_count," in sql


def _roles():
    return dict(owner=101, publisher=102, builder=103, namespace_owner=104, migration=105)


def _entry(key, owner=103):
    spec = custody._CONTRACTS[key]
    arguments, returns, outputs, language, volatility, strict, parallel, security, digest = spec
    oid_by_type = dict(custody._TYPE_OIDS, **{"@work": "301"})
    name = key.split("(")[0]
    definition_by_field = dict(
        oid="501",
        pronamespace="201",
        proname=name,
        proowner=str(owner),
        proacl=None,
        prokind="f",
        prorettype=oid_by_type[returns],
        pronargs=len(arguments),
        proargtypes=[oid_by_type[kind] for _, kind in arguments],
        proargnames=[field for field, _ in (*arguments, *outputs)] or None,
        proargmodes=[*("i" for _ in arguments), *("t" for _ in outputs)] if outputs else None,
        proallargtypes=[oid_by_type[kind] for _, kind in (*arguments, *outputs)] if outputs else None,
        proargdefaults=None,
        pronargdefaults=0,
        provariadic="0",
        prosecdef=security,
        proleakproof=False,
        proisstrict=strict,
        proretset=bool(outputs),
        prosupport="-",
        probin=None,
        prosqlbody=None,
        protrftypes=None,
        procost=100,
        prorows=1000 if outputs else 0,
        provolatile=volatility,
        proparallel=parallel,
        proconfig=None
        if name == "provider_directory_profile_delta_receipt_immutable_v1"
        else ["search_path=pg_catalog, sample"]
        if name.startswith("ptg_wave_")
        else ["search_path=pg_catalog"],
        prosrc=digest,
    )
    acl_rows = [[owner, owner, "EXECUTE", False]]
    if name in custody._PUBLIC_NAMES:
        acl_rows.append([0, owner, "EXECUTE", False])
    return dict(
        definition=definition_by_field,
        language=language,
        signature=key,
        acl=acl_rows,
        access=[[role, False, role == 103, False, False, role == owner] for role in range(101, 106)],
    )


def _retirement_entry(owner=103):
    entry = _entry(_RETIREMENT_KEY, owner=owner)
    sql = retirement.child_guard_function_sql("sample")
    entry["definition"]["prosrc"] = sql.split("AS $function$", 1)[1].rsplit("$function$", 1)[0]
    if owner == 101:
        entry["acl"].append([103, 101, "EXECUTE", False])
    return entry


@pytest.mark.parametrize("schema", ["sample", "alternate_snapshot"])
def test_retirement_guard_matches_frozen_definition(schema):
    sql = retirement.child_guard_function_sql(schema)
    body = sql.split("AS $function$", 1)[1].rsplit("$function$", 1)[0]
    digest = custody._body_digest(body, schema)
    assert custody._CONTRACTS[_RETIREMENT_KEY] == ((), "trigger", (), "plpgsql", "v", False, "u", True, digest)
    assert retirement.CHILD_GUARD in custody.SOURCE_CALLABLE_NAMES
    assert retirement.CHILD_GUARD in custody.SOURCE_DEFINER_NAMES
    assert all(
        retirement.CHILD_GUARD not in names
        for names in (custody._PURE_NAMES, custody._PUBLIC_NAMES, custody._SOURCE_EXEC_NAMES)
    )
    entry = _retirement_entry()
    entry["definition"]["prosrc"] = body
    custody._require_definition(entry, custody._CONTRACTS[_RETIREMENT_KEY], schema, 201, custody._TYPE_OIDS, schema)


@pytest.mark.parametrize(
    "field,value",
    [
        ("pronamespace", "202"),
        ("pronargs", 1),
        ("proargtypes", ["25"]),
        ("proargdefaults", "forged"),
        ("pronargdefaults", 1),
        ("provariadic", "25"),
        ("prosecdef", False),
        ("proleakproof", True),
        ("proisstrict", True),
        ("proretset", True),
        ("prosqlbody", "forged"),
        ("prosrc", "BEGIN RETURN NEW; END"),
        ("proconfig", ["search_path=sample, pg_catalog"]),
        ("prorettype", "25"),
        ("provolatile", "s"),
        ("proparallel", "s"),
    ],
)
def test_retirement_native_drift_is_refused(field, value):
    entry = _retirement_entry()
    entry["definition"][field] = value
    with pytest.raises(ValueError, match="definition_changed"):
        custody._require_definition(
            entry, custody._CONTRACTS[_RETIREMENT_KEY], "sample", 201, custody._TYPE_OIDS, "sample"
        )


@pytest.mark.parametrize("legacy", [True, False])
@pytest.mark.parametrize("owner", [101, 103, 104, 105, 106])
def test_retirement_requires_declared_owner(owner, legacy):
    entry = _retirement_entry(owner)
    if owner == 101 or (legacy and owner == 103):
        custody._require_acl(entry, _roles(), legacy=legacy)
    else:
        with pytest.raises(ValueError, match="owner_changed"):
            custody._require_acl(entry, _roles(), legacy=legacy)


@pytest.mark.parametrize(
    "edge",
    [
        [0, 101, "EXECUTE", False],
        [106, 101, "EXECUTE", False],
        [103, 101, "EXECUTE", True],
        [103, 105, "EXECUTE", False],
    ],
)
def test_retirement_rejects_extra_acl_edges(edge):
    entry = _retirement_entry(owner=101)
    entry["acl"].append(edge)
    with pytest.raises(ValueError, match="acl_changed"):
        custody._require_acl(entry, _roles(), legacy=False)


def _work_guard_body(schema, *, corrected, retried):
    from tests.test_source_validator_argument_types_migration import _load

    order = _load("20260811130000_provider_directory_exact_practitioner_resource_order_repair.py")
    retry = _load("20260824143000_uhc_flex_content_type_retry.py")
    sql = order._rendered_guard_sql(schema, corrected=corrected)
    body = sql.split("AS $guard$", 1)[1].rsplit("$guard$", 1)[0]
    assert body.count(retry._OLD_PREDICATE) == 1
    return body.replace(retry._OLD_PREDICATE, retry._NEW_PREDICATE, 1) if retried else body


@pytest.mark.parametrize("schema", ["sample", "alternate_snapshot"])
def test_work_guard_authenticates_complete_sequence(schema):
    key = "guard_pd_uhc_flex_practitioner_work()"
    entry = _entry(key)
    entry["definition"]["prosrc"] = _work_guard_body(schema, corrected=True, retried=True)
    custody._require_definition(entry, custody._CONTRACTS[key], schema, 201, custody._TYPE_OIDS, schema)


@pytest.mark.parametrize("corrected,retried", [(False, False), (False, True), (True, False)])
def test_work_guard_rejects_incomplete_sequence(corrected, retried):
    key = "guard_pd_uhc_flex_practitioner_work()"
    entry = _entry(key)
    entry["definition"]["prosrc"] = _work_guard_body("sample", corrected=corrected, retried=retried)
    with pytest.raises(ValueError, match="definition_changed"):
        custody._require_definition(entry, custody._CONTRACTS[key], "sample", 201, custody._TYPE_OIDS, "sample")


def test_all_fixed_native_signatures_and_output_modes(monkeypatch):
    monkeypatch.setattr(custody, "_body_digest", lambda body, schema: body)
    for key, specification in custody._CONTRACTS.items():
        custody._require_definition(
            _entry(key), specification, "sample", 201, dict(custody._TYPE_OIDS, **{"@work": "301"}), "sample"
        )
    assert len(custody._CONTRACTS) == 86
    assert set(key for key in custody._CONTRACTS if key.startswith("ptg_wave_canonical_json_ascii_v1(")) == {
        "ptg_wave_canonical_json_ascii_v1(json)",
        "ptg_wave_canonical_json_ascii_v1(jsonb)",
    }


def test_current_wire_contract_matches_the_forward_argument_repair():
    from tests.test_acquisition_validator_argument_types_migration import _current_functions, _migration

    migration = _migration()
    for (name, kinds), attributes in _current_functions().items():
        body = attributes[-1]
        for caller, old, new, count in migration._calls("snapshot_schema"):
            if name == caller:
                assert body.count(old) == count
                body = body.replace(old, new)
        spec = custody._CONTRACTS[name + "(" + kinds + ")"]
        assert spec[-1] == custody._body_digest(body, "snapshot_schema")
        assert (spec[3], spec[4], spec[5], spec[6], spec[7]) == attributes[2:7]


@pytest.mark.parametrize(
    "field,value",
    [
        ("proargtypes", ["1043"]),
        ("proargdefaults", "forged"),
        ("provariadic", "25"),
        ("proisstrict", True),
        ("prosqlbody", "forged"),
        ("prosrc", "forged"),
        ("proconfig", ["search_path=sample, pg_catalog"]),
        ("prorettype", "25"),
    ],
)
def test_native_callable_drift_is_refused(monkeypatch, field, value):
    key = "provider_directory_uhc_flex_practitioner_dataset_valid(text)"
    entry = _entry(key)
    entry["definition"][field] = value
    monkeypatch.setattr(custody, "_body_digest", lambda body, schema: body)
    with pytest.raises(ValueError, match="definition_changed"):
        custody._require_definition(entry, custody._CONTRACTS[key], "sample", 201, custody._TYPE_OIDS, "sample")


def test_work_composite_and_table_output_substitution_are_refused(monkeypatch):
    monkeypatch.setattr(custody, "_body_digest", lambda body, schema: body)
    keys = (
        "validate_provider_directory_rooted_graph_witnesses(regclass,regclass,@work)",
        "read_provider_directory_rooted_graph_initial_work(text,text,integer)",
    )
    for key in keys:
        entry = _entry(key)
        field = "proargtypes" if "@work" in key else "proallargtypes"
        entry["definition"][field] = ["25"]
        with pytest.raises(ValueError, match="definition_changed"):
            custody._require_definition(
                entry, custody._CONTRACTS[key], "sample", 201, dict(custody._TYPE_OIDS, **{"@work": "301"}), "sample"
            )


@pytest.mark.parametrize(
    "edge",
    [
        [0, 103, "EXECUTE", False],
        [106, 103, "EXECUTE", False],
        [101, 103, "EXECUTE", True],
        [101, 106, "EXECUTE", False],
    ],
)
def test_private_stage_extra_execute_edges_are_refused(edge):
    entry = _entry("prepare_pd_uhc_flex_practitioner_stage()")
    entry["acl"].append(edge)
    with pytest.raises(ValueError, match="acl_changed"):
        custody._require_acl(entry, _roles(), legacy=True)


def test_pure_helpers_require_declared_namespace_owner_and_fixed_npi_executor():
    entry = _entry("public_evidence_npi_valid(text)", owner=104)
    entry["acl"].append([103, 104, "EXECUTE", False])
    with pytest.raises(ValueError, match="npi_executor_missing"):
        custody._require_acl(entry, _roles(), legacy=False)
    entry["acl"].append([101, 104, "EXECUTE", False])
    custody._require_acl(entry, _roles(), legacy=False)
    entry["definition"]["proowner"] = "101"
    with pytest.raises(ValueError, match="owner_changed"):
        custody._require_acl(entry, _roles(), legacy=False)
