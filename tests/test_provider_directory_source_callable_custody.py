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


def test_retirement_adoption_preserves_execute_boundaries():
    before = _retirement_entry()
    before["access"].append([106, True, False, False, False, False])
    after = deepcopy(before)
    after["definition"]["proowner"] = "101"
    after["acl"] = custody._expected_function_acl(before, _roles())
    assert after["acl"] == [(101, 101, "EXECUTE", False), (103, 101, "EXECUTE", False)]
    custody._require_acl(after, _roles(), legacy=False)
    after["access"][0][-1] = after["access"][1][-1] = True
    receipt_by_field = dict(
        roles=_roles(), reach=[[role, False, role in {101, 102}, False, False, False] for role in range(101, 107)]
    )
    custody._require_execute_delta(before, after, receipt_by_field)
    for index, changed in ((2, False), (5, True)):
        altered = deepcopy(after)
        altered["access"][index][-1] = changed
        with pytest.raises(ValueError, match="execute_changed"):
            custody._require_execute_delta(before, altered, receipt_by_field)
    after["acl"].pop()
    with pytest.raises(ValueError, match="acl_changed"):
        custody._require_acl(after, _roles(), legacy=False)


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
    assert len(custody._CONTRACTS) == 87
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


@pytest.mark.parametrize("role,field", [(103, 2), (103, 3), (102, 4), (106, 5)])
def test_inherited_or_set_replacement_authority_is_refused(role, field):
    rows = [[value, False, False, False, False, False] for value in range(101, 107)]
    next(row for row in rows if row[0] == role)[field] = True
    with pytest.raises(ValueError, match="authority"):
        custody._require_owner_reach(rows, _roles(), set(range(101, 107)))


def test_execute_projection_requires_exact_true_and_preserves_ordinary_false():
    before = _entry("prepare_pd_uhc_flex_practitioner_stage()")
    after = deepcopy(before)
    reach_rows = [[role, False, role == 101, False, False, False] for role in range(101, 106)]
    receipt_by_field = dict(roles=_roles(), reach=reach_rows)
    with pytest.raises(ValueError, match="execute_changed"):
        custody._require_execute_delta(before, after, receipt_by_field)
    after["access"][0][-1] = True
    custody._require_execute_delta(before, after, receipt_by_field)
    after["access"][2][-1] = False
    with pytest.raises(ValueError, match="execute_changed"):
        custody._require_execute_delta(before, after, receipt_by_field)


def test_npi_additional_execute_is_not_allowed_on_other_pure_helpers():
    before = _entry("ptg_wave_canonical_json_ascii_v1(jsonb)")
    after = deepcopy(before)
    after["access"][0][-1] = True
    receipt_by_field = dict(
        roles=_roles(), reach=[[role, False, role == 101, False, False, False] for role in range(101, 106)]
    )
    with pytest.raises(ValueError, match="execute_changed"):
        custody._require_execute_delta(before, after, receipt_by_field)


@pytest.mark.parametrize(
    "actor,current,legacy,accepted",
    [
        ("builder", "builder", False, True),
        ("builder", "owner", False, False),
        ("reader", "reader", False, False),
        ("reader", "builder", False, False),
        ("publisher", "owner", False, True),
        ("publisher", "publisher", False, True),
        ("builder", "builder", True, False),
        ("admin", "admin", True, True),
    ],
)
def test_readonly_capture_authenticates_original_builder_without_adoption(actor, current, legacy, accepted):
    policy = SimpleNamespace(control_runtime_roles=("builder",), publisher_role="publisher", owner_role="owner")
    identity_by_field = dict(session=actor, current=current, administrator=actor == "admin")
    if accepted:
        custody._require_actor(identity_by_field, policy, legacy=legacy)
    else:
        with pytest.raises(ValueError, match="actor_changed"):
            custody._require_actor(identity_by_field, policy, legacy=legacy)


@pytest.mark.asyncio
@pytest.mark.parametrize("key", ["prepare_pd_uhc_flex_practitioner_stage()", _RETIREMENT_KEY])
async def test_pre_cut_capability_never_allows_builder_owned_definers(monkeypatch, key):
    async def capture(connection, policy, **kwargs):
        assert kwargs["legacy"] is True
        return dict(
            roles=_roles(),
            schema_oid=201,
            role_graph=dict(roles=[]),
            functions={
                "protected": _entry(key, owner=101),
                "legacy": _entry(key, owner=103),
                "invoker": _entry("public_evidence_npi_valid(text)", owner=104),
            },
        )

    monkeypatch.setattr(custody, "capture_source_callable_custody", capture)
    assert await custody.capture_source_callable_adoption_capability(
        SimpleNamespace(
            is_in_transaction=lambda: True,
            fetchrow=AsyncMock(
                side_effect=lambda sql, *args: (
                    dict(actor="unchanged")
                    if sql == custody._IDENTITY_SQL
                    else dict(relation_oid=None, function_oids=[])
                )
            ),
        ),
        custody.GenericSourcePolicy("sample", "owner", "publisher", ("builder",), ("builder",)),
        migration_role="migration",
        namespace_owner_role="namespace",
    ) == (501,)


def _metadata_row(sql, identity, work_by_field):
    if sql in {custody._IDENTITY_SQL, custody._READ_IDENTITY_SQL}:
        return identity
    if "c.relname='cms_native_input_revision'" in sql:
        return dict(relation_oid=None, function_oids=[])
    return work_by_field


@pytest.fixture
def metadata_catalog(monkeypatch):
    policy = custody.GenericSourcePolicy("sample", "owner", "publisher", ("builder", "reader"), ("builder",))
    namespace_by_field = dict(oid=201, identifier="sample")
    role_graph_dict = dict(roles=[dict(oid=role) for role in range(101, 107)])
    native_identity_dict = dict(session="pin", current="pin", backend=7, xid="9", administrator=False)
    work_by_field = dict(
        relation=401,
        owner=101,
        kind="p",
        oid=301,
        category="c",
        relation_oid=401,
        type_owner=101,
        namespace=201,
        element=0,
        array=302,
        array_element=301,
        array_owner=101,
        array_namespace=201,
    )
    entries = []
    for index, key in enumerate(custody._CONTRACTS):
        name = key.split("(", 1)[0]
        owner = 104 if name in custody._PURE_NAMES else 101
        entry = _entry(key, owner=owner)
        entry["definition"]["oid"] = str(701 + index)
        entry["signature"] = str(701 + index)
        entry["access"].append([106, False, False, False, False, name in custody._PUBLIC_NAMES])
        if name not in custody._PUBLIC_NAMES:
            entry["acl"].append([103, owner, "EXECUTE", False])
        if name == "public_evidence_npi_valid":
            entry["acl"].append([101, owner, "EXECUTE", False])
        entries.append(entry)
    catalog_rows = [
        dict(
            oid=int(entry["signature"]),
            namespace=201,
            name=entry["definition"]["proname"],
            arguments=entry["definition"]["proargtypes"],
        )
        for entry in entries
    ]
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchval=AsyncMock(side_effect=lambda sql, *args: "401" if "SELECT c.oid::text" in sql else True),
    )
    connection.fetchrow = AsyncMock(
        side_effect=lambda sql, *args: _metadata_row(sql, native_identity_dict, work_by_field)
    )
    connection.fetch = AsyncMock(
        side_effect=lambda sql, *args: entries if sql == custody._FUNCTION_SQL else catalog_rows
    )
    monkeypatch.setattr(
        custody, "_capture_roles", AsyncMock(return_value=(role_graph_dict, _roles(), namespace_by_field, []))
    )
    monkeypatch.setattr(custody, "_body_digest", lambda body, schema: body)
    return SimpleNamespace(**locals())


async def _metadata_capability(case):
    return await custody.attest_source_callable_capability(
        case.connection, case.policy, migration_role="migration", namespace_owner_role="namespace_owner"
    )


@pytest.mark.asyncio
async def test_metadata_proof_uses_numeric_catalog_identity(metadata_catalog):
    case = metadata_catalog
    actual = await _metadata_capability(case)
    assert actual == tuple(int(entry["signature"]) for entry in case.entries if entry["definition"]["prosecdef"])
    assert case.connection.fetchrow.call_args_list[1].args == (custody._WORK_TYPE_SQL, "401")
    call = next(call for call in case.connection.fetch.call_args_list if call.args[0] == custody._FUNCTION_SQL)
    assert call.args[1] == [entry["signature"] for entry in case.entries]
    assert call.args[2] == "201"
    assert not hasattr(case.connection, "execute")
    queries = [call.args[0] for call in case.connection.fetchrow.call_args_list]
    assert queries[0] == queries[-1] == custody._READ_IDENTITY_SQL
    assert "pg_current_xact_id" not in custody._READ_IDENTITY_SQL
    assert "virtualtransaction" in custody._READ_IDENTITY_SQL


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["unknown", "missing", "duplicate", "oid", "namespace", "arguments"])
async def test_metadata_census_rejects_substitution(metadata_catalog, mutation):
    rows = metadata_catalog.catalog_rows
    if mutation == "missing":
        rows.pop()
    elif mutation == "duplicate":
        rows[-1] = rows[0]
    else:
        field, value = {
            "unknown": ("name", "unknown"),
            "oid": ("oid", 0),
            "namespace": ("namespace", 202),
            "arguments": ("arguments", ["999"]),
        }[mutation]
        rows[0][field] = value
    with pytest.raises(ValueError, match="overloads_changed"):
        await _metadata_capability(metadata_catalog)


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ["oid", "name", "signature", "arguments", "body", "pin_grant", "extra"])
async def test_metadata_rows_retain_full_custody(metadata_catalog, mutation):
    entries = metadata_catalog.entries
    if mutation == "extra":
        entries.append(deepcopy(entries[0]))
    elif mutation == "pin_grant":
        entries[0]["acl"].append([106, 101, "EXECUTE", False])
    elif mutation == "body":
        entries[0]["definition"]["prosrc"] = "tampered"
    elif mutation == "signature":
        entries[0]["signature"] = entries[1]["signature"]
    else:
        field = {"oid": "oid", "name": "proname", "arguments": "proargtypes"}[mutation]
        entries[0]["definition"][field] = ["999"] if mutation == "arguments" else "999"
    with pytest.raises(ValueError, match="overloads_changed|definition_changed|acl_changed"):
        await _metadata_capability(metadata_catalog)


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["session", "current", "backend", "xid"])
async def test_metadata_identity_drift_refuses(metadata_catalog, field):
    case = metadata_catalog
    values = iter([case.native_identity_dict, {**case.native_identity_dict, field: "changed"}])
    case.connection.fetchrow.side_effect = lambda sql, *args: (
        next(values)
        if sql == custody._READ_IDENTITY_SQL
        else dict(relation_oid=None, function_oids=[])
        if "c.relname='cms_native_input_revision'" in sql
        else case.work_by_field
    )
    with pytest.raises(ValueError, match="transaction_changed"):
        await _metadata_capability(case)


@pytest.mark.asyncio
async def test_metadata_requires_original_transaction(metadata_catalog):
    metadata_catalog.connection.is_in_transaction = lambda: False
    with pytest.raises(ValueError, match="policy_required"):
        await _metadata_capability(metadata_catalog)
    metadata_catalog.connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_requires_virtual_transaction_before_role_or_function_reads(metadata_catalog):
    metadata_catalog.native_identity_dict["xid"] = None
    with pytest.raises(ValueError, match="transaction_required"):
        await _metadata_capability(metadata_catalog)
    custody._capture_roles.assert_not_awaited()
    metadata_catalog.connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("reference", [None, "0", "sample.work", "402"])
async def test_metadata_work_identity_refuses(metadata_catalog, reference):
    metadata_catalog.connection.fetchval.side_effect = lambda sql, *args: reference
    with pytest.raises(ValueError, match="work_type_changed"):
        await _metadata_capability(metadata_catalog)


@pytest.mark.asyncio
async def test_metadata_work_name_recheck_refuses(metadata_catalog):
    metadata_catalog.connection.fetchval.side_effect = ["401", False]
    with pytest.raises(ValueError, match="work_type_changed"):
        await _metadata_capability(metadata_catalog)


@pytest.mark.asyncio
async def test_metadata_task_drift_refuses(metadata_catalog, monkeypatch):
    with monkeypatch.context() as patch:
        patch.setattr(custody.asyncio, "current_task", lambda: next(tasks))
        tasks = iter([object(), object()])
        with pytest.raises(ValueError, match="transaction_changed"):
            await _metadata_capability(metadata_catalog)


@pytest.mark.asyncio
@pytest.mark.parametrize("actor", ["pin", "migration", "reader"])
async def test_metadata_does_not_admit_source_actors(metadata_catalog, actor):
    case = metadata_catalog
    case.native_identity_dict.update(session=actor, current=actor)
    with pytest.raises(ValueError, match="actor_changed"):
        await custody.capture_source_callable_custody(
            case.connection, case.policy, migration_role="migration", namespace_owner_role="namespace_owner"
        )
    custody._capture_roles.assert_not_awaited()


@pytest.mark.asyncio
async def test_metadata_public_helper_direct_pin_grant_refuses(metadata_catalog):
    entry = next(
        entry
        for entry in metadata_catalog.entries
        if entry["definition"]["proname"] == "provider_directory_uhc_flex_practitioner_dataset_ready"
    )
    entry["acl"].append([106, 101, "EXECUTE", False])
    with pytest.raises(ValueError, match="acl_changed"):
        await _metadata_capability(metadata_catalog)
