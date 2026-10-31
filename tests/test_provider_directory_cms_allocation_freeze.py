# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Allocation-only custody retains the caller's physical transaction and deadline."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from process import provider_directory_cms_callable_custody as callables
from process import provider_directory_cms_preparation_custody as custody
from tests.test_provider_directory_cms_callee_custody import (
    _catalog_definition,
    _catalog_rows,
    _original_allocation_pins,
    _parse_statements,
)


def _mock_allocation_connection(state):
    async def fetchval(sql, *args):
        if sql == "SHOW search_path":
            return state.actor["search_path"]
        if sql == custody._ALLOCATION_LOCKS_SQL:
            return state.locks
        if sql == custody._ALLOCATION_DEPENDENCIES_SQL:
            return state.dependencies
        if sql == custody._ALLOCATION_ROOTS_SQL:
            return True
        if sql == callables._ROLE_GRAPH_SQL:
            return deepcopy(state.roles)
        return [1255, 1260, 1261, 2612, 2964, 3466]

    async def fetch(sql, *args):
        if sql == custody._ALLOCATION_NAMESPACES_SQL:
            return deepcopy(state.namespaces)
        if sql == custody._ALLOCATION_ADMISSION_SQL:
            return [dict(oid=oid) for oid in state.admission_oids]
        if sql == custody._ALLOCATION_ROUTINES_SQL:
            return [
                dict(oid=int(row["definition"]["oid"]), native={"definition": row["definition"]}) for row in state.rows
            ]
        if sql == callables._FUNCTION_SQL:
            return deepcopy(state.rows)
        return deepcopy(state.settings)

    return SimpleNamespace(
        fetchval=AsyncMock(side_effect=fetchval),
        fetch=AsyncMock(side_effect=fetch),
        fetchrow=AsyncMock(side_effect=lambda *args: state.actor.copy()),
        execute=AsyncMock(),
        is_in_transaction=Mock(return_value=True),
        is_closed=Mock(return_value=False),
        get_server_pid=Mock(return_value=91),
    )


@pytest.fixture
def frozen(monkeypatch):
    from db.migration_provider_directory_dataset_candidates import candidate_functions

    function_rows = _catalog_rows("snapshot_schema")
    name = "prepare_pd_generic_dataset_storage"
    spec = _parse_statements((candidate_functions("snapshot_schema"),), "snapshot_schema")[name]
    function_rows.append(
        dict(
            signature='"snapshot_schema".' + name + "()",
            definition=_catalog_definition(name, spec, 101, 2000),
            language=spec[3],
            acl=[[101, 101, "EXECUTE", False]],
        )
    )
    arguments = _original_allocation_pins(function_rows)
    arguments.pop("catalog_schema_oid")
    expected_function_by_name = arguments.pop("expected_functions")
    state = SimpleNamespace(
        rows=function_rows,
        admission_oids=[200, 201, 202],
        locks=True,
        dependencies=True,
        roles={"roles": [], "memberships": []},
        settings=[],
        namespaces=[
            dict(oid=11, nspname="pg_catalog", nspowner=10, nspacl=None),
            dict(oid=42, nspname="snapshot_schema", nspowner=104, nspacl=None),
            dict(oid=43, nspname="preparation_schema", nspowner=104, nspacl=None),
        ],
        actor=dict(
            administrator=True,
            role_oid=10,
            session="original_admin",
            current="original_admin",
            backend=91,
            transaction="1/1",
            replication="origin",
            isolation="read committed",
            readonly="off",
            search_path="pg_catalog, pg_temp",
            database_oid=1,
        ),
    )
    state.connection = _mock_allocation_connection(state)
    monkeypatch.setattr(
        custody,
        "_callee_frontier",
        AsyncMock(return_value=[dict(oid=int(function["definition"]["oid"])) for function in function_rows]),
    )
    state.arguments = dict(
        **arguments,
        relation_oids=[100, 101, 102, 103, 104, 200, 201, 202],
        expected_pins=dict(
            functions=expected_function_by_name,
            namespaces=deepcopy(state.namespaces),
            roles=deepcopy(state.roles),
        ),
    )
    return state


def _context(case, deadline=None):
    return custody.freeze_cms_allocation_callables(
        case.connection,
        **case.arguments,
        deadline=asyncio.get_running_loop().time() + 2.9 if deadline is None else deadline,
    )


async def test_context_does_not_commit_or_claim_to_release_transaction_locks(frozen):
    async with _context(frozen) as result:
        assert result is None and frozen.connection.is_in_transaction()
    statements = [call.args[0] for call in frozen.connection.execute.await_args_list]
    assert len(statements) == 1 and statements[0].endswith("IN SHARE MODE NOWAIT")
    assert frozen.connection.is_in_transaction()


@pytest.mark.parametrize("damage", ["mapping", "missing", "extra", "functions", "namespaces", "roles"])
async def test_expected_pins_require_exact_keys_and_types(frozen, damage):
    if damage == "mapping":
        frozen.arguments["expected_pins"] = []
    if damage == "missing":
        frozen.arguments["expected_pins"].pop("roles")
    if damage == "extra":
        frozen.arguments["expected_pins"]["metadata"] = frozen.arguments["source_metadata"]
    if damage in {"functions", "namespaces", "roles"}:
        frozen.arguments["expected_pins"][damage] = None
    with pytest.raises(ValueError, match="allocation_pins_required"):
        async with _context(frozen):
            pytest.fail("invalid pins entered")
    frozen.connection.execute.assert_not_awaited()
    frozen.connection.fetchrow.assert_not_awaited()


@pytest.mark.parametrize("damage", ["not_admin", "set_role", "source_owner", "no_transaction"])
async def test_original_admin_is_required_before_catalog_locks(frozen, damage):
    if damage == "not_admin":
        frozen.actor["administrator"] = False
    elif damage == "set_role":
        frozen.actor["session"] = "another_session_role"
    elif damage == "source_owner":
        frozen.actor["role_oid"] = int(
            next(iter(frozen.arguments["source_metadata"].values()))["definition"]["proowner"]
        )
    else:
        frozen.connection.is_in_transaction.return_value = False
    with pytest.raises(ValueError):
        async with _context(frozen):
            pytest.fail("invalid actor entered")
    frozen.connection.execute.assert_not_awaited()


@pytest.mark.parametrize("damage", ["locks", "dependencies", "roles", "namespace", "function", "transaction"])
async def test_exit_requires_original_physical_custody(frozen, damage):
    with pytest.raises(ValueError):
        async with _context(frozen):
            if damage in {"locks", "dependencies"}:
                setattr(frozen, damage, False)
            if damage == "roles":
                frozen.roles["roles"].append({"oid": 999})
            if damage == "namespace":
                frozen.namespaces[1]["nspowner"] = 999
            if damage == "function":
                frozen.rows[0]["definition"]["prosrc"] += " -- changed"
            if damage == "transaction":
                frozen.actor["transaction"] = "1/2"


@pytest.mark.parametrize("deadline", [0, float("inf"), float("nan"), True])
async def test_invalid_deadline_refuses_before_sql(frozen, deadline):
    with pytest.raises(ValueError, match="allocation_deadline_required"):
        async with _context(frozen, deadline):
            pytest.fail("invalid deadline entered")
    frozen.connection.execute.assert_not_awaited()


async def test_cancellation_propagates_and_borrowed_transaction_is_not_committed(frozen):
    with pytest.raises(TimeoutError):
        async with _context(frozen, asyncio.get_running_loop().time() + 0.03):
            await asyncio.Event().wait()
    assert frozen.connection.is_in_transaction()
    assert all("COMMIT" not in call.args[0] for call in frozen.connection.execute.await_args_list)


@pytest.mark.parametrize("path", ["public, pg_catalog", "pg_catalog", "pg_temp, pg_catalog", '"pg_catalog", pg_temp'])
async def test_unsafe_resolution_refuses_before_actor_sql_or_locks(frozen, path):
    frozen.actor["search_path"] = path
    with pytest.raises(ValueError, match="allocation_resolution_required"):
        async with _context(frozen):
            pytest.fail("unsafe path entered")
    frozen.connection.fetchrow.assert_not_awaited()
    frozen.connection.execute.assert_not_awaited()


@pytest.mark.parametrize("database_oid,role_oid", [(0, 0), (1, 0), (0, 10), (1, 10), (1, 101)])
async def test_applicable_settings_cannot_become_observed_authority(frozen, database_oid, role_oid):
    role_by_field = dict(oid=101, rolname="source_owner")
    frozen.roles["roles"] = [role_by_field]
    frozen.arguments["expected_pins"]["roles"]["roles"] = [deepcopy(role_by_field)]
    frozen.settings = [dict(setdatabase=database_oid, setrole=role_oid, setconfig=["search_path=public"])]
    with pytest.raises(ValueError, match="allocation_applicable_role_settings_unbound"):
        async with _context(frozen):
            pytest.fail("unbound applicable setting entered")


async def test_unrelated_roles_and_settings_are_internal_stability_only(frozen):
    frozen.roles["roles"] = [dict(oid=999, rolname="unrelated")]
    frozen.settings = [dict(setdatabase=1, setrole=999, setconfig=["search_path=public"])]
    async with _context(frozen) as result:
        assert result is None
    assert frozen.arguments["expected_pins"]["roles"] == {"roles": [], "memberships": []}
    with pytest.raises(ValueError, match="allocation_role_stability_changed"):
        async with _context(frozen):
            frozen.settings[0]["setconfig"] = ["search_path=pg_catalog"]


async def test_authenticated_related_role_is_required_at_entry(frozen):
    frozen.arguments["expected_pins"]["roles"]["roles"] = [dict(oid=101, rolname="expected")]
    frozen.roles["roles"] = [dict(oid=101, rolname="substituted")]
    with pytest.raises(ValueError, match="allocation_roles_changed"):
        async with _context(frozen):
            pytest.fail("changed related role entered")


@pytest.mark.parametrize(
    "field,value", [("oid", 999), ("nspname", "other"), ("nspowner", 999), ("nspacl", "{other=C/admin}")]
)
async def test_changed_namespace_refuses_before_allocation(frozen, field, value):
    frozen.namespaces[2][field] = value
    with pytest.raises(ValueError, match="allocation_namespace_changed"):
        async with _context(frozen):
            pytest.fail("wrong preparation namespace entered")


def _raw_allocation_evidence(frozen):
    canonical_relations = [
        dict(oid=100 + index, nspname="snapshot_schema", relname=name, native={"constraints": []})
        for index, name in enumerate(("provider_directory_endpoint_dataset", *custody.TABLES[:4]))
    ]
    routines = [
        dict(
            oid=int(row["definition"]["oid"]),
            native=dict(
                definition=deepcopy(row["definition"]),
                language={"lanname": row["language"]},
            ),
        )
        for row in frozen.rows
    ]
    return dict(canonical=canonical_relations, private=[], routines=routines, roles=deepcopy(frozen.roles))


@pytest.fixture
def authenticated(frozen, monkeypatch):
    import json
    from dataclasses import asdict

    from process.provider_directory_generic_source_policy import GenericSourcePolicy
    from process.provider_directory_source_storage_binding import FHIRSourceStorageBinding

    frozen.raw = _raw_allocation_evidence(frozen)
    frozen.policy = GenericSourcePolicy("private_source", "preparation_owner", "preparation_publisher", ("reader",))
    frozen.storage = FHIRSourceStorageBinding("snapshot_schema", "private_source", 1, 42, 44, 101, 104, "a" * 64)
    frozen.profile = dict(
        contract=custody.CONTRACT,
        source_ids=["cms-npd"],
        storage=asdict(frozen.storage),
        policy=json.loads(json.dumps(asdict(frozen.policy))),
        **custody._evidence_digests(frozen.raw),
    )
    frozen.source_custody = dict(
        canonical_authority=dict(
            admission={
                name: dict(oid=200 + index, nspname="snapshot_schema", relname=name)
                for index, name in enumerate(
                    ("provider_directory_source", "provider_directory_api_endpoint", "import_run")
                )
            },
            metadata=deepcopy(frozen.arguments["source_metadata"]),
        )
    )
    frozen.collect = AsyncMock(side_effect=lambda *args: deepcopy(frozen.raw))
    monkeypatch.setattr(custody, "_collect", frozen.collect)
    frozen.lock_inventory = AsyncMock()
    monkeypatch.setattr(custody, "_lock_allocation_inventory", frozen.lock_inventory)
    original_fetch = frozen.connection.fetch.side_effect

    async def fetch(sql, *args):
        if sql == custody._ALLOCATION_ROUTINES_SQL:
            return [
                {
                    **entry,
                    "native": {
                        key: value
                        for key, value in entry["native"].items()
                        if key not in {"acl", "namespace_identifier"}
                    },
                }
                for entry in deepcopy(frozen.raw["routines"])
            ]
        if sql == custody._ALLOCATION_ADMISSION_SQL:
            return await original_fetch(sql, *args)
        if "pg_catalog.pg_namespace" in sql:
            return deepcopy(frozen.namespaces)
        return await original_fetch(sql, *args)

    frozen.connection.fetch.side_effect = fetch
    return frozen


async def _authenticated_pins(case):
    return await custody.capture_authenticated_cms_allocation_pins(
        case.connection,
        storage_binding=case.storage,
        policy=case.policy,
        cms_profile=case.profile,
        source_custody=case.source_custody,
        preparation_schema="preparation_schema",
    )


async def test_authenticated_projection_uses_existing_four_digests(authenticated):
    pins_by_kind, relations = await _authenticated_pins(authenticated)
    assert pins_by_kind == authenticated.arguments["expected_pins"]
    assert relations == [100, 101, 102, 103, 104, 200, 201, 202]
    assert authenticated.collect.await_count == 2
    authenticated.lock_inventory.assert_awaited_once()


async def test_admission_locks_remain_in_scope_without_importing_their_dml_triggers(authenticated):
    before = deepcopy(authenticated.raw)
    expected = custody._evidence_digests(before)
    pins, relations = await _authenticated_pins(authenticated)
    authenticated.arguments.update(expected_pins=pins, relation_oids=relations)
    async with _context(authenticated) as result:
        assert result is None
    locked = authenticated.lock_inventory.await_args.args[1]
    assert {row["oid"] for row in locked} == set(relations)
    routine_calls = [
        call
        for call in authenticated.connection.fetch.await_args_list
        if call.args[0] == custody._ALLOCATION_ROUTINES_SQL
    ]
    assert len(routine_calls) == 3
    assert all(call.args[1] == relations and call.args[3] == [200, 201, 202] for call in routine_calls)
    dependency_calls = [
        call
        for call in authenticated.connection.fetchval.await_args_list
        if call.args[0] == custody._ALLOCATION_DEPENDENCIES_SQL
    ]
    assert all(call.args[2] == relations and call.args[4] == [200, 201, 202] for call in dependency_calls)
    assert authenticated.raw == before and custody._evidence_digests(authenticated.raw) == expected


def test_allocation_query_keeps_all_nontrigger_dependencies_and_fixed_callees():
    excluded = " AND t.tgrelid<>ALL($3::oid[])"
    assert custody._ALLOCATION_ROUTINES_SQL.count(excluded) == 2
    assert custody._ALLOCATION_ROUTINES_SQL.replace(excluded, "") == custody._ROUTINES_SQL
    assert "WHERE tgrelid=ANY($2::oid[]) AND tgrelid<>ALL($4::oid[])" in custody._ALLOCATION_DEPENDENCIES_SQL


@pytest.mark.parametrize("admission_oids", [[], [200, 201], [200, 200, 202], [200, 201, 999]])
async def test_admission_trigger_exclusion_requires_all_three_locked_relations(authenticated, admission_oids):
    authenticated.admission_oids = admission_oids
    with pytest.raises(ValueError, match="allocation_admission_required"):
        await _authenticated_pins(authenticated)
    assert not any(
        call.args[0] == custody._ALLOCATION_ROUTINES_SQL for call in authenticated.connection.fetch.await_args_list
    )


@pytest.mark.parametrize("version", ["v1", "v3"])
@pytest.mark.parametrize("change", ["missing", "body", "owner", "acl", "namespace", "dependencies"])
async def test_required_routine_cannot_be_omitted_or_recaptured_as_authority(
    authenticated, monkeypatch, version, change
):
    if version == "v3":
        _versioned_evidence(authenticated, monkeypatch, version)
    required = deepcopy(authenticated.raw["routines"])
    for routine in required:
        routine["native"].pop("acl", None)
        routine["native"].pop("namespace_identifier", None)
    if change == "missing":
        required[0]["oid"] = 999
    elif change in {"body", "owner", "acl"}:
        required[0]["native"]["definition"][{"body": "prosrc", "owner": "proowner", "acl": "proacl"}[change]] = (
            "changed"
        )
    else:
        required[0]["native"][change] = ["changed"]
    original_fetch = authenticated.connection.fetch.side_effect

    async def fetch(sql, *args):
        if sql == custody._ALLOCATION_ROUTINES_SQL:
            return required
        return await original_fetch(sql, *args)

    authenticated.connection.fetch.side_effect = fetch
    with pytest.raises(ValueError, match="allocation_routine_unbound"):
        await _authenticated_pins(authenticated)


@pytest.mark.parametrize("change", ["grantee", "grantor", "right", "grantable", "missing", "extra"])
async def test_v3_function_acl_must_match_authenticated_expansion(authenticated, monkeypatch, change):
    _versioned_evidence(authenticated, monkeypatch, "v3")
    before = deepcopy(authenticated.raw)
    observed_acl = authenticated.rows[0]["acl"]
    if change == "missing":
        observed_acl.pop()
    elif change == "extra":
        observed_acl.append([999, 101, "EXECUTE", False])
    else:
        position, value = {"grantee": (0, 999), "grantor": (1, 999), "right": (2, "USAGE"), "grantable": (3, True)}[
            change
        ]
        observed_acl[0][position] = value
    with pytest.raises(ValueError, match="allocation_routine_unbound"):
        await _authenticated_pins(authenticated)
    assert authenticated.raw == before


@pytest.mark.parametrize("field", custody._DIGEST_FIELDS)
async def test_each_protected_digest_must_match_before_projection(authenticated, field):
    authenticated.profile[field] = "0" * 64
    with pytest.raises(ValueError, match="native_evidence_changed"):
        await _authenticated_pins(authenticated)
    authenticated.lock_inventory.assert_not_awaited()
    assert not any(call.args[0] == callables._FUNCTION_SQL for call in authenticated.connection.fetch.await_args_list)


async def test_recapture_after_relation_pins_must_still_match(authenticated):
    changed_by_kind = deepcopy(authenticated.raw)
    changed_by_kind["canonical"][0]["relname"] = "replaced"
    authenticated.collect.side_effect = [deepcopy(authenticated.raw), changed_by_kind]
    with pytest.raises(ValueError, match="native_evidence_changed"):
        await _authenticated_pins(authenticated)


async def test_raw_definition_cannot_be_substituted_after_digest_verification(authenticated):
    authenticated.rows[0]["definition"]["proowner"] = 999
    with pytest.raises(ValueError, match="allocation_routine_unbound"):
        await _authenticated_pins(authenticated)


@pytest.mark.parametrize("reference", [201, "201"])
async def test_native_foreign_oid_joins_integer_inventory_without_rewriting_hash_inputs(authenticated, reference):
    authenticated.raw["canonical"][0]["native"]["constraints"] = [
        [dict(contype="f", confrelid=reference), "foreign key"]
    ]
    authenticated.profile.update(custody._evidence_digests(authenticated.raw))
    before_by_kind = deepcopy(authenticated.raw)
    expected_by_field = {field: authenticated.profile[field] for field in custody._DIGEST_FIELDS}
    pins_by_kind, relations = await _authenticated_pins(authenticated)
    assert pins_by_kind == authenticated.arguments["expected_pins"]
    assert relations == [100, 101, 102, 103, 104, 200, 201, 202]
    assert authenticated.raw == before_by_kind
    assert custody._evidence_digests(authenticated.raw) == expected_by_field


async def test_equivalent_oid_representation_still_requires_exact_authenticated_digest(authenticated):
    constraint_by_field = dict(contype="f", confrelid="201")
    authenticated.raw["canonical"][0]["native"]["constraints"] = [[constraint_by_field, "foreign key"]]
    authenticated.profile.update(custody._evidence_digests(authenticated.raw))
    constraint_by_field["confrelid"] = 201
    with pytest.raises(ValueError, match="native_evidence_changed"):
        await _authenticated_pins(authenticated)
    authenticated.lock_inventory.assert_not_awaited()


@pytest.mark.parametrize("reference", [999, "999"])
async def test_actual_header_foreign_relation_must_be_bound(authenticated, reference):
    authenticated.raw["canonical"][0]["native"]["constraints"] = [
        [dict(contype="f", confrelid=reference), "foreign key"]
    ]
    authenticated.profile.update(custody._evidence_digests(authenticated.raw))
    with pytest.raises(ValueError, match="allocation_foreign_relation_unbound"):
        await _authenticated_pins(authenticated)


@pytest.mark.parametrize(
    "reference",
    [True, None, 201.0, 0, -1, 2**32, "0", "4294967296", "0201", "+201", " 201", "2.01e2", "２０１", [], {}],
)
async def test_invalid_native_foreign_oid_refuses_before_callable_projection(authenticated, reference):
    authenticated.raw["canonical"][0]["native"]["constraints"] = [
        [dict(contype="f", confrelid=reference), "foreign key"]
    ]
    authenticated.profile.update(custody._evidence_digests(authenticated.raw))
    with pytest.raises(ValueError, match="allocation_foreign_relation_unbound"):
        await _authenticated_pins(authenticated)
    assert not any(call.args[0] == callables._FUNCTION_SQL for call in authenticated.connection.fetch.await_args_list)


@pytest.mark.parametrize("field", ["metadata", "admission"])
async def test_missing_genuine_source_field_refuses_before_catalog_io(authenticated, field):
    authenticated.source_custody["canonical_authority"].pop(field)
    with pytest.raises(ValueError, match="allocation_source_metadata_required"):
        await _authenticated_pins(authenticated)
    authenticated.connection.execute.assert_not_awaited()


def _versioned_evidence(authenticated, monkeypatch, version):
    from process import provider_directory_cms_dependency_policy as dependencies
    from process import provider_directory_cms_preparation_policy as stable

    contract = stable.CONTRACT if version == "v2" else stable.DEPENDENCY_CONTRACT
    authenticated.profile.update(contract=contract, fixed_descendant_oids=[1000])
    authenticated.raw["canonical"] = dict(relations=authenticated.raw["canonical"], migration_plan={"complete": True})
    if version == "v3":
        authenticated.raw["canonical"]["dependency_relations"] = [
            dict(oid=999, nspname="snapshot_schema", relname="read_dependency", native={"constraints": []})
        ]
        for routine, function_row in zip(authenticated.raw["routines"], authenticated.rows, strict=True):
            routine["native"]["acl"] = deepcopy(function_row["acl"])
            routine["native"]["namespace_identifier"] = "snapshot_schema"
    authenticated.profile.update(custody._evidence_digests(authenticated.raw))
    collector = AsyncMock(side_effect=lambda *args: deepcopy(authenticated.raw))
    monkeypatch.setattr(stable if version == "v2" else dependencies, "_collect", collector)
    return collector


async def test_v3_namespace_identifier_still_requires_original_protected_digest(authenticated, monkeypatch):
    _versioned_evidence(authenticated, monkeypatch, "v3")
    authenticated.raw["routines"][0]["native"]["namespace_identifier"] = "unbound_schema"
    with pytest.raises(ValueError, match="native_evidence_changed"):
        await _authenticated_pins(authenticated)
    authenticated.lock_inventory.assert_not_awaited()


@pytest.mark.parametrize("version", ["v2", "v3"])
async def test_versioned_allocation_authenticates_dependencies_without_expanding_locks(
    authenticated, monkeypatch, version
):
    collector = _versioned_evidence(authenticated, monkeypatch, version)
    before = deepcopy(authenticated.raw)
    pins, relations = await _authenticated_pins(authenticated)
    assert pins == authenticated.arguments["expected_pins"]
    assert authenticated.raw == before
    assert collector.await_count == 2
    assert all(call.args[-1] == [1000] for call in collector.await_args_list)
    authenticated.arguments.update(expected_pins=pins, relation_oids=relations)
    async with _context(authenticated):
        assert relations == [100, 101, 102, 103, 104, 200, 201, 202]
    locked = authenticated.lock_inventory.await_args.args[1]
    assert {row["oid"] for row in locked} == set(relations)
    for call in authenticated.connection.fetch.await_args_list:
        if call.args[0] == custody._ALLOCATION_ROUTINES_SQL:
            assert call.args[1] == relations and call.args[3] == [200, 201, 202]
