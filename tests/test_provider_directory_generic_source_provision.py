# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""No guard retirement before complete writer, profile and native ACL closure."""

from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_generic_source_provision as provision
from process.provider_directory_generic_candidate import GenericCandidateError
from process.provider_directory_generic_source_policy import GenericSourcePolicy


def _policy():
    return GenericSourcePolicy("source_schema", "source_owner", "source_publisher", ("source_reader",))


@asynccontextmanager
async def _recorded_transaction(events):
    events.append("savepoint")
    try:
        yield
    except BaseException:
        events.append("rollback")
        raise


def test_doctors_maps_require_operator_custody_and_exact_set_relationship_replacement():
    from db.models.provider_directory_entity_identity import (
        ProviderDirectoryCMSDoctorsGroupBinding,
        ProviderDirectoryCMSDoctorsSiteBinding,
    )

    for model, root, target, column in (
        (
            ProviderDirectoryCMSDoctorsGroupBinding,
            provision.CMS_DOCTORS_ROOTS[0],
            provision.MINT_ROOTS[0],
            "organization_id",
        ),
        (ProviderDirectoryCMSDoctorsSiteBinding, provision.CMS_DOCTORS_ROOTS[1], provision.MINT_ROOTS[1], "site_id"),
    ):
        table = model.__table__
        assert model.__runtime_schema_sync__ is False
        assert table.info["managed_immutable_storage"] is True
        assert root in provision.PAYLOAD_ROOTS and root not in provision.PARTITIONED_ROOTS
        assert not table.foreign_keys
        assert table.primary_key
        assert {constraint.__class__.__name__ for constraint in table.constraints} >= {
            "PrimaryKeyConstraint",
            "UniqueConstraint",
            "CheckConstraint",
        }
        assert (root, target, (column,), (column,)) in provision._expected_generic_relationships()


def _coverage_receipt():
    return AsyncMock(return_value={"oid": 42, "layout": "x", "guards": "y"})


@pytest.mark.asyncio
@pytest.mark.parametrize("changed_root", provision.TABLES)
async def test_legacy_heap_root_refuses_before_any_guard_retirement(monkeypatch, changed_root):
    relation_rows = [
        dict(
            relname=name,
            nspname="source_schema",
            relowner=17,
            relpersistence="p",
            relkind="r" if name in {changed_root, provision.WITNESS} else "p",
            partition_key=None if name in {changed_root, provision.WITNESS} else "LIST (dataset_id)",
            parents=[],
            relrowsecurity=False,
            relforcerowsecurity=False,
            direct_acl_closed=True,
            readers_closed=True,
        )
        for name in provision.PAYLOAD_ROOTS
    ]
    monkeypatch.setattr(provision, "storage_roles", AsyncMock(return_value=(17, [18])))
    connection = SimpleNamespace(fetch=AsyncMock(return_value=relation_rows), execute=AsyncMock())
    with pytest.raises(GenericCandidateError, match="partitioned_roots_required"):
        await provision.verify_generic_source_family(connection, _policy(), require_inert=False)
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_missing_complete_verifiers_refuse_before_any_catalog_or_ddl():
    connection = SimpleNamespace(is_in_transaction=lambda: True, fetchrow=AsyncMock(), execute=AsyncMock())
    with pytest.raises(GenericCandidateError, match="complete_closure_verifiers_required"):
        await provision.provision_generic_source_storage(
            connection,
            _policy(),
            verify_role_profile=AsyncMock(),
            verify_writer_closure=None,
            verify_legacy_guards=AsyncMock(),
        )
    connection.fetchrow.assert_not_awaited()
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "actor",
    ({"session": "source_reader", "current": "admin"}, {"session": "source_publisher", "current": "source_publisher"}),
)
async def test_set_role_or_publisher_is_never_the_independent_admin(actor):
    connection = SimpleNamespace(
        is_in_transaction=lambda: True, fetchrow=AsyncMock(return_value=actor), execute=AsyncMock()
    )
    with pytest.raises(GenericCandidateError, match="independent_admin_required"):
        await provision.provision_generic_source_storage(
            connection,
            _policy(),
            verify_role_profile=AsyncMock(),
            verify_writer_closure=AsyncMock(),
            verify_legacy_guards=AsyncMock(),
        )
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_complete_closure_order_and_rejected_writer_rolls_back_before_retirement(monkeypatch):
    """An unqualified writer rolls back closed ACLs while every old guard remains."""
    events, family_rows = (
        [],
        [
            dict(oid=index + 1, nspname="source_schema", relname=name)
            for index, name in enumerate((*provision.PAYLOAD_ROOTS, *provision.CONTROL_ROOTS))
        ],
    )

    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value={"session": "admin", "current": "admin"}),
        fetch=AsyncMock(return_value=family_rows),
        execute=AsyncMock(),
        transaction=lambda: _recorded_transaction(events),
    )
    monkeypatch.setattr(provision, "storage_roles", AsyncMock())
    monkeypatch.setattr(provision, "capture_source_coverage_control", _coverage_receipt())
    monkeypatch.setattr(provision, "capture_checkpoint_controls", AsyncMock(return_value={}))
    monkeypatch.setattr(provision, "_retained_empty_dataset_histories", AsyncMock(return_value=[]))
    for function, event in (
        ("grant_source_admission_controls", "admissiongrant"),
        ("_close_legacy_entrypoint_grants", "functionclose"),
        ("_verify_control_closure", "controlnative"),
    ):
        monkeypatch.setattr(provision, function, AsyncMock(side_effect=lambda *args, event=event: events.append(event)))
    monkeypatch.setattr(
        provision, "_close_relation_grants", AsyncMock(side_effect=lambda *args: events.append("close"))
    )
    monkeypatch.setattr(
        provision, "verify_generic_source_family", AsyncMock(side_effect=lambda *args, **kw: events.append("native"))
    )
    retire = AsyncMock()
    monkeypatch.setattr(provision, "_retire_payload_hooks", retire)
    legacy = AsyncMock(side_effect=lambda *args: events.append("legacy"))
    profile = AsyncMock(side_effect=lambda *args, **kw: events.append("profile"))
    writer = AsyncMock(side_effect=RuntimeError("writer image not activated"))
    with pytest.raises(RuntimeError, match="writer image not activated"):
        await provision.provision_generic_source_storage(
            connection,
            _policy(),
            verify_role_profile=profile,
            verify_writer_closure=writer,
            verify_legacy_guards=legacy,
        )
    assert events == [
        "savepoint",
        "profile",
        "legacy",
        *["close"] * len(family_rows),
        "admissiongrant",
        "functionclose",
        "native",
        "controlnative",
        "rollback",
    ]
    retire.assert_not_awaited()


def _profile_family_rows():
    return [
        dict(oid=index, nspname="source_schema", relname=name)
        for index, name in enumerate((*provision.PAYLOAD_ROOTS, *provision.CONTROL_ROOTS), 1)
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("reject_check", [1, 2])
async def test_complete_profile_before_changes_and_after_retirement_rolls_back_on_drift(monkeypatch, reject_check):
    """Both complete profile checks retain original authority on rejected drift."""
    events, profile_calls = [], []

    @asynccontextmanager
    async def transaction():
        try:
            yield
        except ValueError:
            events.append("rollback")
            raise

    family_rows = _profile_family_rows()
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        transaction=transaction,
        fetchrow=AsyncMock(return_value={"session": "admin", "current": "admin"}),
        fetch=AsyncMock(return_value=family_rows),
        execute=AsyncMock(),
    )

    async def verify(*args, **kwargs):
        profile_calls.append("profile")
        events.append("profile")
        if len(profile_calls) == reject_check:
            raise ValueError("profile drift")

    monkeypatch.setattr(provision, "storage_roles", AsyncMock())
    monkeypatch.setattr(provision, "capture_source_coverage_control", _coverage_receipt())
    monkeypatch.setattr(provision, "capture_checkpoint_controls", AsyncMock(return_value={}))
    monkeypatch.setattr(provision, "_retained_empty_dataset_histories", AsyncMock(return_value=[]))
    monkeypatch.setattr(provision, "_verify_retained_empty_histories", AsyncMock())
    monkeypatch.setattr(provision, "verify_generic_source_family", AsyncMock())
    for name in (
        "_close_relation_grants",
        "_close_legacy_entrypoint_grants",
        "_verify_control_closure",
        "retire_proof_control_hooks",
        "retire_bulk_abandonment_hooks",
        "_retire_payload_hooks",
        "_partition_dataset_payload",
        "_retire_header_entrypoints",
    ):
        monkeypatch.setattr(provision, name, AsyncMock(side_effect=lambda *args, name=name: events.append(name)))
    with pytest.raises(ValueError, match="profile drift"):
        await provision.provision_generic_source_storage(
            connection,
            _policy(),
            verify_role_profile=verify,
            verify_writer_closure=AsyncMock(),
            verify_legacy_guards=AsyncMock(),
        )
    assert events[0] == "profile" and events[-1] == "rollback"
    if reject_check == 1:
        assert events == ["profile", "rollback"]
    else:
        assert events[-3:] == ["_retire_header_entrypoints", "profile", "rollback"]


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "parent", "nonempty", "keys", "name"])
async def test_detached_history_requires_exact_completed_plan_oid_and_native_empty_proof(change):
    tables_by_root = {
        root: {"original_oid": index, "parent_oid": index + 100, "keys": []}
        for index, root in enumerate(provision.TABLES, 1)
    }
    if change == "keys":
        tables_by_root[provision.TABLES[0]]["keys"] = ["unexpected"]
    plan_by_field = {"complete": True, "tables": tables_by_root}

    async def history(statement, oid, root):
        return dict(
            oid=oid,
            relname="unexpected" if change == "name" else "pd_dataset_history_" + str(oid),
            nspname="source_schema",
            relkind="r",
            relispartition=False,
            parents=[],
            current_parent=oid + (101 if change == "parent" else 100),
        )

    connection = SimpleNamespace(
        fetchval=AsyncMock(side_effect=[plan_by_field, *[change != "nonempty"] * len(tables_by_root)]),
        fetchrow=AsyncMock(side_effect=history),
        execute=AsyncMock(),
    )
    if change is not None:
        with pytest.raises(GenericCandidateError, match="legacy_history_"):
            await provision._retained_empty_dataset_histories(connection, _policy())
    else:
        retained_rows = await provision._retained_empty_dataset_histories(connection, _policy())
        assert [history_by_field["source_root"] for history_by_field in retained_rows] == list(provision.TABLES)
        assert connection.execute.await_count == len(tables_by_root)
        statement = connection.fetchval.await_args.args[0]
        assert "NOT EXISTS(SELECT FROM" in statement and "convalidated" in statement
        assert "pg_get_expr(conbin,conrelid,true)='false'" in statement


@pytest.mark.asyncio
async def test_attached_history_is_authenticated_by_exact_parent_without_empty_heap_reclassification():
    tables_by_root = {
        root: {"original_oid": index, "parent_oid": index + 100, "keys": ["history"]}
        for index, root in enumerate(provision.TABLES, 1)
    }

    async def history(statement, oid, root):
        return dict(
            oid=oid,
            relname="history",
            nspname="source_schema",
            relkind="r",
            relispartition=True,
            parents=[oid + 100],
            current_parent=oid + 100,
        )

    connection = SimpleNamespace(
        fetchval=AsyncMock(return_value={"complete": True, "tables": tables_by_root}),
        fetchrow=AsyncMock(side_effect=history),
        execute=AsyncMock(),
    )
    assert await provision._retained_empty_dataset_histories(connection, _policy()) == []
    connection.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_global_root_capture_keeps_complete_family_gate_and_exact_nonpartitioned_layout(monkeypatch):
    from process import provider_directory_generic_source_policy as policy_module

    publisher = SimpleNamespace(connection=object(), policy=_policy())
    monkeypatch.setattr(policy_module, "bound_publisher", lambda: publisher)
    root = provision.CMS_DOCTORS_ROOTS[0]
    family = AsyncMock(return_value=[{"relname": root, "oid": 17}])
    monkeypatch.setattr(provision, "verify_generic_source_family", family)
    from process import provider_directory_generic_source_storage as storage

    proof_by_field = {"name": root, "oid": 17, "owner": 19, "layout_sha256": "a" * 64}
    monkeypatch.setattr(storage, "_layout_proof", lambda row: proof_by_field)
    assert await provision.capture_generic_source_global_roots(publisher, (root,)) == {root: proof_by_field}
    family.assert_awaited_once_with(publisher.connection, publisher.policy)
    with pytest.raises(GenericCandidateError, match="global_roots_changed"):
        await provision.capture_generic_source_global_roots(publisher, (provision.TABLES[0],))
    assert family.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "insert", "function", "source", "columns", "rules", "extra"])
async def test_only_exact_manual_review_native_target_hooks_survive_payload_closure(drift):
    triggers = []
    for name, (
        source_root,
        target_root,
        source_columns,
        target_columns,
    ) in provision._NATIVE_CONTROL_REFERENCES.items():
        for event, function in ((9, "RI_FKey_restrict_del"), (17, "RI_FKey_noaction_upd")):
            triggers.append(
                dict(
                    conname=name,
                    tgtype=event,
                    proname=function,
                    tgisinternal=True,
                    tgenabled="O",
                    function_schema="pg_catalog",
                    contype="f",
                    convalidated=True,
                    condeferrable=False,
                    confupdtype="a",
                    confdeltype="r",
                    confrelid=17 if target_root.endswith("evidence") else 18,
                    nspname="source_schema",
                    relname=source_root,
                    has_rules=False,
                    source_columns=list(source_columns),
                    target_columns=list(target_columns),
                )
            )
    if drift == "extra":
        triggers.append(dict(triggers[0]))
    elif drift is not None:
        key, changed_value = {
            "insert": ("tgtype", 5),
            "function": ("function_schema", "source_schema"),
            "source": ("relname", "other_control"),
            "columns": ("target_columns", ["resource_id"]),
            "rules": ("has_rules", True),
        }[drift]
        triggers[0][key] = changed_value
    connection = SimpleNamespace(fetch=AsyncMock(return_value=triggers))
    family_rows = [
        {"oid": oid, "relname": root, "inert": False}
        for oid, root in ((17, provision.CMS_IDENTITY_ROOTS[1]), (18, provision.CMS_IDENTITY_ROOTS[0]))
    ]
    if drift is None:
        assert await provision._native_control_references(connection, "source_schema", family_rows) == {17, 18}
    else:
        with pytest.raises(GenericCandidateError, match="family_guard_or_foreign_key"):
            await provision._native_control_references(connection, "source_schema", family_rows)
    connection.fetch.assert_awaited_once()


def test_resource_uuid_root_is_managed_and_closed_before_guard_retirement():
    from db.models.provider_directory_resource_identity import ProviderDirectoryResourceIdentity

    model = ProviderDirectoryResourceIdentity
    assert model.__runtime_schema_sync__ is False
    assert model.__table__.info["managed_immutable_storage"] is True
    assert model.__tablename__ == provision.CMS_RESOURCE_IDENTITY_ROOT
    assert model.__tablename__ in provision.GLOBAL_ROOTS
    assert not model.__table__.foreign_keys
    assert model.__table__.primary_key
    assert any(index.unique for index in model.__table__.indexes)
    assert "guard_provider_directory_resource_identity" in provision._PAYLOAD_GUARDS


def test_source_release_fixed_contract_closes_complete_retiring_overload_set():
    from process import provider_directory_generic_source_release_checks as checks

    assert set(provision._RETIRED_FUNCTIONS) <= checks._LEGACY_FUNCTIONS.keys()
    assert len(checks._LEGACY_FUNCTIONS) == 17
    assert all(len(spec[-1]) == 64 for spec in checks._LEGACY_FUNCTIONS.values())
    assert "MAINTAIN" in checks._WRITE_REACH
    assert "USAGE,UPDATE" in checks._SEQUENCE_REACH


def test_source_release_root_and_leaf_invocations_preserve_exact_known_transition_contracts():
    from process import provider_directory_generic_source_release_checks as checks

    root = checks._payload_hooks(provision.TABLES[0])
    leaf = checks._payload_hooks(provision.TABLES[0], history=True)
    assert len(leaf) == 4
    assert set(leaf) < root.keys()
    assert root["tin_npi_connector_dataset_resource_update_guard"] == checks._hook(
        "guard_tin_npi_connector_dataset_resource", 16, mode="A", old="old_rows", new="new_rows"
    )
    assert root["pd_rg_dep_resource_row"] == checks._hook(
        "guard_provider_directory_rooted_graph_dependency", 28, mode="A"
    )
    assert root["pd_trr_resource_truncate"] == checks._hook(
        "guard_provider_directory_terminal_root_retirement_child", 34, mode="A"
    )
    assert checks._payload_hooks(provision.TABLES[4], sealed_always=True)["pd_dataset_sealed"][2] == "A"
    assert checks._payload_hooks(provision.TABLES[4])["pd_dataset_sealed"][2] == "O"


def _release_hook_row():
    return dict(
        proname="known_guard",
        tgtype=4,
        tgenabled="O",
        tgoldtable=None,
        tgnewtable="new_rows",
        tgargs=b"",
        update_columns=[],
        unconditional=True,
        tgdeferrable=False,
        tginitdeferred=False,
        tgconstraint=0,
        tgparentid=0,
        tgnargs=0,
        native_trigger=dict(tgconstrrelid="0", tgconstrindid="0", tgisinternal=False, tgfoid="17"),
        native_function=dict(oid="17"),
    )


def _covered_truncate_statements(monkeypatch, filename, function):
    from tests.test_source_validator_argument_types_migration import _load

    migration = _load(filename)
    statements = []
    monkeypatch.setattr(migration, "op", SimpleNamespace(execute=statements.append))
    getattr(migration, function)('"source_schema"')
    return "\n".join(statements)


def test_source_release_covered_truncate_matches_migration_root_and_invocation_contract(monkeypatch):
    import re

    from process import provider_directory_generic_source_release_checks as checks

    sql = _covered_truncate_statements(
        monkeypatch, "20260930020000_cms_npd_serving_coverage.py", "_create_truncate_guards"
    )
    roots = set(re.findall(r'BEFORE TRUNCATE ON "source_schema"\.(\w+) FOR EACH STATEMENT', sql))
    assert roots == {
        "provider_directory_source",
        "provider_directory_api_endpoint",
        "provider_directory_endpoint_dataset",
        "provider_directory_dataset_resource",
        "provider_directory_entity_source_binding",
        "provider_directory_entity_release_evidence",
        "provider_directory_resource_identity",
        "provider_directory_insurance_network_source_binding",
        "provider_directory_insurance_network_plan_evidence",
        "provider_directory_cms_serving_coverage",
    }
    for root in roots & set(provision.PAYLOAD_ROOTS):
        assert checks._payload_hooks(root)["cms_npd_covered_truncate_forbidden"] == (
            "guard_cms_npd_covered_truncate",
            34,
            "O",
            None,
            None,
            b"",
            (),
        )
    assert set(checks._payload_hooks("provider_directory_resource_identity")) == {
        "pd_resource_identity_immutable",
        "pd_resource_identity_no_truncate",
        "cms_npd_covered_truncate_forbidden",
    }


def _covered_truncate_native_row(monkeypatch):
    import re

    from tests.test_provider_directory_source_callable_custody import _entry

    sql = _covered_truncate_statements(
        monkeypatch, "20260930090000_cms_npd_candidate_coverage.py", "_freeze_candidate_inputs"
    )
    body = re.search(r"guard_cms_npd_covered_truncate\(\).*?AS \$\$(.*?)\$\$", sql, re.S)[1]
    definition = _entry("guard_provider_directory_terminal_root_retirement_child()")["definition"]
    definition.update(proname="guard_cms_npd_covered_truncate", prosrc=body, prosecdef=False)
    row = _release_hook_row()
    row.update(
        proname=definition["proname"],
        prosrc=body,
        proowner=103,
        nspname="source_schema",
        lanname="plpgsql",
        provolatile="v",
        prosecdef=False,
        proconfig=["search_path=pg_catalog"],
        native_function=definition,
        function_acl=[[103, 103, "EXECUTE", False], [0, 103, "EXECUTE", False]],
    )
    return row


def test_source_release_covered_truncate_authenticates_native_body_and_acl(monkeypatch):
    from process import provider_directory_generic_source_release_checks as checks
    from process.network_fhir_source_custody import FHIRSourceCustodyError

    row = _covered_truncate_native_row(monkeypatch)
    checks._require_guard_function(row, "source_schema", 201, {103})
    row["prosrc"] += " RETURN NULL;"
    with pytest.raises(FHIRSourceCustodyError, match="fhir_source_custody_unavailable"):
        checks._require_guard_function(row, "source_schema", 201, {103})
    row = _covered_truncate_native_row(monkeypatch)
    row["function_acl"].append([999, 103, "EXECUTE", False])
    with pytest.raises(GenericCandidateError, match="legacy_guard_definition_changed"):
        checks._require_guard_function(row, "source_schema", 201, {103})


@pytest.mark.parametrize(
    "changed", ("enabled", "arguments", "transition", "condition", "constraint", "parent", "function")
)
def test_source_release_rejects_real_invocation_substitution(changed):
    from process import provider_directory_generic_source_release_checks as checks

    row = _release_hook_row()
    if changed == "enabled":
        row["tgenabled"] = "D"
    if changed == "arguments":
        row["tgargs"] = b"unexpected\0"
    if changed == "transition":
        row["tgnewtable"] = "other_rows"
    if changed == "condition":
        row["unconditional"] = False
    if changed == "constraint":
        row["tgconstraint"] = 23
    if changed == "parent":
        row["tgparentid"] = 32
    if changed == "function":
        row["native_trigger"]["tgfoid"] = "99"
    with pytest.raises(GenericCandidateError, match="legacy_invocation_changed"):
        checks._require_hook(row, checks._hook("known_guard", 4, new="new_rows"))


@pytest.mark.asyncio
async def test_source_release_checker_refuses_missing_admin_or_task_before_any_retirement():
    from process import provider_directory_generic_source_release_checks as checks

    connection = SimpleNamespace(is_in_transaction=lambda: False, fetchrow=AsyncMock())
    with pytest.raises(GenericCandidateError, match="explicit_policy_required"):
        await checks.source_release_checks(
            connection, _policy(), migration_role="migrator", namespace_owner_role="namespace"
        )
    connection.fetchrow.assert_not_awaited()


def test_source_release_specs_match_real_templates(monkeypatch):
    import re

    from db.migration_provider_directory_dataset_candidates import candidate_functions
    from process import provider_directory_generic_source_release_checks as checks
    from process.provider_directory_source_callable_custody import _body_digest
    from tests.test_source_validator_argument_types_migration import _load

    statements = [candidate_functions("snapshot_schema")]
    identity = _load("20260930010000_provider_directory_resource_identity.py")
    monkeypatch.setattr(identity, "_schema", lambda: '"snapshot_schema"')
    monkeypatch.setattr(identity, "op", SimpleNamespace(execute=statements.append))
    identity.upgrade()
    pattern = re.compile(
        r'CREATE FUNCTION "snapshot_schema"\."?(\w+)"?\((.*?)\)\s+RETURNS\s+(.*?)\s+LANGUAGE\s+(.*?)AS \$(\w*)\$(.*?)\$\5\$',
        re.S,
    )
    actual, bodies = {}, {}
    for name, arguments, returns, options, _, body in pattern.findall("\n".join(statements)):
        parameters = (
            tuple(tuple(argument.strip().split(None, 1)) for argument in arguments.split(",")) if arguments else ()
        )
        signature = name + "(" + ",".join(kind for _, kind in parameters) + ")"
        bodies[signature] = body
        actual[signature] = (
            parameters,
            returns,
            (),
            options.split()[0],
            "i" if "IMMUTABLE" in options else "s" if "STABLE" in options else "v",
            "STRICT" in options,
            "s" if "PARALLEL SAFE" in options else "u",
            "SECURITY DEFINER" in options,
            _body_digest(body, "snapshot_schema"),
        )
    assert actual == checks._LEGACY_FUNCTIONS
    from process import provider_directory_source_callable_custody as custody
    from tests.test_provider_directory_source_callable_custody import _entry

    monkeypatch.setattr(custody, "_CONTRACTS", dict(custody._CONTRACTS, **actual))
    monkeypatch.setattr(custody, "_TYPE_OIDS", dict(custody._TYPE_OIDS, **{"text[]": "1009"}))
    for signature, spec in actual.items():
        entry = _entry(signature, owner=103)
        entry["definition"]["prosrc"] = bodies[signature]
        checks._require_native_function(entry["definition"], spec, entry["language"], "snapshot_schema", 201, {103})


def _release_native_hooks(checks, relations, definitions, *, header=False):
    hooks = []
    for relation in relations:
        if header:
            expected_by_name = {
                "pd_dataset_storage": checks._hook("prepare_pd_generic_dataset_storage", 5),
                "pd_dataset_generic_finalization": checks._hook(
                    "guard_pd_generic_dataset_finalization", 17, columns=("status",)
                ),
            }
        elif relation["root"] in provision.PAYLOAD_ROOTS:
            expected_by_name = checks._payload_hooks(relation["root"], history=relation["relname"] != relation["root"])
        else:
            continue
        for name, expected in expected_by_name.items():
            definition = definitions.setdefault(expected[0], dict(oid=str(len(definitions) + 1000)))
            native_hook = _release_hook_row()
            native_hook.update(
                tgrelid=relation["oid"],
                tgname=name,
                proname=expected[0],
                tgtype=expected[1],
                tgenabled=expected[2],
                tgoldtable=expected[3],
                tgnewtable=expected[4],
                tgargs=expected[5],
                update_columns=list(expected[6]),
                tgnargs=expected[5].count(b"\0"),
                native_function=definition,
                native_trigger=dict(tgconstrrelid="0", tgconstrindid="0", tgisinternal=False, tgfoid=definition["oid"]),
            )
            hooks.append(native_hook)
    return hooks


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", (None, "disabled", "extra"))
async def test_source_release_whole_legacy_verifier_keeps_complete_family_and_invocations(monkeypatch, changed):
    """Pure catalog transport exercises the whole callback; Native authority remains separate."""
    from process import provider_directory_generic_source_release_checks as checks

    roots = tuple(dict.fromkeys((*provision.PAYLOAD_ROOTS, *provision.CONTROL_ROOTS)))
    native_relations = [
        dict(oid=index, nspname="source_schema", relname=root, root=root, relowner=101, relkind="r", parents=[])
        for index, root in enumerate(roots, 1)
    ]
    histories = [
        dict(
            oid=600 + index,
            nspname="source_schema",
            relname="pd_dataset_history_" + str(600 + index),
            root=root,
            relowner=101,
            relkind="r",
            parents=[index + 1],
        )
        for index, root in enumerate(provision.TABLES)
    ]
    native_relations.extend(histories)
    definition_by_name = {}
    payload_hooks = _release_native_hooks(checks, native_relations, definition_by_name)
    header = next(relation for relation in native_relations if relation["root"] == checks.HEADER)
    header_hooks = _release_native_hooks(checks, [header], definition_by_name, header=True)
    if changed == "disabled":
        payload_hooks[0]["tgenabled"] = "D"
    if changed == "extra":
        payload_hooks.append({**payload_hooks[0], "tgname": "unapproved_extra_guard"})
    plan_by_field = dict(
        tables={root: dict(original_oid=600 + index, keys=["dataset"]) for index, root in enumerate(provision.TABLES)}
    )

    async def fetch(statement, selected, *_arguments):
        if statement == checks._FAMILY:
            return native_relations
        assert statement == checks._HOOKS
        return header_hooks if selected == [header["oid"]] else payload_hooks

    connection = SimpleNamespace(fetch=AsyncMock(side_effect=fetch))
    policy = GenericSourcePolicy("source_schema", "source_owner", "publisher", ("builder", "reader"), ("builder",))
    verifier = checks._SourceReleaseChecks(
        connection, policy, {}, dict(role_graph={}, functions={}, roles={}, schema_oid=201)
    )
    verifier._context = AsyncMock()
    verifier._candidate_functions = AsyncMock(
        return_value={int(definition["oid"]): definition for definition in definition_by_name.values()}
    )
    monkeypatch.setattr(checks, "_completed_legacy_dataset_plan", AsyncMock(return_value=plan_by_field))
    if changed is None:
        await verifier.verify_legacy_guards(connection, policy, native_relations)
        assert verifier.family.keys() == {relation["oid"] for relation in native_relations}
        assert {600 + index for index in range(6)} <= verifier.family.keys()
    else:
        with pytest.raises(GenericCandidateError, match="legacy_.*changed"):
            await verifier.verify_legacy_guards(connection, policy, native_relations)
        assert verifier.family is None
