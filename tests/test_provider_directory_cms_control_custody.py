# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fixed control custody rejects changed authority without blocking ordinary DML."""

import asyncio
import importlib
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_control_custody as custody
from tests.test_source_validator_argument_types_migration import (
    _authentic_functions,
    _load,
    _migration,
    _parse_function,
)


def _columns(model):
    return [
        dict(name=name, type=kind, required=required, identity="", generated="")
        for name, (kind, required) in custody._model_columns(model).items()
    ]


def _legacy_control_connection():
    return SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(
            return_value=dict(session="administrator", current="administrator", backend=1, xid="123", native_admin=True)
        ),
        fetch=AsyncMock(return_value=[dict(oid=901)]),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["connection", "ended", "proof_locks", "unissued"])
async def test_control_legacy_observation_rechecks_issued_proof_scope(fault):
    from tests.test_provider_directory_source_builder_delegation import _receipt

    policy = SimpleNamespace(owner_role="owner", publisher_role="publisher", runtime_roles=("builder", "reader"))
    connection = _legacy_control_connection()
    actor = (await custody._admin_identity(connection, policy), asyncio.current_task())
    proof = await custody._control_delegation(connection, policy, _receipt(), True)
    match fault:
        case "connection":
            connection = _legacy_control_connection()
        case "ended":
            connection.is_in_transaction = lambda: False
        case "proof_locks":
            proof = replace(proof, locked_relations=frozenset())
        case "unissued":
            proof = replace(proof, legacy_observation=None)

    connection.fetchrow.reset_mock()
    connection.fetch.reset_mock()
    with pytest.raises(ValueError, match="source_builder_delegation_legacy_(context_changed|issuer_required)"):
        await custody._require_legacy_observation(connection, policy, actor, proof)
    connection.fetchrow.assert_not_awaited()
    connection.fetch.assert_not_awaited()


@pytest.mark.asyncio
async def test_control_legacy_observation_preserves_actor_from_before_proof_issuance():
    from tests.test_provider_directory_source_builder_delegation import _receipt

    policy = SimpleNamespace(owner_role="owner", publisher_role="publisher", runtime_roles=("builder", "reader"))
    connection = _legacy_control_connection()
    actor = (await custody._admin_identity(connection, policy), asyncio.current_task())
    connection.fetchrow.return_value = {**actor[0], "xid": "124"}
    proof = await custody._control_delegation(connection, policy, _receipt(), True)

    with pytest.raises(ValueError, match="admin_transaction_changed"):
        await custody._require_legacy_observation(connection, policy, actor, proof)


@pytest.mark.asyncio
@pytest.mark.parametrize("field,value", [("relrowsecurity", True), ("relkind", "v"), ("relowner", "19")])
async def test_source_history_header_refuses_unsafe_native_reads(field, value):
    from db.models.system import ProviderDirectoryEndpointDataset

    native_by_field = dict(
        relname=ProviderDirectoryEndpointDataset.__tablename__,
        oid="71",
        relowner="17",
        relkind="r",
        relpersistence="p",
        relrowsecurity=False,
        relforcerowsecurity=False,
    )
    native_by_field[field] = value
    catalog_by_field = dict(classes=[native_by_field], rules=None, policies=None, inheritance=None)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=catalog_by_field), fetch=AsyncMock())
    with pytest.raises(ValueError, match="source_header_changed"):
        await custody._capture_source_history_header(
            connection, SimpleNamespace(schema="snapshot_schema"), ProviderDirectoryEndpointDataset, (17, 19)
        )
    connection.fetch.assert_not_awaited()


def _writer_replay_catalog(root, adopted):
    from process.provider_directory_cms_control_privileges import READ_COLUMN_GRANTS

    entry = _adopted_relation(root, _relation(root)) if adopted else _relation(root)
    catalog = entry["catalog"]
    number_by_name = {attribute["attname"]: attribute["attnum"] for attribute in catalog["attributes"]}
    tables, selected = custody._writer_rights(root, number_by_name)
    catalog["access"] = [
        dict(
            role=19,
            superuser=False,
            owner_reach=False,
            builtin=False,
            table=sorted(tables),
            columns=[
                [
                    number,
                    [
                        "SELECT",
                        *(right for right, names in selected.items() if name in names),
                    ],
                ]
                for name, number in number_by_name.items()
            ],
        )
    ]
    if adopted:
        catalog["column_acl"].extend(
            [number_by_name[name], role, 17, "SELECT", False]
            for name in READ_COLUMN_GRANTS.get(root, ())
            for role in (21, 23)
        )
    return entry


def _writer_replay(monkeypatch, root, *, adopted=True):
    """Isolate emitted writer SQL; genuine capture and publication remain Native gates."""
    from process import provider_directory_source_acquisition_custody as acquisition
    from process import provider_directory_source_callable_custody as source
    from process import provider_directory_source_cohort_custody as cohort
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    policy = GenericSourcePolicy("snapshot_schema", "owner", "publisher", ("builder", "reader"), ("builder",))
    before_by_field = dict(
        schema=policy.schema,
        owner=17,
        builder=19,
        migration=29,
        relations={root: _writer_replay_catalog(root, adopted)},
        role_graph=dict(
            roles=[
                dict(rolname=name, oid=oid)
                for name, oid in (
                    ("owner", 17),
                    ("builder", 19),
                    ("publisher", 21),
                    ("reader", 23),
                    ("migration", 29),
                )
            ]
        ),
        source=dict(roles=dict(namespace_owner=29)),
        acquisition=dict(cohort={}),
    )
    monkeypatch.setattr(custody, "_admin_identity", AsyncMock(return_value=("admin", "admin")))
    monkeypatch.setattr(custody, "_enable_source_endpoint_fk", AsyncMock())
    monkeypatch.setattr(acquisition, "adopt_source_acquisition_custody", AsyncMock())
    monkeypatch.setattr(acquisition, "canonical_acquisition_checks", AsyncMock(return_value={}))
    monkeypatch.setattr(source, "adopt_source_callable_custody", AsyncMock())
    monkeypatch.setattr(cohort, "retire_cohort_intake_guards", AsyncMock())
    _writer_delegation_fixture(before_by_field, root)
    return (
        SimpleNamespace(is_in_transaction=lambda: True, execute=AsyncMock()),
        policy,
        before_by_field,
    )


def _writer_delegation_fixture(before_by_field, root):
    """Supply complete native role/access metadata to the existing SQL replay fixture."""
    graph = before_by_field["role_graph"]
    graph["memberships"] = []
    for role in graph["roles"]:
        role.update(
            rolcanlogin=role["oid"] in {19, 21, 23},
            rolsuper=False,
            rolcreaterole=False,
            rolcreatedb=False,
            rolreplication=False,
            rolbypassrls=False,
        )
    before_by_field["source"].update(
        roles=dict(owner=17, publisher=21, builder=19, namespace_owner=29, migration=29),
        role_graph=graph,
        functions={},
    )
    access = before_by_field["relations"][root]["catalog"]["access"]
    access[0]["builder_reach"] = True
    access.extend(
        dict(
            role=role["oid"],
            superuser=False,
            owner_reach=role["oid"] in {17, 21, 29},
            builder_reach=False,
            builtin=False,
            table=["SELECT"],
            columns=[[number, ["SELECT"]] for number, _ in access[0]["columns"]],
        )
        for role in graph["roles"]
        if role["oid"] != 19
    )


@pytest.mark.parametrize("root", custody.CONTROL_ROOTS)
async def test_exact_owned_writer_replay_preserves_builder_and_reader_acl_order(monkeypatch, root):
    connection, policy, before = _writer_replay(monkeypatch, root)
    original = deepcopy(before)
    assert await custody.adopt_cms_control_custody(connection, policy, before) == {}
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert not any('"builder"' in statement for statement in statements)
    assert len(statements) == 2 and statements[0].startswith("ALTER TABLE ")
    assert statements[1].startswith("REVOKE SELECT(") and statements[1].endswith(' FROM "owner"')
    assert before == original
    custody._enable_source_endpoint_fk.assert_awaited_once_with(connection, policy, before)


@pytest.mark.parametrize(
    "change",
    (
        "table_write",
        "missing_column",
        "reader_write",
        "grant_option",
        "effective_write",
    ),
)
async def test_owned_writer_replay_refuses_any_inexact_native_writer_contract(monkeypatch, change):
    root = custody.CONTROL_ROOTS[0]
    connection, policy, before = _writer_replay(monkeypatch, root)
    catalog = before["relations"][root]["catalog"]
    match change:
        case "table_write":
            catalog["table_acl"].append([19, 17, "TRIGGER", False])
        case "missing_column":
            catalog["column_acl"].pop(0)
        case "reader_write":
            catalog["column_acl"].append([1, 23, 17, "UPDATE", False])
        case "grant_option":
            catalog["column_acl"][0] = [*catalog["column_acl"][0][:-1], True]
        case "effective_write":
            catalog["access"][0]["table"].append("TRIGGER")
    with pytest.raises(ValueError, match="relation_acl_changed|writer_effective_acl_changed"):
        await custody.adopt_cms_control_custody(connection, policy, before)
    assert all(call.args[0].startswith("ALTER TABLE ") for call in connection.execute.await_args_list)


async def test_builder_owned_initial_cut_keeps_the_complete_writer_closure(monkeypatch):
    connection, policy, before = _writer_replay(monkeypatch, custody.CONTROL_ROOTS[0], adopted=False)
    await custody.adopt_cms_control_custody(connection, policy, before)
    statements = [call.args[0] for call in connection.execute.await_args_list]
    assert any(
        statement.startswith("REVOKE ALL ON TABLE ") and statement.endswith(' FROM "builder"')
        for statement in statements
    )
    assert any(
        statement.startswith("REVOKE SELECT(") and statement.endswith(' FROM "builder","owner"')
        for statement in statements
    )
    assert any(
        statement.startswith("GRANT INSERT(") and statement.endswith(' TO "builder"') for statement in statements
    )
    assert any(
        statement.startswith("GRANT UPDATE(") and statement.endswith(' TO "builder"') for statement in statements
    )


@pytest.mark.parametrize("field", ("owner", "builder"))
async def test_writer_replay_requires_exact_declared_native_role_oids(monkeypatch, field):
    connection, policy, before = _writer_replay(monkeypatch, custody.CONTROL_ROOTS[0])
    before[field] = 99
    with pytest.raises(ValueError, match="adoption_roles_changed"):
        await custody.adopt_cms_control_custody(connection, policy, before)
    connection.execute.assert_not_awaited()


def _indexes(model):
    from sqlalchemy import PrimaryKeyConstraint, UniqueConstraint

    index_rows = []
    for number, constraint in enumerate(model.__table__.constraints):
        if isinstance(constraint, (PrimaryKeyConstraint, UniqueConstraint)):
            index_rows.append(
                dict(
                    name=str(number),
                    method="btree",
                    unique=True,
                    primary=isinstance(constraint, PrimaryKeyConstraint),
                    ready=True,
                    plain=True,
                    columns=[column.name for column in constraint.columns],
                )
            )
    for specification in getattr(model, "__my_additional_indexes__", ()):
        index_rows.append(
            dict(
                name=specification["name"],
                method="btree",
                unique=False,
                primary=False,
                ready=True,
                plain=True,
                columns=list(specification["index_elements"]),
            )
        )
    return index_rows


def test_current_typed_source_graph_has_exactly_eleven_authentic_contracts():
    contracts = _migration()._CONTRACTS
    assert len(contracts) == 11 and len(custody._CALLABLES) == 13
    for name, arguments, kinds, returns, language, volatility, strict, parallel, _before, digest in contracts:
        assert custody._CALLABLES[name] == (
            arguments,
            kinds,
            returns,
            language,
            volatility,
            strict,
            parallel,
            True,
            digest,
        )
    assert custody.CONTROL_ROOTS[1] == "provider_directory_api_endpoint"


@pytest.mark.parametrize(
    "filename,creator",
    [
        ("20260730110000_provider_directory_profile_delta.py", "_create_delta_receipt_table"),
        ("20260929040000_reference_source_generation_guard.py", "_revision_function"),
    ],
)
def test_delta_and_revision_contracts_follow_authentic_compiled_ddl(monkeypatch, filename, creator):
    migration, statements = _load(filename), []
    monkeypatch.setattr(
        migration,
        "op",
        SimpleNamespace(
            create_table=lambda *args, **kwargs: None,
            create_index=lambda *args, **kwargs: None,
            execute=statements.append,
        ),
    )
    getattr(migration, creator)("snapshot_schema")
    definitions = [sql for sql in statements if sql.strip().upper().startswith("CREATE FUNCTION")]
    assert len(definitions) == 1
    # Dollar-quote tags do not change the compiled body; the shared parser expects a named tag.
    name, attributes = _parse_function(definitions[0].replace("$$", "$guard$"))
    arguments, kinds, returns, language, volatility, strict, parallel, body = attributes
    assert custody._CALLABLES[name] == (
        arguments,
        kinds,
        returns,
        language,
        volatility,
        strict,
        parallel,
        "SECURITY DEFINER" in definitions[0],
        custody._body_digest(body, "snapshot_schema"),
    )


def _authentic_entry(name):
    arguments, kinds, returns, language, volatility, strict, parallel, body = _authentic_functions()[name]
    for caller, old, new, count in _migration()._calls("snapshot_schema"):
        if caller == name:
            assert body.count(old) == count
            body = body.replace(old, new)
    definition_by_field = dict(
        oid="41",
        proname=name,
        pronamespace="44",
        proowner="19",
        prokind="f",
        prorettype=custody._TYPE_OIDS[returns],
        pronargs=len(kinds.split(",")) if kinds else 0,
        proargtypes=[custody._TYPE_OIDS[kind] for kind in kinds.split(",")] if kinds else [],
        proargnames=arguments.split(",") if arguments else None,
        proargmodes=None,
        proallargtypes=None,
        proargdefaults=None,
        pronargdefaults=0,
        provariadic="0",
        prosecdef=True,
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
        proconfig=["search_path=pg_catalog"],
        prosrc=body,
    )
    return dict(definition=definition_by_field, language=language)


@pytest.mark.parametrize("name", [entry[0] for entry in _migration()._CONTRACTS])
def test_exact_latest_authentic_callable_bodies_and_types_are_required(name):
    custody._require_callable_definition(_authentic_entry(name), "snapshot_schema", 44)


@pytest.mark.parametrize(
    "field,value",
    [
        ("prosrc", "changed"),
        ("prosecdef", False),
        ("proconfig", []),
        ("proargdefaults", "malicious"),
        ("provariadic", "25"),
        ("proargtypes", ["1043"]),
        ("proleakproof", True),
    ],
)
def test_callable_body_default_overload_security_and_settings_drift_refuses(field, value):
    entry = _authentic_entry("provider_directory_reviewed_subset_activation_valid")
    entry["definition"][field] = value
    with pytest.raises(ValueError, match="callable_definition_changed"):
        custody._require_callable_definition(entry, "snapshot_schema", 44)


def _source_endpoint_foreign_key(relations_by_root=None):
    """Capture both native representations of the original alias constraint."""
    relations_by_root = relations_by_root or {
        root: dict(
            catalog=dict(
                classes=[dict(relname=root, oid=str(number))],
                attributes=[dict(attrelid=str(number), attname="endpoint_id", attnum=position)],
                triggers=[],
            )
        )
        for root, number, position in zip(custody.CONTROL_ROOTS[:2], (41, 42), (8, 1), strict=True)
    }
    natives = [relations_by_root[root]["catalog"]["classes"][0] for root in custody.CONTROL_ROOTS[:2]]
    columns = [
        next(
            field["attnum"]
            for field in relations_by_root[root]["catalog"]["attributes"]
            if field["attname"] == "endpoint_id"
        )
        for root in custody.CONTROL_ROOTS[:2]
    ]
    constraint_by_field = dict(
        oid="81",
        conname="provider_directory_source_endpoint_id_fkey",
        contype="f",
        conrelid=natives[0]["oid"],
        confrelid=natives[1]["oid"],
        conkey=[columns[0]],
        confkey=[columns[1]],
        confmatchtype="s",
        confupdtype="a",
        confdeltype="n",
        confdelsetcols=None,
        condeferrable=False,
        condeferred=False,
        convalidated=True,
        conenforced=True,
    )
    hooks = _source_endpoint_hooks(natives)
    for root, native in zip(custody.CONTROL_ROOTS[:2], natives, strict=True):
        relations_by_root[root]["catalog"]["triggers"].extend(
            deepcopy(hook) for hook in hooks if hook["tgrelid"] == native["oid"]
        )
    return [
        dict(
            constraint=constraint_by_field,
            hooks=hooks,
            child=[int(natives[0]["oid"]), "snapshot_schema", custody.CONTROL_ROOTS[0], 19],
            parent=[int(natives[1]["oid"]), "snapshot_schema", custody.CONTROL_ROOTS[1], 19],
        )
    ], relations_by_root


def _source_endpoint_hooks(natives):
    return [
        dict(
            oid=str(101 + number),
            tgname=f"RI_ConstraintTrigger_{101 + number}",
            tgconstraint="81",
            tgrelid=native["oid"],
            tgtype=mask,
            tgisinternal=True,
            tgenabled="O",
            tgfoid="901",
        )
        for number, (native, mask) in enumerate(((natives[0], 5), (natives[0], 17), (natives[1], 9), (natives[1], 17)))
    ]


def _set_source_endpoint_modes(edges, relations_by_root, mode):
    for edge in edges:
        if edge["constraint"]["conname"] == "provider_directory_source_endpoint_id_fkey":
            for hook in edge["hooks"]:
                hook["tgenabled"] = mode
    for root in custody.CONTROL_ROOTS[:2]:
        for hook in relations_by_root[root]["catalog"]["triggers"]:
            if hook["tgconstraint"] == "81":
                hook["tgenabled"] = mode


@pytest.mark.parametrize(
    "change", [None, "missing", "duplicate", "parent", "columns", "action", "deferrable", "enforced"]
)
def test_source_api_native_foreign_key_is_required_with_its_original_semantics(change):
    edges, relations_by_root = _source_endpoint_foreign_key()
    match change:
        case "missing":
            edges.clear()
        case "duplicate":
            edges.append(deepcopy(edges[0]))
        case "parent":
            edges[0]["constraint"]["confrelid"] = "99"
        case "columns":
            edges[0]["constraint"]["conkey"] = [9]
        case "action":
            edges[0]["constraint"]["confdeltype"] = "a"
        case "deferrable":
            edges[0]["constraint"]["condeferrable"] = True
        case "enforced":
            edges[0]["constraint"]["conenforced"] = False
    if change is None:
        custody._require_source_endpoint_fk(edges, relations_by_root, legacy=True)
    else:
        with pytest.raises(ValueError, match="cms_control_source_endpoint_fk"):
            custody._require_source_endpoint_fk(edges, relations_by_root, legacy=True)


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("mode", ["A", "O", "R", "D"])
def test_source_api_fk_requires_always_after_adoption(legacy, mode):
    edges, relations_by_root = _source_endpoint_foreign_key()
    _set_source_endpoint_modes(edges, relations_by_root, mode)
    if mode == "A" or legacy and mode == "O":
        assert custody._require_source_endpoint_fk(edges, relations_by_root, legacy=legacy) is edges[0]
    else:
        with pytest.raises(ValueError, match="source_endpoint_fk_hooks_changed"):
            custody._require_source_endpoint_fk(edges, relations_by_root, legacy=legacy)


@pytest.mark.parametrize(
    "change", ["missing", "extra", "duplicate", "external", "constraint", "relation", "event", "catalog"]
)
async def test_source_api_fk_transition_refuses_inexact_captured_hooks_before_sql(change):
    edges, relations_by_root = _source_endpoint_foreign_key()
    hooks = edges[0]["hooks"]
    match change:
        case "missing":
            hooks.pop()
        case "extra":
            hooks.append({**hooks[0], "oid": "999"})
        case "duplicate":
            hooks[1]["oid"] = hooks[0]["oid"]
        case "external":
            hooks[0]["tgisinternal"] = False
        case "constraint":
            hooks[0]["tgconstraint"] = "999"
        case "relation":
            hooks[0]["tgrelid"] = "999"
        case "event":
            hooks[0]["tgtype"] = 9
        case "catalog":
            relations_by_root[custody.CONTROL_ROOTS[0]]["catalog"]["triggers"][0]["tgfoid"] = "999"
    connection = SimpleNamespace(execute=AsyncMock())
    with pytest.raises(ValueError, match="source_endpoint_fk"):
        await custody._enable_source_endpoint_fk(
            connection, SimpleNamespace(schema="snapshot_schema"), dict(foreign=edges, relations=relations_by_root)
        )
    connection.execute.assert_not_awaited()


async def test_source_api_fk_transition_changes_only_original_hooks_and_is_replayable():
    edges, relations_by_root = _source_endpoint_foreign_key()
    before_by_field = dict(foreign=edges, relations=relations_by_root)
    original = deepcopy(before_by_field)
    connection = SimpleNamespace(execute=AsyncMock())
    await custody._enable_source_endpoint_fk(connection, SimpleNamespace(schema="snapshot_schema"), before_by_field)
    assert [call.args[0] for call in connection.execute.await_args_list] == [
        f'ALTER TABLE "snapshot_schema"."{root}" ENABLE ALWAYS TRIGGER "RI_ConstraintTrigger_{number}"'
        for root, number in zip(
            (custody.CONTROL_ROOTS[0], custody.CONTROL_ROOTS[0], custody.CONTROL_ROOTS[1], custody.CONTROL_ROOTS[1]),
            range(101, 105),
            strict=True,
        )
    ]
    assert before_by_field == original
    _set_source_endpoint_modes(edges, relations_by_root, "A")
    connection.execute.reset_mock()
    await custody._enable_source_endpoint_fk(connection, SimpleNamespace(schema="snapshot_schema"), before_by_field)
    connection.execute.assert_not_awaited()


@pytest.mark.parametrize("model", list(custody._models().values()), ids=lambda model: model.__name__)
@pytest.mark.asyncio
async def test_installed_managed_mutable_models_require_no_schema_ddl(model):
    assert model.__runtime_schema_sync__ is False
    assert not model.__table__.info.get("managed_immutable_storage")
    database = SimpleNamespace(all=AsyncMock(side_effect=[_columns(model), _indexes(model)]), status=AsyncMock())
    await custody.require_cms_control_model_layout(database, model, "snapshot_schema")
    assert database.all.await_count == 2
    database.status.assert_not_awaited()


@pytest.mark.parametrize(
    "drift", ["missing", "extra", "type", "nullability", "identity", "generated", "index", "wrong_index"]
)
@pytest.mark.asyncio
async def test_runtime_drift_refuses_instead_of_altering_tables_or_indexes(drift):
    model = custody._models()[custody.CONTROL_ROOTS[0]]
    columns, indexes = _columns(model), _indexes(model)
    if drift == "missing":
        columns.pop()
    elif drift == "extra":
        columns.append({**columns[-1], "name": "unrelated"})
    elif drift in {"type", "nullability", "identity", "generated"}:
        columns[0][{"nullability": "required"}.get(drift, drift)] = False if drift == "nullability" else "changed"
    elif drift == "index":
        indexes.clear()
    else:
        indexes[-1]["columns"] = ["source_id"]
    database = SimpleNamespace(all=AsyncMock(side_effect=[columns, indexes]), status=AsyncMock())
    with pytest.raises(ValueError, match="administrator_(shape|index)_required"):
        await custody.require_cms_control_model_layout(database, model, "snapshot_schema")
    database.status.assert_not_awaited()


@pytest.mark.asyncio
async def test_fhir_runtime_guard_is_read_only_and_existing_upsert_remains_usable(monkeypatch):
    directory = importlib.import_module("process.provider_directory_fhir")

    model = custody._models()[custody.CONTROL_ROOTS[0]]
    check = AsyncMock()
    monkeypatch.setattr(custody, "require_cms_control_model_layout", check)
    monkeypatch.setattr(directory.db, "status", AsyncMock(side_effect=AssertionError("runtime DDL")))
    await directory._ensure_provider_directory_model_columns(model, "snapshot_schema")
    check.assert_awaited_once_with(directory.db, model, "snapshot_schema")
    session = SimpleNamespace(execute=AsyncMock())
    assert (
        await directory._upsert_rows_values(
            model,
            [{"source_id": "source", "org_name": "Example"}],
            ["source_id", "org_name"],
            ["source_id"],
            skip_unchanged=True,
            transaction_session=session,
        )
        == 1
    )
    assert "ON CONFLICT" in str(session.execute.await_args.args[0])


@pytest.mark.asyncio
async def test_fhir_missing_managed_source_refuses_before_create_schema(monkeypatch):
    directory = importlib.import_module("process.provider_directory_fhir")
    from process import provider_directory_generic_source_adapters as adapters

    monkeypatch.setattr(adapters, "require_generic_source_layout", AsyncMock())
    monkeypatch.setattr(directory, "_is_table_present", AsyncMock(return_value=False))
    monkeypatch.setattr(directory.db, "status", AsyncMock())
    with pytest.raises(RuntimeError, match="administrator_cut_required"):
        await directory._ensure_provider_directory_tables()
    directory.db.status.assert_not_awaited()


def _foreign():
    return [
        {
            "constraint": {"oid": "71", "convalidated": True, "conenforced": True},
            "child": [41, "snapshot_schema", "provider_directory_source", 19],
            "parent": [42, "snapshot_schema", "provider_directory_api_endpoint", 19],
            "hooks": [{"tgisinternal": True, "tgenabled": "O", "tgconstraint": "71"}],
        }
    ]


def test_external_fk_owner_requires_protected_custody():
    roots_by_name = {"source": {"oid": 41}, "endpoint": {"oid": 42}}
    custody._require_foreign_scope(_foreign(), roots_by_name, "snapshot_schema", 17)
    external = _foreign()
    external[0]["child"] = [55, "snapshot_schema", "external_child", 19]
    with pytest.raises(ValueError, match="external_fk_owner_required:snapshot_schema.external_child"):
        custody._require_foreign_scope(external, roots_by_name, "snapshot_schema", 17)
    external[0]["child"][-1] = 17
    with pytest.raises(ValueError, match="external_fk_owner_required"):
        custody._require_foreign_scope(external, roots_by_name, "snapshot_schema", 17)
    external[0]["child"][2] = "provider_directory_dataset_rehydration_checkpoint"
    custody._require_foreign_scope(external, roots_by_name, "snapshot_schema", 17)


@pytest.mark.parametrize("change", ["disabled", "not_internal", "wrong_constraint", "unvalidated"])
def test_native_fk_hook_or_constraint_drift_refuses(change):
    edges = _foreign()
    if change == "unvalidated":
        edges[0]["constraint"]["convalidated"] = False
    else:
        key = {"disabled": "tgenabled", "not_internal": "tgisinternal", "wrong_constraint": "tgconstraint"}[change]
        edges[0]["hooks"][0][key] = "D" if change == "disabled" else False if change == "not_internal" else "72"
    with pytest.raises(ValueError, match="foreign_key"):
        custody._require_foreign_scope(edges, {"source": {"oid": 41}, "endpoint": {"oid": 42}}, "snapshot_schema", 17)


def test_owner_reach_checks_all_roles_including_nonruntime_logins():
    roles = [dict(role=55, superuser=False, owner_reach=True)]
    with pytest.raises(ValueError, match="owner_authority"):
        custody._require_owner_reach(roles, 17, 21, 23)
    custody._require_owner_reach([{**roles[0], "role": 23}], 17, 21, 23)


def _relation(root):
    model = custody._models().get(root)
    columns = list(custody._model_columns(model)) if model else [column[0] for column in custody._EXTRA_COLUMNS[root]]
    attributes = [
        dict(attname=name, attnum=number, attrelid="41", attacl=None) for number, name in enumerate(columns, 1)
    ]
    return dict(
        catalog=dict(
            classes=[dict(oid="41", relname=root, relowner="19", relacl=None)],
            attributes=attributes,
            table_acl=[[19, 19, right, False] for right in custody._RIGHTS],
            column_acl=[],
            access=[],
            indexes=[],
            constraints=[],
            triggers=[],
        ),
        composite=dict(oid="51", typowner="19"),
        rows=dict(count=1, sha256="unchanged"),
    )


def _adopted_relation(root, before):
    after = deepcopy(before)
    tables, columns = custody._expected_relation_acl(root, before["catalog"], 17, 19)
    after["catalog"]["classes"][0].update(relowner="17", relacl=["native_owner_acl"])
    after["catalog"]["table_acl"], after["catalog"]["column_acl"] = sorted(tables), sorted(columns)
    after["composite"]["typowner"] = "17"
    for attribute in after["catalog"]["attributes"]:
        attribute["attacl"] = (
            ["native_column_acl"] if any(entry[0] == attribute["attnum"] for entry in columns) else None
        )
    return after


@pytest.mark.parametrize("root", custody.CONTROL_ROOTS)
def test_exact_owner_acl_delta_preserves_oid_layout_data_and_writer_columns(root):
    before = _relation(root)
    after = _adopted_relation(root, before)
    custody._require_relation_delta(root, before, after, 17, 19)
    table_rights, column_rights = custody._writer_rights(
        root, [entry["attname"] for entry in before["catalog"]["attributes"]]
    )
    assert "TRUNCATE" not in table_rights and "TRIGGER" not in table_rights and "REFERENCES" not in table_rights
    assert all(right in {"INSERT", "UPDATE"} for right in column_rights)


@pytest.mark.parametrize("change", ["oid", "data", "hook", "constraint", "extra_acl", "column_acl", "composite"])
def test_adoption_cannot_bless_other_native_or_payload_changes(change):
    root = custody.CONTROL_ROOTS[0]
    before = _relation(root)
    after = _adopted_relation(root, before)
    match change:
        case "oid":
            after["catalog"]["classes"][0]["oid"] = "99"
        case "data":
            after["rows"]["count"] = 2
        case "hook":
            after["catalog"]["triggers"].append({"oid": "99"})
        case "constraint":
            after["catalog"]["constraints"].append({"oid": "99"})
        case "extra_acl":
            after["catalog"]["table_acl"].append([19, 17, "TRIGGER", False])
        case "column_acl":
            after["catalog"]["column_acl"].append([1, 55, 17, "UPDATE", False])
        case "composite":
            after["composite"]["oid"] = "99"
    with pytest.raises(ValueError, match="cms_control_adoption"):
        custody._require_relation_delta(root, before, after, 17, 19)


def _adoption_foreign_edge(oid, child, parent, name):
    return dict(
        constraint=dict(
            oid=str(oid),
            conname=name,
            contype="f",
            conrelid=str(child[0]),
            confrelid=str(parent[0]),
            conkey=[1],
            confkey=[1],
            convalidated=True,
            condeferrable=True,
            condeferred=True,
            confdeltype="a",
            confupdtype="a",
        ),
        child=child,
        parent=parent,
        hooks=[dict(oid=str(oid + 100), tgconstraint=str(oid), tgenabled="O")],
    )


def _control_adoption(monkeypatch, legacy=True):
    from process import provider_directory_source_acquisition_custody as acquisition
    from process import provider_directory_source_callable_custody as source
    from process.provider_directory_source_cohort_custody import HEADER, HISTORY

    monkeypatch.setattr(acquisition, "require_source_acquisition_delta", lambda *args: None)
    monkeypatch.setattr(source, "require_source_callable_delta", lambda *args: None)
    header_fields = [61, "snapshot_schema", HEADER, 19]
    history_fields = [62, "snapshot_schema", HISTORY, 19]
    parents = [
        [41, "snapshot_schema", custody.CONTROL_ROOTS[0], 19],
        [42, "snapshot_schema", custody.CONTROL_ROOTS[1], 19],
        [43, "snapshot_schema", "provider_directory_endpoint_dataset", 19],
    ]
    headers = [
        _adoption_foreign_edge(71 + number, header_fields, parent, "header_" + str(number))
        for number, parent in enumerate(parents)
    ]
    retired = _adoption_foreign_edge(74, history_fields, header_fields, "pd_uhc_flex_npi_member_cohort_fkey")
    relations_by_root = {root: _relation(root) for root in custody.CONTROL_ROOTS}
    endpoint = relations_by_root[custody.CONTROL_ROOTS[1]]["catalog"]
    endpoint["classes"][0]["oid"] = "42"
    for attribute in endpoint["attributes"]:
        attribute["attrelid"] = "42"
    source_edges, _ = _source_endpoint_foreign_key(relations_by_root)
    before_by_field = dict(
        schema="snapshot_schema",
        owner=17,
        builder=19,
        functions={},
        source=dict(functions={}, roles={}),
        relations=relations_by_root,
        foreign=headers + ([retired] if legacy else []) + source_edges,
        acquisition=dict(
            relations={},
            cohort=dict(
                relations={
                    HEADER: dict(catalog=dict(constraints=[edge["constraint"] for edge in headers])),
                    HISTORY: dict(catalog=dict(constraints=[retired["constraint"]] if legacy else [])),
                }
            ),
        ),
    )
    after = deepcopy(before_by_field)
    after["relations"] = {
        root: _adopted_relation(root, before_by_field["relations"][root]) for root in custody.CONTROL_ROOTS
    }
    after["foreign"] = deepcopy(headers + source_edges)
    for edge in after["foreign"]:
        for field in ("child", "parent"):
            if edge[field][2] in custody.CONTROL_ROOTS:
                edge[field][3] = 17
    _set_source_endpoint_modes(after["foreign"], after["relations"], "A")
    return before_by_field, after


@pytest.mark.parametrize("legacy", [True, False])
def test_control_adoption_preserves_header_foreign_keys(monkeypatch, legacy):
    before, after = _control_adoption(monkeypatch, legacy)
    custody.require_cms_control_adoption_delta(before, after, {})
    assert len(after["foreign"]) == 4


@pytest.mark.parametrize("change", ["removed", "oid", "parent", "columns", "hook", "action"])
def test_control_adoption_refuses_header_foreign_drift(monkeypatch, change):
    before, after = _control_adoption(monkeypatch)
    edge = after["foreign"][0]
    match change:
        case "removed":
            after["foreign"].pop(0)
        case "oid":
            edge["constraint"]["oid"] = "999"
        case "parent":
            edge["parent"][0] = 999
        case "columns":
            edge["constraint"]["conkey"] = [2]
        case "hook":
            edge["hooks"][0]["oid"] = "999"
        case "action":
            edge["constraint"]["confdeltype"] = "c"
    with pytest.raises(ValueError, match="cms_control_adoption_foreign_changed"):
        custody.require_cms_control_adoption_delta(before, after, {})


def test_control_adoption_keeps_unrelated_history_foreign(monkeypatch):
    from process.provider_directory_source_cohort_custody import HISTORY

    before, after = _control_adoption(monkeypatch)
    unrelated = deepcopy(
        next(
            edge for edge in before["foreign"] if edge["constraint"]["conname"] == "pd_uhc_flex_npi_member_cohort_fkey"
        )
    )
    unrelated["constraint"].update(oid="75", conname="unrelated_history_foreign")
    before["foreign"].append(unrelated)
    before["acquisition"]["cohort"]["relations"][HISTORY]["catalog"]["constraints"].append(unrelated["constraint"])
    with pytest.raises(ValueError, match="cms_control_adoption_foreign_changed"):
        custody.require_cms_control_adoption_delta(before, after, {})


@pytest.mark.parametrize("already_enabled", [False, True])
def test_source_api_adoption_delta_allows_only_verified_enablement(monkeypatch, already_enabled):
    before, after = _control_adoption(monkeypatch)
    if already_enabled:
        _set_source_endpoint_modes(before["foreign"], before["relations"], "A")
    original = deepcopy(before)
    custody.require_cms_control_adoption_delta(before, after, {})
    assert before == original


@pytest.mark.parametrize("change", ["mode", "oid", "function", "name", "constraint", "unrelated", "foreign"])
def test_source_api_enablement_cannot_hide_other_hook_or_constraint_changes(monkeypatch, change):
    before, after = _control_adoption(monkeypatch)
    edge = next(
        edge
        for edge in after["foreign"]
        if edge["constraint"]["conname"] == "provider_directory_source_endpoint_id_fkey"
    )
    catalog_hook = after["relations"][custody.CONTROL_ROOTS[0]]["catalog"]["triggers"][0]
    if change in {"mode", "oid", "function", "name"}:
        field, value = {
            "mode": ("tgenabled", "O"),
            "oid": ("oid", "999"),
            "function": ("tgfoid", "999"),
            "name": ("tgname", "replacement"),
        }[change]
        edge["hooks"][0][field] = catalog_hook[field] = value
    elif change == "constraint":
        edge["constraint"]["conindid"] = "999"
    elif change == "unrelated":
        hook_by_field = dict(oid="999", tgconstraint="0", tgenabled="O")
        root = custody.CONTROL_ROOTS[0]
        before["relations"][root]["catalog"]["triggers"].append(hook_by_field)
        after["relations"][root]["catalog"]["triggers"].append({**hook_by_field, "tgenabled": "A"})
    else:
        after["foreign"][0]["hooks"][0]["tgenabled"] = "A"
    with pytest.raises(ValueError, match="cms_control_(source_endpoint_fk|adoption)"):
        custody.require_cms_control_adoption_delta(before, after, {})


def _read_control_adoption(monkeypatch):
    from process.provider_directory_cms_control_privileges import cms_control_read_edges
    from process.provider_directory_generic_source_policy import GenericSourcePolicy

    policy = GenericSourcePolicy(
        "snapshot_schema", "source_owner", "source_publisher", ("source_builder", "source_reader"), ("source_builder",)
    )
    before, after = _control_adoption(monkeypatch)
    graph_by_field = {
        "roles": [
            {"rolname": name, "oid": str(oid)}
            for name, oid in (
                ("source_owner", 17),
                ("source_publisher", 18),
                ("source_builder", 19),
                ("source_reader", 20),
            )
        ]
    }
    before["role_graph"] = deepcopy(graph_by_field)
    after["role_graph"] = deepcopy(graph_by_field)
    for root, relation in after["relations"].items():
        catalog = relation["catalog"]
        additions = cms_control_read_edges(root, catalog["attributes"], (18, 20), 17)
        catalog["column_acl"] = sorted({tuple(entry) for entry in catalog["column_acl"]} | additions)
        for attribute in catalog["attributes"]:
            if any(edge[0] == attribute["attnum"] for edge in additions):
                attribute["attacl"] = ["native_selected_acl"]
    return before, after, policy


def test_final_adoption_accepts_only_predeclared_source_read_edges(monkeypatch):
    before, after, policy = _read_control_adoption(monkeypatch)
    custody.require_cms_control_adoption_delta(before, after, {}, read_policy=policy)
    with pytest.raises(ValueError, match="adoption_acl_changed"):
        custody.require_cms_control_adoption_delta(before, after, {})


@pytest.mark.parametrize("change", ["foreign", "builder", "grantor", "grant_option", "write", "table", "body", "role"])
def test_final_read_cut_never_adopts_an_unplanned_acl_or_custody_delta(monkeypatch, change):
    before, after, policy = _read_control_adoption(monkeypatch)
    catalog = after["relations"]["entity_address_result_generation"]["catalog"]
    edge = next(edge for edge in catalog["column_acl"] if edge[1] == 20 and edge[3] == "SELECT")
    catalog["column_acl"] = [list(entry) for entry in catalog["column_acl"]]
    index = next(index for index, entry in enumerate(catalog["column_acl"]) if tuple(entry) == tuple(edge))
    if change in {"foreign", "builder", "grantor", "grant_option", "write"}:
        position, value = {
            "foreign": (1, 31),
            "builder": (1, 19),
            "grantor": (2, 19),
            "grant_option": (4, True),
            "write": (3, "UPDATE"),
        }[change]
        catalog["column_acl"][index][position] = value
    elif change == "table":
        catalog["table_acl"].append([20, 17, "SELECT", False])
    elif change == "body":
        catalog["triggers"].append({"oid": "unplanned"})
    else:
        after["role_graph"]["roles"][-1]["oid"] = "31"
    with pytest.raises(ValueError, match="cms_control_adoption"):
        custody.require_cms_control_adoption_delta(before, after, {}, read_policy=policy)


def test_read_cut_preserves_existing_read_grant_option_without_inventing_another_edge(monkeypatch):
    before, after, policy = _read_control_adoption(monkeypatch)
    root = "entity_address_result_generation"
    before["relations"][root]["catalog"]["column_acl"].append([1, 20, 19, "SELECT", True])
    current = after["relations"][root]["catalog"]
    current["column_acl"] = [entry for entry in current["column_acl"] if tuple(entry) != (1, 20, 17, "SELECT", False)]
    current["column_acl"].append([1, 20, 17, "SELECT", True])
    custody.require_cms_control_adoption_delta(before, after, {}, read_policy=policy)
