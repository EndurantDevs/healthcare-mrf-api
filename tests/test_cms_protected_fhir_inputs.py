# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host catalog-transport doubles; genuine native admission remains separate."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_inputs as inputs
from process import provider_directory_cms_native_registration as registration
from process import provider_directory_generic_source_policy as policy_module
from process import provider_directory_generic_source_provision as provision
from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES


def _policy():
    return policy_module.GenericSourcePolicy("mrf", "source_owner", "source_publisher", ("source_reader",))


def _family():
    return [
        dict(
            oid=1000 + ordinal,
            nspname="mrf",
            relname=root,
            relkind="p" if root in provision.PARTITIONED_ROOTS else "r",
            relpersistence="p",
            relispartition=False,
            relowner=71,
            relrowsecurity=False,
            relforcerowsecurity=False,
            bound=None,
            partition_key="LIST (dataset_id)" if root in provision.PARTITIONED_ROOTS else None,
            parents=[],
            columns=[["dataset_id", "character varying", True, None]],
            constraints=[dict(name="native_key", type="p", valid=True, definition="PRIMARY KEY (dataset_id)")],
            indexes=[dict(oid=2000 + ordinal, valid=True)],
            inert=True,
            native_checks=True,
            direct_acl_closed=True,
            readers_closed=True,
        )
        for ordinal, root in enumerate(provision.PAYLOAD_ROOTS)
    ]


def _roles():
    return [
        dict(
            oid=oid,
            rolname=name,
            rolcanlogin=name != "source_owner",
            elevated=False,
            rolsuper=False,
            rolcreatedb=False,
            rolcreaterole=False,
            rolreplication=False,
            rolbypassrls=False,
            owner_set=name == "source_publisher",
            publisher_set=False,
            namespace_owner_reachable=False,
            namespace_create=False,
            namespace_owner_protected=True,
            namespace_usage=True,
        )
        for oid, name in ((71, "source_owner"), (72, "source_publisher"), (73, "source_reader"))
    ]


class CatalogConnection:
    """Per-test query replies, without a native authority or database claim."""

    def __init__(self):
        self.family = _family()
        self.roles = _roles()
        self.kinds = ["p"] * 3
        self.events = []
        self.identity_reads = 0
        self.change_identity = False
        self.cancel_physical = False
        self.root_oid_drift = False

    def is_in_transaction(self):
        return True

    async def execute(self, sql, *parameters):
        self.events.append(("execute", str(sql), parameters))

    async def fetchrow(self, sql, *parameters):
        self.events.append(("fetchrow", sql, parameters))
        if "AS transaction_id" in sql:
            assert "pg_current_xact_id()" not in sql and "virtualtransaction" in sql
            self.identity_reads += 1
            return dict(
                database_oid=91,
                session="source_publisher",
                current="source_owner",
                backend_pid=92,
                transaction_id="94" if self.change_identity and self.identity_reads > 1 else "93",
            )
        if "AS incumbent_oid" in sql:
            row = next(row for row in self.family if row["relname"] == parameters[0].split('"')[-2])
            return dict(
                relation_oid=row["oid"],
                owner_oid=71,
                incumbent_oid=row["oid"],
                relkind="r",
                relpersistence="p",
                relrowsecurity=False,
                relforcerowsecurity=False,
                relispartition=False,
                inherited=False,
            )
        raise AssertionError(sql)

    async def fetch(self, sql, *parameters):
        self.events.append(("fetch", sql, parameters))
        if sql == provision.ROLE_SQL:
            return deepcopy(self.roles)
        if sql == provision.CATALOG_SQL:
            assert parameters[0] == [f'"mrf"."{name}"' for name in provision.PAYLOAD_ROOTS]
            return deepcopy(self.family)
        if "requested.namespace" in sql:
            return self._current_roots(parameters)
        if "AS kind FROM unnest" in sql:
            return [dict(kind=kind) for kind in self.kinds]
        if "pg_relation_filenode" in sql:
            if self.cancel_physical:
                raise asyncio.CancelledError
            return [
                dict(
                    oid=row["oid"],
                    relfilenode=None if row["relkind"] == "p" else 9000 + row["oid"],
                    schema_oid=74,
                    schema_owner_oid=75,
                    raw_acl="{source_reader=r/source_owner}",
                    raw_schema_acl="{source_reader=U/namespace_owner}",
                    column_acls=[],
                )
                for row in self.family
                if row["oid"] in parameters[0]
            ]
        raise AssertionError(sql)

    def _current_roots(self, parameters):
        by_name = {row["relname"]: row for row in self.family}
        return [
            dict(
                namespace=namespace,
                name=name,
                oid=by_name[name]["oid"] + int(self.root_oid_drift) if name in provision.FHIR_INPUT_ROOTS else None,
                kind=self.kinds[provision.FHIR_INPUT_ROOTS.index(name)] if name in provision.FHIR_INPUT_ROOTS else None,
            )
            for namespace, name in zip(*parameters, strict=True)
        ]


@pytest.fixture
def catalog(monkeypatch):
    # Separate low-volume control dependencies retain their own native suites.
    for name in ("verify_proof_control", "capture_source_coverage_control", "capture_checkpoint_controls"):
        monkeypatch.setattr(provision, name, AsyncMock(return_value={}))
    return CatalogConnection()


@pytest.mark.asyncio
async def test_empty_three_parents_use_complete_family_and_native_null_filenodes(catalog):
    proof = await inputs.capture_protected_fhir_input(catalog, source_policy=_policy())
    assert proof["version"] == 1 and proof["database_oid"] == 91 and proof["owner_oid"] == 71
    assert [name for name, _ in proof["parent_oids"]] == [
        "provider_directory_dataset_affiliation_organization",
        "provider_directory_dataset_insurance_plan",
        "provider_directory_dataset_network_plan",
    ]
    assert len(proof["inventory"]) == 3
    assert all(row["relfilenode"] is None and row["parents"] == [] for row in proof["inventory"])
    assert any(event[1] == provision.ROLE_SQL for event in catalog.events)
    assert any(event[1] == provision.CATALOG_SQL for event in catalog.events)
    assert all("INSERT INTO" not in event[1] and "CREATE TRIGGER" not in event[1] for event in catalog.events)
    assert provision.require_protected_fhir_input(deepcopy(proof)) == proof


@pytest.mark.asyncio
@pytest.mark.parametrize("kinds", [[None] * 3, ["r"] * 3])
async def test_legacy_or_absent_preserve_none_without_family_or_registration(catalog, kinds):
    catalog.kinds = kinds
    assert await inputs.capture_protected_fhir_input(catalog, source_policy=_policy()) is None
    assert not any(event[1] == provision.CATALOG_SQL or event[0] == "execute" for event in catalog.events)


@pytest.mark.asyncio
@pytest.mark.parametrize("kinds", [["p", None, "p"], ["p", "r", "p"]])
async def test_partial_or_mixed_roots_refuse_before_family_or_registration(catalog, kinds):
    catalog.kinds = kinds
    with pytest.raises(ValueError, match="protected_fhir_roots_changed"):
        await inputs.capture_protected_fhir_input(catalog, source_policy=_policy())
    assert not any(event[0] == "execute" for event in catalog.events)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("nspname", "foreign_schema"),
        ("relowner", 99),
        ("partition_key", "RANGE (dataset_id)"),
        ("relrowsecurity", True),
        ("direct_acl_closed", False),
        ("readers_closed", False),
        ("native_checks", False),
        ("bound", "DEFAULT"),
        ("inert", False),
    ],
)
async def test_unsafe_parent_custody_or_layout_refuses(catalog, field, value):
    next(row for row in catalog.family if row["relname"] == provision.FHIR_INPUT_ROOTS[0])[field] = value
    with pytest.raises(ValueError):
        await inputs.capture_protected_fhir_input(catalog, source_policy=_policy())


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["elevated", "owner_set", "namespace_create", "namespace_owner_reachable"])
async def test_real_role_validator_refuses_unsafe_reader_before_family(catalog, field):
    catalog.roles[-1][field] = True
    with pytest.raises(ValueError):
        await inputs.capture_protected_fhir_input(catalog, source_policy=_policy())
    assert not any(event[1] == provision.CATALOG_SQL for event in catalog.events)


def _add_leaf(catalog):
    parent = next(row for row in catalog.family if row["relname"] == provision.FHIR_INPUT_ROOTS[0])
    leaf_by_field = dict(
        deepcopy(parent),
        oid=3000,
        relname="pd_dataset_partition",
        relkind="r",
        relispartition=True,
        partition_key=None,
        parents=[parent["oid"]],
        bound="FOR VALUES IN ('dataset_exact')",
    )
    catalog.family.append(leaf_by_field)
    return leaf_by_field


@pytest.mark.asyncio
async def test_direct_leaf_binds_filenode_edge_bound_layout_and_acl(catalog):
    _add_leaf(catalog)
    proof = await inputs.capture_protected_fhir_input(catalog, source_policy=_policy())
    leaf = next(row for row in proof["inventory"] if row["relname"] == "pd_dataset_partition")
    assert leaf["relfilenode"] == 12000 and leaf["parents"] == [proof["parent_oids"][0][1]]
    assert leaf["bound"] == "FOR VALUES IN ('dataset_exact')"
    assert leaf["columns"] == [["dataset_id", "character varying", True, None]]
    assert leaf["raw_acl"] == "{source_reader=r/source_owner}"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("parents", [777]),
        ("parents", [1000, 1001]),
        ("relkind", "p"),
        ("bound", "DEFAULT"),
        ("bound", "FOR VALUES FROM ('a') TO ('z')"),
        ("relispartition", False),
        ("readers_closed", False),
    ],
)
async def test_foreign_nested_default_or_unclosed_leaf_refuses(catalog, field, value):
    _add_leaf(catalog)[field] = value
    with pytest.raises(ValueError):
        await inputs.capture_protected_fhir_input(catalog, source_policy=_policy())


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["identity", "cancel"])
async def test_identity_change_or_cancel_never_yields_proof_or_registers(catalog, failure):
    catalog.change_identity = failure == "identity"
    catalog.cancel_physical = failure == "cancel"
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else ValueError):
        await inputs.capture_protected_fhir_input(catalog, source_policy=_policy())
    assert all("INSERT" not in event[1] for event in catalog.events if event[0] == "execute")


@pytest.mark.asyncio
async def test_initial_registration_accounts_protected_three_without_heap_enrollment(catalog):
    selected = await registration.capture_current_native_input_candidates(
        catalog, ledger_schema="mrf", source_policy=_policy()
    )
    assert selected["candidates"] == ()
    assert [row["table"] for row in selected["protected_roots"]] == list(provision.FHIR_INPUT_ROOTS)
    assert all(row["proof"] == selected["protected_fhir"] for row in selected["protected_roots"])
    assert set(selected["absent"]) == set(registration.native_input_registration_roots("mrf")) - {
        ("mrf", name) for name in provision.FHIR_INPUT_ROOTS
    }


@pytest.mark.asyncio
async def test_initial_registration_requires_policy_and_same_observed_oids(catalog):
    with pytest.raises(RuntimeError, match="source_policy_required"):
        await registration.capture_current_native_input_candidates(catalog, ledger_schema="mrf")
    catalog.root_oid_drift = True
    with pytest.raises(RuntimeError, match="candidate_identity_changed"):
        await registration.capture_current_native_input_candidates(
            catalog, ledger_schema="mrf", source_policy=_policy()
        )


@pytest.mark.asyncio
async def test_initial_legacy_candidates_keep_original_result_shape(catalog):
    catalog.kinds = ["r"] * 3
    selected = await registration.capture_current_native_input_candidates(catalog, ledger_schema="mrf")
    assert set(selected) == {"candidates", "absent"}
    assert {row.target_table for row in selected["candidates"]} == set(provision.FHIR_INPUT_ROOTS)


class FenceSession:
    def __init__(self, driver, isolation="repeatable read"):
        self.isolation = isolation
        self.raw_connection = SimpleNamespace(driver_connection=driver)
        self.commands = []

    async def execute(self, sql, parameters=None):
        self.commands.append(str(sql))
        return SimpleNamespace(
            scalar_one=lambda: self.isolation if "SHOW" in str(sql) else None,
            mappings=lambda: SimpleNamespace(
                one=lambda: dict(database_name="synthetic", database_oid=91, system_identifier="123")
            ),
        )


def _physical(proof):
    physical_by_name = {
        f'"{schema}"."{name}"': dict(
            relation_oid=None, relfilenode=None, relkind=None, relpersistence=None, inherited=False
        )
        for schema, name in inputs._relations("mrf")
    }
    for name, oid in proof["parent_oids"]:
        physical_by_name[f'"mrf"."{name}"'] = dict(
            relation_oid=oid,
            relfilenode=None,
            relkind="p",
            relpersistence="p",
            inherited=any(row["parents"] == [oid] for row in proof["inventory"]),
        )
    return physical_by_name


@pytest.mark.asyncio
async def test_worker_requires_original_task_driver_and_factory(catalog):
    source_policy = _policy()
    factory = SimpleNamespace(policy=source_policy, source_callable_capability=AsyncMock(return_value=(700,)))
    publisher = policy_module._Publisher(catalog, source_policy, asyncio.current_task(), "repeatable read", factory)
    publisher_token = policy_module._PUBLISHER.set(publisher)
    factory_token = policy_module._FACTORY.set(factory)
    try:
        assert await inputs._capture_bound_fhir(FenceSession(catalog), "mrf") is not None
        with pytest.raises(RuntimeError, match="source_binding_changed"):
            await inputs._capture_bound_fhir(FenceSession(CatalogConnection()), "mrf")
        with pytest.raises(ValueError, match="publisher_connection_required"):
            await asyncio.create_task(inputs._capture_bound_fhir(FenceSession(catalog), "mrf"))
    finally:
        policy_module._FACTORY.reset(factory_token)
        policy_module._PUBLISHER.reset(publisher_token)


@pytest.mark.asyncio
@pytest.mark.parametrize("cutover", [False, True])
async def test_native_fence_captures_fhir_and_recaptures_after_recursive_locks(catalog, monkeypatch, cutover):
    proof = await provision.capture_protected_fhir_input(catalog, source_policy=_policy())
    physical, session = _physical(proof), FenceSession(catalog, "read committed" if cutover else "repeatable read")
    monkeypatch.setattr(inputs, "capture_selected_captured_tiger_input", AsyncMock(return_value=None))
    monkeypatch.setattr(inputs, "_physical_relations", AsyncMock(return_value=physical))
    capture = AsyncMock(return_value=proof)
    monkeypatch.setattr(inputs, "_capture_bound_fhir", capture, raising=False)
    monkeypatch.setattr(inputs, "_npi_revision", AsyncMock(return_value={}))
    monkeypatch.setattr(
        inputs, "_reference_authorities", AsyncMock(return_value={"cms-doctors": {}, "geo": {}, "mrf": {}, "tiger": {}})
    )
    monkeypatch.setattr(inputs, "_geo_bindings", lambda *args, **kwargs: {})
    monkeypatch.setattr(inputs, "_revision_guards", AsyncMock(return_value=[]))
    monkeypatch.setattr(
        inputs, "_cms_office_read_relations", AsyncMock(return_value=dict.fromkeys(CMS_OFFICE_READ_TABLES, {}))
    )
    observed = await inputs.capture_native_address_input_fence(session, "mrf", cutover=cutover)
    assert observed["version"] == 5 and observed["fhir_protected"] == proof and capture.await_count == 2
    commands = [sql for sql in session.commands if sql.startswith("LOCK TABLE")]
    assert len(commands) == 1 and commands[0].endswith(
        " IN SHARE MODE NOWAIT" if cutover else " IN ACCESS SHARE MODE NOWAIT"
    )
    assert all(f'"mrf"."{name}"' in commands[0] for name in provision.FHIR_INPUT_ROOTS)
    if cutover:
        session.isolation = "repeatable read"
        with pytest.raises(RuntimeError, match="cutover_isolation_changed"):
            await inputs.capture_native_address_input_fence(session, "mrf", cutover=True)
        session.isolation = "read committed"
    capture.side_effect = [proof, dict(proof, family_sha256="a" * 64)]
    with pytest.raises(RuntimeError, match="native_inputs_changed"):
        await inputs.capture_native_address_input_fence(session, "mrf", cutover=cutover)
    missing_fence_by_field = dict(observed)
    missing_fence_by_field.pop("fhir_protected")
    with pytest.raises(RuntimeError, match="fhir_proof_changed"):
        inputs.require_supported_native_address_inputs(missing_fence_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("tiger_kind", [None, "installed", "captured"])
@pytest.mark.parametrize("nucc", [False, True])
async def test_fhir_v5_preserves_every_existing_tiger_and_nucc_combination(catalog, tiger_kind, nucc):
    from process.provider_directory_cms_native_registration import _NativeNuccSession
    from process.provider_directory_cms_tiger_inputs import captured_tiger_bindings
    from tests.test_cms_native_input_registration import ProtectedNuccProtocol
    from tests.test_cms_tiger_native_inputs import _captured_proof, _proof
    from tests.test_cms_tiger_native_inputs import _physical as tiger_physical

    proof = await provision.capture_protected_fhir_input(catalog, source_policy=_policy())
    proof["database_oid"] = 81  # Match the existing independent pure Tiger fixture.
    fence_by_field = dict(
        version=5,
        schema="mrf",
        database=dict(database_oid=81),
        relations=_physical(proof),
        fhir_protected=proof,
        geo_bindings={},
        reference_authorities=dict.fromkeys(("cms-doctors", "geo", "mrf", "tiger"), {}),
        cms_office_read_relations=dict.fromkeys(CMS_OFFICE_READ_TABLES, {}),
    )
    prior_version = 1
    if nucc:
        fence_by_field["nucc_protected"] = await inputs.capture_protected_nucc_input(
            _NativeNuccSession(ProtectedNuccProtocol()), schema="mrf"
        )
        fence_by_field["relations"]['"mrf"."nucc_taxonomy"'] = dict(relation_oid=201, relkind="p", inherited=True)
        prior_version = 2
    if tiger_kind == "installed":
        fence_by_field["tiger_protected"] = _proof()
        fence_by_field["relations"].update(tiger_physical())
        prior_version = 3
    if tiger_kind == "captured":
        fence_by_field["tiger_captured"] = _captured_proof()
        fence_by_field["geo_bindings"] = captured_tiger_bindings(fence_by_field["tiger_captured"])
        for name, binding in fence_by_field["geo_bindings"].items():
            fence_by_field["relations"]['"' + name.replace(".", '"."') + '"'] = dict(
                relation_oid=binding["relation_oid"],
                relfilenode=binding["relfilenode"],
                relkind="r",
                relpersistence="p",
                inherited=False,
            )
        prior_version = 4
    expected = deepcopy(fence_by_field)
    inputs.require_supported_native_address_inputs(fence_by_field)
    assert fence_by_field == expected
    prior = deepcopy(fence_by_field)
    prior.pop("fhir_protected")
    prior["version"] = prior_version
    for name, _ in proof["parent_oids"]:
        prior["relations"][f'"mrf"."{name}"'] = dict(relation_oid=None, relkind=None)
    inputs.require_supported_native_address_inputs(prior)
    assert "fhir_protected" not in prior and prior["version"] == prior_version


@pytest.mark.asyncio
async def test_only_verified_three_leave_revision_set_without_creating_guards(catalog, monkeypatch):
    proof = await provision.capture_protected_fhir_input(catalog, source_policy=_policy())
    queries = []

    async def execute(sql, parameters=None):
        queries.append((str(sql), parameters))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))

    monkeypatch.setattr(inputs, "_require_ledger_guards", AsyncMock())
    assert (
        await inputs._revision_guards(SimpleNamespace(execute=execute), "mrf", _physical(proof), protected_fhir=proof)
        == []
    )
    assert all(parameters["oids"] == [] for _, parameters in queries if parameters is not None and "oids" in parameters)
    assert all(not sql.startswith(("INSERT", "CREATE", "ALTER")) for sql, _ in queries)
    changed = deepcopy(proof)
    changed["inventory"][0]["relfilenode"] = 0
    with pytest.raises(ValueError, match="protected_fhir_parent_changed"):
        await inputs._revision_guards(SimpleNamespace(execute=execute), "mrf", _physical(proof), protected_fhir=changed)


@pytest.mark.asyncio
@pytest.mark.parametrize("cutover", [False, True])
async def test_office_read_source_parents_require_original_recapture(catalog, monkeypatch, cutover):
    proof = await provision.capture_protected_fhir_input(catalog, source_policy=_policy())
    catalog_rows = [
        dict(
            name=name,
            relation_oid=5000 + ordinal,
            owner_oid=proof["owner_oid"],
            relkind="r" if name == "provider_directory_cms_candidate_coverage" else "p",
            relpersistence="p",
            relispartition=False,
            inherited=name != "provider_directory_cms_candidate_coverage",
            relrowsecurity=False,
            relforcerowsecurity=False,
        )
        for ordinal, name in enumerate(CMS_OFFICE_READ_TABLES)
    ]
    commands = []

    async def execute(statement, parameters=None):
        commands.append(str(statement))
        return SimpleNamespace(mappings=lambda: deepcopy(catalog_rows))

    capture = AsyncMock(return_value=proof)
    monkeypatch.setattr(inputs, "_capture_bound_fhir", capture)
    session = SimpleNamespace(execute=execute)
    assert await inputs._cms_office_read_relations(session, "mrf", cutover=cutover, protected_fhir=proof) == {
        catalog_row["name"]: catalog_row for catalog_row in catalog_rows
    }
    assert capture.await_count == 2
    locks = [command for command in commands if command.startswith("LOCK TABLE")]
    assert len(locks) == 3
    assert all(command.endswith("SHARE MODE NOWAIT" if cutover else "ACCESS SHARE MODE NOWAIT") for command in locks)
    for rejected in (None, dict(proof, schema="other"), dict(proof, family_sha256="a" * 64)):
        with pytest.raises((RuntimeError, ValueError)):
            await inputs._cms_office_read_relations(session, "mrf", protected_fhir=rejected)
    for name, field, invalid_value in (
        (CMS_OFFICE_READ_TABLES[0], "owner_oid", 99),
        (CMS_OFFICE_READ_TABLES[0], "relispartition", True),
        (CMS_OFFICE_READ_TABLES[0], "relpersistence", "u"),
        (CMS_OFFICE_READ_TABLES[0], "relrowsecurity", True),
        ("provider_directory_cms_candidate_coverage", "relkind", "p"),
    ):
        catalog_row = next(catalog_row for catalog_row in catalog_rows if catalog_row["name"] == name)
        original = catalog_row[field]
        catalog_row[field] = invalid_value
        with pytest.raises(RuntimeError, match="office_read_heap_unavailable"):
            await inputs._cms_office_read_relations(session, "mrf", protected_fhir=proof)
        catalog_row[field] = original
    capture.side_effect = [proof, dict(proof, family_sha256="a" * 64)]
    with pytest.raises(RuntimeError, match="fhir_proof_changed"):
        await inputs._cms_office_read_relations(session, "mrf", protected_fhir=proof)
    capture.side_effect = None
    for catalog_row in catalog_rows:
        catalog_row.update(relkind="r", inherited=False)
    capture.reset_mock()
    assert await inputs._cms_office_read_relations(session, "mrf") == {
        catalog_row["name"]: catalog_row for catalog_row in catalog_rows
    }
    capture.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_structural_capture_refuses_complete_family_drift_before_return(catalog, monkeypatch):
    original_fetch = catalog.fetch

    async def fetch(sql, *parameters):
        result_rows = await original_fetch(sql, *parameters)
        if "pg_relation_filenode" in sql:
            catalog.family[0]["columns"][0][1] = "text"
        return result_rows

    monkeypatch.setattr(catalog, "fetch", fetch)
    with pytest.raises(ValueError, match="protected_fhir_catalog_changed"):
        await inputs.capture_protected_fhir_input(catalog, source_policy=_policy())


@pytest.mark.asyncio
async def test_native_capture_requires_nonempty_virtual_transaction_before_catalog_reads(catalog, monkeypatch):
    identity = AsyncMock(return_value={"transaction_id": None})
    monkeypatch.setattr(provision, "_fhir_input_identity", identity)
    with pytest.raises(ValueError, match="transaction_required"):
        await provision.capture_protected_fhir_input(catalog, source_policy=_policy())
    assert catalog.events == []


@pytest.mark.asyncio
async def test_physical_catalog_preserves_native_function_null_for_nucc_parent():
    statements = []

    async def execute(statement, parameters):
        statements.append(str(statement))
        return SimpleNamespace(
            mappings=lambda: [
                dict(
                    name='"mrf"."nucc_taxonomy"',
                    relation_oid=201,
                    relfilenode=None,
                    relkind="p",
                    relpersistence="p",
                    inherited=True,
                )
            ]
        )

    physical_by_name = await inputs._physical_relations(SimpleNamespace(execute=execute), "mrf")
    assert physical_by_name['"mrf"."nucc_taxonomy"']["relfilenode"] is None
    assert "pg_relation_filenode(relation.oid)::bigint AS relfilenode" in statements[0]
    assert "coalesce(pg_relation_filenode" not in statements[0].lower()
