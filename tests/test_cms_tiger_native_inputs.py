# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Host dependency protocols, never proof of native protected authority."""

import asyncio
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_cms_native_inputs as inputs
from process import provider_directory_cms_tiger_inputs as tiger
from process import provider_directory_generic_source_policy as policy


def _proof():
    child_rows = [
        dict(
            schema_name="reference_family_archive_" + "a" * 32,
            relation_name=name,
            relation_oid=201 + offset,
            schema_oid=301,
            schema_owner_oid=401,
            owner_oid=401,
            relfilenode=501 + offset,
        )
        for offset, name in enumerate(tiger.ROOTS)
    ]
    return dict(
        version=1,
        database_oid=81,
        node_id="fixture-node",
        package_id="fixture:package-v1",
        generation_id="11111111-1111-4111-8111-111111111111",
        installation_id="22222222-2222-4222-8222-222222222222",
        fence_token="33333333-3333-4333-8333-333333333333",
        registration_sha256="a" * 64,
        receipt_sha256="b" * 64,
        parent_inventory_sha256="c" * 64,
        parent_oids=[["zcta5", 101], ["zip_state", 102]],
        inventory=dict(database_oid=81, relations=child_rows),
    )


def _catalog(proof):
    catalog_rows = []
    children_by_name = {child["relation_name"]: child for child in proof["inventory"]["relations"]}
    for name, oid in proof["parent_oids"]:
        catalog_rows.append(
            dict(
                children_by_name[name],
                parent_name=name,
                parent_oid=oid,
                parent_kind="r",
                parent_persistence="p",
                parent_filenode=oid + 500,
                parent_owner=401,
                parent_partition=False,
                parent_rls=False,
                parent_force_rls=False,
                child_count=1,
                parent_is_child=False,
                child_kind="r",
                child_persistence="p",
                child_partition=False,
                child_rls=False,
                child_force_rls=False,
                child_has_children=False,
                child_parent_count=1,
                database_oid=81,
                parent_read_lock=True,
                child_read_lock=True,
            )
        )
    return catalog_rows


def _physical():
    return {
        f'"tiger"."{name}"': dict(
            relation_oid=oid, relfilenode=oid + 500, relkind="r", relpersistence="p", inherited=True
        )
        for name, oid in _proof()["parent_oids"]
    }


class SourceSession:
    def __init__(self):
        self.active = True
        self.rows = _catalog(_proof())
        self.calls = []

    def in_transaction(self):
        return self.active

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        self.calls.append((sql, parameters))
        if sql == tiger.CATALOG_SQL:
            if getattr(self, "close_after_catalog", False):
                self.active = False
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: deepcopy(self.rows)))
        if "SHOW transaction_isolation" in sql:
            return SimpleNamespace(scalar_one=lambda: "repeatable read")
        if "current_database() AS database_name" in sql:
            return SimpleNamespace(mappings=lambda: SimpleNamespace(one=lambda: dict(database_oid=81)))
        if "SELECT to_regclass" in sql:
            return SimpleNamespace(scalar_one=lambda: 701)
        if "WHERE importer_id=ANY" in sql:
            return SimpleNamespace(mappings=lambda: [])
        if sql.startswith("LOCK"):
            assert not any(f'"tiger"."{name}"' in sql for name in tiger.ROOTS)
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: []))


class IssuedFactory:
    def __init__(self, callback):
        self.policy = policy.GenericSourcePolicy(
            "source_schema", "source_owner", "source_publisher", ("source_reader",)
        )
        self.producer_identity = ("a" * 40, "sha256:" + "b" * 64)
        self.task = asyncio.current_task()
        self.native_tiger_input_fence = callback

    def transaction(self):
        raise AssertionError("capture must reuse original Source transaction")

    def _require_current_policy(self):
        assert asyncio.current_task() is self.task


@pytest.mark.parametrize(
    "field,bad",
    [
        ("version", True),
        ("database_oid", True),
        ("node_id", ""),
        ("node_id", "x" * 129),
        ("package_id", "file/path"),
        ("package_id", "x" * 257),
        ("generation_id", "1"),
        ("installation_id", "22222222222242228222222222222222"),
        ("fence_token", None),
        ("registration_sha256", "A" * 64),
        ("receipt_sha256", "x" * 64),
        ("parent_inventory_sha256", "c" * 63),
        ("parent_oids", [["zcta5", 101]]),
        ("parent_oids", [["zcta5", 101], ["zip_state", 101]]),
        ("parent_oids", [["zip_state", 102], ["zcta5", 101]]),
        ("parent_oids", [["zcta5", 101], ["zip_state", 102], ["other", 103]]),
    ],
)
def test_typed_tiger_linkage_rejects_changed_identity(field, bad):
    proof = _proof()
    proof[field] = bad
    with pytest.raises(RuntimeError, match="tiger_proof_changed"):
        tiger.require_protected_tiger_proof(proof)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("schema_name", "other"),
        ("relation_name", "other"),
        ("relation_oid", True),
        ("schema_oid", 0),
        ("schema_owner_oid", 999),
        ("owner_oid", 999),
        ("relfilenode", 0),
    ],
)
def test_typed_tiger_inventory_preserves_exact_two_child_contract(field, bad):
    proof = _proof()
    proof["inventory"]["relations"][0][field] = bad
    with pytest.raises(RuntimeError, match="tiger_proof_changed"):
        tiger.require_protected_tiger_proof(proof)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("parent_oid", 999),
        ("database_oid", 82),
        ("parent_name", "other"),
        ("parent_kind", "p"),
        ("parent_persistence", "u"),
        ("parent_filenode", 0),
        ("parent_owner", 999),
        ("child_count", 2),
        ("child_parent_count", 2),
        ("parent_partition", True),
        ("parent_rls", True),
        ("parent_force_rls", True),
        ("parent_is_child", True),
        ("child_kind", "p"),
        ("child_persistence", "u"),
        ("child_partition", True),
        ("child_rls", True),
        ("child_force_rls", True),
        ("child_has_children", True),
        ("parent_read_lock", False),
        ("child_read_lock", False),
        ("relfilenode", 999),
        ("schema_name", "other"),
    ],
)
def test_native_recapture_refuses_catalog_or_lock_drift(field, bad):
    rows = _catalog(_proof())
    rows[0][field] = bad
    with pytest.raises(RuntimeError, match="tiger_proof_changed"):
        tiger.require_tiger_catalog(_proof(), rows)


@pytest.mark.asyncio
async def test_source_callback_keeps_actual_factory_task_and_session():
    session = SourceSession()
    callback = AsyncMock(return_value=_proof())
    factory = IssuedFactory(callback)
    async with policy.bind_generic_source_transaction_factory(factory):
        captured = await tiger.capture_protected_tiger_input(session, _physical())
        assert captured == _proof() and captured is not callback.return_value
        callback.assert_awaited_once_with(session)
        with pytest.raises(AssertionError):
            await asyncio.create_task(tiger.capture_protected_tiger_input(session, _physical()))
    assert session.calls == [(tiger.CATALOG_SQL, None)]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "missing_factory",
        "missing_callback",
        "transaction",
        "producer",
        "database",
        "partial",
        "filenode",
        "closed_after_query",
    ],
)
async def test_source_callback_rejects_missing_or_changed_binding(fault):
    session = SourceSession()
    physical_by_name = _physical()
    factory = IssuedFactory(AsyncMock(return_value=_proof()))
    if fault == "missing_factory":
        with pytest.raises(policy.GenericCandidateError, match="transaction_factory_required"):
            await tiger.capture_protected_tiger_input(session, physical_by_name)
        return
    if fault == "missing_callback":
        factory.native_tiger_input_fence = None
    if fault == "transaction":
        session.active = False
    if fault == "closed_after_query":
        session.close_after_catalog = True
    if fault == "producer":

        async def changed_producer(_session):
            factory.producer_identity = ("c" * 40, "sha256:" + "b" * 64)
            return _proof()

        factory.native_tiger_input_fence = changed_producer
    if fault == "database":
        session.rows[0]["database_oid"] = 82
    if fault == "filenode":
        physical_by_name['"tiger"."zcta5"']["relfilenode"] += 1
    if fault == "partial":
        physical_by_name.pop('"tiger"."zip_state"')
    async with policy.bind_generic_source_transaction_factory(factory):
        with pytest.raises(RuntimeError, match="tiger_proof_changed"):
            await tiger.capture_protected_tiger_input(session, physical_by_name)


@pytest.mark.asyncio
@pytest.mark.parametrize("cutover", [False, True])
async def test_common_capture_preserves_geo_and_mutable_input_authorities(monkeypatch, cutover):
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES

    session = SourceSession()
    archive_owner = AsyncMock()
    monkeypatch.setattr("process.provider_directory_cms_archive.require_source_archive_owner", archive_owner)
    physical_by_name = {
        f'"{schema}"."{name}"': dict(relation_oid=None, relkind=None, inherited=False)
        for schema, name in inputs._relations("mrf")
    }
    physical_by_name.update(_physical())
    physical_by_name['"mrf"."mrf_address"'] = dict(
        relation_oid=901, relfilenode=902, relkind="r", relpersistence="p", inherited=False
    )
    monkeypatch.setattr(inputs, "_physical_relations", AsyncMock(return_value=physical_by_name))
    monkeypatch.setattr(inputs, "_npi_revision", AsyncMock(return_value={"literal": "npi"}))
    monkeypatch.setattr(inputs, "_geo_bindings", lambda *args: {"literal": "physical-parent-geo"})
    authorities = AsyncMock(return_value={family: {} for family in ("cms-doctors", "geo", "mrf")})
    monkeypatch.setattr(inputs, "_reference_authorities", authorities)
    revisions = AsyncMock(return_value=[{"literal_revision": 9}])
    monkeypatch.setattr(inputs, "_revision_guards", revisions)
    office_by_name = {name: {} for name in CMS_OFFICE_READ_TABLES}
    monkeypatch.setattr(inputs, "_cms_office_read_relations", AsyncMock(return_value=office_by_name))
    async with policy.bind_generic_source_transaction_factory(IssuedFactory(AsyncMock(return_value=_proof()))):
        fence = await inputs.capture_native_address_input_fence(session, "mrf", cutover=cutover)
    archive_owner.assert_awaited_once_with(session, "mrf")
    assert fence == dict(
        version=3,
        schema="mrf",
        database=dict(database_oid=81),
        relations=physical_by_name,
        npi={"literal": "npi"},
        reference_authorities={family: {} for family in ("cms-doctors", "geo", "mrf")},
        geo_bindings={"literal": "physical-parent-geo"},
        revisions=[{"literal_revision": 9}],
        cms_office_read_relations=office_by_name,
        tiger_protected=_proof(),
    )
    authorities.assert_awaited_once_with(session, "mrf", protected_tiger=_proof())
    revisions.assert_awaited_once_with(session, "mrf", physical_by_name, protected_tiger=_proof())
    mode = "SHARE" if cutover else "ACCESS SHARE"
    locks = [sql for sql, _ in session.calls if sql.startswith("LOCK")]
    assert locks == [
        f'LOCK TABLE "mrf"."mrf_address" IN {mode} MODE NOWAIT',
        f'LOCK TABLE "mrf"."npi_result_generation" IN {mode} MODE NOWAIT',
        f'LOCK TABLE "mrf"."reference_family_result_generation" IN {mode} MODE NOWAIT',
    ]
    inputs.require_supported_native_address_inputs(fence)
    for changed in [
        dict(fence, version=1),
        dict(fence, version=2),
        dict(fence, tiger_protected=None),
        dict(fence, database=dict(database_oid=82)),
    ]:
        with pytest.raises(RuntimeError):
            inputs.require_supported_native_address_inputs(changed)


@pytest.mark.asyncio
async def test_generic_revision_and_lock_sets_exclude_only_verified_tiger(monkeypatch):
    session = SourceSession()
    monkeypatch.setattr(inputs, "_require_ledger_guards", AsyncMock())
    assert await inputs._revision_guards(session, "mrf", _physical(), protected_tiger=_proof()) == []
    assert all(parameters["oids"] == [] for _, parameters in session.calls if parameters and "oids" in parameters)
    for bad in [None, dict(_proof(), parent_oids=[["zcta5", 101], ["zip_state", 999]])]:
        session.calls.clear()
        with pytest.raises(RuntimeError, match="tiger_proof_changed"):
            await inputs._lock_inputs(session, "mrf", _physical(), cutover=True, protected_tiger=bad)
        assert session.calls == []
    with pytest.raises(RuntimeError, match="tiger_proof_changed"):
        await inputs._lock_inputs(session, "mrf", _physical(), register=True, protected_tiger=_proof())
    with pytest.raises(RuntimeError):
        inputs._require_heaps(_physical())


class AdminConnection:
    def __init__(self):
        self.catalog = _catalog(_proof())
        self.calls = []
        self.roots = [
            dict(
                namespace=schema,
                name=name,
                oid=dict(_proof()["parent_oids"]).get(name) if schema == "tiger" else None,
                kind="r",
            )
            for schema, name in inputs.native_input_registration_roots("mrf")
        ]

    def is_in_transaction(self):
        return True

    async def fetch(self, sql, *arguments):
        self.calls.append((sql, arguments))
        return deepcopy(self.catalog if sql == tiger.CATALOG_SQL else self.roots)


@pytest.mark.asyncio
async def test_raw_capture_reports_two_verified_roots_without_factory_or_enrollment():
    native = AdminConnection()
    captured = await inputs.capture_current_native_input_candidates(
        native, ledger_schema="mrf", protected_tiger=_proof()
    )
    assert captured["candidates"] == ()
    assert len(captured["absent"]) == 16
    assert captured["protected_roots"] == tuple(
        dict(schema="tiger", table=name, proof=_proof()) for name in tiger.ROOTS
    )
    assert len(native.calls) == 2 and native.calls[1] == (tiger.CATALOG_SQL, ())


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["database", "oid", "extra", "shape", "root_oid"])
async def test_raw_capture_never_accepts_changed_proof_or_physical_pair(fault):
    native = AdminConnection()
    proof = _proof()
    if fault == "database":
        proof["database_oid"] = proof["inventory"]["database_oid"] = 82
    elif fault == "oid":
        proof["parent_oids"][0][1] = 999
    elif fault == "extra":
        proof["parent_oids"].append(["other", 103])
    elif fault == "shape":
        proof["unexpected"] = True
    else:
        native.roots[-1]["oid"] = 999
    with pytest.raises(RuntimeError):
        await inputs.capture_current_native_input_candidates(native, ledger_schema="mrf", protected_tiger=proof)


@pytest.mark.asyncio
async def test_protected_tiger_never_fabricates_legacy_reference_generation():
    session = SourceSession()
    assert await inputs._reference_authorities(session, "mrf", protected_tiger=_proof()) == {}
    assert session.calls[-1][1] == {"families": ["cms-doctors", "facility-anchors", "geo", "mrf-address", "mrf"]}
    assert await inputs._reference_authorities(session, "mrf") == {}
    assert session.calls[-1][1]["families"] == ["cms-doctors", "facility-anchors", "geo", "mrf-address", "mrf", "tiger"]


@pytest.mark.asyncio
async def test_tiger_version_three_preserves_optional_verified_nucc_wire():
    from process.provider_directory_cms_native_registration import _NativeNuccSession
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES
    from tests.test_cms_native_input_registration import ProtectedNuccProtocol

    nucc_proof = await inputs.capture_protected_nucc_input(_NativeNuccSession(ProtectedNuccProtocol()), schema="mrf")
    physical_by_name = {f'"{schema}"."{name}"': dict(relation_oid=None) for schema, name in inputs._relations("mrf")}
    physical_by_name.update(_physical())
    physical_by_name['"mrf"."nucc_taxonomy"'] = dict(relation_oid=201, relkind="p", inherited=True)
    fence_by_field = dict(
        version=3,
        schema="mrf",
        database=dict(database_oid=81),
        relations=physical_by_name,
        tiger_protected=_proof(),
        nucc_protected=nucc_proof,
        geo_bindings={"literal": "geo"},
        reference_authorities={family: {} for family in ("cms-doctors", "geo", "mrf")},
        cms_office_read_relations={name: {} for name in CMS_OFFICE_READ_TABLES},
    )
    inputs.require_supported_native_address_inputs(fence_by_field)
    assert fence_by_field["nucc_protected"] == nucc_proof and fence_by_field["tiger_protected"] == _proof()
    with pytest.raises(RuntimeError, match="nucc_proof_changed"):
        inputs.require_supported_native_address_inputs(dict(fence_by_field, nucc_protected=dict(nucc_proof, version=2)))


@pytest.mark.parametrize(
    "fault", ["extra_field", "missing_child", "reordered_children", "duplicate_file", "child_parent_overlap"]
)
def test_exact_proof_rejects_ambiguous_or_incomplete_inventory(fault):
    proof = _proof()
    if fault == "extra_field":
        proof["inventory"]["relations"][0]["unexpected"] = 1
    elif fault == "missing_child":
        proof["inventory"]["relations"].pop()
    elif fault == "reordered_children":
        proof["inventory"]["relations"].reverse()
    elif fault == "duplicate_file":
        proof["inventory"]["relations"][1]["relfilenode"] = 501
    else:
        proof["parent_oids"][0][1] = 201
    with pytest.raises(RuntimeError, match="tiger_proof_changed"):
        tiger.require_protected_tiger_proof(proof)


def _captured_proof():
    from process import tiger_held_inputs as held

    proof_by_field = dict(
        version=2,
        database_oid=81,
        node_id="fixture-node",
        selection_kind="captured-source",
        source_id="11111111-1111-4111-8111-111111111111",
        package_id="d" * 64,
        registration_sha256="e" * 64,
        inventory=_proof()["inventory"],
    )
    proof_by_field["selection_sha256"] = held._digest(proof_by_field)
    return proof_by_field


def _captured_catalog(proof):
    return [
        dict(child, database_oid=proof["database_oid"], protected=True, inherited=False, read_lock=True)
        for child in proof["inventory"]["relations"]
    ]


class CapturedSourceSession(SourceSession):
    def __init__(self):
        super().__init__()
        self.captured_rows = _captured_catalog(_captured_proof())
        self.physical = {
            f'"{namespace}"."{name}"': dict(
                name=f'"{namespace}"."{name}"', relation_oid=None, relkind=None, inherited=False
            )
            for namespace, name in inputs._relations("mrf")
        }
        for index, name in enumerate(inputs._GEO):
            self.physical[f'"mrf"."{name}"'].update(
                relation_oid=1001 + index, relfilenode=2001 + index, relkind="r", relpersistence="p"
            )
        for canonical, binding in tiger.captured_tiger_bindings(_captured_proof()).items():
            self.physical['"' + canonical.replace(".", '"."') + '"'].update(
                relation_oid=binding["relation_oid"],
                relfilenode=binding["relfilenode"],
                relkind="r",
                relpersistence="p",
            )

    async def execute(self, statement, parameters=None):
        sql = str(statement)
        if sql == tiger.CAPTURED_CATALOG_SQL:
            self.calls.append((sql, parameters))
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: deepcopy(self.captured_rows)))
        if "requested(name,physical_name)" in sql:
            self.calls.append((sql, parameters))
            for canonical, binding in tiger.captured_tiger_bindings(_captured_proof()).items():
                index = parameters["names"].index('"' + canonical.replace(".", '"."') + '"')
                assert parameters["physical_names"][index] == f'"{binding["schema_name"]}"."{binding["table_name"]}"'
            return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: deepcopy(list(self.physical.values()))))
        return await super().execute(statement, parameters)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("version", True),
        ("database_oid", True),
        ("node_id", ""),
        ("selection_kind", "installed"),
        ("source_id", "not-uuid"),
        ("package_id", "A" * 64),
        ("registration_sha256", "x" * 64),
        ("selection_sha256", "f" * 64),
        ("extra", 1),
    ],
)
def test_captured_proof_rejects_malformed_or_relabelled_linkage(field, bad):
    proof = _captured_proof()
    proof[field] = bad
    with pytest.raises(RuntimeError, match="tiger_proof_changed"):
        tiger.require_protected_tiger_proof(proof)


@pytest.mark.parametrize(
    "field,bad",
    [
        ("protected", False),
        ("inherited", True),
        ("read_lock", False),
        ("database_oid", 82),
        ("relation_oid", 999),
        ("relfilenode", 999),
        ("owner_oid", 999),
        ("schema_owner_oid", 999),
    ],
)
def test_captured_native_catalog_rejects_drift_without_actor_privilege_waiver(field, bad):
    catalog = _captured_catalog(_captured_proof())
    catalog[0][field] = bad
    with pytest.raises(RuntimeError, match="tiger_proof_changed"):
        tiger.require_captured_tiger_catalog(_captured_proof(), catalog)
    assert "current_user" not in tiger.CAPTURED_CATALOG_SQL
    assert (
        "NOT r.rolcanlogin" in tiger.CAPTURED_CATALOG_SQL and "privilege_type<>'SELECT'" in tiger.CAPTURED_CATALOG_SQL
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("cutover", [False, True])
async def test_captured_fence_maps_exact_heaps_before_queries_and_preserves_legacy_graph(monkeypatch, cutover):
    from process.provider_directory_cms_typed_offices import CMS_OFFICE_READ_TABLES

    session = CapturedSourceSession()
    archive_owner = AsyncMock()
    monkeypatch.setattr("process.provider_directory_cms_archive.require_source_archive_owner", archive_owner)
    monkeypatch.setattr(inputs, "_npi_revision", AsyncMock(return_value={"literal": "npi"}))
    references = AsyncMock(return_value={family: {} for family in ("cms-doctors", "geo", "mrf")})
    monkeypatch.setattr(inputs, "_reference_authorities", references)
    revisions = AsyncMock(return_value=[])
    monkeypatch.setattr(inputs, "_revision_guards", revisions)
    monkeypatch.setattr(
        inputs, "_cms_office_read_relations", AsyncMock(return_value={name: {} for name in CMS_OFFICE_READ_TABLES})
    )
    callback = AsyncMock(return_value=_captured_proof())
    async with policy.bind_generic_source_transaction_factory(IssuedFactory(callback)):
        fence = await inputs.capture_native_address_input_fence(session, "mrf", cutover=cutover)
    archive_owner.assert_awaited_once_with(session, "mrf")
    assert fence["version"] == 4 and fence["tiger_captured"] == _captured_proof()
    assert "tiger_protected" not in fence
    assert {
        name: fence["geo_bindings"][name] for name in ("tiger.zcta5", "tiger.zip_state")
    } == tiger.captured_tiger_bindings(_captured_proof())
    callback.assert_awaited_once_with(session)
    references.assert_awaited_once_with(session, "mrf", protected_tiger=_captured_proof())
    revisions.assert_awaited_once_with(session, "mrf", session.physical, captured_tiger=_captured_proof())
    sqls = [sql for sql, _ in session.calls]
    assert sqls.index(tiger.CAPTURED_CATALOG_SQL) < next(
        index for index, sql in enumerate(sqls) if "requested(name,physical_name)" in sql
    )
    assert all('"tiger".' not in sql for sql in sqls if sql.startswith("LOCK"))
    assert len([sql for sql in sqls if "requested(name,physical_name)" in sql]) == 2
    inputs.require_supported_native_address_inputs(fence)
    for changed in [dict(fence, version=3), dict(fence, tiger_captured=None), dict(fence, geo_bindings={})]:
        with pytest.raises(RuntimeError):
            inputs.require_supported_native_address_inputs(changed)


@pytest.mark.asyncio
async def test_raw_captured_verification_uses_same_driver_catalog_and_requires_held_locks():
    class Raw:
        active = True
        rows = _captured_catalog(_captured_proof())

        def is_in_transaction(self):
            return self.active

        async def fetch(self, statement, *arguments):
            assert "AS protected" in statement and arguments == (tiger.captured_tiger_names(_captured_proof()),)
            return deepcopy(self.rows)

    raw = Raw()
    assert policy._FACTORY.get() is None
    assert await tiger.verify_raw_protected_tiger_input(raw, _captured_proof()) == _captured_proof()
    raw.rows[0]["read_lock"] = False
    with pytest.raises(RuntimeError, match="tiger_proof_changed"):
        await tiger.verify_raw_protected_tiger_input(raw, _captured_proof())


@pytest.mark.asyncio
async def test_captured_revision_exclusion_keeps_mutable_guards_and_exact_pair(monkeypatch):
    session = SourceSession()
    relations_by_name = {
        name: relation for name, relation in CapturedSourceSession().physical.items() if name.startswith('"tiger".')
    }
    monkeypatch.setattr(inputs, "_require_ledger_guards", AsyncMock())
    assert await inputs._revision_guards(session, "mrf", relations_by_name, captured_tiger=_captured_proof()) == []
    assert all(parameters["oids"] == [] for _, parameters in session.calls if parameters and "oids" in parameters)
    relations_by_name['"mrf"."openaddresses_geocode"'] = dict(
        relation_oid=777, relfilenode=778, relkind="r", relpersistence="p", inherited=False
    )
    with pytest.raises(RuntimeError, match="native_revision_guard_unavailable"):
        await inputs._revision_guards(session, "mrf", relations_by_name, captured_tiger=_captured_proof())
    assert any(parameters and parameters.get("oids") == [777] for _, parameters in session.calls)
    await inputs._lock_inputs(session, "mrf", relations_by_name, cutover=True, captured_tiger=_captured_proof())
    assert any('"mrf"."openaddresses_geocode" IN SHARE MODE' in sql for sql, _ in session.calls)
    relations_by_name['"tiger"."zcta5"']["relfilenode"] += 100
    session.calls.clear()
    with pytest.raises(RuntimeError, match="tiger_proof_changed"):
        await inputs._lock_inputs(session, "mrf", relations_by_name, cutover=True, captured_tiger=_captured_proof())
    assert session.calls == []
