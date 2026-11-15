"""Portable baseline/source/data contracts and bounded native guard retirement."""

from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import nucc_protected_bootstrap as bootstrap
from tests.test_nucc_protected_control import source_generation
from tests.test_nucc_snapshot_partition import legacy_parent_catalog, parent_catalog


def native_registration():
    import hashlib

    from process.provider_directory_cms_native_inputs import _advance_body

    return dict(
        schema_name="mrf",
        table_name="nucc_taxonomy",
        revision=7,
        mutation_xid=None,
        function_sha256=hashlib.sha256(_advance_body("mrf").encode()).hexdigest(),
    )


def baseline(*, legacy=False, registered=False):
    names = bootstrap.partition.validate_parent_catalog(parent_catalog())
    authority = source_generation(4)
    baseline_by_field = dict(
        contract=bootstrap.NATIVE_CONTRACT if registered else bootstrap.CONTRACT,
        parent=list(names["parent"]),
        parent_index=list(names["parent_index"]),
        child=list(names["child"]),
        semantic_sha256="c" * 64,
        row_count=4,
        source_authority=authority,
        source_revision_tracked=True,
        adopted_at="2026-10-17T05:00:00.000000Z",
    )
    if legacy:
        baseline_by_field.pop("parent")
        baseline_by_field.pop("parent_index")
        baseline_by_field.pop("child")
        baseline_by_field.update(
            child_schema=names["child"][0],
            child_index_oids=["original PK", "original index"],
            old_native_sha256="a" * 64,
            protected_native_sha256="b" * 64,
            protected_child_sha256="d" * 64,
        )
        for field in bootstrap._LEGACY_FIELDS - set(baseline_by_field) - {"baseline_sha256"}:
            baseline_by_field[field] = "opaque original " + field
        baseline_by_field["source_authority"] = {**authority, "relation_oids": "opaque original generation"}
        baseline_by_field["source_authority"].pop("relations")
    if registered:
        baseline_by_field["native_registration"] = native_registration()
    baseline_by_field["baseline_sha256"] = "f" * 64 if legacy else bootstrap.receipt_digest(baseline_by_field)
    return baseline_by_field, names, authority


@pytest.mark.parametrize("legacy", (False, True))
@pytest.mark.parametrize("registered", (False, True))
def test_baseline_preserves_raw_editions_and_validates_named_child_source_and_content(legacy, registered):
    value, names, authority = baseline(legacy=legacy, registered=registered)
    before = deepcopy(value)
    assert bootstrap.validate_baseline(value, names, authority) == before == value
    assert set(value).isdisjoint({"generation_id", "publication_fence", "run_id"})
    if not legacy:
        assert not any("oid" in key or "owner" in key or "native_sha" in key for key in value)


@pytest.mark.parametrize("field", ("child", "parent", "parent_index"))
def test_baseline_refuses_different_declared_names(field):
    value, names, authority = baseline()
    names[field] = ("other", names[field][1])
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="named .*differs"):
        bootstrap.validate_baseline(value, names, authority)


@pytest.mark.parametrize("legacy", (False, True))
def test_baseline_refuses_changed_logical_source_generation(legacy):
    value, names, authority = baseline(legacy=legacy)
    authority = deepcopy(authority)
    authority["local_generation"] += 1
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="Source predecessor differs"):
        bootstrap.validate_baseline(value, names, authority)


@pytest.mark.parametrize(
    "field,value",
    [
        ("row_count", -1),
        ("row_count", 10001),
        ("row_count", True),
        ("semantic_sha256", "invalid"),
        ("source_revision_tracked", None),
    ],
)
def test_baseline_refuses_invalid_content_metadata_even_with_valid_new_digest(field, value):
    marker, names, authority = baseline()
    marker[field] = value
    marker["baseline_sha256"] = bootstrap.receipt_digest({k: v for k, v in marker.items() if k != "baseline_sha256"})
    with pytest.raises((bootstrap.native.ReferenceFamilyArchiveError, ValueError)):
        bootstrap.validate_baseline(marker, names, authority)


@pytest.mark.parametrize("field", ["unknown_provenance", "generation_id", "run_id", "claim_token"])
def test_baseline_refuses_invented_history_or_authority(field):
    value, names, authority = baseline()
    value[field] = "invented"
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="fields differ"):
        bootstrap.validate_baseline(value, names, authority)


@pytest.mark.parametrize(
    "field,value",
    [("table_name", "foreign"), ("revision", True), ("mutation_xid", "01"), ("function_sha256", "f" * 64)],
)
def test_native_baseline_validates_semantic_registration_even_with_new_outer_digest(field, value):
    marker, names, authority = baseline(registered=True)
    marker["native_registration"][field] = value
    marker["baseline_sha256"] = bootstrap.receipt_digest({k: v for k, v in marker.items() if k != "baseline_sha256"})
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="native registration differs"):
        bootstrap.validate_baseline(marker, names, authority)


@pytest.mark.parametrize("legacy", (False, True))
def test_preserved_adoption_keeps_both_complete_index_contracts(legacy):
    captured = legacy_parent_catalog() if legacy else parent_catalog()
    child = deepcopy(captured["children"][0]["relation"])
    assert bootstrap.partition.validate_preserved_parent_catalog(
        captured, child
    ) == bootstrap.partition.validate_parent_catalog(captured)
    child["indexes"][0]["columns"] = ["other"]
    with pytest.raises(bootstrap.partition.NuccPartitionError):
        bootstrap.partition.validate_preserved_parent_catalog(captured, child)


@pytest.mark.asyncio
async def test_bootstrap_refuses_serialized_administrator_before_io():
    with pytest.raises(
        bootstrap.native.ReferenceFamilyArchiveError, match="original administrator transaction required"
    ):
        await bootstrap.adopt_nucc_parent({"role": "administrator"}, dataset_id="invalid")


@pytest.mark.asyncio
async def test_missing_generation_seed_refuses_before_heap_or_ddl(monkeypatch):
    monkeypatch.setattr(bootstrap, "_original_admin", AsyncMock())
    read = AsyncMock(side_effect=RuntimeError("authority unavailable"))
    heap = AsyncMock()
    monkeypatch.setattr(bootstrap.native, "read_reference_family_result_generation_authority", read)
    monkeypatch.setattr(bootstrap, "_old_heap", heap)
    with pytest.raises(RuntimeError, match="authority unavailable"):
        await bootstrap.adopt_nucc_parent(object(), dataset_id=bootstrap.UUID(int=1))
    heap.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", (False, True))
async def test_native_guard_retirement_uses_named_semantics_and_propagates_changed_guard(monkeypatch, changed):
    driver = object()
    monkeypatch.setattr(bootstrap, "_registration_driver", AsyncMock(return_value=driver))
    guards = AsyncMock(return_value=[dict(tgname="cms_native_input_mutation", safe=not changed)])
    retired = AsyncMock()
    monkeypatch.setattr(bootstrap.registration, "_native_relation_guards", guards)
    monkeypatch.setattr(bootstrap.registration, "verify_retired_native_input_adoption", retired)
    session = SimpleNamespace(execute=AsyncMock())
    schema = "reference_family_archive_" + "a" * 32
    if changed:
        with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="native guard changed"):
            await bootstrap._retire_native_guard(session, {}, {}, f'"{schema}"."nucc_taxonomy"', native_registration())
        session.execute.assert_not_awaited()
        retired.assert_not_awaited()
    else:
        await bootstrap._retire_native_guard(session, {}, {}, f'"{schema}"."nucc_taxonomy"', native_registration())
        assert (
            str(session.execute.await_args.args[0])
            == f'DROP TRIGGER "cms_native_input_mutation" ON "{schema}"."nucc_taxonomy"'
        )
        retired.assert_awaited_once_with(driver, evidence=native_registration())
    guards.assert_awaited_once_with(driver, schema, "nucc_taxonomy", "mrf")


@pytest.mark.asyncio
async def test_preserved_content_equality_remains_required_before_baseline(monkeypatch):
    session = SimpleNamespace(scalar=AsyncMock(return_value=parent_catalog()))
    catalog = parent_catalog()
    child = catalog["children"][0]["relation"]
    named = bootstrap.partition.validate_parent_catalog(catalog)
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="preserved content changed"):
        await bootstrap._prove_preserved(session, child, named, (4, "a" * 64), (4, "b" * 64))
    session.scalar.assert_not_awaited()


@pytest.mark.asyncio
async def test_native_hook_is_authenticated_before_legacy_classification(monkeypatch):
    class Reply:
        def mappings(self):
            return self

        def all(self):
            return [
                dict(
                    oid=601,
                    tgname="cms_native_input_mutation",
                    proname="cms_native_input_advance",
                    prosrc="native body",
                    shape=False,
                )
            ]

    class Session:
        async def execute(self, statement, parameters):
            assert parameters == {"oid": 202}
            return Reply()

    async def capture(_session, oid, owner_oid):
        assert (oid, owner_oid) == (202, 110)
        return native_registration()

    monkeypatch.setattr(bootstrap, "_capture_native_registration", capture)
    observed = await bootstrap._legacy_hook(Session(), 202, 110)
    assert observed["native_registration"] == native_registration()


@pytest.mark.asyncio
@pytest.mark.parametrize("has_reference_hook", [False, True])
async def test_historical_hook_classification_preserves_exact_legacy_contract(monkeypatch, has_reference_hook):
    from process.cms_doctors_immutable import _legacy_contracts
    from process.reference_source_generation import REVISION_TRIGGER

    contract = _legacy_contracts("mrf")[REVISION_TRIGGER]
    hooks = (
        [dict(oid=601, tgname=REVISION_TRIGGER, proname=contract[1], prosrc=contract[3], shape=True)]
        if has_reference_hook
        else []
    )

    class Reply:
        def mappings(self):
            return self

        def all(self):
            return hooks

    class Session:
        async def execute(self, statement, parameters):
            return Reply()

    async def no_registration(_session, oid, owner_oid):
        assert (oid, owner_oid) == (202, 110)
        return None

    monkeypatch.setattr(bootstrap, "_capture_native_registration", no_registration)
    observed = await bootstrap._legacy_hook(Session(), 202, 110)
    assert observed == (hooks[0] if has_reference_hook else None)


def _empty_parent_case(monkeypatch, denied):
    from process import provider_directory_generic_source_provision as provision

    statements = []

    async def execute(statement, *arguments):
        statement = str(statement)
        statements.append(statement)
        if denied and statement.startswith('REVOKE ALL ON TABLE "mrf"."nucc_taxonomy" FROM "snapshot_owner"'):
            raise PermissionError("native parent revoke denied")

    driver = SimpleNamespace(
        execute=AsyncMock(side_effect=execute),
        fetch=AsyncMock(
            return_value=[
                {"role": "legacy_writer", "column_name": None},
                {"role": "column_writer", "column_name": "code"},
            ]
        ),
    )
    session = SimpleNamespace(execute=AsyncMock(side_effect=execute))
    ownership = object()
    monkeypatch.setattr(bootstrap.native, "capture_model_family_stage_ownership", AsyncMock(return_value=ownership))
    monkeypatch.setattr(bootstrap.native, "seal_model_family_storage", AsyncMock())
    monkeypatch.setattr(bootstrap, "_registration_driver", AsyncMock(return_value=driver))
    selected = AsyncMock(side_effect=AssertionError("general selected freeze must reject partition parents"))
    monkeypatch.setattr(provision, "freeze_fhir_source_snapshot", selected)
    freeze = AsyncMock(wraps=provision._freeze_snapshot_relation)
    monkeypatch.setattr(provision, "_freeze_snapshot_relation", freeze)
    return SimpleNamespace(
        session=session, driver=driver, ownership=ownership, statements=statements, selected=selected, freeze=freeze
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("denied", (False, True))
async def test_empty_parent_freeze_precedes_attach_and_propagates_native_denial(monkeypatch, denied):
    case = _empty_parent_case(monkeypatch, denied)
    dataset_id = bootstrap.UUID(int=1)
    schema = bootstrap.native.reference_family_stage_schema(dataset_id)
    owner_by_field = {"name": '"snapshot_owner"', "role": "snapshot_owner", "readers": ("api_reader", "profile_reader")}
    if denied:
        with pytest.raises(PermissionError, match="native parent revoke denied"):
            await bootstrap._adopt_heap(case.session, {}, None, owner_by_field, dataset_id)
    else:
        assert await bootstrap._adopt_heap(case.session, {}, None, owner_by_field, dataset_id) is case.ownership
    case.selected.assert_not_awaited()
    case.freeze.assert_awaited_once_with(
        case.driver,
        "mrf",
        {"relname": "nucc_taxonomy", "relkind": "p"},
        '"snapshot_owner"',
        ('"api_reader"', '"profile_reader"'),
    )
    bootstrap.native.seal_model_family_storage.assert_awaited_once_with(case.session, case.ownership)
    assert case.driver.fetch.await_args.args[1] == '"mrf"."nucc_taxonomy"'
    assert 'REVOKE ALL PRIVILEGES ON TABLE "mrf"."nucc_taxonomy" FROM "legacy_writer" CASCADE' in case.statements
    assert (
        'REVOKE ALL PRIVILEGES ("code") ON TABLE "mrf"."nucc_taxonomy" FROM "column_writer" CASCADE' in case.statements
    )
    attach_statements = [statement for statement in case.statements if "ATTACH PARTITION" in statement]
    if denied:
        assert not attach_statements and not any(statement.startswith("GRANT ") for statement in case.statements)
    else:
        select = 'GRANT SELECT ON TABLE "mrf"."nucc_taxonomy" TO "api_reader","profile_reader"'
        assert select in case.statements
        assert attach_statements == [
            f'ALTER TABLE "mrf"."nucc_taxonomy" ATTACH PARTITION "{schema}"."nucc_taxonomy" FOR VALUES FROM (MINVALUE) TO (MAXVALUE)'
        ]
        parent = next(statement for statement in case.statements if statement.startswith('CREATE TABLE "mrf".'))
        assert "PARTITION BY RANGE(code)" in parent
        assert (
            case.statements.index(parent) < case.statements.index(select) < case.statements.index(attach_statements[0])
        )
        assert not any(
            statement.startswith(("GRANT INSERT", "GRANT UPDATE", "GRANT DELETE")) for statement in case.statements
        )
