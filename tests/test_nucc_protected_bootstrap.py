"""Closed bootstrap comments describe preserved genuine storage, never fabricated history."""

from copy import deepcopy

import pytest

from process import nucc_protected_bootstrap as bootstrap
from tests.test_nucc_protected_control import source_generation
from tests.test_nucc_snapshot_partition import parent_catalog


def baseline():
    physical = bootstrap.partition.validate_parent_catalog(parent_catalog())
    source_authority = source_generation(4, physical["child_oid"])
    baseline_by_field = {
        "contract": bootstrap.CONTRACT,
        "database_oid": 100,
        **{
            key: physical[key]
            for key in (
                "owner_oid",
                "parent_oid",
                "parent_index_oid",
                "child_oid",
                "child_schema",
                "child_schema_oid",
                "child_filenode",
            )
        },
        "child_index_oids": [502, 503, 504, 505],
        "old_owner_oid": 110,
        "old_schema_oid": 301,
        "old_schema_owner_oid": 111,
        "old_native_sha256": "a" * 64,
        "protected_native_sha256": "b" * 64,
        "protected_child_sha256": "d" * 64,
        "semantic_sha256": "c" * 64,
        "row_count": 4,
        "source_authority": source_authority,
        "source_revision_tracked": True,
        "adopted_at": "2026-10-17T05:00:00.000000Z",
    }
    baseline_by_field["baseline_sha256"] = bootstrap.receipt_digest(baseline_by_field)
    return baseline_by_field, physical, source_authority


def test_baseline_binds_preserved_child_and_actual_source_authority():
    baseline_by_field, physical, source_authority = baseline()
    assert bootstrap.validate_baseline(baseline_by_field, physical, source_authority) == baseline_by_field
    assert (
        "generation_id" not in baseline_by_field
        and "publication_fence" not in baseline_by_field
        and "run_id" not in baseline_by_field
    )


@pytest.mark.parametrize("field", ["child_oid", "child_filenode", "parent_oid", "owner_oid"])
def test_baseline_rejects_changed_native_object(field):
    baseline_by_field, physical, source_authority = baseline()
    physical[field] += 1
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="physical predecessor differs"):
        bootstrap.validate_baseline(baseline_by_field, physical, source_authority)


def test_baseline_refuses_mutated_source_even_with_valid_old_digest():
    baseline_by_field, physical, source_authority = baseline()
    changed = deepcopy(source_authority)
    changed["local_generation"] += 1
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="Source predecessor differs"):
        bootstrap.validate_baseline(baseline_by_field, physical, changed)


@pytest.mark.parametrize("field", ["unknown_provenance", "generation_id", "run_id", "claim_token"])
def test_baseline_refuses_invented_history_or_authority(field):
    baseline_by_field, physical, source_authority = baseline()
    baseline_by_field[field] = "invented"
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="fields differ"):
        bootstrap.validate_baseline(baseline_by_field, physical, source_authority)


@pytest.mark.asyncio
async def test_bootstrap_refuses_serialized_administrator_before_io():
    with pytest.raises(
        bootstrap.native.ReferenceFamilyArchiveError, match="original administrator transaction required"
    ):
        await bootstrap.adopt_nucc_parent({"superuser": True}, dataset_id=None)


@pytest.mark.asyncio
async def test_missing_migration_seed_refuses_before_heap_or_catalog_changes(monkeypatch):
    from uuid import uuid4

    class Reply:
        def mappings(self):
            return self

        def one_or_none(self):
            return None

    class Session:
        async def execute(self, statement, parameters):
            assert str(statement).startswith("SELECT importer_id,")
            assert "FOR UPDATE" in str(statement) and parameters == {"importer_id": "nucc"}
            return Reply()

    async def admitted_admin(_session):
        return {"admitted": True}

    async def forbidden_heap(_session):
        raise AssertionError("missing authority must refuse before heap locks or DDL")

    monkeypatch.setattr(bootstrap, "_original_admin", admitted_admin)
    monkeypatch.setattr(bootstrap, "_old_heap", forbidden_heap)
    with pytest.raises(RuntimeError, match="generation authority is unavailable"):
        await bootstrap.adopt_nucc_parent(Session(), dataset_id=uuid4())


@pytest.mark.parametrize("original_model", [False, True])
def test_original_admin_adoption_preserves_both_closed_index_contracts(original_model):
    from tests.test_nucc_snapshot_partition import legacy_parent_catalog

    captured = parent_catalog() if original_model else legacy_parent_catalog()
    old_child = deepcopy(captured["children"][0]["relation"])
    old_child.update(owner=110, schema="mrf", schema_owner=111, schema_oid=301, is_partition=False, bound=None)
    old_child["indexes"][0]["parents"] = []
    for index in old_child["indexes"]:
        index["owner"] = 110
    captured["parent"].pop("baseline", None)
    observed = bootstrap.partition.validate_preserved_parent_catalog(captured, old_child)
    assert observed["child_oid"] == old_child["oid"]
    old_child["indexes"][1]["oid"] += 1
    with pytest.raises(bootstrap.partition.NuccPartitionError):
        bootstrap.partition.validate_preserved_parent_catalog(captured, old_child)


def native_registration():
    """A declared native preimage for host decoding checks, never database authority."""
    import hashlib

    from process.provider_directory_cms_native_inputs import _advance_body

    return dict(
        relation_oid=202,
        schema_name="mrf",
        table_name="nucc_taxonomy",
        revision=7,
        mutation_xid=None,
        ledger_oid=401,
        ledger_owner_oid=102,
        trigger_oid=601,
        function_oid=602,
        function_sha256=hashlib.sha256(_advance_body("mrf").encode()).hexdigest(),
    )


def test_native_baseline_keeps_historical_shape_separate():
    marker, physical, authority = baseline()
    old_marker = deepcopy(marker)
    marker.update(contract=bootstrap.NATIVE_CONTRACT, native_registration=native_registration())
    marker["baseline_sha256"] = bootstrap.receipt_digest(
        {key: value for key, value in marker.items() if key != "baseline_sha256"}
    )
    assert bootstrap.validate_baseline(marker, physical, authority) == marker
    assert bootstrap.validate_baseline(old_marker, physical, authority) == old_marker
    assert set(old_marker) == bootstrap.FIELDS and "native_registration" not in old_marker
    marker["contract"] = bootstrap.CONTRACT
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="fields differ"):
        bootstrap.validate_baseline(marker, physical, authority)


@pytest.mark.parametrize(
    "field,value",
    [
        ("relation_oid", 203),
        ("table_name", "foreign"),
        ("revision", True),
        ("mutation_xid", "01"),
        ("function_sha256", "f" * 64),
    ],
)
def test_native_baseline_refuses_tampered_registration_even_with_new_digest(field, value):
    marker, physical, authority = baseline()
    marker.update(contract=bootstrap.NATIVE_CONTRACT, native_registration=native_registration())
    marker["native_registration"][field] = value
    marker["baseline_sha256"] = bootstrap.receipt_digest(
        {key: value for key, value in marker.items() if key != "baseline_sha256"}
    )
    with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="native registration differs"):
        bootstrap.validate_baseline(marker, physical, authority)


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
@pytest.mark.parametrize("ordinary_write", [False, True])
async def test_native_guard_retires_only_after_owner_and_acl_closure(monkeypatch, ordinary_write):
    from types import SimpleNamespace
    from uuid import UUID

    events = []
    child_by_field = dict(oid=202, owner=110, acl=[[110, 110, "INSERT", False]])
    owner_by_field = dict(oid=101, name='"retained_owner"')
    registration = native_registration()

    class Session:
        async def execute(self, statement):
            events.append(str(statement))

        async def scalar(self, statement, parameters):
            if str(statement) == bootstrap.partition.CHILD_CATALOG_SQL:
                return dict(owner=101, ordinary_write=ordinary_write)
            assert parameters == {"oid": 110}
            return '"former_writer"'

    async def ownership(*_arguments):
        return SimpleNamespace(schema_name="archive")

    async def capture(_session, oid, owner_oid):
        assert (oid, owner_oid) == (202, 101)
        events.append("recheck native registration")
        return registration

    async def driver(_session):
        return "original driver"

    async def verify(connection, *, evidence):
        assert connection == "original driver" and evidence == registration
        events.append("verify unchanged ledger")

    monkeypatch.setattr(bootstrap.native, "capture_model_family_stage_ownership", ownership)
    monkeypatch.setattr(bootstrap, "_capture_native_registration", capture)
    monkeypatch.setattr(bootstrap, "_registration_driver", driver)
    monkeypatch.setattr(bootstrap.registration, "verify_retired_native_input_adoption", verify)
    arguments = (
        Session(),
        child_by_field,
        dict(tgname="cms_native_input_mutation", native_registration=registration),
        owner_by_field,
        UUID(int=9),
    )
    if ordinary_write:
        with pytest.raises(bootstrap.native.ReferenceFamilyArchiveError, match="writer custody differs"):
            await bootstrap._adopt_heap(*arguments)
        assert not any("DROP TRIGGER" in event or "CREATE TABLE" in event for event in events)
        return
    await bootstrap._adopt_heap(*arguments)
    drop = next(index for index, event in enumerate(events) if "DROP TRIGGER" in event)
    assert all(
        next(index for index, event in enumerate(events) if token in event) < drop
        for token in ("SET SCHEMA", "OWNER TO", "REVOKE ALL", "recheck native registration")
    )
    assert events[drop + 1] == "verify unchanged ledger"
    assert next(index for index, event in enumerate(events) if "CREATE TABLE" in event) > drop


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


@pytest.mark.parametrize("field", ["native_registration", "unknown_field"])
def test_native_baseline_requires_closed_outer_and_registration_fields(field):
    marker, physical, authority = baseline()
    marker.update(contract=bootstrap.NATIVE_CONTRACT, native_registration=native_registration())
    if field == "native_registration":
        marker.pop(field)
    else:
        marker["native_registration"][field] = "foreign"
    marker["baseline_sha256"] = bootstrap.receipt_digest(
        {key: value for key, value in marker.items() if key != "baseline_sha256"}
    )
    with pytest.raises(RuntimeError):
        bootstrap.validate_baseline(marker, physical, authority)
