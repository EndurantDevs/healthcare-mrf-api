# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded ACA office carry, exact replay and unchanged legacy receipt encoding."""

import asyncio
import hashlib
import json
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID

import pytest

from process import network_custom_address_source as custom
from process import network_legacy_membership_source as legacy
from process import registry_aca_address_carry as carry
from process import registry_source_recipe_composition as composition
from process.network_address_artifact import NetworkAddressArtifactError
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_compact_layout import CompactAddressRelation
from process.registry_aca_source_recipe import _recipe
from tests.test_registry_aca_source_recipe import _ROLES, _prepared
from tests.test_registry_ptg_address_carry import _artifact


@pytest.fixture
async def recipe(monkeypatch):
    prepared, validation = await _prepared(monkeypatch)
    return _recipe(prepared, validation, _ROLES)


def _batch(recipe, *, empty=False):
    approved = ApprovedMembershipSource(4, "a" * 64)
    encoded = (
        b"[]"
        if empty
        else b'[{"network_id":7,"provider_system":"npi","provider_id":"1000000491","location_id":"00000000-0000-0000-0000-000000000008","evidence_id":"example"}]'
    )
    return legacy.ACAMembershipBatch(
        recipe.source_pin,
        0 if empty else 1,
        0 if empty else 1,
        0,
        None,
        encoded,
        hashlib.sha256(encoded).hexdigest(),
        approved,
        recipe.binding_coordinates,
        True,
        accepted_evidence_checksums=() if empty else (1,),
        complete_raw_facts=True,
        accepted_source_rows=0 if empty else 1,
    )


def _pages(monkeypatch, recipe):
    batch, terminal = _batch(recipe), _batch(recipe, empty=True)
    reader = AsyncMock(side_effect=[(batch, 1, 1000), (terminal, None, 1000)])
    monkeypatch.setattr(composition, "_read_recipe_page", reader)
    custody = AsyncMock()
    monkeypatch.setattr(carry, "_require_archive", custody)
    page = AsyncMock(return_value=({"native_office_proof": "b" * 64}, 1))
    monkeypatch.setattr(carry, "_page", page)
    return batch.approved_source, reader, custody, page


def test_aca_receipt_preserves_exact_legacy_bytes():
    original = _artifact()
    original_fields = asdict(original)
    original_fields.pop("ptg_carry_sha256")
    original_fields.pop("aca_carry_sha256")
    original_fields.pop("source_aliases")
    original_fields.pop("fhir_carry_sha256")
    assert original.as_dict() == {"component": "network_custom_address_source", "revision": 2, **original_fields}
    for ptg_digest in (None, "a" * 64):
        old = replace(original, ptg_carry_sha256=ptg_digest)
        added = replace(old, aca_carry_sha256="b" * 64)
        assert added.as_dict()["revision"] == 4
        assert custom._from_document(json.dumps(added.as_dict())) == added
        assert custom._generation(added) != custom._generation(old)
        expected_by_field = {
            "component": "network_custom_address_source",
            "revision": 3 if ptg_digest else 2,
            **original_fields,
        }
        if ptg_digest:
            expected_by_field["ptg_carry_sha256"] = ptg_digest
        assert json.dumps(old.as_dict(), sort_keys=True) == json.dumps(expected_by_field, sort_keys=True)


@pytest.mark.parametrize(
    "changes",
    [
        {"revision": 2},
        {"revision": 3},
        {"aca_carry_sha256": None},
        {"aca_carry_sha256": "B" * 64},
        {"aca_carry_sha256": True},
        {"ptg_carry_sha256": None},
        {"unknown": 1},
    ],
)
def test_aca_receipt_rejects_revision_null_digest_and_field_smuggling(changes):
    receipt = replace(_artifact(), aca_carry_sha256="b" * 64)
    with pytest.raises(custom.NetworkCustomAddressSourceError):
        custom._from_document(json.dumps({**receipt.as_dict(), **changes}))


@pytest.mark.asyncio
async def test_absent_aca_carry_never_reads_or_writes():
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock())
    assert (
        await carry.prepare_aca_address_carry(connection, (), object(), "control_fixture", "target_fixture")
        == carry.RegistryACAAddressCarry()
    )
    connection.execute.assert_not_awaited()
    connection.fetchval.assert_not_awaited()


@pytest.mark.asyncio
async def test_bounded_source_pages_bind_exact_recipe_and_readonly_replay(monkeypatch, recipe):
    approved, reader, custody, page = _pages(monkeypatch, recipe)
    written = await carry.prepare_aca_address_carry(
        object(), (recipe,), approved, "control_fixture", "target_fixture", writing=True
    )
    assert (written.pages, written.rows) == (1, 1)
    assert not hasattr(written, "records")
    custody.assert_awaited_once_with(custody.await_args.args[0], recipe.source_pin)
    assert page.await_args.kwargs == {"writing": True}
    assert reader.await_args_list[0].args[3:6] == (None, "control_fixture", 1000)
    _pages(monkeypatch, recipe)
    replayed = await carry.prepare_aca_address_carry(object(), (recipe,), approved, "control_fixture", "target_fixture")
    assert replayed == written
    assert carry._page.await_args.kwargs == {"writing": False}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fault",
    [
        "cursor",
        "input",
        "accounting",
        "scope",
        "unresolved",
        "cancel",
        "approval",
        "coordinates",
        "unfiltered",
        "boolean_count",
        "oversize",
    ],
)
async def test_changed_page_or_cancellation_refuses_whole_carry(monkeypatch, recipe, fault):
    approved, reader, _, page = _pages(monkeypatch, recipe)
    batch = _batch(recipe)
    if fault == "cancel":
        reader.side_effect = asyncio.CancelledError()
    else:
        changes_by_fault = {
            "input": {"input_sha256": "0" * 64},
            "accounting": {"accepted_source_rows": 0},
            "scope": {"source": None},
            "unresolved": {"unresolved_source_rows": 1},
            "approval": {"approved_source": None},
            "coordinates": {"binding_coordinates": None},
            "unfiltered": {"approved_only": False},
            "boolean_count": {"source_rows": True},
            "oversize": {"source_rows": 1001, "accepted_source_rows": 1001},
        }
        if fault in changes_by_fault:
            batch = replace(batch, **changes_by_fault[fault])
        reader.side_effect = [(batch, None if fault == "cursor" else 1, 1000)]
    with pytest.raises(asyncio.CancelledError if fault == "cancel" else ValueError):
        await carry.prepare_aca_address_carry(
            object(), (recipe,), approved, "control_fixture", "target_fixture", writing=True
        )
    page.assert_not_awaited()


@pytest.mark.asyncio
async def test_source_proof_uses_effective_physical_pin_and_native_canonical_encoder(monkeypatch, recipe):
    batch = _batch(recipe)
    physical = replace(recipe.source_pin)
    monkeypatch.setattr(legacy, "aca_physical_source", lambda _pin: physical, raising=False)
    documents = [{"provider_id": "1000000491", "location_id": str(UUID(int=8)), "location_key": "c" * 64}]
    connection = SimpleNamespace(
        fetchrow=AsyncMock(
            return_value={"invalid": False, "pairs": 1, "covered_pairs": 1, "records_json": json.dumps(documents)}
        )
    )
    monkeypatch.setattr(
        carry,
        "_canonical_source",
        lambda pin: (31, "c.payload") if pin is physical else pytest.fail("anchor used as physical"),
    )
    native = AsyncMock()
    monkeypatch.setattr(carry, "_canonical_bindings", native)
    assert await carry._source_documents(connection, batch) == documents
    sql, *args = connection.fetchrow.await_args.args
    assert "encode(sha256(convert_to" in sql and "'base'::text" in sql and "e.address_key" in sql
    assert args[1:5] == [physical.import_id, physical.issuer_id, physical.year, physical.source_url]
    assert "$2::text IS NULL" not in sql and "$5::text IS NULL" not in sql
    native.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid", [True, None])
async def test_binding_mismatch_refuses_even_on_readonly_replay(invalid):
    connection = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=invalid))
    bindings = json.dumps(
        [
            dict(
                zip(
                    carry._BINDING_COLUMNS,
                    ("npi", "1000000491", str(UUID(int=8)), "c" * 64, "npi", "1000000491"),
                    strict=True,
                )
            )
        ]
    )
    with pytest.raises(ValueError, match="binding_conflict"):
        await carry._bindings(connection, "target_fixture", bindings, writing=False)
    connection.execute.assert_not_awaited()
    assert "IS DISTINCT FROM" in connection.fetchval.await_args.args[0]


@pytest.mark.asyncio
async def test_new_rows_are_setwise_and_existing_offices_are_never_overwritten(monkeypatch, recipe):
    batch = _batch(recipe)
    documents = [{"provider_id": "1000000491"}]
    records = [{"native": "retained"}]
    source = AsyncMock(return_value=documents)
    offices = AsyncMock(return_value=records)
    bindings = AsyncMock(return_value=[])
    checked = AsyncMock()
    monkeypatch.setattr(carry, "_source_documents", source)
    monkeypatch.setattr(carry, "_office_records", offices)
    monkeypatch.setattr(carry, "_canonical_bindings", bindings)
    monkeypatch.setattr(carry, "_bindings", checked)
    monkeypatch.setattr(carry, "_expected_bindings", AsyncMock(return_value=("[]", [], [])))
    connection = SimpleNamespace(execute=AsyncMock())
    await carry._page(connection, batch, "target_fixture", writing=True)
    sql = connection.execute.await_args.args[0]
    assert "WHERE NOT EXISTS" in sql and "existing.npi=office.provider_id::bigint" in sql
    assert "existing.address_key=office.location_id" in sql and "UPDATE" not in sql and "ON CONFLICT" not in sql
    assert "'practice'" in sql and "'base'" in sql and "'street'" in sql
    connection.execute.reset_mock()
    await carry._page(connection, batch, "target_fixture", writing=False)
    connection.execute.assert_not_awaited()
    assert checked.await_args.kwargs == {"writing": False}


def _existing_address_connection(table_name, table_oid=123):
    physical_by_field = {"schema_name": "base_fixture", "relname": table_name, "oid": table_oid}
    return SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(side_effect=[physical_by_field, physical_by_field, {"bounded": True, "copied_rows": 1}]),
        fetchval=AsyncMock(return_value=table_oid),
        execute=AsyncMock(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("retained", [False, True])
async def test_existing_office_copy_locks_exact_native_origin(recipe, retained):
    table_name = "entity_address_unified_retained_7b" if retained else "entity_address_unified"
    connection = _existing_address_connection(table_name)
    origin = CompactAddressRelation("base_fixture", table_name, 123)
    await carry._copy_existing_offices(connection, _batch(recipe), "target_fixture", origin)
    assert connection.execute.await_args.args == (
        f'LOCK TABLE ONLY "base_fixture"."{table_name}" IN ACCESS SHARE MODE NOWAIT',
    )
    assert connection.fetchval.await_args.args == (
        "SELECT to_regclass($1)::oid::bigint",
        f'"base_fixture"."{table_name}"',
    )
    query = connection.fetchrow.await_args.args[0]
    assert f'FROM "base_fixture"."{table_name}" address' in query
    assert "ON CONFLICT(location_key) DO NOTHING" in query
    assert "address.address_key=pairs.location_id" in query


@pytest.mark.asyncio
@pytest.mark.parametrize("fault", ["logical_pin", "transaction", "name", "oid", "renamed", "bounds"])
async def test_existing_office_copy_refuses_unverified_or_changed_origin(recipe, fault):
    origin = CompactAddressRelation("base_fixture", "entity_address_unified_retained_7b", 123)
    connection = _existing_address_connection(origin.table_name)
    match fault:
        case "logical_pin":
            origin = PinnedAddressSource(origin.schema_name, origin.table_name, "same-label")
        case "transaction":
            connection.is_in_transaction = lambda: False
        case "name":
            connection = _existing_address_connection("entity_address_unified_retained_7c")
        case "oid":
            connection.fetchval.return_value = 124
        case "renamed":
            origin = CompactAddressRelation(origin.schema_name, "entity_address_unified", origin.table_oid)
        case "bounds":
            physical_by_field = {
                "schema_name": origin.schema_name,
                "relname": origin.table_name,
                "oid": origin.table_oid,
            }
            connection.fetchrow.side_effect = [
                physical_by_field,
                physical_by_field,
                {"bounded": False, "copied_rows": 0},
            ]
    with pytest.raises((ValueError, NetworkAddressArtifactError)):
        await carry._copy_existing_offices(connection, _batch(recipe), "target_fixture", origin)
    if fault in {"logical_pin", "transaction"}:
        connection.fetchrow.assert_not_awaited()
        connection.execute.assert_not_awaited()
    else:
        assert connection.fetchrow.await_count <= (3 if fault == "bounds" else 2)


@pytest.mark.asyncio
async def test_retained_artifact_native_proof_precedes_aca_page_reads(monkeypatch, recipe):
    from process import network_approved_membership_source as approved_module

    original = _artifact()
    events = []

    async def verify(*_args):
        events.append("native")

    async def prepare(*_args, **kwargs):
        events.append("pages")
        assert kwargs == {"writing": False}
        return carry.RegistryACAAddressCarry("a" * 64)

    monkeypatch.setattr(custom, "verify_custom_address_source", verify)
    monkeypatch.setattr(
        approved_module, "pin_approved_membership_source", AsyncMock(return_value=ApprovedMembershipSource(4, "a" * 64))
    )
    monkeypatch.setattr(carry, "prepare_aca_address_carry", prepare)
    result = await custom._aca_address_carry(
        object(), ((recipe,), None), original, "target_fixture", retained_receipt=original
    )
    assert result.sha256 == "a" * 64 and events == ["native", "pages"]


@pytest.mark.asyncio
async def test_changed_native_office_proof_changes_carry_digest(monkeypatch, recipe):
    approved, _, _, page = _pages(monkeypatch, recipe)
    original = await carry.prepare_aca_address_carry(object(), (recipe,), approved, "control_fixture", "target_fixture")
    approved, _, _, page = _pages(monkeypatch, recipe)
    page.return_value = ({"native_office_proof": "c" * 64}, 1)
    changed = await carry.prepare_aca_address_carry(object(), (recipe,), approved, "control_fixture", "target_fixture")
    assert changed.sha256 != original.sha256
    assert (changed.pages, changed.rows) == (original.pages, original.rows)


@pytest.mark.asyncio
async def test_carry_failure_stops_freeze_after_office_index(monkeypatch):
    events = []
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=None))

    async def heaps(*_args):
        events.append("heaps")
        return SimpleNamespace(sha256=None)

    async def index(*_args):
        events.append("index")

    async def fail(*_args, **kwargs):
        events.append("carry")
        assert kwargs == {"writing": True}
        raise asyncio.CancelledError

    freeze = AsyncMock()
    monkeypatch.setattr(custom, "_build_heaps", heaps)
    monkeypatch.setattr(custom, "_prepare_office_lookup", index)
    monkeypatch.setattr(custom, "_aca_address_carry", fail)
    monkeypatch.setattr(custom, "_freeze", freeze)
    with pytest.raises(asyncio.CancelledError):
        await custom._prepare_artifact(connection, _artifact(), "target_fixture", "control_fixture", ((), None))
    assert events == ["heaps", "index", "carry"]
    freeze.assert_not_awaited()


@pytest.mark.asyncio
async def test_loading_canonicalizes_once_and_replay_never_canonicalizes(monkeypatch, recipe):
    batch = _batch(recipe)
    documents = [{"source": "complete-source-row-proof"}]
    office_records = [
        {
            "provider_id": "1000000491",
            "location_id": str(UUID(int=8)),
            "location_key": "c" * 64,
            "address_row_sha256": "d" * 64,
            "evidence_sha256": "e" * 64,
        }
    ]
    binding = ("npi", "1000000491", UUID(int=8), "c" * 64, "npi", "1000000491")
    binding_documents = [dict(zip(carry._BINDING_COLUMNS, map(str, binding), strict=True))]
    monkeypatch.setattr(carry, "_source_documents", AsyncMock(return_value=documents))
    monkeypatch.setattr(carry, "_office_records", AsyncMock(return_value=office_records))
    monkeypatch.setattr(
        carry,
        "_expected_bindings",
        AsyncMock(return_value=(json.dumps(binding_documents), binding_documents, [binding])),
    )
    native = AsyncMock(return_value=[binding])
    monkeypatch.setattr(carry, "_canonical_bindings", native)
    check = AsyncMock()
    monkeypatch.setattr(carry, "_bindings", check)
    connection = SimpleNamespace(execute=AsyncMock())
    written = await carry._page(connection, batch, "target_fixture", writing=True)
    native.assert_awaited_once_with(connection, office_records)
    native.reset_mock()
    connection.execute.reset_mock()
    replayed = await carry._page(connection, batch, "target_fixture", writing=False)
    assert written == replayed
    assert written[0]["offices"] == office_records and written[0]["bindings"] == binding_documents
    assert written[0]["source"] == documents and written[1] == 1
    native.assert_not_awaited()
    connection.execute.assert_not_awaited()
    assert check.await_args.kwargs == {"writing": False}


@pytest.mark.asyncio
async def test_loading_refuses_native_binding_disagreement_before_binding_insert(monkeypatch, recipe):
    monkeypatch.setattr(carry, "_source_documents", AsyncMock(return_value=[]))
    monkeypatch.setattr(carry, "_office_records", AsyncMock(return_value=[]))
    monkeypatch.setattr(carry, "_expected_bindings", AsyncMock(return_value=("[]", [], [])))
    monkeypatch.setattr(carry, "_canonical_bindings", AsyncMock(return_value=[("changed",)]))
    bindings = AsyncMock()
    monkeypatch.setattr(carry, "_bindings", bindings)
    with pytest.raises(ValueError, match="native_binding_differs"):
        await carry._page(SimpleNamespace(execute=AsyncMock()), _batch(recipe), "target_fixture", writing=True)
    bindings.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("valid", [False, None])
async def test_expected_binding_derivation_refuses_nonunique_or_invalid_set(valid):
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"valid": valid, "bindings_json": "[]"}))
    with pytest.raises(ValueError, match="binding_conflict"):
        await carry._expected_bindings(connection, [])
    sql = connection.fetchrow.await_args.args[0]
    assert "count(DISTINCT(provider_id,location_id))" in sql
    assert "count(DISTINCT location_key)" in sql


@pytest.mark.asyncio
async def test_expected_binding_documents_match_exact_native_tuple_shape():
    binding = ("npi", "1000000491", str(UUID(int=8)), "c" * 64, "npi", "1000000491")
    encoded = json.dumps([dict(zip(carry._BINDING_COLUMNS, binding, strict=True))])
    connection = SimpleNamespace(fetchrow=AsyncMock(return_value={"valid": True, "bindings_json": encoded}))
    raw, documents, tuples = await carry._expected_bindings(connection, [{"full_office_row": "retained"}])
    assert raw == encoded and documents == json.loads(encoded)
    assert tuples == [(*binding[:2], UUID(int=8), *binding[3:])]


@pytest.mark.asyncio
@pytest.mark.parametrize("proof_field", ["source", "offices", "bindings"])
async def test_full_replay_input_mutation_changes_proof(monkeypatch, recipe, proof_field):
    approved, _, _, page = _pages(monkeypatch, recipe)
    original_by_field = {field: [{"full": "original"}] for field in ("source", "offices", "bindings")}
    page.return_value = (original_by_field, 1)
    original = await carry.prepare_aca_address_carry(object(), (recipe,), approved, "control_fixture", "target_fixture")
    approved, _, _, page = _pages(monkeypatch, recipe)
    page.return_value = ({**original_by_field, proof_field: [{"full": "changed"}]}, 1)
    changed = await carry.prepare_aca_address_carry(object(), (recipe,), approved, "control_fixture", "target_fixture")
    assert changed.sha256 != original.sha256


@pytest.mark.asyncio
async def test_replay_changed_carry_refuses_retained_receipt(monkeypatch):
    original = replace(_artifact(), aca_carry_sha256="b" * 64)
    connection = SimpleNamespace(fetchval=AsyncMock(return_value=json.dumps(original.as_dict())))
    monkeypatch.setattr(custom, "_ptg_address_carry", AsyncMock(return_value=SimpleNamespace(sha256=None)))
    monkeypatch.setattr(custom, "_aca_address_carry", AsyncMock(return_value=carry.RegistryACAAddressCarry("c" * 64)))
    verify = AsyncMock()
    monkeypatch.setattr(custom, "verify_custom_address_source", verify)
    with pytest.raises(custom.NetworkCustomAddressSourceError, match="recipe differs"):
        await custom._prepare_artifact(connection, _artifact(), "target_fixture", "control_fixture", ((), None))
    verify.assert_not_awaited()


@pytest.fixture
async def global_source(monkeypatch, recipe):
    from process.registry_aca_global_epoch import PinnedACAGlobalEpoch
    from tests.test_registry_aca_global_epoch import _global_prepared

    prepared, validation = await _global_prepared(monkeypatch)
    epoch = PinnedACAGlobalEpoch(prepared, validation, _ROLES)
    return replace(recipe.source_pin, retained_epoch=epoch)


@pytest.mark.asyncio
async def test_global_epoch_reads_all_imports_only_for_original_issuer_year(monkeypatch, recipe, global_source):
    events = []
    documents = [
        {"source_record_id": "office-a", "evidence_sha256": "a" * 64},
        {"source_record_id": "office-b", "evidence_sha256": "b" * 64},
    ]

    async def custody(connection, source):
        assert source is global_source
        events.append("custody")

    async def read(*_args):
        events.append("facts")
        return {"invalid": False, "pairs": 2, "covered_pairs": 2, "records_json": json.dumps(documents)}

    monkeypatch.setattr(carry, "_require_archive", custody)
    connection = SimpleNamespace(fetchrow=AsyncMock(side_effect=read))
    result = await carry._source_documents(connection, replace(_batch(recipe), source=global_source))
    assert result == documents and events == ["custody", "facts"]
    sql, *args = connection.fetchrow.await_args.args
    assert "($2::text IS NULL OR e.import_id=$2)" in sql
    assert "($5::text IS NULL OR e.source_url=$5)" in sql
    assert "e.issuer_id=$3 AND e.year=$4" in sql
    assert args[1:5] == [None, global_source.issuer_id, global_source.year, None]
    assert "to_jsonb(evidence)" in sql and "source_record_id" in sql
    assert "completed_publication" not in sql


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["import_id", "source_url"])
async def test_flat_none_filter_is_refused_before_fact_io(monkeypatch, recipe, field):
    corrupt = replace(recipe.source_pin)
    object.__setattr__(corrupt, field, None)
    monkeypatch.setattr(legacy, "aca_physical_source", lambda _source: corrupt)
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="physical_scope_invalid"):
        await carry._source_documents(connection, _batch(recipe))
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_foreign_scope_cannot_enable_null_filters(monkeypatch, recipe):
    monkeypatch.setattr(legacy, "aca_physical_source", lambda _source: SimpleNamespace(import_id=None, source_url=None))
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="physical_scope_invalid"):
        await carry._source_documents(connection, _batch(recipe))
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["issuer_id", "year"])
async def test_global_view_cannot_change_original_issuer_year(monkeypatch, recipe, global_source, field):
    scope = legacy.aca_physical_source(global_source)
    changed = replace(scope, **{field: getattr(scope, field) + 1})
    monkeypatch.setattr(legacy, "aca_physical_source", lambda _source: changed)
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="global_scope_invalid"):
        await carry._source_documents(connection, replace(_batch(recipe), source=global_source))
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_ambiguous_attachments_are_refused_before_fact_io(monkeypatch, recipe, global_source):
    object.__setattr__(global_source, "retained_successor", recipe.source_pin)
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError):
        await carry._source_documents(connection, replace(_batch(recipe), source=global_source))
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_global_native_custody_failure_prevents_fact_read(monkeypatch, recipe, global_source):
    monkeypatch.setattr(carry, "_require_archive", AsyncMock(side_effect=ValueError("native_epoch_changed")))
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="native_epoch_changed"):
        await carry._source_documents(connection, replace(_batch(recipe), source=global_source))
    connection.fetchrow.assert_not_awaited()


@pytest.mark.asyncio
async def test_foreign_epoch_lineage_is_refused_before_fact_io(recipe, global_source):
    from copy import copy

    epoch = copy(global_source.retained_epoch)
    generation = replace(epoch.prepared.manifest.source_serving_generation, origin_lineage_id=str(UUID(int=99)))
    manifest = replace(epoch.prepared.manifest, source_serving_generation=generation)
    object.__setattr__(epoch, "prepared", replace(epoch.prepared, manifest=manifest))
    object.__setattr__(global_source, "retained_epoch", epoch)
    connection = SimpleNamespace(fetchrow=AsyncMock())
    with pytest.raises(ValueError, match="lineage differs"):
        await carry._source_documents(connection, replace(_batch(recipe), source=global_source))
    connection.fetchrow.assert_not_awaited()
