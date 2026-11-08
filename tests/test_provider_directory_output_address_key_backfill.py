# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure address contracts; native role and snapshot authority are separate proofs."""

import asyncio
import hashlib
import importlib
from contextlib import asynccontextmanager
from contextvars import ContextVar
from datetime import datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_output_address_key_backfill as repair

directory = importlib.import_module("process.provider_directory_fhir")


def _directory():
    return SimpleNamespace(
        _schema=lambda: "mrf",
        _clean_source_id_list=directory._clean_source_id_list,
        _state_fips_restore_sql=directory._state_fips_restore_sql,
        _country_restore_sql=directory._country_restore_sql,
        _build_location_key_record=directory._build_location_key_record,
        _PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES=ContextVar("address_override", default={}),
    )


@pytest.mark.parametrize("damage", ["schema", "seen", "scratch", "run", "batch", "boolbatch"])
def test_untrusted_selectors_refuse_before_database(damage):
    owner = _directory()
    kwargs_by_name = dict(db_schema=None, run_id=None, source_ids=None, seen_table=None, batch_size=None)
    if damage == "scratch":
        owner._PROVIDER_DIRECTORY_ARTIFACT_RELATION_OVERRIDES.set({repair.TARGET: "ordinary_heap"})
    else:
        key, value = {
            "schema": ("db_schema", "other"),
            "seen": ("seen_table", "ordinary_seen"),
            "run": ("run_id", ""),
            "batch": ("batch_size", 0),
            "boolbatch": ("batch_size", True),
        }[damage]
        kwargs_by_name[key] = value
    with pytest.raises(RuntimeError, match="provider_directory_output_snapshot_"):
        repair._selector(owner, **kwargs_by_name)


def test_builtin_resolution_retains_exact_predicates_and_zip_enrichment():
    owner = _directory()
    selector = repair._selector(owner, None, "run-a", ["source-b", "source-b"], None, 1)
    sql, params = repair.address_projection(owner, selector)
    assert params == {"run_id": "run-a", "source_ids": ["source-b"]}
    assert "r.last_seen_run_id=CAST(:run_id AS varchar)" in sql
    assert 'LEFT JOIN "mrf"."geo_zip_lookup" g ON g.zip_code=n.computed_zip5' in sql
    assert "[^0-9]" in sql and "length(digits) IN (3,4)" in sql
    assert "WHEN '12' THEN 'FL'" in sql and "UNITEDSTATESOFAMERICA" in sql
    assert "g.state" in sql and "g.city" in sql and "repair_eligible" in sql
    assert "SELECT r.*,COALESCE((" in sql and "),FALSE) AS repair_eligible" in sql
    assert not any(fragment in sql for fragment in ("addr_", "UPDATE ", "INSERT "))
    all_sql, all_params = repair.address_projection(owner, {"run_id": None, "source_ids": None})
    assert all_params == {} and "r.last_seen_run_id=" not in all_sql and "ANY(" not in all_sql


def _row(**updates):
    row = dict.fromkeys(str(column.name) for column in repair.output_models()[repair.TARGET].__table__.columns)
    row.update(
        source_id="source-a",
        resource_id="loc-a",
        updated_at=datetime(2026, 1, 1),
        country_code="840",
        first_line="1 Main Street",
        fhir_meta={"versionId": "1"},
    )
    row.update(
        repair_eligible=True,
        computed_zip5="00123",
        normalized_country="US",
        restored_city_name="City",
        restore_state_name=True,
    )
    row.update(updates)
    return row


@pytest.mark.parametrize("eligible,key", [(True, "00000000-0000-0000-0000-000000000001"), (False, "key"), (True, None)])
def test_application_exact_null_coalesce_and_immutable_cells(eligible, key):
    timestamp = datetime(2026, 1, 2)
    original = _row(repair_eligible=eligible)
    rows, changed = repair.repair_address_rows(_directory(), [original], [(key, "FL", "city")], timestamp)
    assert original["address_key"] is None and original["country_code"] == "840"
    result = rows[0]
    assert result["fhir_meta"] == original["fhir_meta"] and result["first_line"] == original["first_line"]
    if eligible and key:
        assert changed == 1 and result["updated_at"] == timestamp
        assert {name: result[name] for name in repair.ADDRESS_FIELDS} == dict(
            address_key=key,
            zip5="00123",
            city_name="City",
            state_name="FL",
            state_code="FL",
            city_norm="city",
            country_code="US",
        )
    else:
        assert changed == 0
        assert result == {name: original[name] for name in result}
    assert "repair_eligible" not in result


def test_nonnull_existing_cells_survive_null_native_values():
    original = _row(
        zip5="old",
        city_name="Old City",
        state_name="Old State",
        state_code="OS",
        city_norm="old",
        country_code="CA",
        computed_zip5=None,
        normalized_country=None,
        restored_city_name=None,
        restore_state_name=False,
    )
    rows, changed = repair.repair_address_rows(
        _directory(), [original], [("new-key", None, None)], datetime(2026, 1, 2)
    )
    assert changed == 1
    assert {name: rows[0][name] for name in repair.ADDRESS_FIELDS if name != "address_key"} == {
        name: original[name] for name in repair.ADDRESS_FIELDS if name != "address_key"
    }


@pytest.mark.parametrize("damage", ["count", "shape", "eligibility"])
def test_native_corruption_denies(damage):
    original = _row(repair_eligible=None if damage == "eligibility" else True)
    native = [] if damage == "count" else [("bad",)] if damage == "shape" else [("key", "FL", "city")]
    with pytest.raises(RuntimeError):
        repair.repair_address_rows(_directory(), [original], native, datetime(2026, 1, 2))


def test_complete_tuple_digest_is_framed_ordered_and_value_sensitive():
    original, copied, damaged = hashlib.sha256(), hashlib.sha256(), hashlib.sha256()
    repair._digest_rows(original, [{"a": "ab", "b": "c"}, {"a": "d"}])
    repair._digest_rows(copied, [{"b": "c", "a": "ab"}])
    repair._digest_rows(copied, [{"a": "d"}])
    repair._digest_rows(damaged, [{"a": "a", "b": "bc"}, {"a": "d"}])
    assert original.digest() == copied.digest() != damaged.digest()


@pytest.mark.parametrize("module", [None, SimpleNamespace(canonicalize_location_batch=lambda rows: [])])
def test_missing_or_non_native_module_refuses(monkeypatch, module):
    monkeypatch.setattr(repair.address_fast, "_fast_module", lambda: module)
    with pytest.raises(RuntimeError, match="address_native_required"):
        repair._native_module()


@pytest.mark.asyncio
async def test_fhir_dispatch_preserves_all_value_filters(monkeypatch):
    publish = AsyncMock(return_value=7)
    monkeypatch.setattr(repair, "publish_location_address_keys", publish)
    assert (
        await directory.publish_provider_directory_location_address_keys(
            "mrf", run_id="run-a", source_ids=["source-a"], seen_table="seen", batch_size=2
        )
        == 7
    )
    publish.assert_awaited_once_with(
        directory, "mrf", run_id="run-a", source_ids=["source-a"], seen_table="seen", batch_size=2
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "exists,rights,code", [(False, False, "snapshot_required"), (True, False, "cut_fence_required")]
)
async def test_missing_native_geo_or_fence_refuses_before_mutation(exists, rights, code):
    session = SimpleNamespace(scalar=AsyncMock(side_effect=[exists, rights]))
    with pytest.raises(RuntimeError, match=code):
        await repair._require_geo_fence_rights(session, "mrf")
    assert session.scalar.await_count == (2 if exists else 1)


@pytest.mark.asyncio
async def test_final_cut_locks_heap_and_revision_before_recheck(monkeypatch):
    session = SimpleNamespace(execute=AsyncMock())
    capability = SimpleNamespace(session=session, policy=SimpleNamespace(schema="mrf"))
    expected_by_field = {"origin": "actual"}
    recheck = AsyncMock(return_value=expected_by_field)
    monkeypatch.setattr(repair, "_geo_snapshot", recheck)
    await repair._lock_geo_cut(capability, expected_by_field)
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert "IN SHARE MODE NOWAIT" in statements[0] and "FOR SHARE NOWAIT" in statements[1]
    recheck.assert_awaited_once_with(session, "mrf")


@pytest.mark.asyncio
async def test_explicit_native_batch_ceiling_is_honored():
    sizes = []

    def canonicalize(batch):
        sizes.append(len(batch))
        return [("key", "FL", "city")] * len(batch)

    owner = SimpleNamespace(_location_key_inputs=lambda batch: batch)
    result = await repair._canonical_page(
        owner, SimpleNamespace(canonicalize_location_batch=canonicalize), [1, 2, 3], 2
    )
    assert sizes == [2, 1] and len(result) == 3


@pytest.mark.asyncio
async def test_each_native_chunk_count_is_checked_before_concatenation():
    owner = SimpleNamespace(_location_key_inputs=lambda batch: batch)
    native = SimpleNamespace(canonicalize_location_batch=lambda batch: [])
    with pytest.raises(RuntimeError, match="address_native_count_mismatch"):
        await repair._canonical_page(owner, native, [1, 2, 3], 2)


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["copy", "cancel", "fence"])
async def test_failed_attempt_cleans_only_issued_stage_and_never_publishes(monkeypatch, failure):
    owner = _directory()
    incumbent, stage = object(), object()
    operation_by_field = {"kind": "address_key_backfill"}
    pinned = SimpleNamespace(operation=operation_by_field, expected_changed=1, incumbent=incumbent, geo={})
    capability = object()

    @asynccontextmanager
    async def pinned_inputs(*args):
        yield pinned

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        yield capability

    error = asyncio.CancelledError() if failure == "cancel" else RuntimeError("deliberate_" + failure)
    fill = AsyncMock(side_effect=error) if failure != "fence" else AsyncMock(return_value=(object(), 1))
    fence = AsyncMock(side_effect=error) if failure == "fence" else AsyncMock()
    cleanup = AsyncMock()
    bundle = importlib.import_module("process.provider_directory_artifact_bundle_preparation")
    publish = AsyncMock()
    monkeypatch.setattr(repair, "_native_module", lambda: object())
    monkeypatch.setattr(repair, "_pinned_inputs", pinned_inputs)
    monkeypatch.setattr(repair.custody, "output_snapshot_transaction", transaction)
    monkeypatch.setattr(repair.custody, "_capture", AsyncMock(return_value={repair.TARGET: incumbent}))
    monkeypatch.setattr(repair.stages, "prepare_output_stage", AsyncMock(return_value=stage))
    monkeypatch.setattr(repair.journal, "record_output_progress", AsyncMock())
    monkeypatch.setattr(repair, "_fill", fill)
    monkeypatch.setattr(repair, "_lock_geo_cut", fence)
    monkeypatch.setattr(repair, "_discard", cleanup)
    monkeypatch.setattr(bundle, "apply_prepared_output_bundle", publish)
    with pytest.raises(type(error)) as raised:
        await repair.publish_location_address_keys(owner)
    assert raised.value is error
    cleanup.assert_awaited_once_with(owner, operation_by_field, [stage])
    publish.assert_not_awaited()


@pytest.mark.asyncio
async def test_pinned_inputs_keep_canonicalizer_through_first_page(monkeypatch):
    """The actual pinned reader and page boundary retain the supplied canonicalizer."""
    provision = importlib.import_module("process.provider_directory_output_snapshot_provision")
    owner, original = _directory(), _row()
    owner._location_key_inputs = directory._location_key_inputs
    headers = [SimpleNamespace(_mapping={"source_id": "source-a", "resource_id": "loc-a", "record_bytes": 100})]
    location_rows = [SimpleNamespace(_mapping=original)]
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=[SimpleNamespace(all=lambda batch=batch: batch) for batch in (headers, location_rows, [])]
        ),
        in_transaction=lambda: True,
        rollback=AsyncMock(),
        close=AsyncMock(),
    )
    database = SimpleNamespace(session_factory=lambda: session)
    owner.db = SimpleNamespace(
        _transaction_binding=lambda: None,
        has_reader_session=lambda: False,
        _connect_reader=AsyncMock(return_value=database),
    )
    decision_at = datetime(2026, 1, 2)
    publisher = SimpleNamespace(
        policy=SimpleNamespace(schema="mrf"), connection=SimpleNamespace(fetchval=AsyncMock(return_value=decision_at))
    )
    driver = SimpleNamespace(execute=AsyncMock(), fetchval=AsyncMock(return_value=41))
    incumbent = SimpleNamespace(oid=41, to_payload=lambda: {"oid": 41})
    batches = []

    def canonicalize(batch):
        batches.append(batch)
        return [("key", "FL", "city")] * len(batch)

    @asynccontextmanager
    async def transaction(*args, **kwargs):
        yield object()

    native = SimpleNamespace(canonicalize_location_batch=canonicalize)
    monkeypatch.setattr(repair, "source_transaction", transaction)
    monkeypatch.setattr(repair, "bound_publisher", lambda: publisher)
    monkeypatch.setattr(provision, "_capture", AsyncMock(return_value={repair.TARGET: {"oid": 41}}))
    monkeypatch.setattr(repair.custody, "_native_receipt", lambda *args: incumbent)
    monkeypatch.setattr(repair, "_require_geo_fence_rights", AsyncMock())
    monkeypatch.setattr(repair, "_require_reader", AsyncMock(return_value=driver))
    monkeypatch.setattr(repair, "_same_database", AsyncMock())
    monkeypatch.setattr(repair, "_geo_snapshot", AsyncMock(return_value={"oid": 42}))
    async with repair._pinned_inputs(owner, {"run_id": None, "source_ids": None}, native, 1) as pinned:
        assert pinned.native is native and pinned.incumbent is incumbent
        assert pinned.expected_changed == 1 and pinned.reader.is_active
        expected = hashlib.sha256()
        repaired, _ = repair.repair_address_rows(owner, [original], [("key", "FL", "city")], decision_at)
        repair._digest_rows(expected, repaired)
        assert pinned.expected_digest == expected.hexdigest()
    assert batches == [directory._location_key_inputs([original])]
    assert not pinned.reader.is_active and session.execute.await_count == 3
    session.rollback.assert_awaited_once()
    session.close.assert_awaited_once()
