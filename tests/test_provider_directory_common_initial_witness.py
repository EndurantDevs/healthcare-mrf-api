# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Initial witnesses retain original history and authenticate the actual read actor."""

import asyncio
from contextlib import nullcontext
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import provider_directory_common_initial_witness as witness
from tests.test_provider_directory_common_serving_recovery_document import common, native


def _initial_targets(before):
    roots = before["roots"]
    return dict(
        contract_id=witness.initial_contract.TARGET_CONTRACT,
        resolution="empty",
        serving_singleton_absent=True,
        initial_commit_receipt_absent=True,
        evidence_target_oid=roots["provider_directory_profile_evidence"]["oid"],
        profile_target_oid=roots["provider_directory_profile"]["oid"],
        evidence_target_storage_fingerprint="a" * 64,
        profile_target_storage_fingerprint="a" * 64,
        evidence_target_bytes=0,
        profile_target_bytes=0,
        evidence_rows=0,
        profile_rows=0,
        historical_publication=None,
    )


def _journal_native():
    result_by_field = native(witness.receipts.JOURNAL, 999)
    result_by_field["columns"] = [
        dict(name=name, type=kind, required=True, default=None, generated="", identity="")
        for name, kind in [
            ("generation", "uuid"),
            ("phase", "text"),
            ("source_id", "text"),
            ("document", "jsonb"),
            ("document_sha256", "text"),
        ]
    ]
    result_by_field["constraints"] = [
        dict(name=name, type="c", definition=definition, validated=True, deferrable=False, deferred=False)
        for name, definition in witness.recovery._JOURNAL_CHECKS.items()
    ]
    result_by_field["constraints"].append(
        dict(name="pd_serving_recovery_key", type="p", validated=True, deferrable=False, deferred=False)
    )
    result_by_field["indexes"] = [dict(primary=True, keys=["generation", "phase"], valid=True, ready=True, live=True)]
    return result_by_field


def _observer_and_receipt(published, reference):
    """Use a real observer identity distinct from the original native owner."""
    actor_by_field = dict(session="source_login", current="source_reader", owner=21, backend=31, database_oid=50)
    driver = SimpleNamespace(
        fetch=AsyncMock(),
        fetchrow=AsyncMock(),
        fetchval=AsyncMock(),
        execute=AsyncMock(),
        is_in_transaction=Mock(return_value=True),
    )
    session = SimpleNamespace(in_transaction=lambda: True)
    fhir = SimpleNamespace(
        db=SimpleNamespace(),
        ImportRun=SimpleNamespace(__tablename__="import_run"),
        _provider_directory_profile_relation_storage_fingerprint=AsyncMock(
            return_value=SimpleNamespace(exact_fingerprint="a" * 64)
        ),
    )
    context = witness._ReadContext(fhir, session, driver, None, actor_by_field, "repeatable read", "on", "1/2")
    receipt_by_field = {
        **reference,
        "contract_id": witness.initial_contract.COMMIT_CONTRACT,
        "publication_xid": 40,
        "attestation_id": "d" * 64,
        "committed_at": "2026-09-29T00:00:00Z",
        "payload": {
            "serving": deepcopy(published["after"]["metadata"]["profile"]),
            "initial_target_state_sha256": witness.initial_contract.target_state_sha256(
                published["before"]["initial_targets"]
            ),
        },
    }
    return context, receipt_by_field


@pytest.fixture
def case(common, monkeypatch):
    """A valid complete initial cut and its exact scoped inverse."""
    from process import provider_directory_common_serving_recovery as inverse

    # This fixture isolates historical native-read semantics.  Real signed
    # inverse-consumption verification has its own independent regressions.
    verified_inverse = AsyncMock()
    monkeypatch.setattr(inverse, "validate_consumed_inverse", verified_inverse)
    published = deepcopy(common.document)
    before = published["before"]
    before["metadata"]["profile"] = None
    before["initial_targets"] = _initial_targets(before)
    reference_by_field = dict(
        build_id="pdpb_" + "1" * 32,
        generation_id=published["preparation"]["profile_result"]["profile_generation_id"],
        run_id="run_" + UUID(published["generation"]).hex,
        payload_sha256="c" * 64,
    )
    published["preparation"]["initial_receipt"] = reference_by_field
    published["journal"] = _journal_native()
    for family in ("profile", "evidence"):
        root = witness.common.documents.FAMILIES[family]
        published["after"]["roots"][root]["oid"] += 10000
        entry = next(entry for entry in published["preparation"]["retention"]["families"] if entry["family"] == family)
        entry.update(
            name=witness.receipts.retained_name(entry["source_oid"]),
            oid=entry["source_oid"],
            columns=[],
            rows=0,
            data_sha256=None,
            copied_bytes=0,
            reused_initial=True,
        )
    recovered = deepcopy(published)
    recovered.update(
        phase="recovered",
        predecessor="a" * 64,
        before=deepcopy(published["after"]),
        after=deepcopy(before),
        inverse=dict(operation_sha256="e" * 64, run_id="run_" + "f" * 32, lease_digest="d" * 64),
    )
    recovered["after"]["metadata"]["address"]["local_generation"] += 2
    events_by_phase = {
        "published": dict(document=published, sha256="a" * 64),
        "recovered": dict(document=recovered, sha256="b" * 64),
    }
    for phase, event in events_by_phase.items():
        witness.recovery._document(event["document"], phase, published["generation"], published["source"]["source_id"])
    witness.common.validate_history(events_by_phase)
    context, receipt = _observer_and_receipt(published, reference_by_field)
    return SimpleNamespace(
        published=published,
        recovered=recovered,
        events=events_by_phase,
        context=context,
        receipt=receipt,
        original=before["initial_targets"],
        reference=reference_by_field,
        verified_inverse=verified_inverse,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("phase_set", [("published",), ("published", "recovered")])
async def test_immutable_initial_link_survives_later_serving_state(case, monkeypatch, phase_set):
    rows = [
        dict(
            phase=phase,
            source_id=case.published["source"]["source_id"],
            document=case.events[phase]["document"],
            document_sha256=case.events[phase]["sha256"],
            valid_hash=True,
        )
        for phase in phase_set
    ]
    case.context.driver.fetch.return_value = rows
    monkeypatch.setattr(witness, "_readable", AsyncMock())
    monkeypatch.setattr(
        witness, "_catalog", AsyncMock(return_value={witness.receipts.JOURNAL: case.published["journal"]})
    )
    admitted = AsyncMock(return_value=case.original)
    monkeypatch.setattr(witness, "_admitted_targets", admitted)
    result = await witness._history(case.context, "unit", case.receipt)
    assert case.verified_inverse.await_count == int("recovered" in phase_set)
    assert set(result) == set(phase_set)
    assert case.context.driver.fetch.await_args.args[1] == UUID(case.published["generation"])
    admitted.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["hash", "xid", "reference", "original", "catalog", "database", "inverse"])
async def test_history_rejects_unbound_initial_evidence(case, monkeypatch, damage):
    journal_rows = [
        dict(
            phase=phase,
            source_id=case.published["source"]["source_id"],
            document=deepcopy(event["document"]),
            document_sha256=event["sha256"],
            valid_hash=True,
        )
        for phase, event in case.events.items()
    ]
    catalog = deepcopy(case.published["journal"])
    original = deepcopy(case.original)
    if damage == "hash":
        journal_rows[0]["valid_hash"] = False
    if damage == "xid":
        case.receipt["publication_xid"] = 41
    if damage == "reference":
        case.receipt["payload_sha256"] = "e" * 64
    if damage == "original":
        journal_rows[0]["document"]["before"]["initial_targets"]["profile_target_bytes"] = 1
        journal_rows[1]["document"]["after"]["initial_targets"]["profile_target_bytes"] = 1
    if damage == "catalog":
        catalog["oid"] += 1
    if damage == "database":
        case.context.actor["database_oid"] += 1
    if damage == "inverse":
        journal_rows[1]["document"]["after"]["metadata"]["alias"]["generation"] += 1
    case.context.driver.fetch.return_value = journal_rows
    monkeypatch.setattr(witness, "_readable", AsyncMock())
    monkeypatch.setattr(witness, "_catalog", AsyncMock(return_value={witness.receipts.JOURNAL: catalog}))
    monkeypatch.setattr(witness, "_admitted_targets", AsyncMock(return_value=original))
    with pytest.raises(ValueError):
        await witness._history(case.context, "unit", case.receipt)


@pytest.mark.asyncio
async def test_reader_uses_stored_native_owner_without_impersonating_it(case, monkeypatch):
    owner = case.published["writer"]["owner"]
    assert case.context.actor["owner"] != owner
    catalog = AsyncMock(return_value=case.recovered["after"]["roots"])
    monkeypatch.setattr(witness, "_catalog", catalog)
    monkeypatch.setattr(witness, "_metadata", AsyncMock(return_value=case.recovered["after"]["metadata"]))
    monkeypatch.setattr(witness, "_retained", AsyncMock(return_value=case.published["preparation"]["retention"]))
    monkeypatch.setattr(witness, "_restored_rows", AsyncMock())
    monkeypatch.setattr(witness.receipts.generations, "read_entity_address_result_generation_authority", AsyncMock())
    original_history = AsyncMock()
    monkeypatch.setattr(witness.initial_recovery, "capture_original_history", original_history)
    result = await witness._current_inverse(case.context, "unit", case.events)
    assert result["initial_targets"] == case.original
    assert result["initial_receipt"] == case.reference
    assert result["recovery_id"] == case.published["generation"]
    assert result["recovered_event_sha256"] == "b" * 64
    assert all(call.args[-1] == owner for call in catalog.await_args_list)
    assert original_history.await_args.args[-1] == result


@pytest.mark.asyncio
@pytest.mark.parametrize("damage", ["roots", "metadata", "rows", "initial_layout"])
async def test_current_inverse_requires_complete_restored_state(case, monkeypatch, damage):
    monkeypatch.setattr(witness.receipts, "normalized_receipt_scope", lambda *_args: nullcontext())
    roots = deepcopy(case.recovered["after"]["roots"])
    metadata = deepcopy(case.recovered["after"]["metadata"])
    if damage == "roots":
        roots["entity_address_unified"]["oid"] += 1
    if damage == "metadata":
        metadata["address"]["local_generation"] += 1
    monkeypatch.setattr(witness, "_catalog", AsyncMock(return_value=roots))
    monkeypatch.setattr(witness, "_metadata", AsyncMock(return_value=metadata))
    monkeypatch.setattr(witness, "_retained", AsyncMock(return_value=case.published["preparation"]["retention"]))
    monkeypatch.setattr(witness.receipts.generations, "read_entity_address_result_generation_authority", AsyncMock())
    monkeypatch.setattr(witness.initial_recovery, "capture_original_history", AsyncMock())
    from process import entity_address_snapshot_receipt as snapshots

    monkeypatch.setattr(
        snapshots,
        "_projected_row_identity",
        AsyncMock(return_value=(1, "b" * 64) if damage != "rows" else (2, "b" * 64)),
    )
    case.context.driver.fetchval.return_value = 0
    if damage == "initial_layout":
        case.context.fhir._provider_directory_profile_relation_storage_fingerprint.return_value = SimpleNamespace(
            exact_fingerprint="f" * 64
        )
    with pytest.raises(ValueError):
        await witness._current_inverse(case.context, "unit", case.events)


@pytest.mark.asyncio
async def test_rc_requires_native_freeze_before_metadata_protocol(case, monkeypatch):
    context = SimpleNamespace(**{name: getattr(case.context, name) for name in case.context.__dataclass_fields__})
    context.isolation = "read committed"
    context.readonly = "off"
    context.actor = {**context.actor, "owner": 20}
    oids_by_name = {name: index + 100 for index, name in enumerate(witness.common.documents.FAMILIES.values())}
    monkeypatch.setattr(witness, "_readable", AsyncMock(return_value=oids_by_name))
    freeze = AsyncMock()
    metadata = AsyncMock()
    monkeypatch.setattr(witness.receipts, "_require_freeze", freeze)
    monkeypatch.setattr(witness.receipts, "lock_metadata", metadata)
    await witness._lock_read_committed(context, "unit")
    assert freeze.await_count == 10
    metadata.assert_awaited_once_with(context.fhir.db, "unit")
    freeze.side_effect = ValueError("missing freeze")
    metadata.reset_mock()
    with pytest.raises(ValueError):
        await witness._lock_read_committed(context, "unit")
    metadata.assert_not_awaited()
    context.readonly = "on"
    with pytest.raises(ValueError, match="read_committed_unlocked"):
        await witness._lock_read_committed(context, "unit")


@pytest.mark.asyncio
async def test_missing_native_rights_fail_without_mutating_actor(case):
    case.context.driver.fetch.return_value = [dict(relname="provider_directory_profile", oid=1, readable=False)]
    with pytest.raises(ValueError, match="native_read_privilege_required"):
        await witness._readable(case.context, "unit", ("provider_directory_profile",))
    case.context.driver.execute.assert_not_awaited()
    assert case.context.actor["current"] == "source_reader"


@pytest.mark.asyncio
async def test_scope_copy_hash_and_owner_are_checked_on_observer(case, monkeypatch):
    from process import entity_address_snapshot_receipt as snapshots

    entries = [
        entry
        for entry in (
            *case.published["preparation"]["retention"]["scopes"],
            *case.published["preparation"]["retention"]["families"],
        )
        if not entry["reused_initial"]
    ]
    by_name = {entry["name"]: entry for entry in entries}
    monkeypatch.setattr(
        witness,
        "_readable",
        AsyncMock(side_effect=lambda _ctx, _schema, names: {name: by_name[name]["oid"] for name in names}),
    )
    monkeypatch.setattr(witness.receipts, "normalized_receipt_scope", lambda *_args: nullcontext())
    monkeypatch.setattr(snapshots, "_row_identity", AsyncMock(return_value=(1, "b" * 64)))
    monkeypatch.setattr(witness.capacity, "_custody", AsyncMock(return_value=dict(owner_oid=20, persistence="p")))
    monkeypatch.setattr(witness.capacity, "_catalog", AsyncMock(return_value=({}, None)))
    monkeypatch.setattr(witness.capacity, "_layout_digest", lambda _catalog: "a" * 64)
    retained_by_field = await witness._retained(case.context, "unit", case.published)
    assert retained_by_field == case.published["preparation"]["retention"]
    snapshots._row_identity.side_effect = asyncio.CancelledError
    with pytest.raises(asyncio.CancelledError):
        await witness._retained(case.context, "unit", case.published)
    snapshots._row_identity.side_effect = None
    snapshots._row_identity.return_value = (2, "b" * 64)
    with pytest.raises(ValueError, match="retained_rows_changed"):
        await witness._retained(case.context, "unit", case.published)


@pytest.mark.asyncio
async def test_full_admitted_v2_snapshot_comes_from_original_signed_lease(case, monkeypatch):
    from process import provider_directory_profile_initial_recovery as recovered

    reference_by_field = {
        **{
            "initial_targets": case.original,
            "initial_receipt": case.reference,
            "recovery_id": case.published["generation"],
            "recovered_event_sha256": "b" * 64,
        }
    }
    current_by_field = {**case.original, "profile_target_bytes": 10}
    admitted_by_field = recovered.recovered_target_state(current_by_field, reference_by_field)
    signed_by_field = {
        "serving_generation_preflight": admitted_by_field,
        "serving_generation_preflight_sha256": witness.initial_contract.target_state_sha256(admitted_by_field),
    }
    receipt = case.receipt
    receipt["payload"].update(
        capacity_geometry={},
        lease_digest="e" * 64,
        preflight_receipt_sha256="f" * 64,
        initial_target_state_sha256=witness.initial_contract.target_state_sha256(admitted_by_field),
    )
    receipt["payload"]["serving"].update(source_vector_hash="a" * 64, source_context_vector_hash="b" * 64)
    lease = SimpleNamespace(
        lease_digest="e" * 64,
        nonce="f" * 64,
        attestation_id=receipt["attestation_id"],
        signing_preflight_guard={"healthcare_receipt": signed_by_field},
    )
    fhir = case.context.fhir
    fhir.ProviderDirectoryProfileCapacityLeaseConsumption = SimpleNamespace(__tablename__="consumed")
    fhir.ProviderDirectoryProfileCapacityPreflightReceipt = SimpleNamespace(__tablename__="preflight")
    fhir.profile_capacity = SimpleNamespace(validated_capacity_geometry=Mock(return_value=object()))
    fhir._replay_bound_consumption = AsyncMock(return_value={"original": True})
    fhir._verified_provider_directory_profile_replay_lease = Mock(return_value=lease)
    fhir._assert_replay_timeline = Mock()
    fhir._pagination_checkpoint_row_mapping = dict
    fhir.db.first = AsyncMock(
        return_value=dict(
            receipt_json=signed_by_field,
            consumed_run_id=receipt["run_id"],
            consumed_attestation_id=receipt["attestation_id"],
            consumed_at="original",
        )
    )
    monkeypatch.setattr(witness, "_readable", AsyncMock())
    assert await witness._admitted_targets(case.context, "unit", receipt) == case.original
    fhir._verified_provider_directory_profile_replay_lease.assert_called_once()
    signed_by_field["serving_generation_preflight_sha256"] = "0" * 64
    with pytest.raises(ValueError, match="initial_target_hash_changed"):
        await witness._admitted_targets(case.context, "unit", receipt)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "damage",
    [
        None,
        "hash",
        "contract",
        "ids",
        "layout",
        "checkpoint",
        "missing",
        "historical",
        "new_key_fingerprint",
        "unrecognized_history",
    ],
)
async def test_initial_receipt_requires_immutable_native_binding(case, monkeypatch, damage):
    fhir = case.context.fhir
    receipt_row = deepcopy(case.receipt)
    receipt_row.update(valid_hash=True)
    receipt_row["payload"].update(contract_id=witness.initial_contract.COMMIT_CONTRACT, capacity_geometry={})
    if damage == "hash":
        receipt_row["valid_hash"] = False
    if damage == "contract":
        receipt_row["contract_id"] = "invalid"
    if damage == "ids":
        receipt_row["run_id"] = "run_other"
    geometry = object.__new__(witness.initial_contract.InitialCapacityGeometry)
    for name, field_value in dict(
        initial_receipt_oid=900, initial_receipt_storage_fingerprint="a" * 64, tablespace_oid=1
    ).items():
        object.__setattr__(geometry, name, field_value)
    layout = SimpleNamespace(relation_oid=900, exact_fingerprint="a" * 64, effective_tablespace_oids=(1,))
    if damage == "layout":
        layout.relation_oid = 901
    if damage in {"historical", "new_key_fingerprint", "unrecognized_history"}:
        layout.exact_fingerprint = "b" * 64
        receipt_row["singleton_key"] = "8" if damage == "new_key_fingerprint" else "global"
    fhir.profile_capacity = SimpleNamespace(validated_capacity_geometry=Mock(return_value=geometry))
    fhir.ProviderDirectoryProfileBuildCheckpoint = SimpleNamespace(__tablename__="checkpoint")
    fhir._pagination_checkpoint_row_mapping = dict
    fhir._provider_directory_profile_checkpoint_ref = lambda schema: witness.receipts.qualified(schema, "checkpoint")
    fhir.db.first = AsyncMock(return_value=None if damage == "missing" else receipt_row)
    fhir.db.scalar = AsyncMock(return_value=damage == "checkpoint")
    monkeypatch.setattr(witness, "_readable", AsyncMock())
    monkeypatch.setattr(witness.initial, "receipt_layout", AsyncMock(return_value=layout))
    historical = AsyncMock(return_value=damage == "historical")
    monkeypatch.setattr(witness.initial_history, "is_historical_fingerprint", historical)
    payload_check = Mock()
    monkeypatch.setattr(witness.initial, "_assert_receipt_payload", payload_check)
    if damage in {"hash", "contract", "ids", "layout", "checkpoint", "new_key_fingerprint", "unrecognized_history"}:
        with pytest.raises(ValueError):
            await witness._receipt(case.context, "unit", case.receipt["build_id"])
    else:
        result_by_field = await witness._receipt(case.context, "unit", case.receipt["build_id"])
        assert (result_by_field is None) is (damage == "missing")
        if damage in {None, "historical"}:
            payload_check.assert_called_once()
    if damage in {"historical", "unrecognized_history"}:
        historical.assert_awaited_once_with(fhir, "unit", witness.initial_contract.RECEIPT_TABLE, 900, "a" * 64)
    else:
        historical.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("change", [None, "session", "actor", "virtual"])
async def test_read_snapshot_retains_actual_session_and_actor(case, monkeypatch, change):
    from process import network_fhir_source_epoch_pin as epoch

    fhir, driver = case.context.fhir, case.context.driver
    binding = SimpleNamespace(session=case.context.session)
    fhir.db._transaction_binding = lambda: binding
    driver.fetchrow.return_value = case.context.actor
    driver.fetchval.side_effect = lambda sql: "repeatable read" if sql == "SHOW transaction_isolation" else "on"
    virtual = AsyncMock(return_value="1/2")
    monkeypatch.setattr(epoch, "_virtual_xid", virtual)
    monkeypatch.setattr(witness, "_copy_driver", AsyncMock(return_value=driver))

    async def observe():
        async with witness._read_transaction(fhir) as observed:
            assert observed.actor == case.context.actor
            assert observed.actor["owner"] != case.published["writer"]["owner"]
            if change == "session":
                binding.session = object()
            if change == "actor":
                driver.fetchrow.return_value = {**case.context.actor, "current": "changed"}
            if change == "virtual":
                virtual.return_value = "1/3"

    if change is None:
        await observe()
    else:
        with pytest.raises(ValueError, match="actor_changed"):
            await observe()
    driver.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_signed_reactivation_snapshot_never_changes_original_history(case, monkeypatch):
    case.receipt["payload"]["initial_target_state_sha256"] = "f" * 64
    case.context.driver.fetch.return_value = [
        dict(
            phase=phase,
            source_id=case.published["source"]["source_id"],
            document=event["document"],
            document_sha256=event["sha256"],
            valid_hash=True,
        )
        for phase, event in case.events.items()
    ]
    monkeypatch.setattr(witness, "_readable", AsyncMock())
    monkeypatch.setattr(
        witness, "_catalog", AsyncMock(return_value={witness.receipts.JOURNAL: case.published["journal"]})
    )
    full_admitted = AsyncMock(return_value=case.original)
    monkeypatch.setattr(witness, "_admitted_targets", full_admitted)
    events = await witness._history(case.context, "unit", case.receipt)
    assert events["published"]["document"]["before"]["initial_targets"] == case.original
    full_admitted.assert_awaited_once_with(case.context, "unit", case.receipt)


@pytest.mark.asyncio
async def test_public_witness_selects_exact_current_inverse(case, monkeypatch):
    from process import entity_address_snapshot_receipt as snapshots

    monkeypatch.setattr(snapshots, "_normalize_receipt_session", AsyncMock())
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def snapshot(_fhir):
        yield case.context

    event_by_field = dict(
        generation=case.published["generation"],
        source_id=case.published["source"]["source_id"],
        document=case.recovered,
        document_sha256="b" * 64,
        valid_hash=True,
    )
    case.context.driver.fetch.return_value = [event_by_field]
    monkeypatch.setattr(witness, "_read_transaction", snapshot)
    monkeypatch.setattr(witness, "_lock_read_committed", AsyncMock())
    monkeypatch.setattr(witness, "_readable", AsyncMock())
    monkeypatch.setattr(witness, "_metadata", AsyncMock(return_value=case.recovered["after"]["metadata"]))
    monkeypatch.setattr(witness, "_receipt", AsyncMock(return_value=case.receipt))
    monkeypatch.setattr(witness, "_history", AsyncMock(return_value=case.events))
    inverse = AsyncMock(return_value={"original": True})
    monkeypatch.setattr(witness, "_current_inverse", inverse)
    assert await witness.recovered_initial_targets(case.context.fhir, "unit") == {"original": True}
    query_args = case.context.driver.fetch.await_args.args
    assert query_args[-1] == witness.encoded(case.recovered["after"]["metadata"]["address"])
    assert "ORDER BY" not in query_args[0]
    assert witness._receipt.await_args.args[-1] == case.reference["build_id"]
    case.context.driver.fetch.return_value = [event_by_field, event_by_field]
    with pytest.raises(ValueError, match="ambiguous"):
        await witness.recovered_initial_targets(case.context.fhir, "unit")


@pytest.mark.asyncio
async def test_published_initial_link_does_not_require_current_inverse(case, monkeypatch):
    from process import entity_address_snapshot_receipt as snapshots

    monkeypatch.setattr(snapshots, "_normalize_receipt_session", AsyncMock())
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def snapshot(_fhir):
        yield case.context

    monkeypatch.setattr(witness, "_read_transaction", snapshot)
    monkeypatch.setattr(witness, "_receipt", AsyncMock(return_value=case.receipt))
    monkeypatch.setattr(witness, "_history", AsyncMock(return_value={"published": case.events["published"]}))
    mutable_state = AsyncMock(side_effect=AssertionError("historical linkage must not adopt live state"))
    monkeypatch.setattr(witness, "_current_inverse", mutable_state)
    assert await witness.assert_initial_common_receipt(
        case.context.fhir, "unit", case.reference["build_id"], case.reference["generation_id"], 40
    )
    assert not await witness.assert_initial_common_receipt(
        case.context.fhir, "unit", case.reference["build_id"], case.reference["generation_id"], 41
    )
    mutable_state.assert_not_awaited()
