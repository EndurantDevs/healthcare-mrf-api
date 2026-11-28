# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared local reads require real protected custody and destination controls."""

from copy import deepcopy
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive
from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_candidate_validation as validation
from tests.test_ptg2_physical_binding import _binding
from tests.test_result_archive_published_identity import _published_row


@dataclass(frozen=True, kw_only=True)
class _InitializedLocalData(initialization.InitializedResultArchiveCandidate):
    """Historical local initialization evidence retained in published local controls."""

    destination_layout_key: int


def _ownership(physical_binding):
    """Build the recorded historical carrier for unchanged archive-writer cases."""
    return archive.ReferenceFamilyStageOwnership(
        native.local_data_family_spec().importer_id,
        physical_binding.dataset_id,
        physical_binding.schema_name,
        101,
        tuple((name, 200 + number) for number, (_schema, name) in enumerate(physical_binding.relations)),
        tuple(
            (name, 300 + number, table, column)
            for number, (_schema, name, table, column) in enumerate(physical_binding.sequences)
        ),
    )


def _control_fixture():
    """Keep the destination metadata key distinct from the sealed payload key."""
    physical_binding = _binding()
    candidate = deepcopy(_published_row())
    candidate.update(
        snapshot_id=physical_binding.snapshot_id,
        snapshot_key=physical_binding.destination_layout_key,
        status="building",
        run_status="running",
        options={"source_key": "source_a"},
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        staged_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
        current_snapshot_id=None,
        previous_snapshot_id=None,
    )
    candidate["manifest"]["activation"]["state"] = "building"
    candidate["manifest"].pop("serving_index")
    # This fixture's independently preserved payload key is 19, not the source
    # example's key 17 and not the locally allocated metadata key 701.
    candidate["layout_manifest"]["serving_index"]["shared_snapshot_key"] = physical_binding.payload_snapshot_key
    scope_by_field = {
        "snapshot_id": physical_binding.payload_snapshot_id,
        "source_key": "source_a",
        "coverage_scope_id": candidate["coverage_scope_id"].hex(),
        "primary_plan": ["12-3456789", "group"],
        "plan_scopes": [["12-3456789", "group"]],
        "source_assignments": [{"raw_container_sha256": (b"r" * 32).hex()}],
    }
    initialized = _InitializedLocalData(
        initialization.RESULT_ARCHIVE_CANDIDATE_INITIALIZATION_CONTRACT,
        physical_binding.snapshot_id,
        "source-run",
        physical_binding.payload_snapshot_id,
        1,
        1,
        None,
        False,
        destination_layout_key=physical_binding.destination_layout_key,
    )
    candidate["manifest"].update(
        physical_binding_contract=native.PHYSICAL_BINDING_CONTRACT,
        local_data_preparation={
            "dataset_id": str(physical_binding.dataset_id),
            "payload_snapshot_id": physical_binding.payload_snapshot_id,
            "payload_snapshot_key": physical_binding.payload_snapshot_key,
            "destination_layout_key": physical_binding.destination_layout_key,
        },
    )
    return physical_binding, candidate, scope_by_field, initialized


def _stage_audit_identity(candidate, physical_binding, source_records):
    """Build the real sealed identity used by the existing called-stage assertions."""
    from process.ptg_parts.ptg2_candidate_attestation import _candidate_evidence_identity

    serving_index = validation._attach_destination_source_identity(
        candidate["layout_manifest"]["serving_index"], source_key="source_a", source_records=source_records
    )
    identity = _candidate_evidence_identity(
        {
            **candidate,
            "snapshot_key": physical_binding.payload_snapshot_key,
            "raw_container_sha256_values": [(b"r" * 32).hex()],
        },
        activation_by_field=candidate["manifest"]["activation"],
        serving_index_by_field=serving_index,
        layout_serving_index_by_field=candidate["layout_manifest"]["serving_index"],
        storage_generation="shared_blocks_v4",
    )
    audit_by_field = {
        "control_sha256": "a" * 64,
        "identity": {
            key: identity_value.hex() if isinstance(identity_value, bytes) else identity_value
            for key, identity_value in identity.items()
        },
    }
    return audit_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("declaration", [None, native.PHYSICAL_BINDING_CONTRACT])
async def test_missing_preparation_never_downgrades_a_local_declaration(declaration):
    snapshot_by_field = {
        "snapshot_id": "synthetic",
        "import_run_id": "run",
        "manifest": {} if declaration is None else {"physical_binding_contract": declaration},
    }
    session = SimpleNamespace(
        in_transaction=lambda: True,
        execute=AsyncMock(
            return_value=SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [snapshot_by_field]))
        ),
        scalar=AsyncMock(return_value=False),
    )
    if declaration is None:
        assert await native.local_candidate_audit_state(session, snapshot_id="synthetic") is None
    else:
        with pytest.raises(native.PTG2PhysicalBindingError, match="authority is unavailable"):
            await native.local_candidate_audit_state(session, snapshot_id="synthetic")


@pytest.mark.asyncio
async def test_explicit_control_schema_mismatch_refuses_before_any_query():
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock())
    with pytest.raises(native.PTG2PhysicalBindingError, match="control schema differs"):
        await native.local_candidate_audit_state(session, snapshot_id="synthetic", schema_name="other_control")
    session.execute.assert_not_awaited()


def _published_control_fixture(timestamp=None):
    """The protected staged preimage survives only the normal activation transition."""
    from process.ptg_parts.ptg2_candidate_attestation import PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT
    from process.ptg_parts.source_pointers import activated_snapshot_attributes, candidate_snapshot_attributes

    physical_binding, candidate, _scope, initialized = _control_fixture()
    candidate["manifest"]["serving_index"] = validation._attach_destination_source_identity(
        candidate["layout_manifest"]["serving_index"],
        source_key="source_a",
        source_records=[{"raw_container_sha256": (b"r" * 32).hex()}],
    )
    staged = candidate_snapshot_attributes(candidate, source_key="source_a", previous_snapshot_id=None)
    timestamp = timestamp or datetime(2026, 1, 3, tzinfo=timezone.utc)
    published = activated_snapshot_attributes(
        staged, activated_at=timestamp, activation_mode="reviewed_audit_only_control"
    )
    published.update(
        run_status="validated",
        run_report=deepcopy(staged["manifest"]),
        audit_contract=PTG2_CANDIDATE_ATTESTATION_CURRENT_CONTRACT,
        audit_activation_intent="audit_only",
        audit_activated_at=timestamp,
        audit_report_digest=b"a" * 32,
        current_snapshot_id="a-later-current-snapshot",
    )
    evidence_by_field = _published_control_evidence(physical_binding, candidate, initialized, staged)
    publication_by_field = _published_control_receipt(physical_binding, candidate, published, timestamp)
    return physical_binding, published, evidence_by_field, publication_by_field


def _published_control_evidence(physical_binding, candidate, initialized, staged):
    """Retain the exact staged preimage, typed coordinates and ownership used in the native proof."""
    ownership_by_field = asdict(_ownership(physical_binding))
    ownership_by_field.pop("auxiliary_oid")
    ownership_by_field["dataset_id"] = str(physical_binding.dataset_id)
    evidence_by_field = {
        "ownership": ownership_by_field,
        "initialization": asdict(initialized),
        "catalog_sha256": "c" * 64,
        "data": {
            "payload_snapshot_id": physical_binding.payload_snapshot_id,
            "payload_snapshot_key": physical_binding.payload_snapshot_key,
        },
        "native_audit": {
            "identity": {
                "plan_id": "12-3456789",
                "plan_market_type": "group",
                "coverage_scope_id": candidate["coverage_scope_id"].hex(),
            }
        },
        "activation_evidence": {"source_key": "source_a", "expected_current_snapshot_id": None},
        "control_sha256": initialization._local_control_sha256(
            staged["manifest"], candidate["options"], (("12-3456789", "group"),)
        ),
    }
    return evidence_by_field


def _published_control_receipt(physical_binding, candidate, published, timestamp):
    """Preserve declared physical ordering and actual published-control receipt encoding."""
    publication_by_field = {
        "contract": native.PHYSICAL_BINDING_CONTRACT,
        "destination_snapshot_id": physical_binding.snapshot_id,
        **{
            key: getattr(physical_binding, key)
            for key in (
                "destination_layout_key",
                "payload_snapshot_id",
                "payload_snapshot_key",
                "schema_name",
            )
        },
        "dataset_id": str(physical_binding.dataset_id),
        "relations": [{"schema": schema, "name": name} for schema, name in physical_binding.relations],
        "sequences": [
            {"schema": schema, "name": name, "owner_table": table, "owner_column": column}
            for schema, name, table, column in physical_binding.sequences
        ],
        "destination_activation": {
            "source_key": "source_a",
            "snapshot_id": physical_binding.snapshot_id,
            "previous_snapshot_id": None,
            "activated_at": timestamp.isoformat(),
            "audit_report_digest": (b"a" * 32).hex(),
        },
        "published_control_sha256": initialization._local_control_sha256(
            published["manifest"], candidate["options"], (("12-3456789", "group"),)
        ),
    }
    return publication_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("audit_time", ["utc", "offset", "different", "missing", "naive", "unpublished"])
async def test_published_timestamp_matches_native_column_contract(audit_time):
    """The UTC-naive snapshot and aware consumed audit must identify the same instant."""
    from db.models import PTG2Snapshot, PTG2V3CandidateAuditAttestation
    from process.ptg_parts import ptg2_candidate_attestation as attestation
    from process.ptg_parts import source_pointers as pointers

    assert not PTG2Snapshot.__table__.c.published_at.type.timezone
    assert PTG2V3CandidateAuditAttestation.__table__.c.activated_at.type.timezone
    clock = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(scalar_one=lambda: datetime(2026, 1, 3))))
    timestamp = await pointers._database_utc_timestamp(clock)
    assert timestamp.tzinfo is None
    assert "timezone('UTC', clock_timestamp())" in str(clock.execute.await_args.args[0])
    consumed = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(first=lambda: ("synthetic",))))
    await attestation.consume_candidate_audit_attestation_in_transaction(
        consumed,
        schema_name="mrf",
        snapshot_id="synthetic",
        report_digest=b"a" * 32,
        activated_at=timestamp,
        activation_intent="audit_only",
        expected_attestation_digest=attestation.candidate_attestation_digest(b"a" * 32, "audit_only"),
    )
    audit_timestamp = consumed.execute.await_args.args[1]["activated_at"]
    assert audit_timestamp == timestamp.replace(tzinfo=timezone.utc)
    physical_binding, candidate, evidence_by_field, publication_by_field = _published_control_fixture(timestamp)
    candidate["audit_activated_at"] = {
        "utc": audit_timestamp,
        "offset": audit_timestamp.astimezone(timezone(timedelta(hours=2))),
        "different": audit_timestamp + timedelta(microseconds=1),
        "missing": None,
        "naive": timestamp,
        "unpublished": audit_timestamp,
    }[audit_time]
    plans = [{"plan_id": "12-3456789", "plan_market_type": "group"}]
    assert publication_by_field["destination_activation"]["activated_at"] == timestamp.isoformat()
    assert candidate["manifest"]["activation"]["activated_at"] == timestamp.isoformat()
    if audit_time == "unpublished":
        candidate["published_at"] = None
    if audit_time in {"utc", "offset"}:
        assert (
            native._require_local_published_postimage(
                candidate, plans, evidence_by_field, publication_by_field, physical_binding
            )
            is None
        )
    else:
        with pytest.raises(native.PTG2PhysicalBindingError, match="activation differs"):
            native._require_local_published_postimage(
                candidate, plans, evidence_by_field, publication_by_field, physical_binding
            )


@pytest.mark.asyncio
async def test_ordinary_activation_rejects_local_declaration_before_pointer_locks(monkeypatch):
    """Legacy activation cannot promote a snapshot-local marker as canonical payload authority."""
    from process.ptg_parts import source_pointers as pointers

    session = SimpleNamespace(scalar=AsyncMock(return_value=True))
    lock = AsyncMock()
    monkeypatch.setattr(pointers, "_acquire_source_pointer_gc_lock", lock)
    with pytest.raises(ValueError, match="protected installed publication"):
        await pointers.activate_ptg2_candidate_in_transaction(
            session, schema_name="mrf", source_key="source_a", snapshot_id="synthetic"
        )
    lock.assert_not_awaited()
    assert "manifest::jsonb" in str(session.scalar.await_args.args[0])


@pytest.mark.asyncio
@pytest.mark.parametrize("lock_controls", [True, False])
async def test_local_candidate_control_reads_one_destination_row(lock_controls):
    physical_binding = _binding()
    row_by_field = {"snapshot_id": physical_binding.snapshot_id}
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: row_by_field))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    observed = await native._local_candidate_control(session, "mrf", physical_binding, lock_controls=lock_controls)
    assert observed is row_by_field
    statement, parameters_by_name = session.execute.await_args.args
    assert ("FOR SHARE" in str(statement)) is lock_controls
    assert parameters_by_name == {
        "snapshot_id": physical_binding.snapshot_id,
        "payload_key": physical_binding.payload_snapshot_key,
    }


@pytest.mark.parametrize("drift", [None, "status"])
def test_prepared_control_postimage_matches_only_the_staged_marker(drift):
    physical_binding, candidate, _scope, initialized = _control_fixture()
    plans = (("12-3456789", "group"),)
    candidate.update(
        status="validated",
        run_status="validated",
        import_run_id=initialized.destination_import_run_id,
        run_report=deepcopy(candidate["manifest"]),
    )
    evidence_by_field = {
        "ownership": {"dataset_id": str(physical_binding.dataset_id)},
        "data": {
            "payload_snapshot_id": physical_binding.payload_snapshot_id,
            "payload_snapshot_key": physical_binding.payload_snapshot_key,
        },
        "initialization": asdict(initialized),
        "activation_evidence": {"source_key": "source_a", "expected_current_snapshot_id": None},
        "control_sha256": initialization._local_control_sha256(candidate["manifest"], candidate["options"], plans),
    }
    if drift:
        candidate["status"] = "building"
        with pytest.raises(native.PTG2PhysicalBindingError, match="candidate control changed"):
            native._require_local_control_postimage(candidate, plans, evidence_by_field, physical_binding)
    else:
        observed = native._require_local_control_postimage(candidate, plans, evidence_by_field, physical_binding)
        assert observed == candidate


def test_published_postimage_refuses_a_changed_staged_preimage():
    timestamp = datetime(2026, 1, 3)
    physical_binding, candidate, evidence_by_field, publication_by_field = _published_control_fixture(timestamp)
    candidate["audit_activated_at"] = timestamp.replace(tzinfo=timezone.utc)
    evidence_by_field["control_sha256"] = "0" * 64
    plans = [{"plan_id": "12-3456789", "plan_market_type": "group"}]
    with pytest.raises(native.PTG2PhysicalBindingError, match="staged preimage changed"):
        native._require_local_published_postimage(
            candidate, plans, evidence_by_field, publication_by_field, physical_binding
        )


def test_named_publication_fields_refuse_an_unknown_receipt_shape():
    with pytest.raises(native.PTG2PhysicalBindingError, match="publication fields differ"):
        native._named_publication_fields({"contract": native.PHYSICAL_BINDING_CONTRACT}, _binding())


@pytest.mark.asyncio
async def test_local_candidate_evidence_recomputes_identity_from_source_rows(monkeypatch):
    from process.ptg_parts import ptg2_candidate_attestation as attestation

    physical_binding, candidate, _scope, _initialized = _control_fixture()
    source_records = [{"source_key": 0, "raw_container_sha256": (b"r" * 32).hex()}]
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: source_records))
    session = SimpleNamespace(execute=AsyncMock(return_value=result))
    monkeypatch.setattr(attestation, "_candidate_identity", lambda _candidate, **_kwargs: {"digest": b"\x01"})
    evidence_by_field = {"native_audit": {"identity": {"digest": "01"}}}
    state = await native._local_candidate_evidence_state(session, candidate, evidence_by_field, physical_binding)
    assert state["source_records"] == source_records
    assert state["candidate"]["raw_container_sha256_values"] == [(b"r" * 32).hex()]
    evidence_by_field["native_audit"]["identity"] = {"digest": "02"}
    with pytest.raises(native.PTG2PhysicalBindingError, match="audit identity changed"):
        await native._local_candidate_evidence_state(session, candidate, evidence_by_field, physical_binding)
