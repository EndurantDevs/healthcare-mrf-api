# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepared local reads require real protected custody and destination controls."""

from copy import deepcopy
from dataclasses import asdict
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import reference_family_archive as archive
from process.ptg_parts import ptg2_physical_binding as native
from process.ptg_parts import result_archive_candidate_initialization as initialization
from process.ptg_parts import result_archive_candidate_preparation as preparation
from process.ptg_parts import result_archive_candidate_validation as validation
from tests.test_ptg2_physical_binding import _binding
from tests.test_result_archive_published_identity import _published_row


def _ownership(physical_binding):
    return archive.ReferenceFamilyStageOwnership(
        native.local_data_family_spec().importer_id,
        physical_binding.dataset_id,
        physical_binding.schema_name,
        physical_binding.schema_oid,
        tuple(sorted(physical_binding.relation_oids)),
        physical_binding.sequence_oids,
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
    initialized = initialization.InitializedLocalDataCandidate(
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
@pytest.mark.parametrize("drift", [None, "identity", "layout"])
async def test_called_stage_checks_actual_identity_before_metadata_writes(monkeypatch, drift):
    """The normal validator sees isolated evidence before genuine staging occurs."""
    physical_binding, candidate, scope_by_field, initialized = _control_fixture()
    source_records = [{"source_key": 0, "raw_container_sha256": (b"r" * 32).hex()}]
    audit_by_field = _stage_audit_identity(candidate, physical_binding, source_records)
    if drift == "identity":
        audit_by_field["identity"]["audit_sample_digest"] = "0" * 64
    if drift == "layout":
        candidate["snapshot_key"] += 1
    monkeypatch.setattr(native, "validate_local_serving_scope", lambda scope: scope)
    monkeypatch.setattr(native, "_local_candidate_control", AsyncMock(return_value=candidate))
    monkeypatch.setattr(preparation, "_local_audit_control", AsyncMock())
    monkeypatch.setattr(initialization, "_rows", AsyncMock(return_value=source_records))
    monkeypatch.setattr(validation, "acquire_ptg2_source_lifecycle_lock", AsyncMock())
    stage, complete = AsyncMock(), AsyncMock()
    monkeypatch.setattr(validation, "_stage_snapshot_in_pointer_transaction", stage)
    monkeypatch.setattr(validation, "_complete_local_run", complete)
    arguments_by_name = {
        "ownership": _ownership(physical_binding),
        "metadata": {
            "source_snapshot_id": physical_binding.payload_snapshot_id,
            "source_snapshot_key": physical_binding.payload_snapshot_key,
            "closure_metadata": {"serving_scope": scope_by_field},
        },
        "initialized": initialized,
        "audit": audit_by_field,
        "owner_oid": physical_binding.owner_oid,
    }
    if drift:
        with pytest.raises(validation.ResultArchiveCandidateValidationError, match="differs"):
            await validation.stage_local_data_candidate_for_audit(
                SimpleNamespace(in_transaction=lambda: True), **arguments_by_name
            )
        stage.assert_not_awaited()
        complete.assert_not_awaited()
    else:
        receipt = await validation.stage_local_data_candidate_for_audit(
            SimpleNamespace(in_transaction=lambda: True), **arguments_by_name
        )
        assert receipt["destination_snapshot_key"] == physical_binding.destination_layout_key
        assert receipt["next_parameters"]["candidate_audit_mode"] == "audit_only"
        assert receipt["identity"]["snapshot_key"] == physical_binding.destination_layout_key
        attributes = stage.await_args.kwargs["snapshot_attributes"]
        assert attributes["status"] == "validated" and attributes["published_at"] is None
        assert attributes["manifest"]["serving_index"]["shared_snapshot_key"] == physical_binding.payload_snapshot_key
        assert complete.await_args.kwargs["candidate_attributes"] is attributes


def _protected_preparation_fixture():
    physical_binding = _binding()
    ownership_by_field = asdict(_ownership(physical_binding))
    ownership_by_field["dataset_id"] = str(physical_binding.dataset_id)
    ownership_by_field.pop("auxiliary_oid")
    model_sha256 = native.local_data_model_digest()
    evidence_by_field = {
        "contract": "ptg_result.postgres.v2",
        "ownership": ownership_by_field,
        "initialization": {
            "destination_snapshot_id": physical_binding.snapshot_id,
            "destination_layout_key": physical_binding.destination_layout_key,
        },
        "data": {
            "payload_snapshot_id": physical_binding.payload_snapshot_id,
            "payload_snapshot_key": physical_binding.payload_snapshot_key,
            "model_sha256": model_sha256,
        },
        "native_audit": {
            "contract": "ptg-local-data.native-set-audit.v1",
            "model_sha256": model_sha256,
            "catalog_sha256": "c" * 64,
        },
        "catalog_sha256": "c" * 64,
        "control_sha256": "d" * 64,
        "activation_evidence": {"control_sha256": "d" * 64},
    }
    validation_by_field = {"inventory_sha256": "e" * 64, "evidence": evidence_by_field}
    preparation_by_field = {
        "validation": validation_by_field,
        "validation_sha256": native._native_metadata_digest(validation_by_field),
        "inventory_sha256": "e" * 64,
        "state": "validated",
        "frozen_owner_oid": physical_binding.owner_oid,
        "stage_schema": physical_binding.schema_name,
        "stage_schema_oid": physical_binding.schema_oid,
    }
    return physical_binding, preparation_by_field


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", [None, "digest", "consumed", "model", "catalog", "namespace"])
async def test_prepared_resolver_locks_complete_family_then_rechecks_catalog(monkeypatch, drift):
    """Protected validation is necessary; every heap is pinned before post-lock custody."""
    physical_binding, preparation_by_field = _protected_preparation_fixture()
    if drift == "digest":
        preparation_by_field["validation_sha256"] = "0" * 64
    elif drift == "consumed":
        preparation_by_field["state"] = "consumed"
    elif drift == "namespace":
        preparation_by_field["stage_schema_oid"] += 1
    elif drift == "model":
        preparation_by_field["validation"]["evidence"]["data"]["model_sha256"] = "0" * 64
        preparation_by_field["validation_sha256"] = native._native_metadata_digest(preparation_by_field["validation"])
    events = []

    async def execute(statement, _parameters=None):
        events.append(str(statement))
        return SimpleNamespace(mappings=lambda: SimpleNamespace(all=lambda: [preparation_by_field]))

    async def recheck(_session, _ownership):
        assert sum(query.startswith("LOCK TABLE ONLY") for query in events) == 42
        events.append("post-lock-custody")

    monkeypatch.setattr(native, "_require_local_preparation_inventory", AsyncMock())
    monkeypatch.setattr(native, "verify_local_data_family", recheck)
    monkeypatch.setattr(native, "_require_closed_local_custody", AsyncMock())
    monkeypatch.setattr(
        native, "local_data_catalog_digest", AsyncMock(return_value="0" * 64 if drift == "catalog" else "c" * 64)
    )
    session = SimpleNamespace(in_transaction=lambda: True, execute=execute)
    if drift:
        with pytest.raises(native.PTG2PhysicalBindingError):
            await native._prepared_local_header(
                session, physical_binding.snapshot_id, owner_oid=physical_binding.owner_oid
            )
    else:
        _header, _evidence, observed = await native._prepared_local_header(
            session, physical_binding.snapshot_id, owner_oid=physical_binding.owner_oid
        )
        assert observed == physical_binding
        assert events[-1] == "post-lock-custody"
        assert all("ACCESS SHARE MODE NOWAIT" in query for query in events if query.startswith("LOCK"))


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
                "schema_oid",
                "owner_oid",
            )
        },
        "dataset_id": str(physical_binding.dataset_id),
        "relation_oids": [list(pair) for pair in physical_binding.relation_oids],
        "sequence_oids": [list(entry) for entry in physical_binding.sequence_oids],
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


def _drift_published_control(candidate, publication_by_field, drift):
    """Apply one changed persisted control or preimage to the synthetic fixture."""
    changes_by_name = {
        "predecessor": {"previous_snapshot_id": "foreign"},
        "metadata-key": {"snapshot_key": candidate["snapshot_key"] + 1},
        "coverage": {"coverage_scope_id": b"0" * 32},
        "attestation": {"audit_activated_at": None},
    }
    candidate.update(changes_by_name.get(drift, {}))
    if drift == "preimage":
        candidate["manifest"]["unexpected"] = True
        publication_by_field["published_control_sha256"] = initialization._local_control_sha256(
            candidate["manifest"], candidate["options"], (("12-3456789", "group"),)
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift", [None, "predecessor", "metadata-key", "coverage", "catalog", "attestation", "preimage"]
)
async def test_retained_control_proof_rejoins_actual_transition_not_current_pointer(monkeypatch, drift):
    """Older retained publications are valid; copied or altered control identities are not."""
    physical_binding, candidate, evidence_by_field, publication_by_field = _published_control_fixture()
    _drift_published_control(candidate, publication_by_field, drift)
    custody_check = AsyncMock()
    monkeypatch.setattr(native, "require_closed_local_driver_custody", custody_check)
    monkeypatch.setattr(
        native, "local_data_driver_catalog_digest", AsyncMock(return_value="0" * 64 if drift == "catalog" else "c" * 64)
    )
    connection = SimpleNamespace(
        is_in_transaction=lambda: True,
        fetchrow=AsyncMock(return_value=candidate),
        fetch=AsyncMock(return_value=[{"plan_id": "12-3456789", "plan_market_type": "group"}]),
    )
    if drift:
        with pytest.raises(native.PTG2PhysicalBindingError):
            await native.require_local_published_control(
                connection, evidence=evidence_by_field, publication=publication_by_field
            )
    else:
        assert (
            await native.require_local_published_control(
                connection, evidence=evidence_by_field, publication=publication_by_field
            )
            == physical_binding
        )
        query = connection.fetchrow.await_args.args[0]
        assert "FOR SHARE OF snapshot,run,binding,scope,layout NOWAIT" in query
        assert physical_binding.schema_name in query and '"ptg2_v4_snapshot_map_root"' in query
        assert custody_check.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("refusal", [None, "custody", "attestation"])
async def test_native_local_publisher_uses_existing_held_activation_only_after_custody(monkeypatch, refusal):
    """The native cut uses real staged controls; custody refusal precedes pointer mutation."""
    from uuid import UUID

    from process.ptg_parts import source_pointers as pointers
    from process.ptg_parts.source_pointers import candidate_snapshot_attributes

    physical_binding, published, evidence_by_field, _publication = _published_control_fixture()
    candidate_by_field = candidate_snapshot_attributes(published, source_key="source_a", previous_snapshot_id=None)
    candidate_by_field.update(
        snapshot_key=physical_binding.destination_layout_key,
        plan_id="12-3456789",
        plan_market_type="group",
        coverage_scope_id=b"c" * 32,
        storage_generation="shared_blocks_v4",
    )
    session = SimpleNamespace(in_transaction=lambda: True)
    operation_by_field = {"operation_id": str(UUID(int=1))}
    monkeypatch.setattr(native, "require_local_binding_publisher", AsyncMock())
    monkeypatch.setattr(
        native, "_prepared_local_header", AsyncMock(return_value=({}, evidence_by_field, physical_binding))
    )
    custody = AsyncMock(
        side_effect=native.PTG2PhysicalBindingError("custody refused") if refusal == "custody" else None
    )
    monkeypatch.setattr(native, "require_frozen_local_preparation", custody)
    monkeypatch.setattr(native, "_require_local_control", AsyncMock(return_value=candidate_by_field))
    monkeypatch.setattr(pointers, "_acquire_source_pointer_gc_lock", AsyncMock())
    monkeypatch.setattr(pointers, "_database_utc_timestamp", AsyncMock(return_value=published["published_at"]))
    monkeypatch.setattr(
        pointers, "_candidate_plan_pointer_entries", AsyncMock(return_value=[{"plan_id": "12-3456789"}])
    )
    predecessor_pin, complete = AsyncMock(), AsyncMock()
    monkeypatch.setattr(pointers, "pin_reviewed_activation_predecessor", predecessor_pin)
    monkeypatch.setattr(pointers, "_complete_candidate_activation", complete)
    receipt = AsyncMock(return_value={"synthetic": "published-proof"})
    monkeypatch.setattr(native, "_local_publication_receipt", receipt)
    arguments_by_name = {
        "operation": operation_by_field,
        "expected_attestation_digest": b"a" * (31 if refusal == "attestation" else 32),
        "rollback_owner_id": "synthetic-owner",
    }
    if refusal:
        with pytest.raises(native.PTG2PhysicalBindingError):
            await native.publish_local_data_candidate_in_transaction(session, **arguments_by_name)
        predecessor_pin.assert_not_awaited()
        complete.assert_not_awaited()
        receipt.assert_not_awaited()
    else:
        assert await native.publish_local_data_candidate_in_transaction(session, **arguments_by_name) == {
            "synthetic": "published-proof"
        }
        assert complete.await_args.kwargs["expected_audit_only_attestation_digest"] == b"a" * 32
        assert complete.await_args.kwargs["activation_context"].candidate == candidate_by_field
        assert predecessor_pin.await_args.kwargs["rollback_owner_id"] == "synthetic-owner"


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
