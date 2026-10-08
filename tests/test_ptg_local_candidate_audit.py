# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Snapshot-local audit callers keep physical custody distinct from logical identity."""

import asyncio
import importlib
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.ptg_parts import ptg2_candidate_attestation as attestation
from process.ptg_parts import ptg2_physical_binding as native
from tests.test_ptg2_physical_binding import _binding
from tests.test_ptg_candidate_audit_importer import RAW_DIGEST, _candidate_row, _passing_report, _target

audit = importlib.import_module("process.ptg_candidate_audit")


def local_audit_case(monkeypatch):
    """Double the native authority boundary, not the normal target/identity validators."""
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "mrf")
    monkeypatch.delenv("DB_SCHEMA", raising=False)
    binding = replace(_binding(), payload_snapshot_key=17)
    candidate_by_field = _candidate_row(storage_generation="shared_blocks_v4")
    candidate_by_field.update(
        snapshot_id=binding.snapshot_id, snapshot_key=binding.destination_layout_key, coverage_scope_id=b"c" * 32
    )
    candidate_by_field["manifest"]["serving_index"].update(coverage_scope_id="63" * 32, source_count=1)
    state_by_field = {
        "candidate": candidate_by_field,
        "source_records": [{"source_key": 0, "raw_container_sha256": RAW_DIGEST}],
        "physical_binding": binding,
    }
    case = SimpleNamespace(state=state_by_field, binding=binding, events=[], is_reading=False)
    case.session = SimpleNamespace(
        execute=AsyncMock(side_effect=AssertionError("canonical query is forbidden")),
        commit=AsyncMock(),
        rollback=AsyncMock(),
    )

    @asynccontextmanager
    async def transaction():
        assert not case.is_reading
        case.is_reading = True
        case.events.append("begin")
        try:
            yield case.session
        except BaseException:
            case.events.append("rollback")
            raise
        else:
            case.events.append("end")
        finally:
            case.is_reading = False

    async def resolve(session, **_coordinates):
        assert session is case.session and case.is_reading
        case.events.append("authenticated-local")
        return case.state

    case.transaction = transaction
    case.resolve = AsyncMock(side_effect=resolve)
    case.legacy_rows = AsyncMock(side_effect=AssertionError("canonical target fallback is forbidden"))
    case.legacy_sources = AsyncMock(side_effect=AssertionError("canonical sources fallback is forbidden"))
    monkeypatch.setattr(audit.db, "transaction", transaction)
    monkeypatch.setattr(native, "local_candidate_audit_state", case.resolve)
    monkeypatch.setattr(audit, "_candidate_rows", case.legacy_rows)
    monkeypatch.setattr(audit, "_candidate_raw_sources", case.legacy_sources)
    return case


async def local_target(case):
    return await audit.load_candidate_audit_target(
        candidate_run_id=case.state["candidate"]["import_run_id"],
        snapshot_id=case.binding.snapshot_id,
        import_id="derived-import",
    )


@pytest.mark.asyncio
async def test_local_target_precedes_canonical_joins_and_preserves_two_layout_keys(monkeypatch):
    case = local_audit_case(monkeypatch)
    target = await local_target(case)
    assert target.physical_binding == case.binding
    assert target.snapshot_id == case.binding.snapshot_id and target.snapshot_key == 701
    assert target.physical_binding.payload_snapshot_key == 17
    assert case.state["candidate"]["manifest"]["serving_index"]["shared_snapshot_key"] == 17
    assert case.events == ["begin", "authenticated-local", "end"]
    case.resolve.assert_awaited_once_with(
        case.session,
        candidate_run_id="ptg2:derived-import",
        snapshot_id=case.binding.snapshot_id,
        schema_name="mrf",
    )
    case.legacy_rows.assert_not_awaited()
    case.legacy_sources.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["custody", "building", "run", "source", "predecessor", "root", "snapshot", "import"])
async def test_local_target_never_falls_back_when_authority_or_state_changes(monkeypatch, drift):
    case = local_audit_case(monkeypatch)
    candidate_by_field = case.state["candidate"]
    if drift == "custody":
        case.resolve.side_effect = native.PTG2PhysicalBindingError("custody changed")
    updates_by_drift = {
        "building": ("status", "building"),
        "run": ("import_run_id", "another-run"),
        "predecessor": ("current_snapshot_id", "another-current"),
        "root": ("v4_root_state", "building"),
    }
    if drift in updates_by_drift:
        field, value = updates_by_drift[drift]
        candidate_by_field[field] = value
    if drift == "source":
        case.state["source_records"][0]["raw_container_sha256"] = "ff" * 32
    with pytest.raises((ValueError, native.PTG2PhysicalBindingError)):
        await audit.load_candidate_audit_target(
            candidate_run_id="ptg2:derived-import",
            snapshot_id="another-snapshot" if drift == "snapshot" else case.binding.snapshot_id,
            import_id="another-import" if drift == "import" else "derived-import",
        )
    assert case.events[-1] == "rollback"
    case.legacy_rows.assert_not_awaited()
    case.legacy_sources.assert_not_awaited()


@pytest.mark.asyncio
async def test_explicit_legacy_resolution_keeps_the_original_loader(monkeypatch):
    case = local_audit_case(monkeypatch)
    case.state = None
    case.legacy_rows.side_effect = None
    case.legacy_rows.return_value = [_candidate_row()]
    case.legacy_sources.side_effect = None
    case.legacy_sources.return_value = (RAW_DIGEST,)
    target = await audit.load_candidate_audit_target(candidate_run_id="ptg2:derived-import")
    assert target.physical_binding is None and target.snapshot_key == 17
    case.legacy_rows.assert_awaited_once_with("ptg2:derived-import")
    case.legacy_sources.assert_awaited_once_with("candidate-snapshot")


@pytest.mark.asyncio
async def test_local_attestation_uses_authenticated_sources_and_real_destination_key(monkeypatch):
    case = local_audit_case(monkeypatch)
    async with case.transaction():
        identity_by_field = await attestation._locked_candidate_identity(
            case.session,
            schema_name="mrf",
            snapshot_id=case.binding.snapshot_id,
        )
    assert identity_by_field["snapshot_key"] == case.binding.destination_layout_key
    assert identity_by_field["coverage_scope_id"] == b"c" * 32
    assert identity_by_field["source_set_digest"].hex() == audit.source_set_digest((RAW_DIGEST,))
    case.resolve.assert_awaited_once_with(case.session, snapshot_id=case.binding.snapshot_id, schema_name="mrf")
    case.session.execute.assert_not_awaited()
    case.session.commit.assert_not_awaited()
    case.session.rollback.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["custody", "building", "source", "layout", "copied-report"])
async def test_local_attestation_does_not_replace_native_identity_with_copied_audit_status(monkeypatch, drift):
    case = local_audit_case(monkeypatch)
    if drift == "custody":
        case.resolve.side_effect = native.PTG2PhysicalBindingError("custody changed")
    elif drift == "building":
        case.state["candidate"]["status"] = "building"
    elif drift == "source":
        case.state["source_records"][0]["raw_container_sha256"] = "ff" * 32
    elif drift == "layout":
        case.state["candidate"]["snapshot_key"] = case.binding.payload_snapshot_key
    else:
        case.state["candidate"].update(status="published", audit_report={"status": "pass"})
    async with case.transaction():
        with pytest.raises((ValueError, native.PTG2PhysicalBindingError)):
            await attestation._locked_candidate_identity(
                case.session, schema_name="mrf", snapshot_id=case.binding.snapshot_id
            )
    case.session.execute.assert_not_awaited()


def evidence_loaders(monkeypatch, case, *, failure=None):
    """Observe both native evidence queries before the exact read transaction ends."""

    async def witness(**coordinates):
        assert case.is_reading
        assert coordinates["schema_name"] == case.binding.schema_name
        assert coordinates["snapshot_key"] == case.binding.payload_snapshot_key
        case.events.append("witness")
        if failure == "witness":
            raise RuntimeError("witness changed")
        if failure == "cancel":
            raise asyncio.CancelledError
        return SimpleNamespace(metadata={"occurrence_witness_count": 1})

    async def sample(**coordinates):
        assert case.is_reading
        assert coordinates["schema_name"] == case.binding.schema_name
        assert coordinates["snapshot_key"] == case.binding.payload_snapshot_key
        case.events.append("sample")
        if failure == "sample":
            raise RuntimeError("sample changed")
        return object()

    case.witness = AsyncMock(side_effect=witness)
    case.sample = AsyncMock(side_effect=sample)
    monkeypatch.setattr(audit, "load_shared_source_witness", case.witness)
    monkeypatch.setattr(audit, "load_persisted_audit_sample", case.sample)


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["partitioned", "rolling"])
async def test_both_importer_reads_pin_fresh_custody_through_all_payload_queries(monkeypatch, path):
    case = local_audit_case(monkeypatch)
    target = await local_target(case)
    case.events.clear()
    evidence_loaders(monkeypatch, case)
    monkeypatch.setattr(audit, "_progress", AsyncMock())
    report = AsyncMock(return_value={"report": "fresh"})
    monkeypatch.setattr(audit, "run_release_audit", report)
    if path == "partitioned":
        await audit._load_partitioned_audit_evidence(target)
    else:
        assert await audit._execute_rolling_release_audit(target, control_run_id=None, http_config=None) == {
            "report": "fresh"
        }
        report.assert_awaited_once()
    assert case.events == [
        "begin",
        "authenticated-local",
        "witness",
        *(["sample"] if path == "partitioned" else []),
        "end",
    ]
    assert not case.is_reading and case.resolve.await_count == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["witness", "sample", "cancel"])
async def test_local_evidence_failure_unwinds_the_owned_read_transaction(monkeypatch, failure):
    case = local_audit_case(monkeypatch)
    target = await local_target(case)
    case.events.clear()
    evidence_loaders(monkeypatch, case, failure=failure)
    with pytest.raises(asyncio.CancelledError if failure == "cancel" else RuntimeError):
        await audit._load_partitioned_audit_evidence(target)
    assert case.events[-1] == "rollback" and not case.is_reading


@pytest.mark.asyncio
@pytest.mark.parametrize("drift", ["legacy", "heap", "key", "identity"])
async def test_cached_local_target_cannot_bypass_a_new_custody_check(monkeypatch, drift):
    case = local_audit_case(monkeypatch)
    target = await local_target(case)
    case.state = deepcopy(case.state)
    if drift == "legacy":
        case.state = None
    elif drift == "heap":
        relations = case.binding.relation_oids
        case.state["physical_binding"] = replace(case.binding, relation_oids=((relations[0][0], 9999), *relations[1:]))
    elif drift == "key":
        case.state["physical_binding"] = replace(case.binding, payload_snapshot_key=18)
    else:
        case.state["candidate"]["manifest"]["serving_index"]["source_witness"]["payload_sha256"] = "ee" * 32
    evidence_loaders(monkeypatch, case)
    with pytest.raises(ValueError, match="changed"):
        await audit._load_partitioned_audit_evidence(target)
    case.witness.assert_not_awaited()
    case.sample.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_audit_cannot_use_the_standalone_activation_path(monkeypatch):
    case = local_audit_case(monkeypatch)
    target = await local_target(case)
    execute = AsyncMock()
    monkeypatch.setattr(audit, "_execute_release_audit", execute)
    with pytest.raises(ValueError, match="audit-only publication handoff"):
        await audit._audit_and_activate(target, control_run_id=None)
    execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_audit_only_keeps_the_existing_attestation_handoff(monkeypatch):
    case = local_audit_case(monkeypatch)
    target = await local_target(case)
    report_by_field = _passing_report()
    monkeypatch.setattr(audit, "_execute_release_audit", AsyncMock(return_value=report_by_field))
    record = AsyncMock(return_value=({"status": "attested"}, "dd" * 32))
    promote = AsyncMock(side_effect=AssertionError("standalone promotion is forbidden"))
    monkeypatch.setattr(audit, "_record_passing_attestation", record)
    monkeypatch.setattr(audit, "_promote_audited_candidate", promote)
    monkeypatch.setattr(audit, "_publish_audit_only_complete", AsyncMock())
    result_by_field = await audit._audit_and_activate(
        target, control_run_id=None, candidate_audit_mode=audit.CANDIDATE_AUDIT_MODE_AUDIT_ONLY
    )
    assert result_by_field["snapshot_id"] == target.snapshot_id
    assert result_by_field["activation_status"] == "deferred"
    record.assert_awaited_once_with(
        target, report=report_by_field, control_run_id=None, activation_intent=audit.CANDIDATE_AUDIT_MODE_AUDIT_ONLY
    )
    promote.assert_not_awaited()


@pytest.mark.asyncio
async def test_legacy_evidence_loader_keeps_its_existing_canonical_coordinates(monkeypatch):
    case = local_audit_case(monkeypatch)
    witness = AsyncMock(return_value=object())
    sample = AsyncMock(return_value=object())
    monkeypatch.setattr(audit, "load_shared_source_witness", witness)
    monkeypatch.setattr(audit, "load_persisted_audit_sample", sample)
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "canonical")
    await audit._load_partitioned_audit_evidence(_target())
    assert not case.events
    assert witness.await_args.kwargs["schema_name"] == sample.await_args.kwargs["schema_name"] == "canonical"
    assert witness.await_args.kwargs["snapshot_key"] == sample.await_args.kwargs["snapshot_key"] == 17
