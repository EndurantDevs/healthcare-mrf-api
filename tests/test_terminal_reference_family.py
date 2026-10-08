# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Manual terminal captures reuse the native model-family pipeline."""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock
from uuid import UUID

import pytest

from process import reference_family_archive as archive


@pytest.fixture(autouse=True)
def _native_catalog_boundary(monkeypatch):
    """Database actor/catalog qualification is a separate native boundary, never inferred by these host fixtures."""
    monkeypatch.setattr(archive, "protected_publisher_owner", AsyncMock(return_value=70))
    monkeypatch.setattr(archive, "require_native_read_catalog", AsyncMock())
    monkeypatch.setattr(archive, "_relation_oid", AsyncMock(return_value=101))


def _terminal():
    return {
        "run_id": "synthetic-run",
        "status": "succeeded",
        "phase_detail": "claims-pricing finalized",
        "created_at": "2026-01-01 00:00:00",
        "started_at": "2026-01-01 00:01:00",
        "finished_at": "2026-01-01 00:02:00",
        "metrics": {"schema": "mrf", "stage_suffix": "synthetic-stage"},
    }


def _session(terminal):
    result = SimpleNamespace(mappings=lambda: SimpleNamespace(one_or_none=lambda: terminal))
    return SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(return_value=result))


@pytest.mark.asyncio
@pytest.mark.parametrize("importer_id", ("claims-pricing", "drug-claims"))
async def test_terminal_capture_locks_complete_model_family_before_history(importer_id):
    """Current data and terminal provenance are observed together, not relabeled as a generation."""
    terminal = _terminal()
    terminal["phase_detail"] = archive.TERMINAL_CAPABILITIES[importer_id].phase
    session = _session(terminal)
    metadata = await archive.capture_terminal_reference_source(
        session, importer_id=importer_id, schema_name="mrf", run_id="synthetic-run"
    )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert statements[0].startswith("LOCK TABLE ") and statements[0].endswith("IN SHARE MODE NOWAIT")
    assert all(
        '"' + archive._source_model_name(model) + '"' in statements[0]
        for model in archive.reference_family_spec(importer_id).model_types
    )
    assert "ORDER BY created_at DESC NULLS FIRST" in statements[1]
    assert session.execute.await_args_list[1].args[1] == {
        "importers": list(archive.TERMINAL_CAPABILITIES[importer_id].importers)
    }
    assert metadata["contract"] == archive.TERMINAL_CAPTURE_CONTRACT
    assert metadata["observed_run_id"] == "synthetic-run" and "serving_generation" not in metadata


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ["missing", "run_id", "status", "phase", "time", "schema", "stage"])
@pytest.mark.parametrize("importer_id", ("claims-pricing", "drug-claims"))
async def test_terminal_capture_refuses_incomplete_or_superseded_history(change, importer_id):
    terminal = _terminal()
    terminal["phase_detail"] = archive.TERMINAL_CAPABILITIES[importer_id].phase
    changes_by_field = {
        "run_id": ("run_id", "superseding-run"),
        "status": ("status", "running"),
        "phase": ("phase_detail", "different phase"),
        "time": ("finished_at", None),
    }
    if change in changes_by_field:
        name, replacement = changes_by_field[change]
        terminal[name] = replacement
    if change == "schema":
        terminal["metrics"]["schema"] = "different"
    if change == "stage":
        terminal["metrics"]["stage_suffix"] = None
    session = _session(None if change == "missing" else terminal)
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="unavailable or superseded"):
        await archive.capture_terminal_reference_source(
            session, importer_id=importer_id, schema_name="mrf", run_id="synthetic-run"
        )


@pytest.mark.asyncio
async def test_direct_generic_capture_reauthenticates_terminal_metadata():
    """A direct shared-helper caller cannot substitute peer metadata for terminal evidence."""
    session = _session(_terminal())
    spec = archive.reference_family_spec("claims-pricing")
    metadata = await archive.capture_terminal_reference_source(
        session, importer_id=spec.importer_id, schema_name="mrf", run_id="synthetic-run"
    )
    assert await archive._capture_source_generation(session, spec, "mrf", metadata, None) is None
    with pytest.raises(archive.ReferenceFamilyArchiveError, match="authority differs"):
        await archive._capture_source_generation(
            session, spec, "mrf", {**metadata, "stage_suffix": "substituted"}, None
        )


@pytest.mark.asyncio
async def test_manual_claims_activation_retains_generic_oid_cutover_without_generation_triggers(monkeypatch):
    """Existing stage/owner/OID fences remain; the retained caller records actual publication."""
    spec = archive.reference_family_spec("claims-pricing")
    pairs = tuple(sorted((name, index) for index, name in enumerate(spec.table_names, 101)))
    ownership = archive.ReferenceFamilyStageOwnership(spec.importer_id, UUID(int=1), "example", 50, pairs)
    incumbent = archive.ReferenceFamilyIncumbent(
        spec.importer_id, "mrf", tuple((name, None) for name in spec.table_names)
    )
    manifest = SimpleNamespace(
        importer_id=spec.importer_id,
        publication_authority="manual-only",
        source_serving_generation=None,
        source_metadata_sha256="a" * 64,
    )
    validation = SimpleNamespace(tables=())
    monkeypatch.setattr(archive, "validate_reference_family_manifest", lambda _: manifest)
    monkeypatch.setattr(archive, "validate_reference_family_validation_receipt", lambda _: validation)
    binding, lock, owner = Mock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive, "_require_validated_cutover_binding", binding)
    monkeypatch.setattr(archive, "_lock_and_verify_activation", lock)
    monkeypatch.setattr(archive, "_verify_stage_owner", owner)
    monkeypatch.setattr(archive, "_apply_validated_contribution", AsyncMock())
    rotate = AsyncMock(return_value=(None, list(pairs), None))
    monkeypatch.setattr(archive, "_complete_validated_stage_activation", rotate)
    generation = AsyncMock()
    monkeypatch.setattr(archive, "publish_adopted_reference_family_generation", generation)
    cutover = archive.ReferenceFamilyCutoverAuthority("b" * 64, archive.CONTRACT, 70, 70, "manual")
    receipt = await archive.activate_validated_reference_family_stage(
        _session(None),
        ownership=ownership,
        manifest={},
        expected_incumbent=incumbent,
        validation_receipt={},
        cutover=cutover,
    )
    binding.assert_called_once()
    lock.assert_awaited_once()
    owner.assert_awaited_once_with(lock.await_args.args[0], ownership, 70)
    rotate.assert_awaited_once()
    assert receipt.relation_oids == pairs
    generation.assert_not_awaited()


@pytest.mark.asyncio
async def test_scoped_dictionary_publication_preserves_foreign_slices(monkeypatch):
    """Both native-key collision sets are validated before the first fixed-slice write."""
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(return_value=False))
    equal = AsyncMock(return_value=True)
    monkeypatch.setattr(archive, "_is_model_table_equal", equal)
    await archive.replace_claims_dictionary_slice(
        session, incoming_schema="candidate", current_schema="mrf", destination_schema="mrf"
    )
    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    deletions = [sql for sql in statements if sql.startswith("DELETE ")]
    assert len(deletions) == 2 and all("WHERE source=:source" in sql for sql in deletions)
    assert len([sql for sql in statements if sql.startswith("INSERT ")]) == 2
    assert all(
        "claims_code_" in str(call.args[0]) for call in session.scalar.await_args_list if "JOIN" in str(call.args[0])
    )
    assert len(equal.await_args_list) == 4
    assert all(
        call.kwargs["left_predicate"] == "WHERE canonical.source='cms_physician_provider_service'"
        for call in equal.await_args_list
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["unregistered", "drift", "foreign"])
async def test_scoped_dictionary_rejection_has_no_shared_write(monkeypatch, failure):
    session = SimpleNamespace(in_transaction=lambda: True, execute=AsyncMock(), scalar=AsyncMock(return_value=False))
    monkeypatch.setattr(archive, "validate_claims_dictionary_closure", AsyncMock())
    monkeypatch.setattr(archive, "_is_model_table_equal", AsyncMock(return_value=failure != "drift"))
    if failure != "drift":
        session.scalar.return_value = True
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        await archive.replace_claims_dictionary_slice(
            session,
            incoming_schema="candidate",
            current_schema=None if failure == "unregistered" else "mrf",
            destination_schema="mrf",
        )
    assert not any(
        str(call.args[0]).startswith(("INSERT ", "DELETE ", "UPDATE ")) for call in session.execute.await_args_list
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("check", range(8))
async def test_claims_dictionary_set_closure_rejects_every_invariant(check):
    results = [False] * 8
    results[check] = True
    session = SimpleNamespace(in_transaction=lambda: True, scalar=AsyncMock(side_effect=results))
    with pytest.raises(archive.ReferenceFamilyArchiveError):
        await archive.validate_claims_dictionary_closure(session, "candidate")


@pytest.mark.asyncio
async def test_native_copy_carries_two_exact_source_slices_with_one_family_budget():
    """Shared native COPY projects fixed compiled columns and debits the common byte ceiling."""
    spec = archive.reference_family_spec("claims-pricing")
    receipts = tuple(SimpleNamespace(table_name=model.__tablename__) for model in spec.model_types)
    capture = SimpleNamespace(schema_name="mrf", manifest=SimpleNamespace(tables=receipts))
    copy = SimpleNamespace(max_bytes=1000, copy_rows=AsyncMock(return_value=10))
    await archive._copy_source_tables("owning-session", capture, "candidate", spec, copy, 10**12)
    calls = copy.copy_rows.await_args_list
    assert len(calls) == 10 and [call.kwargs["max_bytes"] for call in calls] == list(range(1000, 900, -10))
    assert calls[-2].kwargs["table_name"] == "claims_code_catalog"
    assert calls[-1].kwargs["table_name"] == "claims_code_crosswalk"
    assert all("WHERE canonical.source='cms_physician_provider_service'" in call.args[1] for call in calls[-2:])
