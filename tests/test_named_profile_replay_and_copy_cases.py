# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Profile replay and interrupted COPY keep named state and original failures."""

import asyncio
import copy
import importlib
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest

from process import native_copy_cleanup as cleanup
from process import provider_directory_profile_native_copy as native_copy
from process import source_profile_result_archive as archive
from tests.test_provider_directory_profile_capacity import _geometry_payload, _legacy_geometry_projection_inputs
from tests.test_source_profile_archive_boundaries import adoption_pins, publication_attachment
from tests.test_source_profile_result_archive import _versioned_manifest

fhir = importlib.import_module("process.provider_directory_fhir")


@pytest.mark.parametrize("failure", [None, "run", "relation", "schema", "extra"])
def test_projection_pointer_uses_complete_qualified_names(failure):
    pointer_by_field = {
        "current_run_id": "a" * 32,
        "previous_run_id": None,
        "current_relation": {"schema": "published_rows", "table_name": "projection"},
        "previous_relation": None,
    }
    if failure == "run":
        pointer_by_field["current_run_id"] = "invalid"
    elif failure == "relation":
        pointer_by_field["current_relation"] = {"table_name": "projection"}
    elif failure == "schema":
        pointer_by_field["current_relation"]["schema"] = "unsafe.schema"
    elif failure == "extra":
        pointer_by_field["current_relation"]["extra"] = True
    original = copy.deepcopy(pointer_by_field)
    if failure:
        with pytest.raises(RuntimeError):
            archive._validate_native_pointer(pointer_by_field, archive.PROJECTION_IMPORTER)
    else:
        archive._validate_native_pointer(pointer_by_field, archive.PROJECTION_IMPORTER)
    assert pointer_by_field == original


@pytest.mark.asyncio
@pytest.mark.parametrize("changed", [False, True])
async def test_existing_adoption_pin_replay_never_attaches_or_writes_again(monkeypatch, changed):
    manifest = _versioned_manifest(archive.CONTRACT)
    prepared = archive.PreparedResult(
        manifest,
        archive.StageOwnership(
            manifest["importer_id"],
            uuid4(),
            tuple(sorted(archive.source_spec(manifest["importer_id"], publication=True).table_names)),
        ),
    )
    publication = publication_attachment(prepared.manifest, prepared.ownership)
    receipt_by_field = archive._validation(prepared, "a" * 64, None, "mrf", None, uuid4(), publication=publication)
    retained_pins = adoption_pins(prepared.manifest, receipt_by_field)
    if changed:
        retained_pins[0]["purpose"] = "other"
    monkeypatch.setattr(archive.pins, "lock_run", AsyncMock())
    monkeypatch.setattr(archive, "_pin_group", AsyncMock(return_value=retained_pins))
    topology = AsyncMock()
    attach, adopt_rows, pin = AsyncMock(), AsyncMock(), AsyncMock()
    monkeypatch.setattr(archive, "_require_stage_topology", topology)
    monkeypatch.setattr(archive, "_attach_publication", attach)
    monkeypatch.setattr(archive, "_adopt_rows", adopt_rows)
    monkeypatch.setattr(archive.pins, "record_pin", pin)
    session = object()
    if changed:
        with pytest.raises(archive.SourceProfileArchiveError, match="adoption authority changed"):
            await archive._adopt_and_pin(session, prepared, "mrf", uuid4(), receipt_by_field)
        topology.assert_not_awaited()
    else:
        await archive._adopt_and_pin(session, prepared, "mrf", uuid4(), receipt_by_field)
        topology.assert_awaited_once_with(session, prepared.ownership, receipt_by_field["publication"])
    for operation in (attach, adopt_rows, pin):
        operation.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("error_type", [asyncio.CancelledError, TimeoutError])
@pytest.mark.parametrize("failed_cleanup", [None, "close", "invalidate", "both"])
async def test_interrupted_copy_preserves_failure_and_attempts_both_cleanup_steps(
    monkeypatch, error_type, failed_cleanup
):
    original = error_type("copy interrupted")
    close_error, invalidate_error = RuntimeError("close failed"), RuntimeError("invalidation failed")
    closing = AsyncMock(side_effect=close_error if failed_cleanup in {"close", "both"} else None)
    invalidating = AsyncMock(side_effect=invalidate_error if failed_cleanup in {"invalidate", "both"} else None)
    monkeypatch.setattr(cleanup, "close_interrupted_copy", closing)
    monkeypatch.setattr(cleanup, "invalidate_copy_owner", invalidating)
    driver = SimpleNamespace(copy_records_to_table=AsyncMock(side_effect=original))
    publisher = SimpleNamespace(policy=SimpleNamespace(schema="candidate_rows"))
    owner = object()
    outcome = SimpleNamespace(session=owner)
    directory = SimpleNamespace(
        _json_columns=lambda _table: set(), _copy_record=lambda row, _cols, _json: tuple(row.values())
    )
    request = SimpleNamespace(
        model=SimpleNamespace(__table__=object()), relation="payload", batch=SimpleNamespace(projected_rows=2)
    )
    monkeypatch.setattr(native_copy, "_origin", AsyncMock(return_value=(publisher, driver)))
    yielded_pages = []

    async def pages():
        for ordinal in range(2):
            yielded_pages.append(ordinal)
            yield [{"value": ordinal}]

    with pytest.raises(error_type) as failure:
        await native_copy._copy_pages(directory, request, pages(), outcome, (publisher, driver), ("value",))
    assert failure.value is original and yielded_pages == [0]
    closing.assert_awaited_once_with(driver)
    invalidating.assert_awaited_once_with(owner, driver)
    assert failure.value.__cause__ is (
        invalidate_error
        if failed_cleanup in {"invalidate", "both"}
        else close_error
        if failed_cleanup == "close"
        else None
    )
    if failed_cleanup == "both":
        assert invalidate_error.__context__ is close_error


@pytest.mark.parametrize("named_relations", [False, True])
def test_retained_geometry_requires_original_semantics_and_named_targets(named_relations):
    payload, retained_hash, evidence = _legacy_geometry_projection_inputs()
    receipt_by_field = {
        "executable_plan_hash": evidence["executable_plan_hash"],
        "profile_as_of": evidence["profile_as_of"],
        "to_source_vector_hash": evidence["source_vector_hash"],
        "to_source_context_vector_hash": evidence["source_context_vector_hash"],
        "evidence_rows": evidence["evidence_rows"],
        "profile_rows": evidence["profile_rows"],
    }
    execution = SimpleNamespace(
        attestation=SimpleNamespace(
            proof_id=evidence["selection_proof_id"], profile_input_digest=evidence["profile_input_digest"]
        )
    )
    original = copy.deepcopy(payload)
    if named_relations:
        geometry = fhir._profile_legacy_replay_geometry(
            payload, retained_hash, receipt_by_field, execution, _geometry_payload()["relations"]
        )
        assert geometry.relations == _geometry_payload()["relations"]
        assert geometry.legacy_payload == payload == original
    else:
        with pytest.raises(fhir.ProviderDirectoryArtifactBuildStale, match="semantic_evidence_required"):
            fhir._profile_legacy_replay_geometry(payload, retained_hash, receipt_by_field, execution, None)


@pytest.mark.asyncio
async def test_failed_acknowledgement_readback_remains_uncertain(monkeypatch):
    @asynccontextmanager
    async def readback():
        raise RuntimeError("readback unavailable")
        yield

    monkeypatch.setattr(fhir, "_provider_directory_artifact_promotion_readback", readback)
    assert await fhir._is_artifact_bundle_promotion_committed((), (), None, None) is False


@pytest.mark.asyncio
@pytest.mark.parametrize("valid_geometry", [False, True])
async def test_initial_receipt_decodes_only_a_complete_current_geometry(valid_geometry):
    from process import provider_directory_profile_capacity as capacity
    from process import provider_directory_profile_initial as initial
    from tests.test_provider_directory_profile_initial import _geometry

    raw = capacity.capacity_geometry_payload(_geometry()) if valid_geometry else None
    directory = SimpleNamespace(profile_capacity=capacity)
    receipt_by_field = {"payload": {"capacity_geometry": raw}}
    if valid_geometry:
        assert await initial._retained_receipt_geometry(directory, "fixture", receipt_by_field, None) == _geometry()
    else:
        with pytest.raises(RuntimeError, match="initial_replay_geometry_invalid"):
            await initial._retained_receipt_geometry(directory, "fixture", receipt_by_field, None)


@pytest.mark.asyncio
async def test_initial_replay_refuses_a_missing_named_receipt_before_layout_measurement():
    from process import provider_directory_profile_initial as initial

    measure = AsyncMock()
    directory = SimpleNamespace(
        _provider_directory_relation_oid=AsyncMock(return_value=None),
        _provider_directory_profile_capacity_relation_layout=measure,
    )
    with pytest.raises(RuntimeError, match="initial_receipt_missing"):
        await initial.receipt_layout(directory, "fixture")
    measure.assert_not_awaited()
