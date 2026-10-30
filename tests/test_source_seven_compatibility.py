# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Synthetic protocol checks only; no native admission or Source authority claim."""

import asyncio
import hashlib
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process import network_fhir_membership_source as fhir
from process import network_initial_cms_office_bindings as offices
from process import registry_initial_source_composition as composition
from process.network_address_projection import PinnedAddressSource
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_fhir_retained_capture import RetainedFHIRRead
from process.network_fhir_retained_capture_contract import SEMANTIC_PROFILE, capture_digest
from process.registry_initial_source_composition import OfficeSummaryRequest
from tests.test_network_fhir_retained_capture import pin, retained
from tests.test_registry_generic_fhir_facts import _recipe


def _semantic_pin(dataset):
    capture = retained()
    origin = replace(
        capture.origin,
        dataset_id=dataset,
        admission_profile=SEMANTIC_PROFILE,
        original_admission_refusal=None,
        admission_metadata_sha256="4" * 64,
        semantic_projection_as_of="2026-01-01",
        descriptor_counts=capture.origin.resource_counts,
    )
    capture = replace(capture, origin=origin)
    return pin(replace(capture, capture_sha256=capture_digest(capture)))


@pytest.mark.parametrize("successor", [False, True])
async def test_office_read_dispatch_keeps_original_and_physical_capabilities(monkeypatch, successor):
    original, physical = _semantic_pin("original"), _semantic_pin("physical")
    source = fhir.with_fhir_successor(original, physical) if successor else original
    connection, events = object(), []
    original_read = RetainedFHIRRead(connection, original.retained_capture, asyncio.current_task(), "unit", object())
    physical_read = RetainedFHIRRead(connection, physical.retained_capture, asyncio.current_task(), "unit", object())

    async def observe(read, actual_connection, capture):
        assert actual_connection is connection
        events.append((read, capture))

    monkeypatch.setattr(RetainedFHIRRead, "verify", observe)
    reads = fhir.RetainedFHIRSuccessorReads(original_read, physical_read) if successor else original_read
    await offices._verify_retained_office_read(connection, source, reads)
    assert events == [(original_read, original.retained_capture)] + (
        [(physical_read, physical.retained_capture)] if successor else []
    )


@pytest.mark.parametrize("foreign", ["original", "physical", "outer"])
async def test_office_successor_rejects_foreign_capability_before_any_verify(monkeypatch, foreign):
    original, physical = _semantic_pin("original"), _semantic_pin("physical")
    source = fhir.with_fhir_successor(original, physical)
    native = RetainedFHIRRead(object(), original.retained_capture, asyncio.current_task(), "unit", object())
    reads = (
        SimpleNamespace(verify=AsyncMock())
        if foreign == "outer"
        else fhir.RetainedFHIRSuccessorReads(
            object() if foreign == "original" else native, object() if foreign == "physical" else native
        )
    )
    verify = AsyncMock()
    monkeypatch.setattr(RetainedFHIRRead, "verify", verify)
    with pytest.raises(offices.InitialCMSOfficeBindingError):
        await offices._verify_retained_office_read(object(), source, reads)
    verify.assert_not_awaited()


async def test_verified_location_page_uses_physical_dataset_and_schema(monkeypatch):
    original, physical = _semantic_pin("original"), _semantic_pin("physical")
    source = fhir.with_fhir_successor(original, physical)
    batch = fhir.FHIRMembershipBatch(
        source,
        0,
        0,
        0,
        None,
        b"[]",
        hashlib.sha256(b"[]").hexdigest(),
        ApprovedMembershipSource(0, "a" * 64),
        _recipe(original).binding_coordinates,
        True,
    )
    connection = SimpleNamespace(
        copy_records_to_table=AsyncMock(return_value="COPY 0"), fetchrow=AsyncMock(return_value={"offices_json": "[]"})
    )
    monkeypatch.setattr(offices, "_canonicalize", AsyncMock(return_value="[]"))
    await offices._stage_verified_retained_page(connection, batch, None, "unit_inputs", "unit_source", {})
    arguments = connection.fetchrow.await_args.args
    assert physical.read_schema_name in arguments[0] and original.read_schema_name not in arguments[0]
    assert arguments[1:] == ("[]", original.source_id, physical.dataset_id)


@pytest.mark.parametrize("legacy", [False, True])
async def test_record_entry_keeps_both_native_rederived_and_legacy_approved_signatures(monkeypatch, legacy):
    address, approved = (
        PinnedAddressSource("unit_address", "entity_address_unified", "generation"),
        ApprovedMembershipSource(0, "a" * 64),
    )
    native, original = AsyncMock(return_value="native"), AsyncMock(return_value="legacy")
    monkeypatch.setattr(composition, "record_source_offices", native)
    monkeypatch.setattr(composition, "record_initial_source_offices_with_reads", original)
    arguments = (approved, address) if legacy else (address,)
    result = await composition.record_initial_source_offices(
        object(),
        object(),
        (),
        *arguments,
        request=OfficeSummaryRequest(runtime_roles=("unit_reader",), control_schema="unit_control"),
    )
    assert result == ("legacy" if legacy else "native")
    assert original.await_count == int(legacy) and native.await_count == int(not legacy)
