# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Genuine Source editions and worker refusal; mapper failure is explicit fault injection."""

import json
import re
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import text

from process import registry_verified_witness_stage as witness_stage
from process.network_cms_registry_address_capture import native_driver
from process.registry_membership_refresh_contract import membership_refresh_command_sha256
from process.registry_membership_refresh_queue import validated_membership_refresh_request_status
from tests import generic_source_successor_postgres_support as source
from tests import test_registry_generic_source_successor_postgres as successor
from tests.test_network_core_layer_provider_postgres import _assert_inventory_migration
from tests.verified_witness_readiness_refusal_support import (
    assert_ic_request_recovery,
    assert_serving_unchanged,
    frozen_ic_recovery,
    request_record,
    serving_state,
)

pytestmark = pytest.mark.asyncio


async def _reject_changed_capture(fixture, capture, original, old_reader, monkeypatch):
    """Install the deterministic non-database mapper failure before readiness observes edition two."""
    fault = ValueError("synthetic deterministic witness mapper refusal")
    mapper = AsyncMock(side_effect=fault)
    with monkeypatch.context() as injected:
        injected.setattr(witness_stage, "map_fhir_source_witnesses_v2", mapper)
        desired = await fixture.bundle.inputs_service()
        assert desired["ready_address_generation"] == desired["address_generation"]
        assert desired["accepted_sources"][0]["capture_generation"] == capture.capture_sha256
        mapper.assert_not_awaited()
        command = fixture.command(desired)
        queued = await fixture.enqueue(command)
        mapper.assert_not_awaited()
        before_by_field = await serving_state(fixture, original[0])
        with pytest.raises(ValueError, match="^registry_publication_request_refused$"):
            await fixture.consume(queued["request_id"])
        mapper.assert_awaited_once()
        assert fault.__cause__ is None
    rejected = await request_record(fixture, queued["request_id"])
    assert rejected["state"] == "rejected"
    rejected_result = rejected["result_json"]
    if type(rejected_result) is str:
        rejected_result = json.loads(rejected_result)
    assert rejected_result == {"error": {"code": "registry_publication_rejected"}}
    assert validated_membership_refresh_request_status(rejected) == command
    after_by_field = await assert_serving_unchanged(fixture, original, old_reader, before_by_field)
    return desired, command, queued["request_id"], rejected, after_by_field


async def _assert_rejected_recovery(fixture, original, old_reader, rejected_state, ic):
    """The removed fault does not create a new request or change the rejected native row."""
    desired, command, request_id, rejected, after_by_field = rejected_state
    recovered = await fixture.bundle.inputs_service()
    expected_link_by_field = {
        "request_id": request_id,
        "state": "rejected",
        "command_sha256": membership_refresh_command_sha256(command),
        "inputs_digest": command["idempotency_key"].removeprefix("registry-membership-refresh:"),
    }
    assert recovered["refresh_request"] == expected_link_by_field
    assert recovered["accepted_sources"] == desired["accepted_sources"]
    assert recovered["ready_address_generation"] == desired["ready_address_generation"]
    assert fixture.command(recovered) == command
    assert await request_record(fixture, request_id) == rejected
    assert await serving_state(fixture, original[0]) == after_by_field
    ic_qualification = await assert_ic_request_recovery(ic, recovered, command, request_id)
    assert await request_record(fixture, request_id) == rejected
    assert await assert_serving_unchanged(fixture, original, old_reader, after_by_field) == after_by_field
    return ic_qualification


async def test_cheap_readiness_defers_mapper_refusal_and_recovers_exact_rejected_request(monkeypatch, record_property):
    ic = frozen_ic_recovery()
    async with source.native_generic_source(monkeypatch) as source_fixture:
        assert re.fullmatch(r"hc_cms_admission_test_[0-9a-f]{32}", source_fixture.component.admin_engine.url.database)
        original_capture = await source.publish_edition(source_fixture, 1)
        async with source.generic_refresh_execution(source_fixture, monkeypatch) as (fixture, serving, stack, head_url):
            await _assert_inventory_migration(fixture)
            await source.start_refresh_runtime(fixture, source_fixture, serving, stack, head_url)
            _, _, original = await successor._consume(fixture)
            assert original[1] == [("Suite 1", [fixture.network_id]), ("Suite 2", [])]
            assert fixture.roles_by_kind["execution"] != fixture.roles_by_kind["custodian"]
            async with fixture.outer() as old_session, old_session.begin():
                await old_session.execute(text("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"))
                old_reader = await native_driver(old_session)
                assert await successor._read(fixture, old_reader) == original
                changed = await source.publish_edition(source_fixture, 2)
                assert changed.capture_sha256 != original_capture.capture_sha256
                rejected_state = await _reject_changed_capture(fixture, changed, original, old_reader, monkeypatch)
                ic_qualification = await _assert_rejected_recovery(fixture, original, old_reader, rejected_state, ic)
            assert source_fixture.commits == [candidate.dataset_id for candidate in source_fixture.candidates]
            assert source_fixture.source_writer_rejections == source_fixture.commits
            assert len(source_fixture.commits) == 2
            assert fixture.cas_checks == [original[0].generation_id]
            record_property("ic_recovery_qualification", ic_qualification)
            record_property(
                "mapper_fault", "deterministic ValueError without database cause; injected before observation"
            )
            record_property(
                "qualification",
                "genuine Source admission/captures/native roles; existing App mock; cheap ready, actual worker refusal, "
                "exact rejected xmin/ctid, unchanged G1 head/offices/heaps/OIDs/custody; no durable IC dispatch proof",
            )
