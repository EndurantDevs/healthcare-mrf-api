# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Rollback address preparation preserves exact request, lease and authority checks."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from process import registry_publication_execution as execution
from process.network_address_projection import PinnedAddressSource
from process.registry_approval_store import RegistryApprovalConflict
from process.registry_candidate_composition import RegistryCompositionAddressSources


def _sources():
    return RegistryCompositionAddressSources(PinnedAddressSource("retained_base", "entity_address_unified", "a" * 64))


@pytest.mark.asyncio
@pytest.mark.parametrize("outcome", ["legacy", "complete", "prepare_failure", "revoked", "lease", "digest"])
async def test_retained_operation_preparation_fences_exact_request_and_purpose(monkeypatch, outcome):
    events = []
    address_sources, prepared = _sources(), _sources()
    source_pin = object()
    request_by_name = {
        "request_id": uuid4(),
        "lease_id": uuid4(),
        "request_sha256": "a" * 64,
        "command_json": {"expected_serving_generation": 7, "expected_approved_revision": 4},
    }

    async def request(*_args):
        events.append("lease")
        if outcome == "lease" and events.count("lease") == 2:
            raise RegistryApprovalConflict("registry_queue_lease_conflict")
        return {**request_by_name, "request_sha256": "b" * 64} if outcome == "digest" else request_by_name

    async def authority(_authorize, actual, operation):
        assert actual == request_by_name and operation == "prepare_source_rollback"
        events.append("authority")
        if outcome == "revoked" and events.count("lease") == 2:
            raise PermissionError("authority_denied")

    async def prepare(**options):
        assert options == dict(
            request_id=request_by_name["request_id"],
            approved_revision=4,
            source_manifest=source_pin,
            expected_head=7,
            address_sources=address_sources,
        )
        events.append("prepare")
        if outcome == "prepare_failure":
            raise ValueError("preparation_refused")
        return prepared

    monkeypatch.setattr(execution, "_request", request)
    monkeypatch.setattr(execution, "_fresh_operation_authority", authority)
    operation = execution._prepared_operation_addresses(
        object(),
        request_by_name,
        source_pin,
        address_sources,
        None if outcome == "legacy" else prepare,
        object(),
        "registry",
        "prepare_source_rollback",
    )
    if outcome in {"legacy", "complete"}:
        assert await operation is (address_sources if outcome == "legacy" else prepared)
        assert events == ([] if outcome == "legacy" else ["lease", "authority", "prepare", "lease", "authority"])
    else:
        with pytest.raises(PermissionError if outcome == "revoked" else ValueError):
            await operation
        assert ("prepare" in events) == (outcome != "digest")


@pytest.mark.asyncio
@pytest.mark.parametrize("callbacks", [{"unknown": None}, {"prepare_address_sources": {}}])
async def test_rollback_preparation_rejects_unknown_or_noncallable_callbacks(callbacks):
    with pytest.raises(ValueError, match="registry_publication_address_preparation_invalid"):
        await execution.complete_queued_registry_source_rollback_preview(
            SimpleNamespace(is_in_transaction=lambda: False),
            uuid4(),
            uuid4(),
            authorize=object(),
            writer_roles={},
            **callbacks,
        )
