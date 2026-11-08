# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Tenant access is a current exact-ID check separate from immutable metadata pins."""

import asyncio
import json

import pytest

from process import registry_benefit_plan_metadata as metadata
from tests.test_registry_benefit_plan_metadata import _client, _document, _ids, _reply

_CLIENT = "client-ā"
_CHECKED = "2026-10-15T07:00:00.123456Z"


def access_document(request):
    body = json.loads(request.content)
    return {
        "client_id": body["client_id"],
        "allowed_ids": body["healthporta_plan_ids"],
        "denied_ids": [],
        "checked_at": _CHECKED,
    }


@pytest.mark.asyncio
async def test_full_selection_bounds_metadata_and_access_calls():
    calls = []

    def handler(request):
        calls.append(request)
        body = json.loads(request.content)
        if request.url.path.endswith("/selected-access"):
            assert body["client_id"] == _CLIENT
            return _reply(access_document(request))
        return _reply(_document(body["healthporta_plan_ids"]))

    async with _client(handler) as client:
        snapshot = await client.validate(_ids(5000), client_id=_CLIENT)
    assert len(calls) == 20 and all(len(json.loads(call.content)["healthporta_plan_ids"]) <= 512 for call in calls)
    assert snapshot.client_id == _CLIENT and snapshot.access_checked_at == (_CHECKED,) * 10
    assert len({(call.url.scheme, call.url.host, call.url.port) for call in calls}) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "failure", ["wrong_client", "missing", "duplicate", "overlap", "foreign", "order", "timestamp", "extra", "truthy"]
)
async def test_malformed_access_never_grants_partial_authority(failure):
    def handler(request):
        body = json.loads(request.content)
        if not request.url.path.endswith("/selected-access"):
            return _reply(_document(body["healthporta_plan_ids"]))
        return _reply(_malformed_access(access_document(request), failure))

    async with _client(handler) as client:
        with pytest.raises(metadata.RegistryBenefitPlanMetadataUnavailable):
            await client.validate(_ids(2), client_id=_CLIENT)


@pytest.mark.asyncio
async def test_denied_exact_partition_is_permission_denial_not_service_failure():
    def handler(request):
        if not request.url.path.endswith("/selected-access"):
            return _reply(_document(json.loads(request.content)["healthporta_plan_ids"]))
        body = access_document(request)
        body["denied_ids"], body["allowed_ids"] = body["allowed_ids"], []
        return _reply(body)

    async with _client(handler) as client:
        with pytest.raises(metadata.RegistryBenefitPlanAccessDenied):
            await client.validate(_ids(), client_id=_CLIENT)


@pytest.mark.asyncio
async def test_metadata_and_access_share_one_operation_deadline(monkeypatch):
    monkeypatch.setattr(metadata, "TIMEOUT_SECONDS", 0.06)
    calls = []

    async def handler(request):
        calls.append(request)
        await asyncio.sleep(0.04)
        if request.url.path.endswith("/selected-access"):
            return _reply(access_document(request))
        return _reply(_document(json.loads(request.content)["healthporta_plan_ids"]))

    async with _client(handler) as client:
        with pytest.raises(metadata.RegistryBenefitPlanMetadataUnavailable):
            await client.validate(_ids(), client_id=_CLIENT)
    assert len(calls) == 2


def _malformed_access(response_by_name, failure):
    if failure == "wrong_client":
        response_by_name["client_id"] = "foreign-client"
    if failure == "missing":
        response_by_name["allowed_ids"].pop()
    if failure == "duplicate":
        response_by_name["allowed_ids"] *= 2
    if failure == "overlap":
        response_by_name["denied_ids"] = response_by_name["allowed_ids"][:1]
    if failure == "foreign":
        response_by_name["allowed_ids"][0] = _ids(1)[0].replace("0", "1")
    if failure == "order":
        response_by_name["allowed_ids"].reverse()
    if failure == "timestamp":
        response_by_name["checked_at"] = "2026-02-30T00:00:00Z"
    if failure == "extra":
        response_by_name["registry_pin"] = {}
    if failure == "truthy":
        response_by_name = {"authorized": True}
    return response_by_name


@pytest.mark.asyncio
@pytest.mark.parametrize("client_id", ["", " client", "client ", "client\0other", "x" * 65, True])
async def test_invalid_client_partition_identity_refuses_before_metadata_transport(client_id):
    calls = []

    def forbidden(request):
        calls.append(request)
        pytest.fail("invalid client identity reached the metadata service")

    async with _client(forbidden) as client:
        with pytest.raises(ValueError, match="registry_benefit_plan_metadata_invalid"):
            await client.validate(_ids(), client_id=client_id)
    assert calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("checked_at", [None, "2026-10-15", "2026-10-15T07:00:00+00:00", "2026-10-15T07:00:00.1Z"])
async def test_access_partition_requires_canonical_current_check_timestamp(checked_at):
    replies = []

    def handler(request):
        if request.url.path.endswith("/selected-access"):
            body = access_document(request)
            body["checked_at"] = checked_at
        else:
            body = _document(json.loads(request.content)["healthporta_plan_ids"])
        replies.append(_reply(body))
        return replies[-1]

    async with _client(handler) as client:
        with pytest.raises(metadata.RegistryBenefitPlanMetadataUnavailable):
            await client.validate(_ids(), client_id=_CLIENT)
    assert len(replies) == 2 and all(reply.is_closed for reply in replies)
