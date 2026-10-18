# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded selected-plan metadata cannot grant authority through client claims."""

import asyncio
import json
import traceback
from dataclasses import FrozenInstanceError

import httpx
import pytest

from process import registry_benefit_plan_metadata as metadata

_ORIGIN = "https://metadata.example.invalid"
_TOKEN = "synthetic-metadata-token"
_PIN = {"generation_id": "11111111-1111-4111-8111-111111111111", "head_fence": 3, "receipt_digest": "a" * 64}


def _ids(count=1):
    return tuple("hpplan_" + f"{index:026d}" for index in range(count))


def _document(plan_ids, **changes):
    return {
        "registry_pin": dict(_PIN),
        "items": [
            {"healthporta_plan_id": plan_id, "status": "active", "plan_kind": "custom_composite", "display_name": None}
            for plan_id in plan_ids
        ],
        "missing_ids": [],
    } | changes


class _Stream(httpx.AsyncByteStream):
    def __init__(self, body, *, entered=None):
        self.body = body
        self.closed = False
        self.entered = entered

    async def __aiter__(self):
        if self.entered is not None:
            self.entered.set()
            await asyncio.Event().wait()
        yield self.body

    async def aclose(self):
        self.closed = True


def _reply(document=None, *, raw=None, status=200, headers=(), stream=None):
    defaults = [("Content-Type", "application/json"), ("Cache-Control", "no-store")]
    overrides = {name.lower() for name, _ in headers}
    defaults = [(name, field) for name, field in defaults if name.lower() not in overrides]
    body = json.dumps(document, separators=(",", ":")).encode() if raw is None else raw
    return httpx.Response(status, headers=defaults + list(headers), stream=stream or _Stream(body))


def _client(handler, **changes):
    return metadata.RegistryBenefitPlanMetadataClient(
        changes.pop("base_url", _ORIGIN),
        changes.pop("service_token", _TOKEN),
        transport=httpx.MockTransport(handler),
        **changes,
    )


def test_optional_trusted_config_names_and_invalid_bindings_are_closed():
    assert metadata.ORIGIN_CONFIG == "HLTHPRT_REGISTRY_PLAN_METADATA_ORIGIN"
    assert metadata.TOKEN_CONFIG == "HLTHPRT_REGISTRY_PLAN_METADATA_TOKEN"
    assert metadata.RegistryBenefitPlanMetadataClient.from_config({}) is None
    for config in ({metadata.ORIGIN_CONFIG: _ORIGIN}, {metadata.TOKEN_CONFIG: _TOKEN}, None):
        with pytest.raises(ValueError, match="registry_benefit_plan_metadata_invalid"):
            metadata.RegistryBenefitPlanMetadataClient.from_config(config)
    for origin in (
        "https://user:secret@example.invalid",
        _ORIGIN + "/choose",
        _ORIGIN + "?pin=x",
        "file:///tmp/x",
        _ORIGIN + ":0",
    ):
        with pytest.raises(ValueError) as caught:
            _client(lambda request: None, base_url=origin)
        assert "secret" not in str(caught.value) and origin not in str(caught.value)
    with pytest.raises(ValueError):
        _client(lambda request: None, service_token="token\nInjected")
    with pytest.raises(ValueError):
        metadata.RegistryBenefitPlanMetadataClient(_ORIGIN, _TOKEN, transport=object())


@pytest.mark.asyncio
async def test_5000_ids_use_fresh_bounded_reads():
    requests = []

    def handler(request):
        requests.append(request)
        assert str(request.url) == _ORIGIN + "/v1/plan-identity/plans/selected-metadata"
        assert request.headers["Authorization"] == "Bearer " + _TOKEN and "cookie" not in request.headers
        assert request.headers["Accept-Encoding"] == "identity" and request.method == "POST"
        body = json.loads(request.content)
        assert set(body) == {"healthporta_plan_ids"} and len(request.content) <= 32768
        assert 1 <= len(body["healthporta_plan_ids"]) <= 512
        return _reply(_document(body["healthporta_plan_ids"]))

    async with _client(handler) as client:
        assert await client.validate(()) is None and requests == []
        snapshot = await client.validate(tuple(reversed(_ids(5000))))
        assert len(requests) == 10 and snapshot.plan_ids == _ids(5000)
        assert snapshot.generation_id == _PIN["generation_id"] and snapshot.receipt_digest == _PIN["receipt_digest"]
        with pytest.raises(FrozenInstanceError):
            snapshot.head_fence = 99
        assert (await client.validate(_ids())).head_fence == 3 and len(requests) == 11


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "plan_ids",
    [None, {}, [True], ["hpversion_" + "0" * 26], ["hpplan_" + "I" * 26], _ids() * 2, _ids(5001)],
    ids=["null", "object", "boolean", "version", "ambiguous-letter", "duplicate", "over-cap"],
)
async def test_invalid_selections_never_make_a_request(plan_ids):
    def forbidden(_request):
        pytest.fail("invalid selection reached the metadata service")

    async with _client(forbidden) as client:
        with pytest.raises(metadata.RegistryBenefitPlanSelectionInvalid):
            await client.validate(plan_ids)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["retired", "merged", "missing"])
async def test_inactive_or_missing_selected_ids_refuse_without_following_another_id(status):
    def handler(request):
        body = _document(json.loads(request.content)["healthporta_plan_ids"])
        if status == "missing":
            body["items"], body["missing_ids"] = [], list(_ids())
        else:
            body["items"][0]["status"] = status
        return _reply(body)

    async with _client(handler) as client:
        with pytest.raises(metadata.RegistryBenefitPlanSelectionInvalid):
            await client.validate(_ids())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,changed",
    [("generation_id", "22222222-2222-4222-8222-222222222222"), ("head_fence", 4), ("receipt_digest", "b" * 64)],
)
async def test_every_chunk_must_share_one_exact_immutable_pin(field, changed):
    observed_requests = []

    def handler(request):
        observed_requests.append(request)
        body = _document(json.loads(request.content)["healthporta_plan_ids"])
        if len(observed_requests) > 1:
            body["registry_pin"][field] = changed
        return _reply(body)

    async with _client(handler) as client:
        with pytest.raises(metadata.RegistryBenefitPlanMetadataUnavailable):
            await client.validate(_ids(513))
    assert len(observed_requests) == 2


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "poison",
    [
        "authorized",
        "missing-coverage",
        "extra-id",
        "duplicate-id",
        "wrong-kind",
        "priceable",
        "boolean-fence",
        "invalid-generation",
        "invalid-digest",
    ],
)
async def test_closed_response_rejects_claims_and_incomplete_or_foreign_metadata(poison):
    body = _document(_ids())
    match poison:
        case "authorized":
            body["authorized"] = True
        case "missing-coverage":
            body["items"] = []
        case "extra-id":
            body["items"][0]["healthporta_plan_id"] = _ids(2)[1]
        case "duplicate-id":
            body["items"] *= 2
        case "wrong-kind":
            body["items"][0]["plan_kind"] = "alias"
        case "priceable":
            body["items"][0]["priceable"] = True
        case "boolean-fence":
            body["registry_pin"]["head_fence"] = True
        case "invalid-generation":
            body["registry_pin"]["generation_id"] = "0" * 32
        case _:
            body["registry_pin"]["receipt_digest"] = "A" * 64
    async with _client(lambda request: _reply(body)) as client:
        with pytest.raises(metadata.RegistryBenefitPlanMetadataUnavailable):
            await client.validate(_ids())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,headers,raw",
    [
        (302, [("Location", "https://other.example.invalid")], None),
        (403, [], None),
        (500, [], None),
        (200, [("Content-Encoding", "gzip")], None),
        (200, [("Set-Cookie", "session=private")], None),
        (200, [("Content-Type", "application/json"), ("Content-Type", "application/json")], None),
        (200, [("Content-Length", "1048577")], None),
        (200, [("Content-Length", "3")], b"{}"),
        (200, [], b"x" * (1024 * 1024 + 1)),
        (200, [], b'{"items":[],"items":[]}'),
        (200, [], b'{"registry_pin":NaN}'),
    ],
    ids=[
        "redirect",
        "denied",
        "server-error",
        "compression",
        "cookie",
        "duplicate-header",
        "declared-oversize",
        "truncated",
        "streamed-oversize",
        "duplicate-json",
        "nonfinite",
    ],
)
async def test_transport_refusals_are_bounded_sanitized_and_close_the_response(status, headers, raw):
    reply = _reply(_document(_ids()), raw=raw, status=status, headers=headers)
    async with _client(lambda request: reply) as client:
        with pytest.raises(metadata.RegistryBenefitPlanMetadataUnavailable) as caught:
            await client.validate(_ids())
        assert reply.is_closed and not client._client.cookies
        rendered = "".join(traceback.format_exception(caught.value))
        assert _TOKEN not in rendered and _ORIGIN not in rendered and "session=private" not in rendered


@pytest.mark.asyncio
async def test_one_deadline_covers_all_chunks_and_cancellation_closes_the_stream(monkeypatch):
    monkeypatch.setattr(metadata, "TIMEOUT_SECONDS", 0.05)
    observed_requests = []

    async def slow(request):
        observed_requests.append(request)
        await asyncio.sleep(0.03)
        return _reply(_document(json.loads(request.content)["healthporta_plan_ids"]))

    async with _client(slow) as client:
        with pytest.raises(metadata.RegistryBenefitPlanMetadataUnavailable):
            await client.validate(_ids(513))
    assert len(observed_requests) == 2
    entered = asyncio.Event()
    stream = _Stream(b"", entered=entered)
    async with _client(lambda request: _reply(stream=stream)) as client:
        task = asyncio.create_task(client.validate(_ids()))
        await entered.wait()
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert stream.closed


@pytest.mark.asyncio
@pytest.mark.parametrize("pin", [None, [], {**_PIN, "source": "untrusted"}, {"generation_id": _PIN["generation_id"]}])
async def test_metadata_pin_requires_the_complete_closed_authoritative_identity(pin):
    reply = _reply(_document(_ids(), registry_pin=pin))
    async with _client(lambda request: reply) as client:
        with pytest.raises(metadata.RegistryBenefitPlanMetadataUnavailable):
            await client.validate(_ids())
    assert reply.is_closed


@pytest.mark.asyncio
async def test_serialized_metadata_request_budget_refuses_before_transport():
    requests = []

    def forbidden(request):
        requests.append(request)
        pytest.fail("oversized metadata request reached transport")

    async with _client(forbidden) as client:
        with pytest.raises(ValueError, match="registry_benefit_plan_metadata_invalid"):
            await client._read("/v1/plan-identity/plans/selected-metadata", {"healthporta_plan_ids": _ids(1000)})
    assert requests == []


@pytest.mark.parametrize("selected", [None, "not-an-ID-list", _ids(513), list(_ids(513))])
def test_benefit_assertion_selection_is_a_bounded_list(selected):
    with pytest.raises(metadata.RegistryBenefitPlanSelectionInvalid):
        metadata.collect_benefit_plan_ids([{"benefit_plan_ids": selected}])


def test_benefit_assertions_enforce_the_whole_unique_id_limit():
    identities = list(_ids(5001))
    assertions = [{"benefit_plan_ids": identities[offset : offset + 512]} for offset in range(0, 5001, 512)]
    with pytest.raises(metadata.RegistryBenefitPlanSelectionInvalid):
        metadata.collect_benefit_plan_ids(assertions)
    assertions[-1]["benefit_plan_ids"].pop()
    assert metadata.collect_benefit_plan_ids(assertions) == _ids(5000)


@pytest.mark.asyncio
@pytest.mark.parametrize("actor", [None, {}, "administrator"])
async def test_benefit_metadata_requires_a_validated_registry_actor_before_transport(actor):
    def forbidden(request):
        pytest.fail("untyped actor reached the metadata service")

    async with _client(forbidden) as client:
        with pytest.raises(ValueError, match="registry_actor_invalid"):
            await metadata.validate_benefit_plan_ids(_ids(), client, actor)
