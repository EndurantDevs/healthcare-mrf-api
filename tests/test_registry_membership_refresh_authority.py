# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dedicated fresh refresh authority reuses the hardened bounded App transport."""

import json

import httpx
import pytest

from process.registry_approval_store import RegistryApprovalConflict
from process.registry_membership_refresh_authority import (
    AUTHORITY_PATH,
    COMMIT_AUTHORITY_PATH,
    RegistryMembershipRefreshAuthorityClient,
)
from process.registry_membership_refresh_contract import (
    RegistryMembershipRefreshUnavailable,
    membership_refresh_command_sha256,
)
from tests.test_registry_membership_refresh_contract import command, envelope
from tests.test_registry_publication_authority import _reply

pytestmark = pytest.mark.asyncio


def client(handler, **changes):
    return RegistryMembershipRefreshAuthorityClient(
        "https://authority.example.invalid",
        "synthetic-refresh-only",
        enabled=changes.pop("enabled", True),
        transport=httpx.MockTransport(handler),
        **changes,
    )


def receipt(document, **changes):
    return {
        "authorized": True,
        "actor": document["actor"],
        "command_sha256": membership_refresh_command_sha256(document["command"]),
        "policy_revision": 7,
        **changes,
    }


async def test_each_invocation_has_exact_dedicated_wire_and_fresh_receipt():
    calls = []

    async def handler(request):
        calls.append(request)
        document = json.loads(request.content)
        assert str(request.url) == "https://authority.example.invalid" + AUTHORITY_PATH
        assert request.headers["authorization"] == "Bearer synthetic-refresh-only"
        assert "cookie" not in request.headers and "session_token_sha256" not in document
        assert document == envelope()
        return _reply(receipt(document, policy_revision=len(calls)))

    async with client(handler) as authorize:
        first = await authorize(**envelope())
        second = await authorize(**envelope())
    assert first["policy_revision"] == 1 and second["policy_revision"] == 2 and len(calls) == 2
    assert authorize._client.is_closed


async def test_disabled_default_never_sends_authority_or_uses_other_credentials():
    async def forbidden(_request):
        pytest.fail("disabled authority sent HTTP")

    async with RegistryMembershipRefreshAuthorityClient(
        "https://authority.example.invalid", "synthetic-refresh-only", transport=httpx.MockTransport(forbidden)
    ) as authorize:
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await authorize(**envelope())


async def test_final_grant_check_has_a_separate_fixed_path_and_fresh_receipt():
    calls = []

    async def handler(request):
        calls.append(request.url.path)
        assert request.url.path == COMMIT_AUTHORITY_PATH
        return _reply(receipt(json.loads(request.content), policy_revision=len(calls)))

    async with client(handler) as authorize:
        first = await authorize.authorize_commit(**envelope())
        second = await authorize.authorize_commit(**envelope())
    assert first["policy_revision"] == 1 and second["policy_revision"] == 2
    assert calls == [COMMIT_AUTHORITY_PATH, COMMIT_AUTHORITY_PATH]


@pytest.mark.parametrize(
    "changes",
    [
        {"authorized": False},
        {"actor": {"kind": "service", "service_id": "other"}},
        {"command_sha256": "d" * 64},
        {"policy_revision": True},
        {"policy_revision": -1},
        {"extra": True},
    ],
)
async def test_actor_command_and_live_policy_receipt_are_closed(changes):
    async def handler(request):
        return _reply(receipt(json.loads(request.content), **changes))

    async with client(handler) as authorize:
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await authorize(**envelope())


@pytest.mark.parametrize(
    "status,error",
    [
        (401, PermissionError),
        (403, PermissionError),
        (302, RegistryMembershipRefreshUnavailable),
        (409, RegistryApprovalConflict),
        (503, RegistryMembershipRefreshUnavailable),
    ],
)
async def test_denial_unavailable_and_redirect_never_authorize(status, error):
    async def handler(request):
        return _reply(receipt(json.loads(request.content)), status=status)

    async with client(handler) as authorize:
        with pytest.raises(error):
            await authorize(**envelope())


@pytest.mark.parametrize(
    "headers",
    [
        [("Set-Cookie", "session=untrusted")],
        [("Content-Encoding", "gzip")],
        [("Cache-Control", "public")],
        [("Content-Type", "text/html")],
        [("Content-Length", "999999")],
    ],
)
async def test_untrusted_response_headers_fail_closed(headers):
    async def handler(request):
        return _reply(receipt(json.loads(request.content)), headers=headers)

    async with client(handler) as authorize:
        with pytest.raises(RegistryMembershipRefreshUnavailable):
            await authorize(**envelope())
        assert not authorize._client.cookies


async def test_first_head_zero_uses_explicit_null_in_full_authority_digest():
    initial = envelope(command=command(expected_serving_generation=0, expected_serving_manifest_sha256=None))

    async def handler(request):
        document = json.loads(request.content)
        assert document["command"]["expected_serving_manifest_sha256"] is None
        return _reply(receipt(document))

    async with client(handler) as authorize:
        assert (await authorize(**initial))["authorized"] is True
