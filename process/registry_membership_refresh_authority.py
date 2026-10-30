# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Dedicated fresh App authority for the fixed background refresh principal."""

import asyncio

import httpx

from process.registry_approval_store import RegistryApprovalConflict
from process.registry_membership_refresh_contract import (
    RegistryMembershipRefreshUnavailable,
    membership_refresh_command_sha256,
    validated_membership_refresh_envelope,
)
from process.registry_publication_authority import _TIMEOUT_SECONDS, RegistryPublicationAuthorityClient, _receipt
from process.registry_refresh_anchor_contract import encoded_document

AUTHORITY_PATH = "/internal/v1/registry/membership-refresh/authorize"
COMMIT_AUTHORITY_PATH = "/internal/v1/registry/membership-refresh/authorize-commit"


class RegistryMembershipRefreshAuthorityClient(RegistryPublicationAuthorityClient):
    """Use only an explicitly installed refresh credential; disabled by default."""

    def __init__(self, base_url, service_token, *, enabled=False, transport=None):
        if type(enabled) is not bool:
            raise ValueError("registry_membership_refresh_enabled_invalid")
        super().__init__(base_url, service_token, transport=transport)
        self.is_enabled = enabled

    async def __call__(self, *, actor, command):
        """Require a new closed actor/hash/policy receipt on every invocation."""
        return await self._authorize(actor, command, AUTHORITY_PATH)

    async def authorize_commit(self, *, actor, command):
        """Recheck service grants without reentering Core's held native source fences."""
        return await self._authorize(actor, command, COMMIT_AUTHORITY_PATH)

    async def _authorize(self, actor, command, path):
        envelope_by_field = validated_membership_refresh_envelope({"actor": actor, "command": command})
        expected_hash = membership_refresh_command_sha256(command)
        body = encoded_document(envelope_by_field)
        if self.is_enabled is not True:
            raise RegistryMembershipRefreshUnavailable("registry_membership_refresh_disabled")
        try:
            outbound = self._client.build_request("POST", self._origin + path, content=body, headers=self._headers)
            outbound.headers.pop("cookie", None)
            async with asyncio.timeout(_TIMEOUT_SECONDS):
                reply = await self._client.send(outbound, stream=True)
                try:
                    if reply.status_code == 409:
                        raise RegistryApprovalConflict("registry_membership_refresh_authority_stale")
                    receipt_by_field = await _receipt(reply, "command_sha256")
                finally:
                    await reply.aclose()
            if receipt_by_field["actor"] != actor or receipt_by_field["command_sha256"] != expected_hash:
                raise ValueError("registry_membership_refresh_authority_mismatch")
            return receipt_by_field
        except RegistryApprovalConflict:
            raise
        except PermissionError:
            raise PermissionError("registry_membership_refresh_authority_denied") from None
        except (
            ValueError,
            TypeError,
            UnicodeError,
            RecursionError,
            httpx.HTTPError,
            OSError,
            TimeoutError,
            RuntimeError,
        ):
            raise RegistryMembershipRefreshUnavailable("registry_membership_refresh_authority_unavailable") from None
        finally:
            self._client.cookies.clear()
