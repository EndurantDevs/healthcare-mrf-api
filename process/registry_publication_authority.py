# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fresh bounded authority transport; execution still validates every semantic pin."""

import asyncio
import json
import re
from urllib.parse import urlsplit

import httpx

from process.registry_publication_queue import (
    validated_registry_reactivation_command,
    validated_registry_source_rollback_preview_command,
    validated_registry_source_rollback_publication_command,
)

_MAX_BYTES = 65_536
_TIMEOUT_SECONDS = 15
_PATHS = {
    "reactivate_generation": "/internal/v1/registry/reactivation/authorize",
    "prepare_source_rollback": "/internal/v1/registry/source-rollback/authorize",
    "publish_source_rollback": "/internal/v1/registry/source-rollback/authorize",
}
_COMMAND_VALIDATORS = {
    "reactivate_generation": validated_registry_reactivation_command,
    "prepare_source_rollback": validated_registry_source_rollback_preview_command,
    "publish_source_rollback": validated_registry_source_rollback_publication_command,
}
_SHA256 = re.compile(r"[0-9a-f]{64}\Z")


def _invalid():
    return ValueError("registry_publication_authority_invalid")


def _origin(value):
    if (
        type(value) is not str
        or not 0 < len(value) <= 512
        or not value.isascii()
        or any(not 32 < ord(character) < 127 for character in value)
        or any(character in value for character in ("?", "#", "\\"))
    ):
        raise _invalid()
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.port is not None
        and not 0 < parsed.port <= 65535
    ):
        raise _invalid()
    return value.removesuffix("/")


def _unique_object(pairs):
    fields_by_name = {}
    for key, value in pairs:
        if key in fields_by_name:
            raise _invalid()
        fields_by_name[key] = value
    return fields_by_name


def _reject_number(value):
    raise _invalid()


def _body(session_token_sha256, actor, selection, command):
    if (
        type(session_token_sha256) is not str
        or _SHA256.fullmatch(session_token_sha256) is None
        or type(actor) is not dict
        or set(actor) not in ({"kind", "user_id", "client_id"}, {"kind", "user_id", "client_id", "impersonator_id"})
        or any(
            type(actor_field) is not str or not 0 < len(actor_field.encode("utf-8")) <= 512
            for actor_field in actor.values()
        )
        or (selection is None) == (command is None)
    ):
        raise _invalid()
    request_by_name = {"session_token_sha256": session_token_sha256, "actor": actor}
    if command is not None:
        operation = command.get("operation") if type(command) is dict else None
        if type(operation) is not str or operation not in _COMMAND_VALIDATORS:
            raise _invalid()
        _COMMAND_VALIDATORS[operation](command)
        path, digest_field = _PATHS[operation], "command_sha256"
        request_by_name["command"] = command
    else:
        if type(selection) is not list or not 1 <= len(selection) <= 100:
            raise _invalid()
        for selected_record_by_name in selection:
            if (
                type(selected_record_by_name) is not dict
                or set(selected_record_by_name) != {"record_kind", "record_id", "revision"}
                or type(selected_record_by_name["record_kind"]) is not str
                or not 0 < len(selected_record_by_name["record_kind"]) <= 32
                or type(selected_record_by_name["record_id"]) not in {str, int}
                or type(selected_record_by_name["record_id"]) is str
                and not 0 < len(selected_record_by_name["record_id"].encode("utf-8")) <= 128
                or type(selected_record_by_name["record_id"]) is int
                and not 0 < selected_record_by_name["record_id"] < 2**63
                or type(selected_record_by_name["revision"]) is not int
                or not 0 < selected_record_by_name["revision"] < 2**63
            ):
                raise _invalid()
        path, digest_field = "/internal/v1/registry/publication/authorize", "selection_sha256"
        request_by_name["selection"] = selection
    body = bytearray()
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    for part in encoder.iterencode(request_by_name):
        encoded = part.encode("ascii")
        if len(body) + len(encoded) > _MAX_BYTES:
            raise _invalid()
        body.extend(encoded)
    return path, digest_field, bytes(body)


def _header(reply, name):
    values = reply.headers.get_list(name)
    if not values:
        return None
    if len(values) != 1 or not values[0].isascii() or not 0 < len(values[0]) <= 128:
        raise _invalid()
    return values[0].lower()


async def _receipt(reply, digest_field):
    if reply.status_code in {401, 403}:
        raise PermissionError("registry_publication_authority_denied")
    if reply.status_code in {408, 429} or reply.status_code >= 500:
        raise RuntimeError("registry_publication_authority_unavailable")
    if (
        reply.status_code != 200
        or _header(reply, "content-type") not in {"application/json", "application/json; charset=utf-8"}
        or _header(reply, "cache-control") not in {"no-store", "private, no-store"}
        or _header(reply, "content-encoding") not in {None, "identity"}
        or reply.headers.get_list("set-cookie")
    ):
        raise _invalid()
    declared, transfer = _header(reply, "content-length"), _header(reply, "transfer-encoding")
    if (
        transfer not in {None, "chunked"}
        or declared is not None
        and (
            transfer is not None or not declared.isdecimal() or len(declared) > 5 or not 0 < int(declared) <= _MAX_BYTES
        )
    ):
        raise _invalid()
    body = bytearray()
    async for chunk in reply.aiter_raw(chunk_size=_MAX_BYTES + 1):
        if len(body) + len(chunk) > _MAX_BYTES:
            raise _invalid()
        body.extend(chunk)
    if not body or declared is not None and len(body) != int(declared):
        raise _invalid()
    receipt = json.loads(body.decode("utf-8"), object_pairs_hook=_unique_object, parse_constant=_reject_number)
    if (
        type(receipt) is not dict
        or set(receipt) != {"authorized", "actor", digest_field, "policy_revision"}
        or receipt["authorized"] is not True
        or type(receipt["actor"]) is not dict
        or type(receipt[digest_field]) is not str
        or _SHA256.fullmatch(receipt[digest_field]) is None
        or type(receipt["policy_revision"]) is not int
        or not 0 <= receipt["policy_revision"] < 2**63
    ):
        raise _invalid()
    return receipt


class RegistryPublicationAuthorityClient:
    """Worker-owned pool for a trusted operator origin and existing service bearer.

    Use as an async context manager and pass the instance as ``authorize=client``.
    Every invocation makes one fresh request; no response grants durable authority.
    """

    def __init__(self, base_url, service_token, *, transport=None):
        try:
            origin = _origin(base_url)
            if (
                type(service_token) is not str
                or not 0 < len(service_token) <= 4096
                or re.fullmatch(r"[A-Za-z0-9._~+/-]+=*", service_token) is None
                or transport is not None
                and type(transport) is not httpx.MockTransport
            ):
                raise _invalid()
        except ValueError, TypeError:
            origin = None
        if origin is None:
            raise _invalid()
        self._origin = origin
        self._headers = {"Authorization": "Bearer " + service_token, "Content-Type": "application/json"}
        self._client = httpx.AsyncClient(
            verify=True,
            trust_env=False,
            follow_redirects=False,
            timeout=httpx.Timeout(_TIMEOUT_SECONDS, connect=5),
            transport=transport,
            headers={"Accept": "application/json", "Accept-Encoding": "identity", "Cache-Control": "no-store"},
        )

    async def __aenter__(self):
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *arguments):
        return await self._client.__aexit__(*arguments)

    async def aclose(self):
        """Release the worker-owned HTTP pool after all authority calls drain."""
        await self._client.aclose()

    async def __call__(self, *, session_token_sha256, actor, selection=None, command=None):
        try:
            path, digest_field, body = _body(session_token_sha256, actor, selection, command)
            outbound = self._client.build_request("POST", self._origin + path, content=body, headers=self._headers)
            outbound.headers.pop("cookie", None)
            async with asyncio.timeout(_TIMEOUT_SECONDS):
                reply = await self._client.send(outbound, stream=True)
                try:
                    return await _receipt(reply, digest_field)
                finally:
                    await reply.aclose()
        except PermissionError:
            failure = PermissionError("registry_publication_authority_denied")
        except ValueError, TypeError, UnicodeError, RecursionError:
            failure = _invalid()
        except httpx.HTTPError, OSError, TimeoutError, RuntimeError:
            failure = RuntimeError("registry_publication_authority_unavailable")
        finally:
            self._client.cookies.clear()
        raise failure
