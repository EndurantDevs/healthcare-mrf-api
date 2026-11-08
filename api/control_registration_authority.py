# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Bounded control and capability routes for immutable registration authority."""

from __future__ import annotations

import asyncio
import base64
import binascii
import datetime as dt
import hmac
import json
import os
import re

from sanic import Blueprint, response
from sanic.exceptions import Forbidden

from api.control_auth import require_control_auth
from process.custom_import.registration_authority import (
    MAX_REGISTRATION_INPUT_BYTES,
    RegistrationAuthorityConflict,
    RegistrationAuthorityDenied,
    RegistrationAuthorityError,
    get_registration_authority,
    mint_registration_authority,
    register_with_authority,
    revoke_registration_authority,
)

blueprint = Blueprint("control_registration_authority", url_prefix="/control/v1")

_PATH = "/custom-import/registration-authorities/<authority_id>"
_NO_STORE = {"Cache-Control": "no-store"}
_TOKEN = re.compile(r"^[A-Za-z0-9_-]{43}$", re.ASCII)
_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_MAX_BODY_BYTES = MAX_REGISTRATION_INPUT_BYTES + 256
_TIMEOUT_SECONDS = 8


class _InvalidRequest(ValueError):
    pass


def _reply(document: dict[str, object], status: int = 200):
    return response.json(document, status=status, headers=_NO_STORE)


def _error(code: str, status: int):
    return _reply({"error": code}, status)


def _object(pairs: list[tuple[str, object]]) -> dict[str, object]:
    document_by_name = dict(pairs)
    if len(document_by_name) != len(pairs):
        raise _InvalidRequest("duplicate JSON key")
    return document_by_name


def _body(request, fields: set[str], *, empty: bool = False) -> dict[str, object]:
    body = getattr(request, "body", None)
    if (
        type(body) is not bytes
        or not 1 <= len(body) <= (64 if empty else _MAX_BODY_BYTES)
        or getattr(request, "query_string", "")
    ):
        raise _InvalidRequest("invalid body")
    try:
        document = json.loads(body, object_pairs_hook=_object)
    except ValueError, UnicodeError, RecursionError:
        raise _InvalidRequest("invalid body") from None
    if type(document) is not dict or set(document) != fields:
        raise _InvalidRequest("invalid fields")
    return document


def _no_body(request) -> None:
    if getattr(request, "body", b"") not in (b"", None) or getattr(request, "query_string", ""):
        raise _InvalidRequest("invalid request")


def _expires_at(value: object) -> dt.datetime:
    if type(value) is not str or len(value) > 40:
        raise _InvalidRequest("invalid expiry")
    try:
        parsed = dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        if parsed.utcoffset() is None:
            raise ValueError("naive expiry")
        return parsed.astimezone(dt.UTC)
    except ValueError, OverflowError:
        raise _InvalidRequest("invalid expiry") from None


def _token_hash(value: object) -> bytes:
    if type(value) is not str or _SHA256.fullmatch(value) is None:
        raise _InvalidRequest("invalid token hash")
    return bytes.fromhex(value)


def _capability(request) -> bytes | None:
    headers = getattr(request, "headers", {}) or {}
    if headers.get("X-HealthPorta-Control-Token") is not None:
        return None
    authorization = headers.get("Authorization", "")
    if type(authorization) is not str or not authorization.startswith("Bearer "):
        return None
    encoded = authorization.removeprefix("Bearer ")
    control_token = (os.getenv("HLTHPRT_CONTROL_API_TOKEN") or "").strip()
    if not control_token.isascii():
        return None
    if _TOKEN.fullmatch(encoded) is None or control_token and hmac.compare_digest(encoded, control_token):
        return None
    try:
        token = base64.urlsafe_b64decode(encoded + "=")
    except ValueError, binascii.Error:
        return None
    if len(token) != 32 or base64.urlsafe_b64encode(token).decode("ascii").rstrip("=") != encoded:
        return None
    return token


def _timestamp(value: dt.datetime | None) -> str | None:
    return None if value is None else value.astimezone(dt.UTC).isoformat().replace("+00:00", "Z")


def _state_document(state) -> dict[str, object]:
    return {
        "authority_id": state.authority_id,
        "input_sha256": None if state.input_sha256 is None else state.input_sha256.hex(),
        "token_sha256": None if state.token_sha256 is None else state.token_sha256.hex(),
        "expires_at": _timestamp(state.expires_at),
        "created_at": _timestamp(state.created_at),
        "revoked_at": _timestamp(state.revoked_at),
        "result": state.result,
    }


def _session(request):
    session = getattr(getattr(request, "ctx", None), "sa_session", None)
    if session is None or session.in_transaction() or session.new or session.dirty or session.deleted:
        raise RuntimeError("registration session is unavailable")
    return session


async def _committed_state(session, authority_id: str):
    async with session.begin():
        state = await get_registration_authority(session, authority_id)
    if state is None:
        raise RuntimeError("registration authority was not committed")
    return state


def _is_broad_authorized(request) -> bool:
    try:
        require_control_auth(request)
        return True
    except Forbidden, TypeError:
        return False


async def serve_mint(request, authority_id: str):
    """Mint immutable authority pins and acknowledge their committed readback."""

    if not _is_broad_authorized(request):
        return _error("forbidden", 403)
    try:
        document = _body(request, {"registration", "expires_at", "token_sha256"})
        expiry = _expires_at(document["expires_at"])
        token_hash = _token_hash(document["token_sha256"])
        session = _session(request)
        async with asyncio.timeout(_TIMEOUT_SECONDS):
            async with session.begin():
                state = await mint_registration_authority(
                    session,
                    authority_id=authority_id,
                    registration=document["registration"],
                    expires_at=expiry,
                    token_sha256=token_hash,
                )
            committed = await _committed_state(session, authority_id)
        if committed != state:
            raise RuntimeError("registration authority readback differs")
        return _reply(_state_document(committed))
    except RegistrationAuthorityConflict:
        return _error("identity_conflict", 409)
    except _InvalidRequest, RegistrationAuthorityError:
        return _error("invalid_request", 400)
    except Exception:
        return _error("registration_unavailable", 503)


async def serve_revoke(request, authority_id: str):
    """Commit a revocation tombstone before returning retained authority state."""

    if not _is_broad_authorized(request):
        return _error("forbidden", 403)
    try:
        _body(request, set(), empty=True)
        session = _session(request)
        async with asyncio.timeout(_TIMEOUT_SECONDS):
            async with session.begin():
                state = await revoke_registration_authority(session, authority_id)
            committed = await _committed_state(session, authority_id)
        if committed != state:
            raise RuntimeError("registration authority readback differs")
        return _reply(_state_document(committed))
    except _InvalidRequest, RegistrationAuthorityError:
        return _error("invalid_request", 400)
    except Exception:
        return _error("registration_unavailable", 503)


async def serve_get(request, authority_id: str):
    """Read authority state only for broad control authentication."""

    if not _is_broad_authorized(request):
        return _error("forbidden", 403)
    try:
        _no_body(request)
        session = _session(request)
        async with asyncio.timeout(_TIMEOUT_SECONDS), session.begin():
            state = await get_registration_authority(session, authority_id)
        return _error("not_found", 404) if state is None else _reply(_state_document(state))
    except _InvalidRequest, RegistrationAuthorityError:
        return _error("invalid_request", 400)
    except Exception:
        return _error("registration_unavailable", 503)


async def serve_register(request, authority_id: str):
    """Register with a scoped capability and return only committed results."""

    token = _capability(request)
    if token is None:
        return _error("forbidden", 403)
    try:
        registration = _body(request, {"dataset_key", "definition", "source_binding"})
        session = _session(request)
        async with asyncio.timeout(_TIMEOUT_SECONDS):
            async with session.begin():
                state = await register_with_authority(
                    session, authority_id=authority_id, registration=registration, token=token
                )
            committed = await _committed_state(session, authority_id)
        if committed != state or committed.result is None:
            raise RuntimeError("registration result was not committed")
        return _reply(committed.result)
    except RegistrationAuthorityConflict:
        return _error("identity_conflict", 409)
    except RegistrationAuthorityDenied:
        return _error("forbidden", 403)
    except _InvalidRequest, RegistrationAuthorityError:
        return _error("invalid_request", 400)
    except Exception:
        return _error("registration_unavailable", 503)


@blueprint.put(_PATH)
async def mint_registration_authority_route(request, authority_id: str):
    """Expose the broad-auth authority mint operation."""

    return await serve_mint(request, authority_id)


@blueprint.post(_PATH + "/revoke")
async def revoke_registration_authority_route(request, authority_id: str):
    """Expose the broad-auth authority revocation operation."""

    return await serve_revoke(request, authority_id)


@blueprint.get(_PATH, ignore_body=False)
async def get_registration_authority_route(request, authority_id: str):
    """Expose the broad-auth authority read operation."""

    return await serve_get(request, authority_id)


@blueprint.post(_PATH + "/register")
async def register_with_authority_route(request, authority_id: str):
    """Expose the scoped-capability registration operation."""

    return await serve_register(request, authority_id)
