# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Caller-owned registration transactions guarded by one immutable capability."""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
import json
import re
from collections.abc import Mapping
from dataclasses import dataclass

from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from db.models.custom_import import CustomImportRegistrationAuthority
from process.custom_import.definition import CustomImportDefinition, canonical_json
from process.custom_import.definition_store import (
    DefinitionRegistrationConflict,
    _normalized_dataset_key,
    _require_clean_session,
    _require_transaction,
)
from process.custom_import.snowflake_binding import SnowflakeSourceBinding, SnowflakeSourceBindingError
from process.custom_import.snowflake_source_binding import (
    SnowflakeSourceBindingReceipt,
    SnowflakeSourceBindingUnavailableError,
    register_snowflake_source_binding,
)

_TABLE = CustomImportRegistrationAuthority.__table__
_IDENTIFIER = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$", re.ASCII)
_SHA256 = re.compile(r"^[0-9a-f]{64}$", re.ASCII)
_ID_FIELDS = (
    "dataset_id",
    "definition_revision_id",
    "schema_revision_id",
    "source_binding_revision_id",
    "source_binding_revision",
)
_DIGEST_FIELDS = ("definition_sha256", "schema_sha256", "source_binding_sha256")
_RECEIPT_FIELDS = frozenset((*_ID_FIELDS, *_DIGEST_FIELDS, "status"))
MAX_RESULT_RECEIPT_BYTES = 4096
MAX_REGISTRATION_INPUT_BYTES = 1024 * 1024


class RegistrationAuthorityError(ValueError):
    """The authority or registration document is invalid."""


class RegistrationAuthorityConflict(RegistrationAuthorityError):
    """An immutable authority already has different request pins."""


class RegistrationAuthorityDenied(RegistrationAuthorityError):
    """The supplied capability cannot perform unfinished registration."""


@dataclass(frozen=True)
class RegistrationAuthorityState:
    """Exact stored pins and historical result; contains no plaintext capability."""

    authority_id: str
    input_sha256: bytes | None
    token_sha256: bytes | None
    expires_at: dt.datetime | None
    created_at: dt.datetime
    revoked_at: dt.datetime | None
    result_receipt: str | None

    @property
    def result(self) -> dict[str, object] | None:
        """Decode the retained redacted receipt without authorizing new work."""

        return None if self.result_receipt is None else json.loads(self.result_receipt)


@dataclass(frozen=True, repr=False)
class _Registration:
    """Reconstructed registration input and its engine-computed semantic digest."""

    dataset_key: str
    definition: CustomImportDefinition
    binding: SnowflakeSourceBinding
    input_sha256: bytes


def _authority_id(identifier: object) -> str:
    """Accept only a bounded opaque engine-local identity."""

    if not isinstance(identifier, str) or _IDENTIFIER.fullmatch(identifier) is None:
        raise RegistrationAuthorityError("registration authority identifier is invalid")
    return identifier


def _digest(digest: object) -> bytes:
    """Normalize an exact binary SHA256 without accepting integer coercions."""

    if not isinstance(digest, (bytes, bytearray, memoryview)):
        raise RegistrationAuthorityError("registration authority digest is invalid")
    try:
        normalized = bytes(digest)
    except TypeError, ValueError:
        raise RegistrationAuthorityError("registration authority digest is invalid") from None
    if len(normalized) != 32:
        raise RegistrationAuthorityError("registration authority digest is invalid")
    return normalized


def _utc(timestamp: object) -> dt.datetime:
    """Require an aware timestamp and retain its exact UTC instant."""

    if not isinstance(timestamp, dt.datetime) or timestamp.utcoffset() is None:
        raise RegistrationAuthorityError("registration authority timestamp is invalid")
    return timestamp.astimezone(dt.UTC)


def _semantic_input_digest(document: Mapping[str, object]) -> bytes:
    """Hash bounded canonical UTF8 independently of ASCII transport escaping."""

    canonical_input = canonical_json(document).encode("utf-8")
    if not 0 < len(canonical_input) <= MAX_REGISTRATION_INPUT_BYTES:
        raise RegistrationAuthorityError("registration input exceeds its bound")
    return hashlib.sha256(canonical_input).digest()


def _registration(document: object) -> _Registration:
    """Reconstruct the closed envelope and hash semantic UTF8, never wire bytes."""

    try:
        if not isinstance(document, dict) or set(document) != {"dataset_key", "definition", "source_binding"}:
            raise ValueError("invalid registration envelope")
        dataset_key = _normalized_dataset_key(document["dataset_key"])
        definition = CustomImportDefinition.from_mapping(document["definition"])
        binding = SnowflakeSourceBinding.from_mapping(document["source_binding"])
        if (
            canonical_json(document["definition"]) != definition.canonical
            or canonical_json(document["source_binding"]) != binding.canonical
        ):
            raise ValueError("noncanonical registration documents")
        binding.bundle_components(definition)
        input_sha256 = _semantic_input_digest(document)
    except TypeError, ValueError, RecursionError:
        raise RegistrationAuthorityError("registration input is invalid") from None
    return _Registration(dataset_key, definition, binding, input_sha256)


def _receipt_text(receipt_by_name: object) -> str:
    """Validate the existing closed numeric-ID/digest registration receipt shape."""

    if not isinstance(receipt_by_name, dict) or set(receipt_by_name) != _RECEIPT_FIELDS:
        raise RegistrationAuthorityError("registration receipt is invalid")
    if (
        any(type(receipt_by_name[name]) is not int or not 0 < receipt_by_name[name] < 2**63 for name in _ID_FIELDS)
        or any(
            not isinstance(receipt_by_name[name], str) or _SHA256.fullmatch(receipt_by_name[name]) is None
            for name in _DIGEST_FIELDS
        )
        or receipt_by_name["status"] not in ("registered", "replayed")
    ):
        raise RegistrationAuthorityError("registration receipt is invalid")
    receipt_text = canonical_json(receipt_by_name)
    if not 0 < len(receipt_text.encode("utf-8")) <= MAX_RESULT_RECEIPT_BYTES:
        raise RegistrationAuthorityError("registration receipt is invalid")
    return receipt_text


def _state(stored_by_name: Mapping[str, object]) -> RegistrationAuthorityState:
    """Validate stored evidence before exposing any pins or historical result."""

    input_sha256 = stored_by_name["input_sha256"]
    token_sha256 = stored_by_name["token_sha256"]
    expiry = stored_by_name["expires_at"]
    revoked = stored_by_name["revoked_at"]
    receipt_text = stored_by_name["result_receipt"]
    pins = (input_sha256, token_sha256, expiry)
    if all(pin is None for pin in pins):
        if revoked is None or receipt_text is not None:
            raise RegistrationAuthorityError("registration authority tombstone is invalid")
    elif any(pin is None for pin in pins):
        raise RegistrationAuthorityError("registration authority pins are incomplete")
    if receipt_text is not None:
        try:
            if not isinstance(receipt_text, str) or _receipt_text(json.loads(receipt_text)) != receipt_text:
                raise ValueError("invalid receipt")
        except ValueError, TypeError, RecursionError:
            raise RegistrationAuthorityError("stored registration receipt is invalid") from None
    return RegistrationAuthorityState(
        authority_id=_authority_id(stored_by_name["authority_id"]),
        input_sha256=None if input_sha256 is None else _digest(input_sha256),
        token_sha256=None if token_sha256 is None else _digest(token_sha256),
        expires_at=None if expiry is None else _utc(expiry),
        created_at=_utc(stored_by_name["created_at"]),
        revoked_at=None if revoked is None else _utc(revoked),
        result_receipt=receipt_text,
    )


async def _read_authority(session: AsyncSession, authority_id: str, *, lock: bool) -> RegistrationAuthorityState | None:
    """Read Core columns so authority writes never dirty the ORM identity map."""

    statement = select(_TABLE).where(_TABLE.c.authority_id == authority_id).execution_options(autoflush=False)
    if lock:
        statement = statement.with_for_update()
    stored_by_name = (await session.execute(statement)).mappings().one_or_none()
    return None if stored_by_name is None else _state(stored_by_name)


async def get_registration_authority(session: AsyncSession, authority_id: str) -> RegistrationAuthorityState | None:
    """Observe current pins/history only; the caller controls the read transaction."""

    return await _read_authority(session, _authority_id(authority_id), lock=False)


async def mint_registration_authority(
    session: AsyncSession,
    *,
    authority_id: str,
    registration: object,
    expires_at: dt.datetime,
    token_sha256: bytes,
) -> RegistrationAuthorityState:
    """Insert immutable pins or return their exact replay in a caller-owned transaction."""

    identifier = _authority_id(authority_id)
    prepared = _registration(registration)
    expiry = _utc(expires_at)
    token_digest = _digest(token_sha256)
    _require_transaction(session)
    _require_clean_session(session)
    await session.execute(
        insert(_TABLE)
        .values(
            authority_id=identifier, input_sha256=prepared.input_sha256, token_sha256=token_digest, expires_at=expiry
        )
        .on_conflict_do_nothing(index_elements=(_TABLE.c.authority_id,))
    )
    state = await _read_authority(session, identifier, lock=True)
    if (
        state is None
        or state.revoked_at is not None
        or state.input_sha256 != prepared.input_sha256
        or state.token_sha256 != token_digest
        or state.expires_at != expiry
    ):
        raise RegistrationAuthorityConflict("registration authority already has different or revoked pins")
    return state


async def revoke_registration_authority(session: AsyncSession, authority_id: str) -> RegistrationAuthorityState:
    """Persist an absent-first tombstone or irreversible revocation; never erase history."""

    identifier = _authority_id(authority_id)
    _require_transaction(session)
    _require_clean_session(session)
    await session.execute(
        insert(_TABLE)
        .values(authority_id=identifier, revoked_at=func.clock_timestamp())
        .on_conflict_do_nothing(index_elements=(_TABLE.c.authority_id,))
    )
    state = await _read_authority(session, identifier, lock=True)
    if state is None:
        raise RegistrationAuthorityError("registration authority is unavailable")
    if state.revoked_at is None:
        stored_by_name = (
            (
                await session.execute(
                    update(_TABLE)
                    .where(_TABLE.c.authority_id == identifier, _TABLE.c.revoked_at.is_(None))
                    .values(revoked_at=func.clock_timestamp())
                    .returning(*_TABLE.c)
                )
            )
            .mappings()
            .one()
        )
        state = _state(stored_by_name)
    return state


def _graph_receipt(registration: SnowflakeSourceBindingReceipt, prepared: _Registration) -> str:
    """Retain the existing redacted receipt, never the registration document."""

    if not isinstance(registration, SnowflakeSourceBindingReceipt) or type(registration.created) is not bool:
        raise RegistrationAuthorityError("registration receipt is invalid")
    receipt_by_name = {
        "dataset_id": registration.dataset_id,
        "definition_revision_id": registration.definition_revision_id,
        "schema_revision_id": registration.schema_revision_id,
        "source_binding_revision_id": registration.source_binding_revision_id,
        "source_binding_revision": registration.revision_number,
        "definition_sha256": prepared.definition.digest,
        "schema_sha256": prepared.definition.schema_digest,
        "source_binding_sha256": prepared.binding.digest,
        "status": "registered" if registration.created else "replayed",
    }
    if not isinstance(registration.source_binding_sha256, bytes) or registration.source_binding_sha256 != bytes.fromhex(
        prepared.binding.digest
    ):
        raise RegistrationAuthorityError("registration receipt binding is invalid")
    return _receipt_text(receipt_by_name)


async def _register_graph(session: AsyncSession, prepared: _Registration) -> SnowflakeSourceBindingReceipt:
    """Translate deterministic graph rejection without masking unavailable state."""

    try:
        return await register_snowflake_source_binding(
            session, dataset_key=prepared.dataset_key, definition=prepared.definition, binding=prepared.binding
        )
    except DefinitionRegistrationConflict as exc:
        raise RegistrationAuthorityConflict("registration graph identity conflicts") from exc
    except SnowflakeSourceBindingUnavailableError:
        raise
    except SnowflakeSourceBindingError as exc:
        raise RegistrationAuthorityError("registration graph input is invalid") from exc


async def register_with_authority(
    session: AsyncSession, *, authority_id: str, registration: object, token: bytes
) -> RegistrationAuthorityState:
    """Lock authority first, then atomically fence graph/result inside the caller transaction.

    Correct matching retries observe completed history even after expiry/revocation.
    A graph savepoint prevents partial writes if the caller catches a denied attempt.
    The caller must commit and read the retained result back before acknowledging it.
    """

    identifier = _authority_id(authority_id)
    prepared = _registration(registration)
    capability = _digest(token)
    _require_transaction(session)
    _require_clean_session(session)
    state = await _read_authority(session, identifier, lock=True)
    if (
        state is None
        or state.token_sha256 is None
        or not hmac.compare_digest(state.token_sha256, hashlib.sha256(capability).digest())
    ):
        raise RegistrationAuthorityDenied("registration authority is unavailable")
    if state.input_sha256 is None or not hmac.compare_digest(state.input_sha256, prepared.input_sha256):
        raise RegistrationAuthorityConflict("registration input differs from immutable authority")
    if state.result_receipt is not None:
        return state
    now = await session.scalar(select(func.clock_timestamp()))
    if state.revoked_at is not None or state.expires_at is None or state.expires_at <= _utc(now):
        raise RegistrationAuthorityDenied("registration authority is unavailable")
    async with session.begin_nested():
        graph_registration = await _register_graph(session, prepared)
        await session.flush()
        receipt_text = _graph_receipt(graph_registration, prepared)
        stored_by_name = (
            (
                await session.execute(
                    update(_TABLE)
                    .where(
                        _TABLE.c.authority_id == identifier,
                        _TABLE.c.input_sha256 == prepared.input_sha256,
                        _TABLE.c.token_sha256 == state.token_sha256,
                        _TABLE.c.expires_at == state.expires_at,
                        _TABLE.c.revoked_at.is_(None),
                        _TABLE.c.result_receipt.is_(None),
                        _TABLE.c.expires_at > func.clock_timestamp(),
                    )
                    .values(result_receipt=receipt_text)
                    .returning(*_TABLE.c)
                )
            )
            .mappings()
            .one_or_none()
        )
        if stored_by_name is None:
            raise RegistrationAuthorityDenied("registration authority is unavailable")
        return _state(stored_by_name)
