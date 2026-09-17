# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unit contracts for generic custom-import read authorization and cursors."""

from __future__ import annotations

import pytest

from process.custom_import import read_core
from process.custom_import.read_core import (
    MAX_PAGE_OFFSET,
    MAX_READ_TIMEOUT_MS,
    CustomImportReadAuthorizationError,
    CustomImportReadCursorError,
    CustomImportReadRequestError,
    CustomImportReadService,
    ExtensionReadAuthorization,
    PinnedReadTarget,
    ReadCursorCodec,
    ReadCursorState,
    SearchRequest,
    WinnerLocator,
)


def _target(generation_id: int = 101) -> PinnedReadTarget:
    return PinnedReadTarget(
        dataset_id=11,
        generation_id=generation_id,
        definition_revision_id=21,
        schema_revision_id=31,
        profile_id="synthetic_profile",
    )


def _cursor_state(*, expires_at: int = 1_100) -> ReadCursorState:
    return ReadCursorState(
        target=_target(),
        query_fingerprint="a" * 64,
        authorization_scope_sha256="b" * 64,
        offset=25,
        issued_at=1_000,
        expires_at=expires_at,
    )


def _codec() -> ReadCursorCodec:
    return ReadCursorCodec(b"c" * 32)


@pytest.mark.parametrize("statement_timeout_ms", (False, 0, MAX_READ_TIMEOUT_MS + 1))
def test_read_service_rejects_an_invalid_statement_timeout(statement_timeout_ms):
    with pytest.raises(CustomImportReadRequestError, match="statement_timeout_ms"):
        CustomImportReadService(
            authorizer=None,
            cursor_secret=b"s" * 32,
            statement_timeout_ms=statement_timeout_ms,
        )


def test_next_cursor_stops_before_exceeding_the_offset_limit():
    service = CustomImportReadService(authorizer=None, cursor_secret=b"s" * 32)
    context = read_core._ReadContext(
        target=_target(),
        pointer_version=1,
        definition=None,
        profile_slot=1,
        profile_context_slot=0,
        collection_slots_by_name={},
        collection_names_by_slot={},
    )
    plan = read_core._SearchPlan(filters=(), order_terms=(), page_size=100, fingerprint="a" * 64)

    allowed_cursor = service._next_search_cursor(
        context,
        plan,
        "b" * 64,
        read_core._PageWindow(
            offset=MAX_PAGE_OFFSET - 100,
            total=MAX_PAGE_OFFSET + 100,
            returned_count=100,
            issued_at=1_000,
            expires_at=1_100,
        ),
    )
    assert allowed_cursor is not None
    assert (
        service._cursor_codec.open(
            allowed_cursor,
            pinned_target=_target(),
            query_fingerprint="a" * 64,
            authorization_scope_sha256="b" * 64,
            trusted_now=1_050,
        ).offset
        == MAX_PAGE_OFFSET
    )

    for offset, returned_count in ((MAX_PAGE_OFFSET, 1), (MAX_PAGE_OFFSET - 25, 50)):
        assert (
            service._next_search_cursor(
                context,
                plan,
                "b" * 64,
                read_core._PageWindow(
                    offset=offset,
                    total=MAX_PAGE_OFFSET + 100,
                    returned_count=returned_count,
                    issued_at=1_000,
                    expires_at=1_100,
                ),
            )
            is None
        )


def test_string_filter_rejects_invalid_unicode_without_exposing_the_value():
    with pytest.raises(
        CustomImportReadRequestError, match="^filter value for synthetic_field is not an indexed string$"
    ):
        read_core._normalized_string("\ud800", "synthetic_field")


def test_cursor_rejects_tampering_scope_generation_and_expiry():
    cursor = _codec().issue(_cursor_state())

    opened = _codec().open(
        cursor,
        pinned_target=_target(),
        query_fingerprint="a" * 64,
        authorization_scope_sha256="b" * 64,
        trusted_now=1_050,
    )
    assert opened.offset == 25

    tampered = cursor[:-1] + ("0" if cursor[-1] != "0" else "1")
    with pytest.raises(CustomImportReadCursorError):
        _codec().open(
            tampered,
            pinned_target=_target(),
            query_fingerprint="a" * 64,
            authorization_scope_sha256="b" * 64,
            trusted_now=1_050,
        )
    with pytest.raises(CustomImportReadCursorError):
        _codec().open(
            cursor,
            pinned_target=_target(),
            query_fingerprint="a" * 64,
            authorization_scope_sha256="d" * 64,
            trusted_now=1_050,
        )
    with pytest.raises(CustomImportReadCursorError):
        _codec().open(
            cursor,
            pinned_target=_target(102),
            query_fingerprint="a" * 64,
            authorization_scope_sha256="b" * 64,
            trusted_now=1_050,
        )
    with pytest.raises(CustomImportReadCursorError):
        _codec().open(
            cursor,
            pinned_target=_target(),
            query_fingerprint="a" * 64,
            authorization_scope_sha256="b" * 64,
            trusted_now=1_100,
        )


def test_cursor_rejects_non_base64url_payload_without_an_encoding_error():
    cursor = _codec().issue(_cursor_state())
    prefix, _payload, signature = cursor.split(".")

    with pytest.raises(CustomImportReadCursorError):
        _codec().open(
            f"{prefix}.payloadé.{signature}",
            pinned_target=_target(),
            query_fingerprint="a" * 64,
            authorization_scope_sha256="b" * 64,
            trusted_now=1_050,
        )


class _RejectingAuthorizer:
    def __init__(self) -> None:
        self.calls = 0

    def authorize(self, authorization, *, target):
        del authorization, target
        self.calls += 1
        return None


class _CacheSpy:
    def __init__(self) -> None:
        self.get_calls = 0
        self.set_calls = 0

    async def get(self, key: str):
        del key
        self.get_calls += 1
        return None

    async def set(self, key: str, value: object, *, expires_at: int) -> None:
        del key, value, expires_at
        self.set_calls += 1


@pytest.mark.asyncio
async def test_invalid_extension_authorization_cannot_touch_read_caches():
    authorizer = _RejectingAuthorizer()
    cache = _CacheSpy()
    service = CustomImportReadService(
        authorizer=authorizer,
        cache=cache,
        cursor_secret=b"s" * 32,
        now=lambda: 1_000,
    )

    with pytest.raises(CustomImportReadAuthorizationError):
        await service.search(
            object(),
            authorization=ExtensionReadAuthorization("denied"),
            request=SearchRequest(target=_target()),
        )

    with pytest.raises(CustomImportReadAuthorizationError):
        await service.root_detail(
            object(),
            authorization=ExtensionReadAuthorization("denied"),
            target=_target(),
            winner=WinnerLocator(
                root_record_id=1,
                family_revision_id=2,
                entity_binding_id=3,
                context_key_sha256=b"w" * 32,
            ),
        )

    assert authorizer.calls == 2
    assert cache.get_calls == 0
    assert cache.set_calls == 0
