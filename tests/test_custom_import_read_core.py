# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Unit contracts for generic custom-import read authorization and cursors."""

from __future__ import annotations

import datetime as dt
import hashlib
import hmac
from contextlib import asynccontextmanager
from dataclasses import replace
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import column
from sqlalchemy.exc import DBAPIError
from sqlalchemy.sql import operators

from process.custom_import import publication, read_core, read_cursor, read_identity
from process.custom_import.definition import CustomImportDefinition, Field, load_json_definition
from process.custom_import.read_core import (
    MAX_PAGE_OFFSET,
    MAX_READ_TIMEOUT_MS,
    CustomImportReadAuthorizationError,
    CustomImportReadCursorError,
    CustomImportReadEntityAbsentError,
    CustomImportReadRequestError,
    CustomImportReadService,
    CustomImportReadUnavailableError,
    EntityLocator,
    ExtensionReadAuthorization,
    PinnedReadTarget,
    ReadCursorCodec,
    ReadCursorState,
    RootDetailRequest,
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


def test_temporal_filters_accept_only_canonical_json_values():
    assert read_core._normalized_date("2031-01-02", "synthetic_field") == (
        dt.date(2031, 1, 2),
        "2031-01-02",
    )
    assert read_core._normalized_timestamp("2031-01-02T03:04:05Z", "synthetic_field") == (
        dt.datetime(2031, 1, 2, 3, 4, 5, tzinfo=dt.UTC),
        "2031-01-02T03:04:05Z",
    )
    for value in ("2031-1-2", "2031-01-02T00:00:00Z"):
        with pytest.raises(CustomImportReadRequestError):
            read_core._normalized_date(value, "synthetic_field")
    for value in ("2031-01-02T03:04:05+00:00", "2031-01-02T03:04:05.1Z"):
        with pytest.raises(CustomImportReadRequestError):
            read_core._normalized_timestamp(value, "synthetic_field")


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


def _field(value_type: str) -> Field:
    return Field("synthetic_field", 1, value_type, True, 1, None)


@pytest.mark.parametrize(
    ("value_type", "value", "expected", "canonical"),
    (
        ("string", "é" * 1_024, "é" * 1_024, "é" * 1_024),
        ("integer", -(2**63), -(2**63), -(2**63)),
        ("integer", 2**63 - 1, 2**63 - 1, 2**63 - 1),
        ("decimal", "-0.000", Decimal(0), "0"),
        ("decimal", "0.000000000001", Decimal("0.000000000001"), "0.000000000001"),
        ("decimal", "999999999999999999", Decimal("999999999999999999"), "999999999999999999"),
        ("decimal", Decimal("12.30"), Decimal("12.30"), "12.30"),
        ("boolean", True, True, True),
        ("boolean", False, False, False),
        ("date", dt.date(2026, 1, 2), dt.date(2026, 1, 2), "2026-01-02"),
        (
            "timestamp",
            dt.datetime(2026, 1, 2, 3, tzinfo=dt.timezone(dt.timedelta(hours=3))),
            dt.datetime(2026, 1, 2, tzinfo=dt.UTC),
            "2026-01-02T00:00:00Z",
        ),
    ),
)
def test_filter_values_preserve_storage_types_and_canonical_cursor_values(value_type, value, expected, canonical):
    actual, encoded = read_core._normalized_filter_value(_field(value_type), "eq", value)
    assert actual == expected
    assert type(actual) is type(expected)
    assert encoded == canonical


@pytest.mark.parametrize(
    ("value_type", "value"),
    (
        ("string", 1),
        ("string", "secret\x00value"),
        ("string", "é" * 1_025),
        ("integer", True),
        ("integer", -(2**63) - 1),
        ("integer", 2**63),
        ("decimal", True),
        ("decimal", 1.2),
        ("decimal", "secret-not-decimal"),
        ("decimal", "NaN"),
        ("decimal", "Infinity"),
        ("decimal", "0.0000000000001"),
        ("decimal", "1000000000000000000"),
        ("boolean", 1),
        ("date", dt.datetime(2026, 1, 2)),
        ("timestamp", dt.date(2026, 1, 2)),
        ("timestamp", dt.datetime(2026, 1, 2)),
    ),
)
def test_filter_values_reject_lossy_or_out_of_storage_values(value_type, value):
    with pytest.raises(CustomImportReadRequestError, match="filter value for synthetic_field") as error:
        read_core._normalized_filter_value(_field(value_type), "eq", value)
    assert "secret" not in str(error.value)


@pytest.mark.parametrize("operator", ("is_null", "is_missing"))
def test_state_predicates_accept_only_absent_comparison_values(operator):
    assert read_core._normalized_filter_value(_field("string"), operator, None) == (None, None)
    with pytest.raises(CustomImportReadRequestError, match="cannot carry"):
        read_core._normalized_filter_value(_field("string"), operator, "secret")


@pytest.mark.parametrize("operator", ("gt", "gte", "lt", "lte"))
def test_range_predicates_require_metric_types(operator):
    with pytest.raises(CustomImportReadRequestError, match="metric-compatible"):
        read_core._normalized_filter_value(_field("string"), operator, "secret")
    assert read_core._normalized_filter_value(_field("integer"), operator, 7) == (7, 7)


def test_comparisons_require_values_and_supported_scalar_types():
    with pytest.raises(CustomImportReadRequestError, match="typed value"):
        read_core._normalized_filter_value(_field("integer"), "eq", None)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="unsupported scalar"):
        read_core._normalized_filter_value(_field("unknown"), "eq", "secret")


@pytest.mark.parametrize(
    ("operator", "expected"),
    (
        ("eq", operators.eq),
        ("neq", operators.ne),
        ("gt", operators.gt),
        ("gte", operators.ge),
        ("lt", operators.lt),
        ("lte", operators.le),
    ),
)
def test_scalar_comparisons_keep_the_requested_operator_and_bind_the_value(operator, expected):
    has_comparison_clause = read_core._has_scalar_comparison(column("synthetic_value"), operator, 7)
    assert has_comparison_clause.operator is expected
    assert has_comparison_clause.left.name == "synthetic_value"
    assert has_comparison_clause.right.value == 7


def test_scalar_comparison_rejects_an_unrecognized_operator():
    with pytest.raises(CustomImportReadRequestError, match="unsupported"):
        read_core._has_scalar_comparison(column("synthetic_value"), "contains", "secret")


def _open_cursor(token, **changes):
    arguments_by_name = {
        "pinned_target": _target(),
        "query_fingerprint": "a" * 64,
        "authorization_scope_sha256": "b" * 64,
        "trusted_now": 1_050,
    }
    return _codec().open(token, **(arguments_by_name | changes))


def _signed_payload(payload: bytes) -> str:
    encoded = read_cursor._base64url_encode(payload)
    signature = hmac.new(b"c" * 32, read_cursor._CURSOR_AAD + encoded.encode("ascii"), hashlib.sha256).hexdigest()
    return f"cir1.{encoded}.{signature}"


@pytest.mark.parametrize("secret", (None, "c" * 32, b"c" * 31, b"c" * 129))
def test_cursor_secret_requires_bounded_key_bytes(secret):
    with pytest.raises(CustomImportReadRequestError, match="secret"):
        ReadCursorCodec(secret)


@pytest.mark.parametrize("token", (None, "", "x" * 2_049, "one.two", "wrong.payload." + "a" * 64))
def test_cursor_rejects_malformed_envelopes(token):
    with pytest.raises(CustomImportReadCursorError):
        _open_cursor(token)


@pytest.mark.parametrize(
    "changes",
    (
        {"pinned_target": None},
        {"query_fingerprint": "x"},
        {"authorization_scope_sha256": "x"},
        {"trusted_now": True},
        {"trusted_now": -1},
        {"trusted_now": 2**63},
    ),
)
def test_cursor_rejects_invalid_verification_context_shape(changes):
    with pytest.raises(CustomImportReadCursorError):
        _open_cursor(_codec().issue(_cursor_state()), **changes)


@pytest.mark.parametrize(
    "changes",
    (
        {"query_fingerprint": "x"},
        {"authorization_scope_sha256": "x"},
        {"offset": True},
        {"offset": -1},
        {"offset": MAX_PAGE_OFFSET + 1},
        {"issued_at": True},
        {"expires_at": True},
        {"issued_at": -1},
        {"expires_at": 1_000},
        {"expires_at": 1_901},
        {"expires_at": 2**63},
    ),
)
def test_cursor_issue_rejects_invalid_state(changes):
    with pytest.raises(CustomImportReadCursorError):
        _codec().issue(replace(_cursor_state(), **changes))


@pytest.mark.parametrize("payload", (b"not json", b"\xff", b"[]", b"{}"))
def test_authenticated_cursor_still_requires_a_valid_document(payload):
    with pytest.raises(CustomImportReadCursorError):
        _open_cursor(_signed_payload(payload))


def test_authenticated_cursor_rejects_duplicate_document_keys():
    payload = read_cursor.canonical_read_document(read_cursor._cursor_payload(_cursor_state()))
    payload = payload.replace(b"{", b'{"auth":"' + b"b" * 64 + b'",', 1)
    with pytest.raises(CustomImportReadCursorError):
        _open_cursor(_signed_payload(payload))


@pytest.mark.parametrize("changes", ({"contract": "other"}, {"dataset": 0}, {"query": "x"}, {"offset": -1}))
def test_authenticated_cursor_revalidates_the_decoded_contract(changes):
    document = read_cursor._cursor_payload(_cursor_state()) | changes
    with pytest.raises(CustomImportReadCursorError):
        _open_cursor(_signed_payload(read_cursor.canonical_read_document(document)))


@pytest.mark.parametrize("part", (None, "!", "A", "AB"))
def test_cursor_base64_requires_a_canonical_complete_encoding(part):
    with pytest.raises(CustomImportReadCursorError):
        read_cursor._base64url_decode(part)


def test_cursor_requires_state_type_query_identity_and_bounded_output(monkeypatch):
    with pytest.raises(CustomImportReadCursorError):
        _codec().issue(None)
    with pytest.raises(CustomImportReadCursorError):
        _open_cursor(_codec().issue(_cursor_state()), query_fingerprint="d" * 64)
    monkeypatch.setattr(read_cursor, "MAX_CURSOR_CHARACTERS", 10)
    with pytest.raises(CustomImportReadCursorError):
        _codec().issue(_cursor_state())


@pytest.mark.parametrize(
    "changes",
    (
        {"dataset_id": True},
        {"generation_id": 0},
        {"schema_revision_id": 2**63},
        {"definition_revision_id": -1},
        {"profile_id": "Invalid"},
    ),
)
def test_pinned_targets_reject_nonidentity_values(changes):
    with pytest.raises(CustomImportReadRequestError):
        replace(_target(), **changes)


@pytest.mark.parametrize("value", (None, "", "private value", "x" * 513))
def test_authorization_credentials_have_a_bounded_opaque_shape(value):
    with pytest.raises(CustomImportReadRequestError):
        ExtensionReadAuthorization(value)


def test_authorization_representations_do_not_expose_credentials_or_scopes():
    assert repr(ExtensionReadAuthorization("synthetic-secret")) == "<extension-read-authorization>"
    assert repr(read_core.ExtensionReadScope("synthetic:scope")) == "<extension-read-scope>"
    with pytest.raises(CustomImportReadAuthorizationError):
        read_core.ExtensionReadScope("invalid scope")


@pytest.mark.parametrize(
    "changes",
    (
        {"target": None},
        {"filters": []},
        {"page_size": True},
        {"page_size": 0},
        {"page_size": 101},
        {"order_terms": []},
        {"cursor": 1},
        {"cursor": "x" * 2_049},
    ),
)
def test_search_request_bounds_precede_database_work(changes):
    with pytest.raises(CustomImportReadRequestError):
        SearchRequest(**({"target": _target()} | changes))


def test_filter_order_and_winner_contracts_reject_invalid_shapes():
    with pytest.raises(CustomImportReadRequestError):
        read_core.ReadFilter("synthetic_field", "contains")
    for direction, nulls in (("sideways", "first"), ("asc", "middle")):
        with pytest.raises(CustomImportReadRequestError):
            read_core.ReadOrderTerm("synthetic_field", direction, nulls)
    with pytest.raises(CustomImportReadRequestError):
        WinnerLocator(1, 2, 3, b"short")
    with pytest.raises(CustomImportReadRequestError):
        EntityLocator("invalid-adapter", "synthetic")
    with pytest.raises(CustomImportReadRequestError):
        RootDetailRequest(_target(), object(), "full_family")


@pytest.mark.parametrize("family_entitlement", (None, False, "root_fields"))
def test_root_detail_request_requires_the_full_family_entitlement(family_entitlement):
    with pytest.raises(CustomImportReadRequestError):
        RootDetailRequest(_target(), EntityLocator("synthetic", "value"), family_entitlement)


def test_entity_locator_has_a_utf8_byte_bound():
    EntityLocator("synthetic", "😀" * 128)
    with pytest.raises(CustomImportReadRequestError):
        EntityLocator("synthetic", "😀" * 129)


@pytest.mark.asyncio
async def test_entity_detail_authorizes_before_any_storage_work():
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unexpected storage work")))
    service = CustomImportReadService(authorizer=None, cursor_secret=b"s" * 32)

    with pytest.raises(CustomImportReadAuthorizationError, match="^extension read is not authorized$"):
        await service.root_detail_for_entity(
            session,
            authorization=ExtensionReadAuthorization("synthetic-secret"),
            request=RootDetailRequest(_target(), EntityLocator("synthetic", "value"), "full_family"),
        )

    session.execute.assert_not_awaited()


@pytest.mark.parametrize("ttl", (True, 0, 901))
def test_service_rejects_an_unbounded_cursor_lifetime(ttl):
    with pytest.raises(CustomImportReadRequestError):
        CustomImportReadService(authorizer=None, cursor_secret=b"s" * 32, cursor_ttl_seconds=ttl)


def test_authorizer_failure_is_sanitized_and_missing_authorizers_fail_closed():
    def fail(*args, **kwargs):
        raise RuntimeError("private failure detail")

    authorization = ExtensionReadAuthorization("synthetic-secret")
    for authorizer, supplied in (
        (None, authorization),
        (SimpleNamespace(authorize=fail), authorization),
        (_RejectingAuthorizer(), object()),
    ):
        service = CustomImportReadService(authorizer=authorizer, cursor_secret=b"s" * 32)
        with pytest.raises(CustomImportReadAuthorizationError, match="^extension read is not authorized$"):
            service._authorize(supplied, _target())


@pytest.mark.parametrize("value", (True, -1, 2**63, float("nan"), float("inf"), "1000"))
def test_trusted_clock_rejects_invalid_or_nonfinite_times(value):
    service = CustomImportReadService(authorizer=None, cursor_secret=b"s" * 32, now=lambda: value)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="trusted read clock"):
        service._trusted_now()


def test_trusted_clock_truncates_fractions_and_sanitizes_clock_failure():
    service = CustomImportReadService(authorizer=None, cursor_secret=b"s" * 32, now=lambda: 1_000.9)
    assert service._trusted_now() == 1_000

    def fail():
        raise RuntimeError("private clock detail")

    service._now = fail
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="^trusted read clock is unavailable$"):
        service._trusted_now()


class _ValueCache:
    def __init__(self, value=None, *, fail=False):
        self.value = value
        self.fail = fail
        self.saved = None

    async def get(self, key):
        if self.fail:
            raise RuntimeError("private cache detail")
        return self.value

    async def set(self, key, value, *, expires_at):
        if self.fail:
            raise RuntimeError("private cache detail")
        self.saved = (key, value, expires_at)


@pytest.mark.asyncio
async def test_cache_transport_failures_are_optional_and_writes_preserve_expiry():
    failing = _ValueCache(fail=True)
    assert await read_core._cache_get(failing, "key") is None
    await read_core._cache_result(failing, "key", "result", 1_100)
    working = _ValueCache()
    await read_core._cache_result(working, "key", "result", 1_100)
    assert working.saved == ("key", "result", 1_100)


@pytest.mark.asyncio
async def test_search_cache_binds_target_query_scope_and_expiry():
    context = SimpleNamespace(target=_target())
    plan = SimpleNamespace(fingerprint="a" * 64)
    page = read_core.SearchPage(_target(), 0, (), None, 1_100, "a" * 64, "b" * 64)
    for changes in (
        {"target": _target(102)},
        {"query_fingerprint": "c" * 64},
        {"authorization_scope_sha256": "c" * 64},
        {"expires_at": 1_000},
    ):
        assert (
            await read_core._cached_search_page(
                _ValueCache(replace(page, **changes)), "key", context, plan, "b" * 64, 1_000
            )
            is None
        )
    assert await read_core._cached_search_page(_ValueCache(page), "key", context, plan, "b" * 64, 1_000) is page


@pytest.mark.asyncio
async def test_detail_cache_binds_target_winner_and_scope():
    context = SimpleNamespace(target=_target())
    winner = WinnerLocator(1, 2, 3, b"w" * 32)
    detail = read_core.RootDetail(_target(), winner, (), (), "b" * 64)
    for changes in (
        {"target": _target(102)},
        {"winner": replace(winner, root_record_id=4)},
        {"authorization_scope_sha256": "c" * 64},
    ):
        assert (
            await read_core._cached_root_detail(
                _ValueCache(replace(detail, **changes)), "key", context, winner, "b" * 64
            )
            is None
        )
    assert await read_core._cached_root_detail(_ValueCache(detail), "key", context, winner, "b" * 64) is detail


@pytest.mark.parametrize("document", ("not JSON", "null", "{}"))
def test_persisted_definition_parse_failure_is_unavailable(document):
    definition = SimpleNamespace(canonical_definition=document)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="valid v1 read contract"):
        read_identity.verified_definition(definition, object())


@pytest.mark.asyncio
async def test_published_generation_validation_uses_the_pinned_target_and_canonical_event():
    pinned_target = _target()
    details = publication._PublicationEventDetails(
        dataset_id=pinned_target.dataset_id,
        definition_revision_id=pinned_target.definition_revision_id,
        schema_revision_id=pinned_target.schema_revision_id,
        execution_id=41,
        event_kind="activated",
        from_generation_id=None,
        to_generation_id=pinned_target.generation_id,
        expected_pointer_version=0,
        committed_pointer_version=1,
    )
    canonical, digest = publication._event_document(details)
    publication_event = SimpleNamespace(
        **details.__dict__,
        finality_contract=publication.FINALITY_EVENT_CONTRACT,
        canonical_event=canonical,
        event_sha256=digest,
    )

    def session_for(event_row):
        return SimpleNamespace(
            execute=AsyncMock(
                side_effect=(
                    SimpleNamespace(scalar_one_or_none=lambda: pinned_target.generation_id),
                    SimpleNamespace(scalars=lambda: SimpleNamespace(first=lambda: event_row)),
                )
            )
        )

    session = session_for(publication_event)
    await read_identity.verify_published_generation(session, pinned_target)

    statements = [str(call.args[0]) for call in session.execute.await_args_list]
    assert "custom_import_current_generation" not in "\n".join(statements)
    assert "to_generation_id" in statements[1]
    assert "finality_contract" in statements[1]

    with pytest.raises(read_core.CustomImportReadUnavailableError, match="pinned generation"):
        await read_identity.verify_published_generation(session_for(None), pinned_target)

    publication_event.canonical_event = "{}"
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="pinned generation"):
        await read_identity.verify_published_generation(session_for(publication_event), pinned_target)


@pytest.fixture
def query_context():
    definition = CustomImportDefinition.from_json(
        (Path(__file__).with_name("fixtures") / "custom_import/v1_valid.json").read_text()
    )
    return read_core._ReadContext(_target(), definition, 1, 1, {"rates": 1}, {1: "rates"})


@pytest.fixture
def aliased_query_context():
    raw = load_json_definition((Path(__file__).with_name("fixtures") / "custom_import/v1_valid.json").read_text())
    raw["query"].update(
        {
            "aliases": {"metric": "amount", "provider": "display_name"},
            "sortable_fields": ["amount"],
        }
    )
    definition = CustomImportDefinition.from_mapping(raw)
    return read_core._ReadContext(_target(), definition, 1, 1, {"rates": 1}, {1: "rates"})


def test_detail_projects_only_declared_root_and_child_fields(query_context):
    root_fields = read_core._detail_root_fields(query_context.definition)
    child_fields = read_core._detail_child_fields_by_slot(query_context)

    assert [field.field_id for field in root_fields] == ["npi", "display_name"]
    assert [field.field_id for field in child_fields[1]] == ["service_code", "amount"]
    assert "rate_npi" not in {field.field_id for field in child_fields[1]}


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("family_rows", "error_type"),
    (
        ((), CustomImportReadEntityAbsentError),
        (((1, 2, 3), (4, 5, 3)), CustomImportReadUnavailableError),
    ),
)
async def test_entity_detail_distinguishes_absent_from_ambiguous_root_families(
    monkeypatch, query_context, family_rows, error_type
):
    session = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(all=lambda: family_rows)),
    )
    verified = AsyncMock()
    monkeypatch.setattr(read_core, "verify_published_generation", verified)

    with pytest.raises(error_type):
        await read_core._entity_winner_locator(session, query_context, EntityLocator("synthetic", "value"))

    assert session.execute.await_count == 1
    if family_rows:
        verified.assert_not_awaited()
    else:
        verified.assert_awaited_once_with(session, query_context.target)


@pytest.mark.asyncio
async def test_entity_detail_rechecks_finality_before_declaring_absence(monkeypatch, query_context):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(all=lambda: ())))
    verified = AsyncMock(side_effect=CustomImportReadUnavailableError("pinned generation is unavailable"))
    monkeypatch.setattr(read_core, "verify_published_generation", verified)

    with pytest.raises(CustomImportReadUnavailableError, match="pinned generation is unavailable"):
        await read_core._entity_winner_locator(session, query_context, EntityLocator("synthetic", "value"))

    verified.assert_awaited_once_with(session, query_context.target)
    assert session.execute.await_count == 1


@pytest.mark.asyncio
async def test_entity_detail_resolves_one_generic_entity_to_a_stable_winner(query_context):
    winner = SimpleNamespace(entity_binding_id=3, context_key_sha256=b"w" * 32)
    family = SimpleNamespace(root_record_id=1, family_revision_id=2)
    statements = []

    class Session:
        def __init__(self) -> None:
            self.results = iter(
                (
                    SimpleNamespace(all=lambda: ((1, 2, 3),)),
                    SimpleNamespace(one_or_none=lambda: (winner, family, object(), object())),
                )
            )

        async def execute(self, statement):
            statements.append(statement)
            return next(self.results)

    locator = await read_core._entity_winner_locator(Session(), query_context, EntityLocator("synthetic", "value"))

    assert locator == WinnerLocator(1, 2, 3, b"w" * 32)
    assert len(statements) == 2
    statement_text = str(statements[0])
    assert "DISTINCT" in statement_text
    assert "custom_import_entity_binding.adapter_id" in statement_text
    for column_name in ("dataset_id", "generation_id", "definition_revision_id", "schema_revision_id", "profile_slot"):
        assert f"custom_import_winner.{column_name}" in statement_text


@pytest.mark.asyncio
async def test_entity_detail_resolves_a_root_scoped_profile(query_context):
    winner = SimpleNamespace(entity_binding_id=3, context_key_sha256=b"w" * 32)
    family = SimpleNamespace(root_record_id=1, family_revision_id=2)
    context = replace(query_context, profile_context_slot=0)
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=(
                SimpleNamespace(all=lambda: ((1, 2, 3),)),
                SimpleNamespace(one_or_none=lambda: (winner, family, object())),
            )
        )
    )

    locator = await read_core._entity_winner_locator(session, context, EntityLocator("synthetic", "value"))

    assert locator == WinnerLocator(1, 2, 3, b"w" * 32)


def test_query_normalization_is_order_independent_and_rejects_repeated_predicates(query_context):
    filters = (read_core.ReadFilter("npi", "eq", "1234567893"), read_core.ReadFilter("amount", "gt", "1.25"))
    request = SearchRequest(_target(), filters=filters)
    plan = read_core._normalize_search_plan(request, query_context)
    reversed_plan = read_core._normalize_search_plan(replace(request, filters=filters[::-1]), query_context)
    assert plan == reversed_plan
    assert {item.field.field_id for item in plan.filters} == {"npi", "amount"}
    with pytest.raises(CustomImportReadRequestError, match="repeat"):
        read_core._normalize_search_plan(replace(request, filters=filters[:1] * 2), query_context)
    with pytest.raises(CustomImportReadRequestError, match="filter count"):
        read_core._normalize_search_plan(replace(request, filters=filters * 2), query_context)


def test_query_aliases_normalize_before_fingerprinting_and_duplicate_checks(aliased_query_context):
    aliased = SearchRequest(_target(), filters=(read_core.ReadFilter("metric", "gt", "1.25"),))
    canonical = SearchRequest(_target(), filters=(read_core.ReadFilter("amount", "gt", "1.25"),))

    assert read_core._normalize_search_plan(aliased, aliased_query_context) == read_core._normalize_search_plan(
        canonical, aliased_query_context
    )
    with pytest.raises(CustomImportReadRequestError, match="repeat"):
        read_core._normalize_search_plan(
            replace(
                aliased,
                filters=(
                    read_core.ReadFilter("metric", "gt", "1.25"),
                    read_core.ReadFilter("amount", "gt", "1.25"),
                ),
            ),
            aliased_query_context,
        )


def test_explicit_sort_uses_query_aliases_and_sortable_allowlist(aliased_query_context):
    aliased = SearchRequest(
        _target(),
        order_terms=(read_core.ReadOrderTerm("metric", "desc", "last"),),
    )
    canonical = replace(
        aliased,
        order_terms=(read_core.ReadOrderTerm("amount", "desc", "last"),),
    )

    assert read_core._normalize_search_plan(aliased, aliased_query_context) == read_core._normalize_search_plan(
        canonical, aliased_query_context
    )
    invalid_orders = (
        (),
        (read_core.ReadOrderTerm("provider", "asc", "last"),),
        (read_core.ReadOrderTerm("metric", "asc", "first"),),
        (
            read_core.ReadOrderTerm("metric", "asc", "last"),
            read_core.ReadOrderTerm("amount", "desc", "last"),
        ),
    )
    for order in invalid_orders:
        with pytest.raises(CustomImportReadRequestError, match="not permitted"):
            read_core._normalize_search_plan(replace(aliased, order_terms=order), aliased_query_context)


def test_search_cannot_override_verified_target_or_definition_order(query_context):
    request = SearchRequest(_target())
    for invalid in (object(), replace(request, target=_target(102))):
        with pytest.raises(CustomImportReadRequestError, match="search target"):
            read_core._normalize_search_plan(invalid, query_context)
    for order in ((), (read_core.ReadOrderTerm("amount", "desc", "last"),)):
        with pytest.raises(CustomImportReadRequestError, match="exactly match"):
            read_core._normalize_search_plan(replace(request, order_terms=order), query_context)


@pytest.mark.parametrize(("direction", "nulls"), (([], "last"), ("asc", {})))
def test_order_terms_reject_non_string_enums(direction, nulls):
    with pytest.raises(CustomImportReadRequestError, match="malformed"):
        read_core.ReadOrderTerm("amount", direction, nulls)


def test_declared_filter_and_order_fields_require_persisted_bindings(query_context):
    definition = SimpleNamespace(query=query_context.definition.query, fields_by_id={})
    context = replace(query_context, definition=definition)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="query field"):
        read_core._normalized_filters((read_core.ReadFilter("npi", "eq", "1234567893"),), context)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="order field"):
        read_core._verify_order_context((read_core.ReadOrderTerm("amount", "asc", "last"),), context)
    with pytest.raises(CustomImportReadRequestError, match="order field"):
        read_core._verify_order_context((read_core.ReadOrderTerm("unknown", "asc", "last"),), query_context)
    with pytest.raises(CustomImportReadRequestError, match="filter field"):
        read_core._normalized_filters((object(),), query_context)


def test_child_fields_cannot_escape_the_selected_collection_context(query_context):
    read_core._verify_field_context(query_context.definition.fields_by_id["npi"], query_context)
    child = query_context.definition.fields_by_id["amount"]
    for context in (
        replace(query_context, profile_context_slot=0),
        replace(query_context, collection_slots_by_name={}),
    ):
        with pytest.raises(CustomImportReadRequestError, match="permitted context"):
            read_core._verify_field_context(child, context)


def _persisted_profile(context):
    declared = context.definition.selection_profiles[0]
    document = read_core._profile_document(declared, "rates")
    return SimpleNamespace(
        profile_id=declared.profile_id,
        profile_slot=1,
        context_collection_slot=1,
        canonical_profile=read_core.canonical_json(document),
        profile_sha256=bytes.fromhex(read_core.canonical_sha256(document, domain="profile")),
    )


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"profile_id": "absent"}, "not declared"),
        ({"canonical_profile": "not JSON"}, "invalid"),
        ({"profile_slot": 0}, "does not match"),
        ({"context_collection_slot": 2}, "does not match"),
        ({"profile_sha256": b"x" * 32}, "does not match"),
    ),
)
def test_persisted_profile_cannot_change_the_declared_selection(query_context, changes, message):
    profile = _persisted_profile(query_context)
    assert read_core._verified_profile(profile, query_context.definition, {"rates": 1}) == (1, 1)
    for key, value in changes.items():
        setattr(profile, key, value)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match=message):
        read_core._verified_profile(profile, query_context.definition, {"rates": 1})


def test_profile_scope_distinguishes_root_selection_and_missing_child_binding(query_context):
    profile = query_context.definition.selection_profiles[0]
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="child collection"):
        read_core._profile_scope_slot(profile, query_context.definition, {})
    root_term = replace(profile.selection_terms[0], field_id="npi")
    root_profile = replace(profile, selection_terms=(root_term,), context_dimensions=())
    root_definition = replace(
        query_context.definition,
        query=replace(
            query_context.definition.query,
            child_collection=None,
            child_fields=(),
            order_terms=(root_term,),
        ),
        selection_profiles=(root_profile,),
    )
    assert read_core._profile_scope_slot(root_profile, root_definition, {}) == (0, None)
    assert read_core._profile_document(root_profile, None)["scope"] == {"kind": "root"}


@pytest.mark.parametrize(
    "value_type,column_name,value",
    (
        ("integer", "integer_value", 7),
        ("string", "string_value", "synthetic"),
        ("decimal", "decimal_value", Decimal("1.25")),
        ("boolean", "boolean_value", True),
        ("date", "date_value", dt.date(2026, 1, 2)),
        ("timestamp", "timestamp_value", dt.datetime(2026, 1, 2, tzinfo=dt.UTC)),
    ),
)
def test_scalar_projection_preserves_typed_values(value_type, column_name, value):
    field = _field(value_type)
    row = SimpleNamespace(field_type=value_type, value_state="value", **{column_name: value})
    assert read_core._field_value(field, row) == read_core.ReadFieldValue("synthetic_field", value_type, "value", value)


def test_scalar_projection_preserves_null_and_missing_and_rejects_corrupt_rows():
    field = _field("string")
    assert read_core._field_value(field, None).state == "missing"
    null = SimpleNamespace(field_type="string", value_state="null")
    assert read_core._field_value(field, null).value is None
    assert read_core._field_value(field, null).state == "null"
    for row in (
        SimpleNamespace(field_type="integer", value_state="value"),
        SimpleNamespace(field_type="string", value_state="missing"),
        SimpleNamespace(field_type="string", value_state="value", string_value=None),
    ):
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="persisted scalar"):
            read_core._field_value(field, row)
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="null violates"):
        read_core._field_value(replace(field, nullable=False), null)


@pytest.mark.parametrize(
    "row",
    (
        None,
        SimpleNamespace(timeout_text=None, timeout_milliseconds="0"),
        SimpleNamespace(timeout_text="secret", timeout_milliseconds="invalid"),
        SimpleNamespace(timeout_text="secret", timeout_milliseconds=None),
        SimpleNamespace(timeout_text="secret", timeout_milliseconds="-1"),
    ),
)
@pytest.mark.asyncio
async def test_invalid_database_timeout_metadata_is_unavailable(row):
    session = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: row)))
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="^bounded read is unavailable$"):
        await read_core._current_statement_timeout(session)


@pytest.mark.parametrize(
    "attributes,expected",
    (({"sqlstate": "57014"}, True), ({"pgcode": "57014"}, True), ({"sqlstate": "23505"}, False), ({}, False)),
)
def test_timeout_detection_uses_driver_codes_without_error_message_matching(attributes, expected):
    driver = RuntimeError("canceling statement due to statement timeout")
    for key, value in attributes.items():
        setattr(driver, key, value)
    assert read_core._is_statement_timeout(DBAPIError(None, None, driver)) is expected


@pytest.mark.asyncio
async def test_timeout_restoration_preserves_the_original_read_error():
    failure = RuntimeError("restore failed")
    session = SimpleNamespace(execute=AsyncMock(side_effect=failure))
    await read_core._restore_statement_timeout(session, "0", has_read_failed=True)
    with pytest.raises(RuntimeError) as error:
        await read_core._restore_statement_timeout(session, "0", has_read_failed=False)
    assert error.value is failure


@pytest.mark.asyncio
async def test_empty_projection_shapes_do_not_touch_database(query_context):
    session = SimpleNamespace(execute=AsyncMock(side_effect=AssertionError("unexpected query")))
    assert await read_core._root_scalar_rows(session, query_context, (), ()) == {}
    assert await read_core._child_scalar_rows(session, query_context, (), (_field("integer"),)) == {}
    assert await read_core._family_child_rows(session, query_context, SimpleNamespace(child_count=0)) == ()
    assert read_core._query_child_fields(query_context.definition, 0) == ()
    session.execute.assert_not_awaited()


@pytest.mark.asyncio
async def test_exact_counts_reject_noninteger_or_negative_results():
    for count in (None, True, -1):
        session = SimpleNamespace(scalar=AsyncMock(return_value=count))
        with pytest.raises(read_core.CustomImportReadUnavailableError, match="exact winner count"):
            await read_core._exact_count(session, read_core.select(1))


def test_undeclared_child_collection_is_not_hydrated(query_context):
    with pytest.raises(read_core.CustomImportReadUnavailableError, match="undeclared child collection"):
        read_core._detail_child(SimpleNamespace(collection_slot=2), object(), query_context, {}, {})


class _AllowingAuthorizer:
    def authorize(self, authorization, *, target):
        del authorization, target
        return read_core.ExtensionReadScope("synthetic:scope")


@pytest.mark.asyncio
async def test_read_service_rejects_unavailable_cursor_and_invalid_public_shapes(monkeypatch):
    authorization = ExtensionReadAuthorization("synthetic-secret")
    service = CustomImportReadService(authorizer=_AllowingAuthorizer())

    with pytest.raises(CustomImportReadUnavailableError, match="cursor is unavailable"):
        await service.search(object(), authorization=authorization, request=SearchRequest(_target()))
    with pytest.raises(CustomImportReadRequestError, match="imported membership mode"):
        await service.prepare_npi_entity_relation(
            object(), authorization=authorization, target=_target(), query=object()
        )
    with pytest.raises(CustomImportReadRequestError, match="provider relation query"):
        await service.hydrate_npi_page(
            object(),
            authorization=authorization,
            pinned_target=_target(),
            prepared=object(),
            entity_values=(),
            query=object(),
        )
    with pytest.raises(CustomImportReadRequestError, match="root detail request"):
        await service.root_detail_for_entity(object(), authorization=authorization, request=object())

    plan = read_core._SearchPlan(filters=(), order_terms=(), page_size=1, fingerprint="a" * 64)
    with pytest.raises(CustomImportReadUnavailableError, match="cursor is unavailable"):
        service._cursor_offset(SearchRequest(_target(), cursor="cursor"), plan, "b" * 64, 1_000)
    with pytest.raises(CustomImportReadUnavailableError, match="cursor is unavailable"):
        service._next_search_cursor(
            SimpleNamespace(target=_target()),
            plan,
            "b" * 64,
            read_core._PageWindow(offset=0, total=2, returned_count=1, issued_at=1_000, expires_at=1_100),
        )

    winner = WinnerLocator(1, 2, 3, b"w" * 32)
    scope = read_core.ExtensionReadScope("synthetic:scope")
    cached = read_core.RootDetail(_target(), winner, (), (), read_core._scope_digest(scope))
    cached_service = CustomImportReadService(authorizer=_AllowingAuthorizer(), cache=_ValueCache(cached))

    async def verified(_session, _target):
        return None

    monkeypatch.setattr(read_core, "verify_published_generation", verified)
    assert await cached_service._root_detail_from_context(
        object(), SimpleNamespace(target=_target()), winner, scope
    ) == (cached, None)


def test_read_contract_helpers_reject_unencodable_and_undeclared_values(query_context):
    for entity_value in (1, "\ud800"):
        with pytest.raises(CustomImportReadRequestError, match="entity value is malformed"):
            EntityLocator("synthetic", entity_value)
    with pytest.raises(CustomImportReadRequestError, match="timezone-aware timestamp"):
        read_core._normalized_timestamp("not-a-timestamp", "observed_at")
    with pytest.raises(CustomImportReadRequestError, match="order_terms must be a tuple"):
        read_core._normalize_query_order_terms([], query_context, explicit=True)
    with pytest.raises(CustomImportReadRequestError, match="order field"):
        read_core._normalized_order_terms((object(),), query_context)
    with pytest.raises(CustomImportReadRequestError, match="order field"):
        read_core._normalized_order_terms((read_core.ReadOrderTerm("unknown", "asc", "last"),), query_context)

    unselected_profile_context = replace(query_context, target=replace(_target(), profile_id="missing"))
    with pytest.raises(CustomImportReadUnavailableError, match="selection profile"):
        read_core._verify_context_filters((), unselected_profile_context)
    with pytest.raises(CustomImportReadUnavailableError, match="selection profile"):
        read_core._require_order_context_filters((), (), unselected_profile_context, require_exact_context=True)

    profile = query_context.definition.selection_profiles[0]
    field = query_context.definition.fields_by_id[profile.context_dimensions[0]]
    null_context = read_core._NormalizedFilter(field, "eq", None, None)
    selected_profile_context = replace(
        query_context,
        target=replace(query_context.target, profile_id=profile.profile_id),
    )
    with pytest.raises(CustomImportReadRequestError, match="context_required"):
        read_core._require_order_context_filters(
            (), (null_context,), selected_profile_context, require_exact_context=True
        )


@pytest.mark.asyncio
async def test_read_context_loaders_reject_missing_or_drifted_persisted_rows(monkeypatch, query_context):
    async def verified(_session, _target):
        return None

    monkeypatch.setattr(read_core, "verify_published_generation", verified)
    missing = SimpleNamespace(execute=AsyncMock(return_value=SimpleNamespace(one_or_none=lambda: None)))
    with pytest.raises(CustomImportReadUnavailableError, match="pinned generation"):
        await read_core._eligible_definition_rows(missing, _target())

    empty_rows = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: ())))
    )
    with pytest.raises(CustomImportReadUnavailableError, match="child collections"):
        await read_core._collection_slots(empty_rows, _target(), query_context.definition)
    with pytest.raises(CustomImportReadUnavailableError, match="field bindings"):
        await read_core._verified_field_rows(empty_rows, _target(), query_context.definition, {"rates": 1})

    persisted_rows = []
    for field in query_context.definition.fields:
        persisted_rows.append(
            SimpleNamespace(
                field_name=field.field_id,
                field_slot=field.field_slot,
                collection_slot=0 if field.collection is None else 1,
                field_type=field.value_type,
                is_nullable=field.nullable,
                projection_slot=0 if field.projection_slot is None else field.projection_slot,
            )
        )
    persisted_rows[0].field_slot += 1
    drifted_rows = SimpleNamespace(
        execute=AsyncMock(return_value=SimpleNamespace(scalars=lambda: SimpleNamespace(all=lambda: persisted_rows)))
    )
    with pytest.raises(CustomImportReadUnavailableError, match="field binding is invalid"):
        await read_core._verified_field_rows(drifted_rows, _target(), query_context.definition, {"rates": 1})


@pytest.mark.asyncio
async def test_read_core_rejects_invalid_membership_flags_before_context_loading():
    service = CustomImportReadService(authorizer=None)
    with pytest.raises(CustomImportReadRequestError, match="imported membership mode"):
        await service._prepare_npi_entity_relation(
            object(),
            pinned_target=_target(),
            query=read_core.NpiEntityRelationQuery(require_match=1),
            authorization_scope=read_core.ExtensionReadScope("synthetic:scope"),
        )


@pytest.mark.asyncio
async def test_root_detail_caches_only_after_a_verified_non_cached_hydration(monkeypatch):
    @asynccontextmanager
    async def unrestricted_window(_session, *, timeout_ms):
        del timeout_ms
        yield

    winner = WinnerLocator(1, 2, 3, b"w" * 32)
    scope = read_core.ExtensionReadScope("synthetic:scope")
    detail = read_core.RootDetail(_target(), winner, (), (), read_core._scope_digest(scope))
    cache = _CacheSpy()
    service = CustomImportReadService(authorizer=_AllowingAuthorizer(), cache=cache)

    async def load_context(_session, target):
        return SimpleNamespace(target=target)

    async def locate_winner(_session, _context, _entity):
        return winner

    monkeypatch.setattr(read_core, "_bounded_read_window", unrestricted_window)
    monkeypatch.setattr(read_core, "_load_read_context", load_context)
    monkeypatch.setattr(read_core, "_entity_winner_locator", locate_winner)
    selected_row = AsyncMock(return_value=object())
    hydrate = AsyncMock(return_value=detail)
    verify = AsyncMock()
    monkeypatch.setattr(read_core, "_selected_winner_row", selected_row)
    monkeypatch.setattr(read_core, "_hydrate_root_detail", hydrate)
    monkeypatch.setattr(read_core, "verify_published_generation", verify)
    request = read_core.RootDetailRequest(
        _target(),
        EntityLocator("synthetic", "value"),
        read_core._FULL_FAMILY_ENTITLEMENT,
    )

    assert (
        await service.root_detail_for_entity(
            object(), authorization=ExtensionReadAuthorization("synthetic-secret"), request=request
        )
        == detail
    )
    assert await service._root_detail(object(), target=_target(), winner=winner, authorization_scope=scope) == detail
    assert cache.set_calls == 2
    assert verify.await_count == 2
    assert selected_row.await_count == 2
    assert hydrate.await_count == 2

    verify.side_effect = CustomImportReadUnavailableError("synthetic finality failure")
    with pytest.raises(CustomImportReadUnavailableError, match="synthetic finality failure"):
        await service._root_detail(object(), target=_target(), winner=winner, authorization_scope=scope)
    assert verify.await_count == 3
    assert cache.set_calls == 2


@pytest.mark.asyncio
async def test_bounded_read_window_rejects_elapsed_and_database_timeouts(monkeypatch):
    @asynccontextmanager
    async def unrestricted_timeout(_session, *, timeout_ms):
        del timeout_ms
        yield

    monkeypatch.setattr(read_core, "_local_statement_timeout", unrestricted_timeout)
    clock_values = iter((0.0, 1.0))
    monkeypatch.setattr(read_core, "time", SimpleNamespace(monotonic=lambda: next(clock_values, 1.0)))
    has_entered_window = False
    with pytest.raises(CustomImportReadUnavailableError, match="bounded read is unavailable"):
        async with read_core._bounded_read_window(object(), timeout_ms=1):
            has_entered_window = True
    assert has_entered_window

    timeout_driver = RuntimeError("database timeout")
    timeout_driver.sqlstate = "57014"
    monkeypatch.setattr(read_core, "time", SimpleNamespace(monotonic=lambda: 0.0))
    with pytest.raises(CustomImportReadUnavailableError, match="bounded read is unavailable"):
        async with read_core._bounded_read_window(object(), timeout_ms=1):
            raise DBAPIError(None, None, timeout_driver)

    non_timeout_driver = RuntimeError("database failure")
    non_timeout_driver.sqlstate = "23505"
    with pytest.raises(DBAPIError):
        async with read_core._bounded_read_window(object(), timeout_ms=1):
            raise DBAPIError(None, None, non_timeout_driver)


def test_read_order_contract_rejects_oversized_declared_order():
    declared_order_terms = tuple(
        read_core.ReadOrderTerm("npi", "asc", "last") for _ in range(read_core.MAX_ORDER_TERMS + 1)
    )
    context = SimpleNamespace(definition=SimpleNamespace(query=SimpleNamespace(order_terms=declared_order_terms)))
    with pytest.raises(CustomImportReadRequestError, match="order term count"):
        read_core._normalize_query_order_terms((), context, explicit=False)


@pytest.mark.asyncio
async def test_entity_detail_rejects_a_winner_removed_between_selection_and_hydration(query_context):
    session = SimpleNamespace(
        execute=AsyncMock(
            side_effect=(
                SimpleNamespace(all=lambda: ((1, 2, 3),)),
                SimpleNamespace(one_or_none=lambda: None),
            )
        )
    )
    with pytest.raises(CustomImportReadUnavailableError, match="selected entity is not eligible"):
        await read_core._entity_winner_locator(session, query_context, EntityLocator("synthetic", "value"))
