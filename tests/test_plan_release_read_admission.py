"""Routed release identity is checked before any pricing work begins."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from multidict import CIMultiDict
from sanic.exceptions import Forbidden, InvalidUsage, ServiceUnavailable
from sanic.request.parameters import RequestParameters

from api import plan_release_read_boundary as boundary
from api import plan_release_serving_resolution as resolution

RELEASE = "hprelease_" + "0" * 26
REVISION = "hpserve_" + "1" * 26
DIGEST = "a" * 64


@pytest.fixture
def admission(monkeypatch):
    session = SimpleNamespace(info={"api_reader_verified": True}, in_transaction=lambda: True)
    query_values_by_name = {
        "plan_release_id": [RELEASE],
        boundary.PLAN_RELEASE_REVISION_SELECTOR: [REVISION],
        boundary.PLAN_RELEASE_DIGEST_SELECTOR: [DIGEST],
    }
    request = SimpleNamespace(
        get_args=lambda **_kwargs: RequestParameters(query_values_by_name),
        headers=CIMultiDict({boundary.PLAN_RELEASE_READ_BOUNDARY_HEADER: "internal"}),
        ctx=SimpleNamespace(sa_session=session),
    )
    current = SimpleNamespace(serving_revision_id=REVISION, binding_set_digest=DIGEST)
    resolver = AsyncMock(return_value=current)
    monkeypatch.setattr(boundary, "has_reader_session", lambda _db: True)
    monkeypatch.setattr(boundary, "current_session", lambda: session)
    monkeypatch.setattr(resolution, "resolve_plan_release_guard_selection", resolver)
    return request, query_values_by_name, session, current, resolver


@pytest.mark.asyncio
async def test_admission_compares_exact_current_tuple_in_the_request_reader(admission):
    request, _values, session, _current, resolver = admission
    assert await boundary.require_internal_plan_release_read(request) is None
    resolver.assert_awaited_once_with(session, RELEASE)


@pytest.mark.asyncio
@pytest.mark.parametrize("change", ("revision", "digest", "absent"))
async def test_changed_tuple_is_an_explicit_pre_admission_conflict(admission, change):
    request, _values, _session, current, resolver = admission
    if change == "revision":
        current.serving_revision_id = "hpserve_" + "2" * 26
    elif change == "digest":
        current.binding_set_digest = "b" * 64
    else:
        resolver.return_value = None
    result = await boundary.require_internal_plan_release_read(request)
    assert result.status == 409
    assert boundary.PLAN_RELEASE_ADMISSION_CHANGED.encode() in result.body


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ("plan_release_id", boundary.PLAN_RELEASE_REVISION_SELECTOR, boundary.PLAN_RELEASE_DIGEST_SELECTOR)
)
@pytest.mark.parametrize("form", ("missing", "blank", "duplicate", "invalid"))
async def test_partial_or_ambiguous_admission_is_not_a_legacy_read(admission, field, form):
    request, query_values_by_name, _session, _current, resolver = admission
    if form == "missing":
        del query_values_by_name[field]
    elif form == "blank":
        query_values_by_name[field] = [""]
    elif form == "duplicate":
        query_values_by_name[field] *= 2
    else:
        query_values_by_name[field] = ["invalid"]
    with pytest.raises(InvalidUsage):
        await boundary.require_internal_plan_release_read(request)
    resolver.assert_not_awaited()


@pytest.mark.asyncio
async def test_private_selectors_alone_still_require_internal_boundary(admission):
    request, query_values_by_name, _session, _current, resolver = admission
    del query_values_by_name["plan_release_id"]
    request.headers.clear()
    with pytest.raises(Forbidden):
        await boundary.require_internal_plan_release_read(request)
    resolver.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("missing", "writer", "other_session", "unverified"))
async def test_admission_never_falls_back_to_a_writer_or_detached_session(admission, monkeypatch, failure):
    request, _values, session, _current, resolver = admission
    if failure == "missing":
        request.ctx.sa_session = None
    elif failure == "writer":
        monkeypatch.setattr(boundary, "has_reader_session", lambda _db: False)
    elif failure == "other_session":
        monkeypatch.setattr(boundary, "current_session", lambda: object())
    elif failure == "unverified":
        session.info.clear()
    with pytest.raises(ServiceUnavailable):
        await boundary.require_internal_plan_release_read(request)
    resolver.assert_not_awaited()


def test_billing_query_signature_binds_the_complete_private_admission_tuple():
    from api.billing_search_request import parse_billing_search_request, validate_billing_search_request
    from api.billing_search_transport_contract import billing_search_query_sha256
    from tests.test_billing_search_request import _MultiParameters, _zip_pairs

    original = _zip_pairs()
    private = (
        (boundary.PLAN_RELEASE_REVISION_SELECTOR, REVISION),
        (boundary.PLAN_RELEASE_DIGEST_SELECTOR, DIGEST),
    )
    parsed = parse_billing_search_request(_MultiParameters(original + private))
    assert validate_billing_search_request(parsed) is parsed
    assert billing_search_query_sha256(parsed.query_pairs) != billing_search_query_sha256(tuple(sorted(original)))
    assert dict(parsed.query_pairs)[boundary.PLAN_RELEASE_REVISION_SELECTOR] == REVISION
    for invalid in (private[:1], private[1:], private + private[:1], ((private[0][0], "invalid"), private[1])):
        with pytest.raises(ValueError):
            parse_billing_search_request(_MultiParameters(original + invalid))


@pytest.mark.asyncio
async def test_verified_reader_can_autobegin_after_an_empty_family_probe(admission):
    request, _values, session, _current, resolver = admission
    session.in_transaction = lambda: False
    assert await boundary.require_internal_plan_release_read(request) is None
    resolver.assert_awaited_once()


def test_signed_billing_retry_keeps_meter_uuid_and_rejects_old_signature():
    from api import billing_search_transport_contract as contract
    from tests.test_billing_search_gateway_transport import _binding, _headers, _payload, _verify
    from tests.test_billing_search_transport_contract import QUERY_PAIRS

    verified_contexts = []
    previous_headers = None
    for revision, digest in ((REVISION, DIGEST), ("hpserve_" + "2" * 26, "b" * 64)):
        pairs = tuple(
            sorted(
                QUERY_PAIRS
                + (
                    (boundary.PLAN_RELEASE_REVISION_SELECTOR, revision),
                    (boundary.PLAN_RELEASE_DIGEST_SELECTOR, digest),
                )
            )
        )
        context_fields_by_name = _payload(query_sha256=contract.billing_search_query_sha256(pairs))
        context_fields_by_name["metering_receipt_sha256"] = contract.billing_search_metering_receipt_sha256(
            method=context_fields_by_name["method"],
            path=context_fields_by_name["path"],
            plan_entitlement_sha256=context_fields_by_name["plan_entitlement_sha256"],
            query_sha256=context_fields_by_name["query_sha256"],
            quota_scope_sha256=context_fields_by_name["quota_scope_sha256"],
            request_id=context_fields_by_name["metering_request_id"],
        )
        headers = _headers(context_fields_by_name)
        binding = _binding(query_pairs=pairs)
        verified_contexts.append(_verify(headers, binding=binding))
        assert _verify(headers, binding=binding).metering_request_id == context_fields_by_name["metering_request_id"]
        if previous_headers is not None:
            with pytest.raises(contract.BillingSearchTransportError):
                _verify(previous_headers, binding=binding)
        previous_headers = headers
    assert verified_contexts[0].metering_request_id == verified_contexts[1].metering_request_id
    assert verified_contexts[0].query_sha256 != verified_contexts[1].query_sha256
    assert verified_contexts[0].metering_receipt_sha256 != verified_contexts[1].metering_receipt_sha256


def test_transport_tuple_preserves_cursors_and_native_generation_binding():
    """Existing cursors keep their filter identity while native generation remains pinned."""
    from dataclasses import replace

    from api import billing_search_pagination as pagination
    from tests.test_billing_search_pagination import (
        KEYRING,
        SNAPSHOT_ID,
        _cursor_binding,
        _request,
        _selection,
        _wire_token,
    )

    selection = _selection()
    initial_request = _request()
    initial_binding = _cursor_binding(initial_request)
    after = (0, 1.25, 0, SNAPSHOT_ID, 1396271656, "address-key")
    sealed = pagination.seal_billing_search_page_cursor(after, keyring=KEYRING, binding=initial_binding)
    next_request = _request(
        **{
            "cursor": _wire_token(sealed, initial_binding),
            boundary.PLAN_RELEASE_REVISION_SELECTOR: selection.serving_revision_id,
            boundary.PLAN_RELEASE_DIGEST_SELECTOR: selection.binding_set_digest,
        }
    )
    next_binding = _cursor_binding(next_request)
    assert initial_binding.request_fingerprint_sha256 == next_binding.request_fingerprint_sha256
    assert pagination.open_billing_search_page_cursor(next_request, keyring=KEYRING, binding=next_binding) == after
    original_digest = pagination.billing_search_snapshot_set_sha256(selection)
    for changed in (
        replace(selection, serving_revision_id="hpserve_" + "2" * 26),
        replace(selection, binding_set_digest="b" * 64),
    ):
        assert pagination.billing_search_snapshot_set_sha256(changed) != original_digest
