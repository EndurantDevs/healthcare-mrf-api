# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import ast
import inspect
import json

import pytest

from api import billing_search_post_transport as transport_module
from api.billing_search_post_transport import (
    BILLING_SEARCH_POST_MAX_BODY_BYTES,
    BILLING_SEARCH_POST_MEDIA_TYPE,
    BILLING_SEARCH_POST_METHOD,
    BILLING_SEARCH_POST_PATH,
    BillingSearchPostTransportError,
    parse_billing_search_post_transport,
)


def _synthetic_ein() -> str:
    return f"{12:02d}{3_456_789:07d}"


def _payload() -> dict[str, object]:
    return {
        "healthporta_plan_id": "hpplan_" + "0" * 26,
        "billing_identity": {
            "tax_identity": {"type": "ein", "value": _synthetic_ein()}
        },
        "procedure": {
            "code_system": "CPT",
            "code": "99213",
            "modifiers": [],
            "place_of_service": [],
        },
        "geo": {"zip5": "12345", "radius_miles": 0},
        "page": {"limit": 25, "cursor": None},
    }


def _body(payload: object | None = None) -> bytes:
    return json.dumps(_payload() if payload is None else payload).encode("utf-8")


def _parse(body: object):
    return parse_billing_search_post_transport(
        body,
        method=BILLING_SEARCH_POST_METHOD,
        path=BILLING_SEARCH_POST_PATH,
        media_type=BILLING_SEARCH_POST_MEDIA_TYPE,
    )


def _assert_transport_invalid(body: object, **coordinate_overrides: object):
    coordinates_by_name = {
        "method": BILLING_SEARCH_POST_METHOD,
        "path": BILLING_SEARCH_POST_PATH,
        "media_type": BILLING_SEARCH_POST_MEDIA_TYPE,
        **coordinate_overrides,
    }
    with pytest.raises(BillingSearchPostTransportError) as captured:
        parse_billing_search_post_transport(body, **coordinates_by_name)
    error = captured.value
    assert str(error) == "billing_search_post_transport_invalid"
    assert error.__cause__ is None
    assert error.__context__ is None
    return error


def test_transport_accepts_only_the_exact_post_resource_and_normalized_media_type() -> (
    None
):
    request = _parse(_body())
    assert request.healthporta_plan_id == "hpplan_" + "0" * 26
    assert request.code_system == "CPT"

    _assert_transport_invalid(_body(), method="GET")
    _assert_transport_invalid(_body(), method="post")
    _assert_transport_invalid(_body(), path=BILLING_SEARCH_POST_PATH + "/")
    _assert_transport_invalid(_body(), path=BILLING_SEARCH_POST_PATH + "?x=1")
    _assert_transport_invalid(_body(), media_type="application/json; charset=utf-8")
    _assert_transport_invalid(_body(), media_type="Application/Json")


def test_transport_accepts_utf8_json_whitespace_and_key_order() -> None:
    body = json.dumps(
        _payload(),
        ensure_ascii=False,
        indent=2,
        sort_keys=True,
    ).encode("utf-8")
    request = _parse(b" \n" + body + b"\n")
    assert request.zip5 == "12345"


def test_transport_rejects_duplicate_keys_at_every_object_depth() -> None:
    body = _body()
    top_level_duplicate = body.replace(
        b"{",
        b'{"healthporta_plan_id":"hpplan_' + b"0" * 26 + b'",',
        1,
    )
    nested_duplicate = body.replace(
        b'"zip5": "12345"',
        b'"zip5": "12345", "zip5": "12345"',
        1,
    )
    selector_duplicate = body.replace(
        b'"type": "ein"',
        b'"type": "ein", "type": "ein"',
        1,
    )
    for duplicate_body in (
        top_level_duplicate,
        nested_duplicate,
        selector_duplicate,
    ):
        _assert_transport_invalid(duplicate_body)


def test_transport_rejects_nonfinite_numbers_malformed_json_and_nonobjects() -> None:
    nonfinite_constant = _body().replace(b'"radius_miles": 0', b'"radius_miles": NaN')
    overflowing_float = _body().replace(b'"radius_miles": 0', b'"radius_miles": 1e400')
    for invalid_body in (
        nonfinite_constant,
        overflowing_float,
        b"\xff",
        b"{",
        b"null",
        b"[]",
        b'"string"',
    ):
        _assert_transport_invalid(invalid_body)


def test_transport_requires_bounded_immutable_bytes() -> None:
    _assert_transport_invalid(b"")
    _assert_transport_invalid(bytearray(_body()))
    _assert_transport_invalid(memoryview(_body()))
    _assert_transport_invalid("{}")
    _assert_transport_invalid(b" " * (BILLING_SEARCH_POST_MAX_BODY_BYTES + 1))


def test_transport_error_does_not_retain_body_in_its_parser_frame() -> None:
    sensitive_value = _synthetic_ein()
    malformed_payload = _payload()
    malformed_payload["billing_identity"]["tax_identity"]["value"] = (
        sensitive_value + "0"
    )
    error = _assert_transport_invalid(_body(malformed_payload))
    assert sensitive_value not in repr(error)

    traceback = error.__traceback__
    parser_frames = []
    while traceback is not None:
        if traceback.tb_frame.f_code.co_name == "parse_billing_search_post_transport":
            parser_frames.append(traceback.tb_frame.f_locals)
        traceback = traceback.tb_next
    assert parser_frames
    assert all("body" not in local_values for local_values in parser_frames)


def test_transport_has_no_raw_body_digest_or_authentication_claim() -> None:
    source = inspect.getsource(transport_module)
    syntax_tree = ast.parse(source)
    imported_roots = {
        alias.name.split(".", 1)[0]
        for node in ast.walk(syntax_tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    imported_roots.update(
        node.module.split(".", 1)[0]
        for node in ast.walk(syntax_tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    )
    assert "hashlib" not in imported_roots
    assert "hmac" not in imported_roots
    assert "body_sha" not in source
    assert "body_digest" not in source
    assert "external gateway must authenticate" in source.lower()
