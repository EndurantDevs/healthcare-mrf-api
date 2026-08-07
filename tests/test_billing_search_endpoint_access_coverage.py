# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Defensive-path coverage for the authenticated billing-search endpoint."""

from __future__ import annotations

from collections.abc import Iterator, Mapping
import copy
from typing import Any

import pytest

from api import billing_search_post_endpoint_access as endpoint_access
from api import billing_search_post_endpoint_journal as endpoint_journal
from api import billing_search_post_query as post_query
from api.billing_search_post_transport import BillingSearchPostTransportError
from api.ptg2_billing_search_contract import BillingSearchServingUnavailableError
from tests.test_billing_search_post_endpoint_access import (
    _authorize,
    _body,
    _headers,
    _keyring,
)
from tests.test_billing_search_transport_keys import _environment


class _HeaderMapping(Mapping[str, Any]):
    def __init__(self, values: dict[str, Any] | None = None) -> None:
        self.values = values or {}

    def __getitem__(self, key: str) -> Any:
        return self.values[key]

    def __iter__(self) -> Iterator[str]:
        return iter(self.values)

    def __len__(self) -> int:
        return len(self.values)


class _NoItems(_HeaderMapping):
    items = None


class _FallbackItemsFailure(_HeaderMapping):
    def items(self, *args: object, **kwargs: object):
        if args or kwargs:
            raise TypeError
        raise RuntimeError("synthetic")


class _MultiItemsFailure(_HeaderMapping):
    def items(self, *args: object, **kwargs: object):
        del args, kwargs
        raise RuntimeError("synthetic")


class _AccessorHeaders(_HeaderMapping):
    def __init__(self, values: dict[str, str], mode: str) -> None:
        super().__init__({} if mode == "accessor-only" else values)
        self._header_values = values
        self._mode = mode

    def getall(self, name: str) -> list[str]:
        if self._mode == "missing":
            raise KeyError(name)
        if self._mode == "failure":
            raise RuntimeError("synthetic")
        return [self._header_values[name]]


@pytest.mark.parametrize(
    "headers",
    [_NoItems(), _FallbackItemsFailure(), _MultiItemsFailure()],
)
def test_endpoint_header_items_fail_closed(headers: Mapping[str, Any]) -> None:
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access._mapping_items(headers)


def test_endpoint_header_accessor_distinguishes_missing_and_failure() -> None:
    valid = _headers(_body())
    assert (
        endpoint_access._accessor_values(
            _AccessorHeaders(valid, "missing"),
            next(iter(valid)),
        )
        is None
    )
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access._accessor_values(
            _AccessorHeaders(valid, "failure"),
            next(iter(valid)),
        )


def test_endpoint_accepts_single_values_exposed_only_by_header_accessor() -> None:
    body = _body()
    access = _authorize(body, _AccessorHeaders(_headers(body), "accessor-only"))

    assert access.request.healthporta_plan_id.startswith("hpplan_")


@pytest.mark.parametrize(
    "headers",
    [
        object(),
        _HeaderMapping({1: "value"}),
        _HeaderMapping({}),
    ],
)
def test_endpoint_rejects_nonmapping_nontext_or_missing_headers(
    headers: object,
) -> None:
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access._closed_header_values(headers)


@pytest.mark.parametrize(
    "invalid_value", [None, "", " padded", "snow-\N{SNOWMAN}", "a\n"]
)
def test_endpoint_rejects_noncanonical_internal_header_values(
    invalid_value: object,
) -> None:
    headers = _headers(_body())
    headers[next(iter(headers))] = invalid_value

    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access._closed_header_values(headers)


def test_endpoint_access_objects_are_factory_only_and_immutable() -> None:
    body = _body()
    access = _authorize(body, _headers(body))

    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access.BillingSearchPostEndpointAccess()
    with pytest.raises(TypeError):
        access.request = access.request
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        access.__reduce_ex__(4)

    assert access.authorization_context is access.transport.authorization_context
    assert copy.copy(access) is access
    assert copy.deepcopy(access) is access


def test_endpoint_revalidation_rejects_wrong_transport_and_state() -> None:
    body = _body()
    valid = _authorize(body, _headers(body))
    fabricated = object.__new__(endpoint_access.BillingSearchPostEndpointAccess)
    object.__setattr__(
        fabricated,
        "_BillingSearchPostEndpointAccess__request",
        valid.request,
    )
    object.__setattr__(
        fabricated,
        "_BillingSearchPostEndpointAccess__transport",
        object(),
    )
    object.__setattr__(
        fabricated,
        "_BillingSearchPostEndpointAccess__state_sha256",
        "0" * 64,
    )

    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access.validate_billing_search_post_endpoint_access(fabricated)

    object.__setattr__(
        valid,
        "_BillingSearchPostEndpointAccess__state_sha256",
        1,
    )
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access.validate_billing_search_post_endpoint_access(valid)


def test_endpoint_can_load_the_keyring_from_the_injected_environment() -> None:
    body = _body()
    access = endpoint_access.authorize_billing_search_post_endpoint(
        body,
        _headers(body),
        method="POST",
        path="/api/v1/pricing/providers/search-by-procedure",
        media_type="application/json",
        trusted_now="2026-08-07T10:00:10Z",
        environment_map=_environment(),
    )

    assert access.request.selector_kind == "tax_identity"


def test_endpoint_rejects_competing_keyring_sources() -> None:
    body = _body()

    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        endpoint_access.authorize_billing_search_post_endpoint(
            body,
            _headers(body),
            method="POST",
            path="/api/v1/pricing/providers/search-by-procedure",
            media_type="application/json",
            trusted_now="2026-08-07T10:00:10Z",
            environment_map=_environment(),
            keyring=_keyring(),
        )


def test_endpoint_maps_request_parser_and_unexpected_failures(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _body()
    headers = _headers(body)

    monkeypatch.setattr(
        endpoint_access,
        "parse_billing_search_post_transport",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            BillingSearchPostTransportError("synthetic")
        ),
    )
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        _authorize(body, headers)

    monkeypatch.setattr(
        endpoint_access,
        "parse_billing_search_post_transport",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("synthetic")),
    )
    with pytest.raises(endpoint_access.BillingSearchPostEndpointAccessError):
        _authorize(body, headers)


def test_journal_duration_rejects_wrong_type_and_nonfinite_clock(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    with pytest.raises(endpoint_journal.BillingSearchPostEndpointJournalError):
        endpoint_journal._bounded_duration_us(1)

    monkeypatch.setattr(endpoint_journal.time, "perf_counter", lambda: float("nan"))
    with pytest.raises(endpoint_journal.BillingSearchPostEndpointJournalError):
        endpoint_journal._bounded_duration_us(1.0)


def test_success_journal_preserves_closed_error_and_wraps_unexpected_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    body = _body()
    access = _authorize(body, _headers(body))
    with pytest.raises(endpoint_journal.BillingSearchPostEndpointJournalError):
        endpoint_journal.billing_search_post_success_journal(
            access,
            generation_bundle_sha256=None,
            trusted_observed_at="2026-08-07T10:00:10Z",
            started_at=1,
        )

    monkeypatch.setattr(
        endpoint_journal,
        "_journal_record",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("synthetic")),
    )
    with pytest.raises(endpoint_journal.BillingSearchPostEndpointJournalError):
        endpoint_journal.billing_search_post_success_journal(
            object(),
            generation_bundle_sha256=None,
            trusted_observed_at="2026-08-07T10:00:10Z",
            started_at=1.0,
        )


def test_post_query_builders_reject_unvalidated_service_query_objects() -> None:
    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="billing_search_serving_generation_unavailable",
    ):
        post_query.build_billing_search_resolved_query(
            object(),
            plan_release_id="hprelease_" + "0" * 26,
            radius_zip_context=None,
            after_sort_key=None,
        )
    with pytest.raises(
        BillingSearchServingUnavailableError,
        match="billing_search_serving_generation_unavailable",
    ):
        post_query.build_billing_search_terminal_query(
            object(),
            plan_release_id="hprelease_" + "0" * 26,
        )
