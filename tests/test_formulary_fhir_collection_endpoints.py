# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed HTTP parsing for current FHIR formulary collections."""

from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from api.endpoint import formulary_fhir as endpoint
from api.formulary_fhir_catalog import PublicFHIRFormularyAliasPage
from api.formulary_fhir_catalog import PublicFHIRFormularyPage
from api.formulary_fhir_drug_values import PublicFHIRFormularyDrugPage
from api.formulary_fhir_serving import FHIRFormularyCursorConflictError


FORMULARY_ID = "fhir_at4rcuzsyttz7txu3xtoxsa734"
ALIAS_ID = "ffa_" + "1" * 48
DRUG_ID = "ffm_" + "2" * 48
DRUG_QUERY_VALUES = {
    "limit": "7",
    "cursor": "abc",
    "rxnorm_id": "12345",
    "ndc11": "12345678901",
    "tier": "preferred",
    "prior_authorization": "true",
    "step_therapy": "false",
    "quantity_limit": "true",
}
DRUG_FILTER_SCOPE = {
    "ndc11": "12345678901",
    "prior_authorization": True,
    "quantity_limit": True,
    "rxnorm_id": "12345",
    "step_therapy": False,
    "tier": "preferred",
}


class _QueryArguments(dict):
    def __init__(self, values_by_field=None, repeated_by_field=None):
        super().__init__(values_by_field or {})
        self._repeated_by_field = repeated_by_field or {}

    def getlist(self, field_name):
        if field_name in self._repeated_by_field:
            return self._repeated_by_field[field_name]
        return [self[field_name]]


def _request(*, arguments=None, session=None):
    request_session = object() if session is None else session
    return SimpleNamespace(
        args=_QueryArguments() if arguments is None else arguments,
        ctx=SimpleNamespace(sa_session=request_session),
    )


def _payload(http_response):
    return json.loads(http_response.body)


@pytest.mark.asyncio
async def test_collection_and_alias_endpoints_forward_only_closed_pagination(
    monkeypatch,
):
    calls = []

    async def read_formularies(session, *, limit, cursor):
        calls.append(("formularies", session, limit, cursor))
        return PublicFHIRFormularyPage((), None)

    async def read_aliases(session, formulary_id, *, limit, cursor):
        calls.append(("aliases", session, formulary_id, limit, cursor))
        return PublicFHIRFormularyAliasPage((), None)

    expected_session = object()
    monkeypatch.setattr(endpoint, "read_current_fhir_formularies", read_formularies)
    monkeypatch.setattr(
        endpoint,
        "read_current_fhir_formulary_aliases",
        read_aliases,
    )

    collection_response = await endpoint.get_current_formularies(
        _request(session=expected_session)
    )
    alias_response = await endpoint.get_current_formulary_aliases(
        _request(
            session=expected_session,
            arguments=_QueryArguments({"limit": "100", "cursor": "abc"}),
        ),
        FORMULARY_ID,
    )

    assert calls == [
        ("formularies", expected_session, 25, None),
        ("aliases", expected_session, FORMULARY_ID, 100, "abc"),
    ]
    for http_response in (collection_response, alias_response):
        assert http_response.status == 200
        assert http_response.headers.get("Cache-Control") == "private, no-store"
        assert _payload(http_response) == {"items": [], "next_cursor": None}


@pytest.mark.asyncio
async def test_drug_page_endpoint_forwards_normalized_filters(monkeypatch):
    """Forward only normalized public filters to the alias-scoped reader."""

    captured_by_field = {}

    async def read_drug_page(
        session,
        formulary_id,
        alias_id,
        *,
        filters,
        limit,
        cursor,
    ):
        captured_by_field.update(
            session=session,
            formulary_id=formulary_id,
            alias_id=alias_id,
            filters=filters.scope_fields(),
            limit=limit,
            cursor=cursor,
        )
        return PublicFHIRFormularyDrugPage((), None)

    expected_session = object()
    monkeypatch.setattr(
        endpoint,
        "read_current_fhir_formulary_drug_page",
        read_drug_page,
    )
    list_response = await endpoint.get_current_formulary_drugs(
        _request(
            arguments=_QueryArguments(DRUG_QUERY_VALUES),
            session=expected_session,
        ),
        FORMULARY_ID,
        ALIAS_ID,
    )

    assert list_response.status == 200
    assert captured_by_field == {
        "session": expected_session,
        "formulary_id": FORMULARY_ID,
        "alias_id": ALIAS_ID,
        "filters": DRUG_FILTER_SCOPE,
        "limit": 7,
        "cursor": "abc",
    }


@pytest.mark.asyncio
async def test_drug_detail_forwards_exact_ids_and_sanitizes_not_found(monkeypatch):
    calls = []

    async def read_drug_detail(session, formulary_id, alias_id, drug_id):
        calls.append((session, formulary_id, alias_id, drug_id))
        raise endpoint.FHIRFormularyNotFoundError("hidden")

    expected_session = object()
    monkeypatch.setattr(
        endpoint,
        "read_current_fhir_formulary_drug",
        read_drug_detail,
    )

    detail_response = await endpoint.get_current_formulary_drug_detail(
        _request(session=expected_session),
        FORMULARY_ID,
        ALIAS_ID,
        DRUG_ID,
    )

    assert calls == [(
        expected_session,
        FORMULARY_ID,
        ALIAS_ID,
        DRUG_ID,
    )]
    assert detail_response.status == 404
    assert _payload(detail_response)["error"]["code"] == "formulary_fhir_not_found"
    assert "hidden" not in detail_response.body.decode("utf-8")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    (
        _QueryArguments({"source_id": "hidden"}),
        _QueryArguments({"limit": "01"}),
        _QueryArguments({"limit": "+1"}),
        _QueryArguments({"limit": "101"}),
        _QueryArguments({"limit": "1.0"}),
        _QueryArguments(
            {"cursor": "first"},
            repeated_by_field={"cursor": ["first", "second"]},
        ),
    ),
)
async def test_collection_rejects_unknown_noncanonical_and_duplicate_query(
    monkeypatch,
    arguments,
):
    async def must_not_read(*_args, **_kwargs):
        raise AssertionError("query validation must precede data access")

    monkeypatch.setattr(endpoint, "read_current_fhir_formularies", must_not_read)

    http_response = await endpoint.get_current_formularies(
        _request(arguments=arguments)
    )

    assert http_response.status == 400
    assert _payload(http_response)["error"]["code"] == (
        "formulary_fhir_invalid_request"
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "arguments",
    (
        _QueryArguments({"prior_authorization": "TRUE"}),
        _QueryArguments({"prior_authorization": "1"}),
        _QueryArguments({"rxnorm_id": "12a"}),
        _QueryArguments({"ndc11": "123"}),
        _QueryArguments({"tier": " preferred"}),
        _QueryArguments({"year": "2026"}),
    ),
)
async def test_drug_list_rejects_noncanonical_or_private_selectors(
    monkeypatch,
    arguments,
):
    async def must_not_read(*_args, **_kwargs):
        raise AssertionError("filter validation must precede data access")

    monkeypatch.setattr(
        endpoint,
        "read_current_fhir_formulary_drug_page",
        must_not_read,
    )

    http_response = await endpoint.get_current_formulary_drugs(
        _request(arguments=arguments),
        FORMULARY_ID,
        ALIAS_ID,
    )

    assert http_response.status == 400
    assert _payload(http_response)["error"]["code"] == (
        "formulary_fhir_invalid_request"
    )


@pytest.mark.asyncio
async def test_stale_cursor_error_is_private_and_status_specific(monkeypatch):
    async def stale(*_args, **_kwargs):
        raise FHIRFormularyCursorConflictError("hidden-current-dataset")

    monkeypatch.setattr(endpoint, "read_current_fhir_formularies", stale)

    http_response = await endpoint.get_current_formularies(_request())

    assert http_response.status == 409
    assert http_response.headers.get("Cache-Control") == "private, no-store"
    assert _payload(http_response) == {
        "error": {
            "code": "formulary_fhir_cursor_stale",
            "message": "FHIR formulary pagination must restart.",
        }
    }
    assert "hidden" not in http_response.body.decode("utf-8")


@pytest.mark.asyncio
@pytest.mark.parametrize("is_drug_detail", (False, True))
async def test_detail_routes_reject_query_selectors_before_data_access(
    monkeypatch,
    is_drug_detail,
):
    async def must_not_read(*_args, **_kwargs):
        raise AssertionError("closed detail query must precede data access")

    request = _request(arguments=_QueryArguments({"dataset_id": "hidden"}))
    if is_drug_detail:
        monkeypatch.setattr(
            endpoint,
            "read_current_fhir_formulary_drug",
            must_not_read,
        )
        http_response = await endpoint.get_current_formulary_drug_detail(
            request,
            FORMULARY_ID,
            ALIAS_ID,
            DRUG_ID,
        )
    else:
        monkeypatch.setattr(
            endpoint,
            "read_current_fhir_formulary",
            must_not_read,
        )
        http_response = await endpoint.get_current_formulary_detail(
            request,
            FORMULARY_ID,
        )

    assert http_response.status == 400
    assert _payload(http_response)["error"]["code"] == (
        "formulary_fhir_invalid_request"
    )
