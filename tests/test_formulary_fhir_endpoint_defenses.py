# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Closed HTTP fallback cases for FHIR formulary serving."""

import datetime as dt
import json
from types import SimpleNamespace

import pytest

from api.endpoint import formulary_fhir as endpoint
from api import formulary_fhir_drug_values as drug_values
from api import formulary_fhir_serving as serving


FORMULARY_ID = "fhir_at4rcuzsyttz7txu3xtoxsa734"
ALIAS_ID = "ffa_" + "1" * 48
DRUG_ID = "ffm_" + "2" * 48
PUBLISHED_AT = dt.datetime(2026, 8, 8, 6, tzinfo=dt.UTC)


def _request(arguments=None, session=object()):
    return SimpleNamespace(
        args=arguments,
        ctx=SimpleNamespace(sa_session=session),
    )


def _response_payload(http_response):
    return json.loads(http_response.body)


def _formulary_detail():
    return serving._detail_from_record(
        {
            "formulary_id": FORMULARY_ID,
            "status": "current",
            "title": "Synthetic Coverage Plan",
            "name": "Synthetic Formulary",
            "period_start": None,
            "period_end": None,
            "last_updated": PUBLISHED_AT,
            "as_of": PUBLISHED_AT,
            "published_at": PUBLISHED_AT,
            "coverage_required": False,
            "coverage_expected_artifact_count": None,
            "coverage_receipt_expected_artifact_count": None,
            "coverage_included_artifact_count": None,
            "coverage_missing_artifact_count": None,
        }
    )


def _public_drug():
    return drug_values.PublicFHIRFormularyDrug(
        formulary_id=FORMULARY_ID,
        alias_id=ALIAS_ID,
        drug_id=DRUG_ID,
        status="active",
        name="Synthetic Medication",
        rxnorm_id=None,
        ndc11=None,
        last_updated=PUBLISHED_AT,
        tier=None,
        prior_authorization=None,
        step_therapy=None,
        quantity_limit=None,
        alternatives=drug_values.PublicFHIRFormularyAlternatives((), 0),
    )


class _BrokenArguments:
    def keys(self):
        raise TypeError("synthetic query failure")


def test_query_parser_accepts_absent_arguments_and_plain_mapping():
    assert endpoint._query_values(
        _request(arguments=None),
        frozenset(),
    ) == {}
    assert endpoint._query_values(
        _request(arguments={"limit": "1"}),
        frozenset({"limit"}),
    ) == {"limit": "1"}


def test_query_parser_maps_broken_argument_container_to_invalid_request():
    with pytest.raises(serving.FHIRFormularyInvalidRequestError):
        endpoint._query_values(_request(arguments=_BrokenArguments()), frozenset())


def test_session_preflight_fails_closed_when_session_is_missing():
    with pytest.raises(serving.FHIRFormularyServingUnavailableError):
        endpoint._get_session(_request(arguments={}, session=None))


def test_unexpected_failure_is_logged_and_sanitized(caplog):
    with caplog.at_level("WARNING"):
        http_response = endpoint._failure_response(RuntimeError("private detail"))

    assert http_response.status == 503
    assert _response_payload(http_response)["error"]["code"] == (
        "formulary_fhir_serving_unavailable"
    )
    assert caplog.records[0].formulary_fhir_failure_class == "RuntimeError"
    assert "private detail" not in http_response.body.decode("utf-8")


def test_expected_unavailability_is_sanitized_without_warning(caplog):
    with caplog.at_level("WARNING"):
        http_response = endpoint._failure_response(
            serving.FHIRFormularyServingUnavailableError("private detail")
        )

    assert http_response.status == 503
    assert caplog.text == ""


@pytest.mark.asyncio
async def test_formulary_detail_success_serializes_closed_payload(monkeypatch):
    expected_session = object()

    async def read_detail(session, formulary_id):
        assert session is expected_session
        assert formulary_id == FORMULARY_ID
        return _formulary_detail()

    monkeypatch.setattr(endpoint, "read_current_fhir_formulary", read_detail)

    http_response = await endpoint.get_current_formulary_detail(
        _request(arguments={}, session=expected_session),
        FORMULARY_ID,
    )

    assert http_response.status == 200
    assert _response_payload(http_response)["formulary_id"] == FORMULARY_ID


@pytest.mark.asyncio
async def test_alias_failure_is_sanitized_as_unavailable(monkeypatch):
    async def fail_aliases(*_args, **_kwargs):
        raise serving.FHIRFormularyServingUnavailableError("private detail")

    monkeypatch.setattr(
        endpoint,
        "read_current_fhir_formulary_aliases",
        fail_aliases,
    )

    http_response = await endpoint.get_current_formulary_aliases(
        _request(arguments={}),
        FORMULARY_ID,
    )

    assert http_response.status == 503
    assert "private detail" not in http_response.body.decode("utf-8")


@pytest.mark.asyncio
async def test_drug_detail_success_serializes_closed_payload(monkeypatch):
    expected_session = object()

    async def read_drug(session, formulary_id, alias_id, drug_id):
        assert (session, formulary_id, alias_id, drug_id) == (
            expected_session,
            FORMULARY_ID,
            ALIAS_ID,
            DRUG_ID,
        )
        return _public_drug()

    monkeypatch.setattr(endpoint, "read_current_fhir_formulary_drug", read_drug)

    http_response = await endpoint.get_current_formulary_drug_detail(
        _request(arguments={}, session=expected_session),
        FORMULARY_ID,
        ALIAS_ID,
        DRUG_ID,
    )

    assert http_response.status == 200
    assert _response_payload(http_response)["drug_id"] == DRUG_ID
