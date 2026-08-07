# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""SQL, HTTP, and static defenses for FHIR formulary detail serving."""

from __future__ import annotations

import datetime as dt
import json
import logging
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy.dialects import postgresql

from api import formulary_fhir_serving as serving
from api.endpoint import formulary_fhir as endpoint


ROOT = Path(__file__).resolve().parents[1]
FORMULARY_ID = "fhir_at4rcuzsyttz7txu3xtoxsa734"
PUBLISHED_AT = dt.datetime(2026, 8, 7, 19, tzinfo=dt.UTC)


def _compiled_sql() -> str:
    compiled = serving._DETAIL_STATEMENT.compile(
        dialect=postgresql.dialect(),
        compile_kwargs={"render_postcompile": True},
    )
    return " ".join(str(compiled).split())


def _request(session=object()):
    return SimpleNamespace(ctx=SimpleNamespace(sa_session=session))


def _payload(http_response):
    return json.loads(http_response.body)


def _detail():
    return serving.PublicFHIRFormularyDetail(
        formulary_id=FORMULARY_ID,
        status="current",
        title="Synthetic Coverage Plan",
        name="Synthetic Formulary",
        period_start=None,
        period_end=None,
        last_updated=PUBLISHED_AT,
        as_of=PUBLISHED_AT,
        published_at=PUBLISHED_AT,
    )


def test_query_source_qualifies_current_dataset_plan_and_version_ownership():
    normalized_sql = _compiled_sql()

    for required_fragment in (
        "fhir_formulary_dataset.source_id = "
        "mrf.fhir_formulary_current.source_id",
        "fhir_formulary_dataset.dataset_id = "
        "mrf.fhir_formulary_current.dataset_id",
        "fhir_formulary_dataset_coverage_plan.source_id = "
        "mrf.fhir_formulary_dataset.source_id",
        "fhir_formulary_dataset_coverage_plan.dataset_id = "
        "mrf.fhir_formulary_dataset.dataset_id",
        "fhir_formulary_coverage_plan.source_id = "
        "mrf.fhir_formulary_dataset_coverage_plan.source_id",
        "fhir_formulary_coverage_plan.public_id = "
        "mrf.fhir_formulary_dataset_coverage_plan.public_id",
        "fhir_formulary_coverage_plan_version.public_id = "
        "mrf.fhir_formulary_dataset_coverage_plan.public_id",
        "fhir_formulary_coverage_plan_version.coverage_version_id = "
        "mrf.fhir_formulary_dataset_coverage_plan.coverage_version_id",
    ):
        assert required_fragment in normalized_sql


def test_query_requires_exact_current_publication_evidence_and_is_bounded():
    normalized_sql = _compiled_sql()

    for required_fragment in (
        "fhir_formulary_dataset.status =",
        "fhir_formulary_dataset.verified_at IS NOT NULL",
        "fhir_formulary_dataset.failed_at IS NULL",
        "fhir_formulary_dataset.error_json IS NULL",
        "fhir_formulary_dataset.published_at = "
        "mrf.fhir_formulary_current.published_at",
        "fhir_formulary_current.generation >",
        "fhir_formulary_dataset.coverage_hash IS NOT NULL",
        "fhir_formulary_dataset.membership_hash IS NOT NULL",
        "fhir_formulary_dataset.publish_requested != "
        "mrf.fhir_formulary_dataset.seed_eligible",
        "fhir_formulary_coverage_plan.public_id = %(public_id)s",
        "LIMIT",
    ):
        assert required_fragment in normalized_sql
    assert "fhir_formulary_source" not in normalized_sql
    assert " OFFSET " not in normalized_sql
    assert " FOR UPDATE" not in normalized_sql


def test_query_selects_no_internal_ownership_or_hash_fields():
    normalized_sql = _compiled_sql().split(" FROM ", 1)[0]

    for forbidden_field in (
        "source_id",
        "dataset_id",
        "run_id",
        "generation",
        "coverage_hash",
        "membership_hash",
        "canonical_identity",
        "upstream_list_id",
        "upstream_version_id",
        "metadata_json",
    ):
        assert forbidden_field not in normalized_sql


@pytest.mark.asyncio
async def test_endpoint_returns_private_allowlisted_success(monkeypatch):
    async def read_detail(session, formulary_id):
        assert session is expected_session
        assert formulary_id == FORMULARY_ID
        return _detail()

    expected_session = object()
    monkeypatch.setattr(endpoint, "read_current_fhir_formulary", read_detail)

    http_response = await endpoint.get_current_formulary_detail(
        _request(expected_session),
        FORMULARY_ID,
    )

    assert http_response.status == 200
    assert http_response.headers.get("Cache-Control") == "private, no-store"
    assert set(_payload(http_response)) == {
        "formulary_id",
        "status",
        "title",
        "name",
        "period",
        "last_updated",
        "as_of",
        "published_at",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("failure", "status", "code"),
    (
        (
            serving.FHIRFormularyNotFoundError("private-source"),
            404,
            "formulary_fhir_not_found",
        ),
        (
            serving.FHIRFormularyServingUnavailableError("private-dataset"),
            503,
            "formulary_fhir_serving_unavailable",
        ),
    ),
)
async def test_endpoint_maps_domain_failures_without_leakage(
    monkeypatch,
    failure,
    status,
    code,
):
    async def fail(*_args, **_kwargs):
        raise failure

    monkeypatch.setattr(endpoint, "read_current_fhir_formulary", fail)

    http_response = await endpoint.get_current_formulary_detail(
        _request(),
        FORMULARY_ID,
    )

    assert http_response.status == status
    assert http_response.headers.get("Cache-Control") == "private, no-store"
    assert _payload(http_response)["error"]["code"] == code
    assert "private" not in http_response.body.decode("utf-8")


@pytest.mark.asyncio
async def test_endpoint_sanitizes_unexpected_failures(monkeypatch, caplog):
    async def fail(*_args, **_kwargs):
        raise RuntimeError("https://private.example/cursor?token=secret")

    monkeypatch.setattr(endpoint, "read_current_fhir_formulary", fail)
    with caplog.at_level(logging.WARNING, logger=endpoint.__name__):
        http_response = await endpoint.get_current_formulary_detail(
            _request(),
            FORMULARY_ID,
        )

    assert http_response.status == 503
    assert _payload(http_response)["error"]["code"] == (
        "formulary_fhir_serving_unavailable"
    )
    assert "private.example" not in caplog.text
    assert "secret" not in caplog.text
    assert caplog.records[-1].formulary_fhir_failure_class == "RuntimeError"


@pytest.mark.asyncio
async def test_endpoint_without_request_session_fails_closed():
    http_response = await endpoint.get_current_formulary_detail(
        SimpleNamespace(ctx=SimpleNamespace()),
        FORMULARY_ID,
    )

    assert http_response.status == 503
    assert _payload(http_response)["error"]["code"] == (
        "formulary_fhir_serving_unavailable"
    )


def test_serving_modules_are_select_only_and_have_no_acquisition_surface():
    serving_source = (ROOT / "api" / "formulary_fhir_serving.py").read_text(
        encoding="utf-8"
    )
    endpoint_source = (
        ROOT / "api" / "endpoint" / "formulary_fhir.py"
    ).read_text(encoding="utf-8")

    for forbidden_symbol in (
        "FHIRFormularyClient",
        "FHIRFormularyRepository",
        "synchronize_verified_dataset",
        "publish_dataset",
        "publish_verified_seed",
        "synthetic_canary",
        "aiohttp",
        "socket",
    ):
        assert forbidden_symbol not in serving_source
        assert forbidden_symbol not in endpoint_source
    for write_statement in ("INSERT ", "UPDATE ", "DELETE ", "LOCK TABLE"):
        assert write_statement not in serving_source
