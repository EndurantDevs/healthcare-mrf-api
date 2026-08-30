# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused read, prewarm, and materialization branch coverage."""

from __future__ import annotations

from dataclasses import replace
from decimal import Decimal
from types import SimpleNamespace

import orjson
import pytest

from api import plan_pricing_prewarm as prewarm
from api import plan_pricing_projection as projection
from api import plan_pricing_projection_materialize as materialize
from api import plan_pricing_projection_source as projection_source
from api import plan_release_serving, plan_release_serving_resolution
from api.plan_pricing_projection_contract import MAX_GEO_CELLS
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError

from .test_plan_pricing_projection import PROJECTION_ID, _selection, _Session
from .test_plan_pricing_prewarm import (
    PLAN_RELEASE_ID,
    SERVICE_ORIGIN,
    SERVING_REVISION_ID,
    TEST_BEARER,
    _selection as prewarm_selection,
)
from .test_plan_release_serving import _binding_row as release_binding_row


class _Result:
    def __init__(self, rows=()):
        self.rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class _ResultSession:
    def __init__(self, *results):
        self.results = list(results)

    async def execute(self, *_args, **_kwargs):
        return self.results.pop(0) if self.results else _Result()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "selection", "error"),
    (
        (
            {"view": "card", "code_system": "CPT", "zip5": "62401"},
            _selection(),
            projection.PlanPricingProjectionUnsupported,
        ),
        (
            {
                "view": "card",
                "include_providers": "false",
                "code_system": "CPT",
                "zip5": "62401",
            },
            _selection(),
            None,
        ),
        (
            {
                "view": "card",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "62401",
            },
            _selection(projection_id=None),
            projection.PlanPricingProjectionUnavailable,
        ),
        (
            {
                "view": "card",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "62401",
                "zip_radius_miles": "bad",
            },
            _selection(),
            projection.PlanPricingProjectionUnsupported,
        ),
        (
            {
                "view": "card",
                "include_providers": "false",
                "code_system": "CPT",
                "code": "27447",
                "zip5": "62401",
                "zip_radius_miles": "bad",
            },
            _selection(),
            None,
        ),
    ),
)
async def test_projection_request_errors_preserve_fallback(args, selection, error):
    session = _Session([])
    call = projection.search_plan_pricing_projection(
        session, selection, args, SimpleNamespace(limit=25, offset=0, page=1)
    )
    if error is None:
        assert await call is None
    else:
        with pytest.raises(error):
            await call
    assert session.statements == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("args", "rows", "message"),
    (
        ({"city": "Example"}, [], "city/state"),
        ({"zip5": "bad"}, [], "valid ZIP5"),
        ({"lat": None, "long": -88, "radius_miles": 25}, [], "coordinates"),
        (
            {"zip5": "62401", "zip_radius_miles": 25},
            [f"{index:05d}" for index in range(MAX_GEO_CELLS + 1)],
            "exceeds",
        ),
    ),
)
async def test_projection_geo_boundaries_fail_closed(args, rows, message):
    with pytest.raises(projection.PlanPricingProjectionUnsupported, match=message):
        await projection._geo_cells(
            _Session(rows), args, result_type="provider_cards"
        )


@pytest.mark.asyncio
async def test_projection_coordinate_and_empty_radius_paths_are_distinct():
    session = _Session(["62401"])
    cells = await projection._geo_cells(
        session,
        {"lat": "39.0", "long": "-88.0", "radius_miles": 25},
        result_type="provider_cards",
    )
    assert cells == ["62401"]
    assert session.statements[0][1]["latitude"] == 39.0

    response = await projection.search_plan_pricing_projection(
        _Session([]),
        _selection(),
        {
            "view": "card",
            "code_system": "CPT",
            "code": "27447",
            "zip5": "62401",
            "zip_radius_miles": 25,
        },
        SimpleNamespace(limit=25, offset=0, page=1),
    )
    assert response["result_state"] == "no_match_in_radius"
    assert response["pagination"]["total"] == 0
    assert projection._unsupported_projection_fields({"order": "sideways"}) == (
        "order",
    )


@pytest.mark.parametrize(
    ("identifiers", "message"),
    (
        (("", SERVING_REVISION_ID, PROJECTION_ID), "plan_release_id"),
        ((PLAN_RELEASE_ID, "", PROJECTION_ID), "serving_revision_id"),
        ((PLAN_RELEASE_ID, SERVING_REVISION_ID, ""), "projection_id"),
    ),
)
def test_prewarm_identifiers_are_exact(identifiers, message):
    with pytest.raises(ValueError, match=message):
        prewarm._validate_identifiers(*identifiers)


@pytest.mark.asyncio
async def test_exact_selection_accepts_current_fenced_projection(monkeypatch):
    selection = prewarm_selection()

    async def current(*_args, **_kwargs):
        return selection

    monkeypatch.setattr(prewarm, "resolve_plan_release_serving", current)
    assert await prewarm._exact_ready_selection(
        object(),
        plan_release_id=PLAN_RELEASE_ID,
        serving_revision_id=SERVING_REVISION_ID,
        projection_id=PROJECTION_ID,
    ) is selection


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "updates",
    ({"projection_id": "b" * 64}, {"provider_count": 0}),
)
async def test_shape_selection_rejects_foreign_or_invalid_rows(updates):
    row_by_field = {
        "projection_id": PROJECTION_ID,
        "code_system": "HCPCS",
        "code": "G0439",
        "geo_cell": "10001",
        "provider_count": 1,
    }
    row_by_field.update(updates)
    with pytest.raises(ValueError, match="foreign projection|aggregate row"):
        await prewarm._select_shapes(
            _ResultSession(_Result([row_by_field])), PROJECTION_ID
        )


def test_shared_cache_rejects_non_envelope_response():
    shape = prewarm.PrewarmShape("HCPCS", "G0439", "10001", 1)
    assert prewarm._shared_cache_result(
        prewarm_selection(), shape, None
    ).error["error"] == "invalid_response"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("status", "expected_error"),
    ((409, "release_identity_mismatch"), (500, "http_status_500"), (200, "invalid_json")),
)
async def test_http_response_failures_are_receipted(status, expected_error):
    async def invalid_json(*, content_type=None):
        assert content_type is None
        raise ValueError("synthetic")

    result = await prewarm._http_response_result(
        SimpleNamespace(status=status, json=invalid_json),
        prewarm_selection(),
        prewarm.PrewarmShape("HCPCS", "G0439", "10001", 1),
    )
    assert result.error["error"] == expected_error


@pytest.mark.asyncio
async def test_transport_failure_is_receipted():
    class FailingHttpSession:
        def get(self, *_args, **_kwargs):
            raise OSError("synthetic")

    result = await prewarm._prewarm_one(
        FailingHttpSession(),
        prewarm.asyncio.Semaphore(1),
        prewarm.PrewarmHttpConfig(SERVICE_ORIGIN, TEST_BEARER, False),
        prewarm_selection(),
        prewarm.PrewarmShape("HCPCS", "G0439", "10001", 1),
    )
    assert result.error["error"] == "transport_error"


def test_materialization_and_source_edges_remain_fail_closed():
    assert orjson.loads(orjson.dumps(materialize.rate_fragment(Decimal("2")))) == 2
    stats = materialize.CardStats(
        {"npi": 1, "zip5": "10001"}, Decimal("5"), Decimal("5"), 1
    )
    stats.add((Decimal("2"), Decimal("8")))
    assert (stats.minimum, stats.maximum, stats.rate_count) == (
        Decimal("2"),
        Decimal("8"),
        3,
    )

    state = materialize._ProjectedRateState()
    provider_by_field = {"npi": 1, "zip5": "10001"}
    materialize._add_provider_rates(
        state, [provider_by_field], (Decimal("5"),)
    )
    materialize._add_provider_rates(
        state, [provider_by_field], (Decimal("2"),)
    )
    assert state.cards_by_identity[("10001", 1)].rate_count == 2

    state = materialize._ProjectedRateState()
    inputs = materialize._BindingRateInputs(
        ({"provider_set_global_id_128": None, "price_set_global_id_128": None},),
        {},
        {},
        {},
    )
    materialize._add_binding_rate_inputs(state, inputs, ("HCPCS", "G0439"))
    assert state.cards_by_identity == {}

    class Serving:
        @staticmethod
        def _canonical_code_metadata_row(raw_row):
            return raw_row

    assert projection_source._group_code_rows(
        [{"reported_code_system": "bad", "reported_code": ""}], Serving
    ) == {}
    provider_rows_by_npi = {}
    projection_source._append_provider_rows(
        provider_rows_by_npi,
        [{"npi": 1, "zip5": "bad"}],
    )
    assert provider_rows_by_npi == {}
    assert projection_source.numeric_rates(
        [
            {"negotiated_rate": "bad"},
            {"negotiated_rate": "-1"},
            {"negotiated_rate": "Infinity"},
            {"negotiated_rate": "2.5"},
        ]
    ) == (Decimal("2.5"),)


@pytest.mark.asyncio
async def test_sealed_layout_must_exist(monkeypatch):
    from api import ptg2_serving as serving

    async def no_rows(*_args, **_kwargs):
        return None

    monkeypatch.setattr(serving, "_merge_manifest_code_variant_rows", no_rows)
    binding = projection_source.BindingProjection(
        {}, SimpleNamespace(network_names=[]), {}, 0
    )
    assert await materialize.project_code(
        object(), PROJECTION_ID, ("HCPCS", "G0439"), [binding], SimpleNamespace()
    ) == (0, 0, 0)
    with pytest.raises(ValueError, match="sealed rate layout"):
        await materialize._binding_rate_inputs(object(), binding, [{}])


def test_prewarm_http_origin_validation(monkeypatch):
    monkeypatch.setenv(prewarm.PREWARM_API_TOKEN_ENV, TEST_BEARER)
    monkeypatch.delenv(prewarm.PREWARM_API_BASE_URL_ENV, raising=False)
    with pytest.raises(ValueError, match=prewarm.PREWARM_API_BASE_URL_ENV):
        prewarm.prewarm_http_config()

    monkeypatch.setenv(
        prewarm.PREWARM_API_BASE_URL_ENV,
        "https://user@example.test/path?query=yes",
    )
    with pytest.raises(ValueError, match="origin is invalid"):
        prewarm.prewarm_http_config()

    monkeypatch.setenv(prewarm.PREWARM_API_BASE_URL_ENV, "https://example.test")
    assert prewarm.prewarm_http_config().verify_tls is True


def _release_selection_with_binding():
    binding = plan_release_serving.PlanReleaseSnapshotBinding(
        binding_ordinal=0,
        snapshot_id="ptg2:synthetic-network",
        source_key="synthetic-network",
        plan_id="99-0000001",
        plan_market_type="group",
        role="in_network",
        required=True,
    )
    return replace(prewarm_selection(), bindings=(binding,))


@pytest.mark.asyncio
async def test_release_resolution_contract_and_guard_reject_invalid_inputs():
    with pytest.raises(ValueError, match="invalid plan release"):
        plan_release_serving_resolution.PlanReleaseServingResolution(
            "ready", None
        )

    session = _Session([])
    assert (
        await plan_release_serving_resolution.resolve_plan_release_guard_selection(
            session, "invalid"
        )
        is None
    )
    assert session.statements == []


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ("not_ready", "artifact", "no_tables"))
async def test_release_binding_validation_fails_closed(monkeypatch, failure):
    async def is_binding_ready(*_args, **_kwargs):
        if failure == "artifact":
            raise PTG2ManifestArtifactError("synthetic")
        return failure != "not_ready"

    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        is_binding_ready,
    )
    assert (
        await plan_release_serving_resolution._validate_release_bindings(
            object(),
            _release_selection_with_binding(),
            include_billing_tax_identity_source=False,
        )
        is None
    )


@pytest.mark.asyncio
async def test_release_resolution_surfaces_failed_binding_validation(monkeypatch):
    async def is_binding_ready(*_args, **_kwargs):
        return False

    monkeypatch.setattr(
        plan_release_serving,
        "is_release_binding_serving_ready",
        is_binding_ready,
    )
    row_by_field = release_binding_row(
        plan_release_id=PLAN_RELEASE_ID,
        pricing_projection_id="not-a-digest",
    )
    result = await plan_release_serving_resolution.resolve_plan_release_serving_resolution(
        _Session([row_by_field]), PLAN_RELEASE_ID
    )
    assert result.state == "unavailable"
    header = plan_release_serving._release_header_from_rows(
        PLAN_RELEASE_ID, [row_by_field]
    )
    assert header is not None
    assert header.pricing_projection_id is None
