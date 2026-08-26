# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from types import SimpleNamespace

import pytest

from api import control_imports, control_workers
from api import plan_pricing_prewarm as prewarm
from api.plan_release_serving import PlanReleaseServingSelection


PROJECTION_ID = "a" * 64
PLAN_RELEASE_ID = "hprelease_" + "2" * 26
SERVING_REVISION_ID = "hpserve_" + "3" * 26
TEST_BEARER = "synthetic" + "-" + "credential"
SERVICE_HOST = "api" + "-" + "layer"
SERVICE_ORIGIN = f"http://{SERVICE_HOST}"


def _selection() -> PlanReleaseServingSelection:
    return PlanReleaseServingSelection(
        serving_revision_id=SERVING_REVISION_ID,
        serving_revision_published_at="2026-08-25T12:34:56.123456Z",
        plan_release_id=PLAN_RELEASE_ID,
        healthporta_plan_id="hpplan_" + "4" * 26,
        plan_version_id="hpversion_" + "5" * 26,
        release_month="2026-08",
        release_status="published",
        binding_set_digest="6" * 64,
        bindings=(),
        pricing_projection_id=PROJECTION_ID,
    )


class _MappingResult:
    def __init__(self, rows):
        self.rows = list(rows)

    def mappings(self):
        return self

    def all(self):
        return list(self.rows)


class _Session:
    def __init__(self, rows):
        self.rows = list(rows)
        self.statements = []

    async def execute(self, statement, params):
        self.statements.append((str(statement), dict(params)))
        return _MappingResult(self.rows)


class _Response:
    def __init__(self, payload, *, status=200):
        self.payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_args):
        return None

    async def json(self, *, content_type=None):
        assert content_type is None
        return self.payload


class _HttpSession:
    def __init__(self, response_by_code):
        self.response_by_code = response_by_code
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return self.response_by_code[kwargs["params"]["code"]]


def _response_data(
    *,
    serving_revision_id=SERVING_REVISION_ID,
    stored_shared=True,
    cache_key_digest="9" * 64,
    payload_bytes=123,
):
    return {
        "data": {
            "plan_release_id": PLAN_RELEASE_ID,
            "serving_revision_id": serving_revision_id,
            "stored_shared": stored_shared,
            "cache_key_digest": cache_key_digest,
            "payload_bytes": payload_bytes,
        }
    }


def test_control_import_registers_exact_prewarm_contract() -> None:
    registry_entry = next(
        entry
        for entry in control_imports.importer_registry()
        if entry["name"] == "plan-pricing-prewarm"
    )
    adapter = control_imports._SINGLE_JOB_ADAPTERS[
        "plan-pricing-prewarm"
    ]
    task_params_by_name = {
        "plan_release_id": PLAN_RELEASE_ID,
        "serving_revision_id": SERVING_REVISION_ID,
        "projection_id": PROJECTION_ID,
    }
    worker_payload = control_imports._adapter_payload(
        adapter,
        {
            "run_id": "run_synthetic",
            "importer": "plan-pricing-prewarm",
            "family": "mrf",
        },
        task_params_by_name,
    )
    assert [
        parameter["name"] for parameter in registry_entry["params_schema"]
    ] == ["plan_release_id", "serving_revision_id", "projection_id"]
    assert registry_entry["schedulable"] is False
    assert adapter == {
        "queue": "arq:PTGCandidateAudit",
        "function": "control_single_job_start",
        "payload": "control_wrapped_kwargs",
        "target_module": "api.plan_pricing_prewarm",
        "target_function": "prewarm_plan_pricing",
        "job_prefix": "plan_pricing_prewarm",
    }
    assert worker_payload["call_style"] == "kwargs"
    assert worker_payload["task"] == {
        "test_mode": False,
        **task_params_by_name,
    }
    worker = next(
        spec
        for spec in control_workers._START_WORKERS
        if "plan-pricing-prewarm" in spec.importers
    )
    assert worker.worker_class == "process.PTGCandidateAudit"
    assert control_workers._single_job_worker_target(
        worker,
        {"importer": "plan-pricing-prewarm", "run_id": "run_synthetic"},
    ) == "plan_pricing_prewarm_run_synthetic"


@pytest.mark.parametrize(
    "params",
    (
        {
            "plan_release_id": PLAN_RELEASE_ID,
            "serving_revision_id": SERVING_REVISION_ID,
        },
        {
            "plan_release_id": PLAN_RELEASE_ID,
            "serving_revision_id": SERVING_REVISION_ID,
            "projection_id": PROJECTION_ID,
            "extra": "forbidden",
        },
        {
            "plan_release_id": PLAN_RELEASE_ID,
            "serving_revision_id": SERVING_REVISION_ID,
            "projection_id": " " + PROJECTION_ID,
        },
    ),
)
def test_control_import_rejects_non_exact_prewarm_params(params) -> None:
    with pytest.raises(ValueError, match="plan-pricing-prewarm params"):
        control_imports._validate_plan_pricing_prewarm_params(
            "plan-pricing-prewarm",
            params,
        )


@pytest.mark.asyncio
async def test_shape_selection_is_ordered_capped_and_canonical() -> None:
    aggregate_rows = [
        {
            "projection_id": PROJECTION_ID,
            "code_system": "HCPCS",
            "code": "27447",
            "geo_cell": "10001",
            "provider_count": 30,
        },
        {
            "projection_id": PROJECTION_ID,
            "code_system": "CPT",
            "code": "27447",
            "geo_cell": "10002",
            "provider_count": 20,
        },
        {
            "projection_id": PROJECTION_ID,
            "code_system": "HCPCS",
            "code": "G0439",
            "geo_cell": "10003",
            "provider_count": 10,
        },
    ]
    session = _Session(aggregate_rows * 683)

    selected = await prewarm._select_shapes(session, PROJECTION_ID)

    statement, params = session.statements[0]
    assert "WHERE projection_id = :projection_id" in statement
    assert (
        "ORDER BY provider_count DESC, code_system, code, geo_cell"
        in statement
    )
    assert "code_system IN ('CPT', 'HCPCS')" in statement
    assert "LIMIT 768" in statement
    assert params == {"projection_id": PROJECTION_ID}
    assert len(selected) == 768
    assert selected[:3] == (
        prewarm.PrewarmShape("CPT", "27447", "10001", 30),
        prewarm.PrewarmShape("CPT", "27447", "10002", 20),
        prewarm.PrewarmShape("HCPCS", "G0439", "10003", 10),
    )


@pytest.mark.asyncio
async def test_shape_selection_excludes_unscoped_em_for_cpt_and_hcpcs() -> None:
    session = _Session(
        [
            {
                "projection_id": PROJECTION_ID,
                "code_system": code_system,
                "code": "99213",
                "geo_cell": zip5,
                "provider_count": 50,
            }
            for code_system, zip5 in (("CPT", "10001"), ("HCPCS", "10002"))
        ]
    )

    assert await prewarm._select_shapes(session, PROJECTION_ID) == ()


def _partial_replay_http_session() -> _HttpSession:
    return _HttpSession(
        {
            "27447": _Response(_response_data()),
            "G0439": _Response(
                _response_data(
                    serving_revision_id="hpserve_" + "7" * 26
                )
            ),
        }
    )


def _assert_partial_replay_receipt(receipt) -> None:
    assert receipt["status"] == "partial"
    assert receipt["ranking_basis"] == "provider_set_member_density"
    assert receipt["ranking_semantics"] == (
        "supply_not_enrollee_or_request_demand"
    )
    assert receipt["per_release_shape_cap"] == 768
    assert receipt["selected_shape_count"] == 2
    assert receipt["attempted_shape_count"] == 2
    assert receipt["warmed_shape_count"] == 1
    assert receipt["stored_shared_count"] == 1
    assert receipt["stored_payload_bytes"] == 123
    assert receipt["failed_shape_count"] == 1
    assert receipt["excluded_e_and_m_count"] == 1
    assert receipt["errors"] == [
        {
            "code_system": "HCPCS",
            "code": "G0439",
            "zip5": "10002",
            "error": "release_identity_mismatch",
        }
    ]


def _assert_default_full_requests(requests) -> None:
    assert [request[1]["params"]["code"] for request in requests] == [
        "27447",
        "G0439",
    ]
    for url, request_by_field in requests:
        params_by_name = request_by_field["params"]
        assert prewarm.PREWARM_PATH == "/internal/v1/plan-pricing/prewarm"
        assert url.endswith(prewarm.PREWARM_PATH)
        assert "view" not in params_by_name
        assert "include_providers" not in params_by_name
        assert params_by_name["zip_radius_miles"] == 25
        assert params_by_name["limit"] == 3
        assert params_by_name["healthporta_plan_id"] == (
            _selection().healthporta_plan_id
        )
        assert params_by_name["plan_release_id"] == PLAN_RELEASE_ID
        assert params_by_name["serving_revision_id"] == (
            SERVING_REVISION_ID
        )
        assert request_by_field["headers"]["Authorization"] == (
            f"Bearer {TEST_BEARER}"
        )
        assert request_by_field["allow_redirects"] is False


@pytest.mark.asyncio
async def test_partial_receipt_is_replay_stable_and_keeps_default_full_path() -> None:
    shapes = (
        prewarm.PrewarmShape("CPT", "27447", "10001", 30),
        prewarm.PrewarmShape("HCPCS", "G0439", "10002", 20),
        prewarm.PrewarmShape("CPT", "99213", "10003", 10),
    )
    config = prewarm.PrewarmHttpConfig(
        base_url=SERVICE_ORIGIN,
        token=TEST_BEARER,
        verify_tls=False,
    )

    first_http_session = _partial_replay_http_session()
    first_receipt = await prewarm._prewarm_shapes(
        first_http_session,
        config,
        _selection(),
        shapes,
    )
    second_http_session = _partial_replay_http_session()
    second_receipt = await prewarm._prewarm_shapes(
        second_http_session,
        config,
        _selection(),
        shapes,
    )

    assert first_receipt == second_receipt
    _assert_partial_replay_receipt(first_receipt)
    _assert_default_full_requests(first_http_session.calls)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("conflict_payload", "expected_error"),
    (
        ({}, "release_identity_mismatch"),
        (
            {"detail": "plan_pricing_prewarm_stale_revision"},
            "release_identity_mismatch",
        ),
        (
            {"detail": "plan_pricing_prewarm_capacity_exceeded"},
            "prewarm_capacity_exceeded",
        ),
    ),
)
async def test_internal_conflict_preserves_capacity_rejection(
    conflict_payload,
    expected_error,
) -> None:
    shape = prewarm.PrewarmShape("HCPCS", "G0439", "10002", 20)
    receipt = await prewarm._prewarm_shapes(
        _HttpSession({"G0439": _Response(conflict_payload, status=409)}),
        prewarm.PrewarmHttpConfig(
            base_url=SERVICE_ORIGIN,
            token=TEST_BEARER,
            verify_tls=False,
        ),
        _selection(),
        (shape,),
    )

    assert receipt["status"] == "partial"
    assert receipt["warmed_shape_count"] == 0
    assert receipt["failed_shape_count"] == 1
    assert receipt["errors"][0]["error"] == expected_error


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("response_fields", "expected_error"),
    (
        ({"stored_shared": False}, "shared_cache_not_stored"),
        ({"cache_key_digest": "invalid"}, "invalid_cache_receipt"),
        ({"payload_bytes": 0}, "invalid_cache_receipt"),
    ),
)
async def test_shared_cache_receipt_is_a_strict_success_gate(
    response_fields,
    expected_error,
) -> None:
    shape = prewarm.PrewarmShape("HCPCS", "G0439", "10002", 20)
    http_session = _HttpSession(
        {"G0439": _Response(_response_data(**response_fields))}
    )

    receipt = await prewarm._prewarm_shapes(
        http_session,
        prewarm.PrewarmHttpConfig(
            base_url=SERVICE_ORIGIN,
            token=TEST_BEARER,
            verify_tls=False,
        ),
        _selection(),
        (shape,),
    )

    assert receipt["status"] == "partial"
    assert receipt["warmed_shape_count"] == 0
    assert receipt["stored_shared_count"] == 0
    assert receipt["failed_shape_count"] == 1
    assert receipt["errors"] == [
        {
            "code_system": "HCPCS",
            "code": "G0439",
            "zip5": "10002",
            "error": expected_error,
        }
    ]


def test_http_config_requires_secret_and_safe_origin(monkeypatch) -> None:
    monkeypatch.setenv(
        prewarm.PREWARM_API_BASE_URL_ENV,
        SERVICE_ORIGIN + ".healthcare.svc.cluster.local",
    )
    monkeypatch.delenv(prewarm.PREWARM_API_TOKEN_ENV, raising=False)
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "not-an-api-client-key")
    with pytest.raises(ValueError, match=prewarm.PREWARM_API_TOKEN_ENV):
        prewarm.prewarm_http_config()

    monkeypatch.setenv(prewarm.PREWARM_API_TOKEN_ENV, TEST_BEARER)
    config = prewarm.prewarm_http_config()
    assert config.verify_tls is False
    assert config.headers["Authorization"] == f"Bearer {TEST_BEARER}"

    monkeypatch.setenv(
        prewarm.PREWARM_API_BASE_URL_ENV,
        "http://public.example.test",
    )
    with pytest.raises(ValueError, match="verified HTTPS"):
        prewarm.prewarm_http_config()

    monkeypatch.setenv(
        prewarm.PREWARM_API_BASE_URL_ENV,
        f"http://unexpected.{SERVICE_HOST}.svc",
    )
    with pytest.raises(ValueError, match="verified HTTPS"):
        prewarm.prewarm_http_config()


@pytest.mark.asyncio
async def test_exact_selection_rejects_release_coordinate_drift(monkeypatch) -> None:
    async def changed_selection(*_args, **_kwargs):
        return SimpleNamespace(
            plan_release_id=PLAN_RELEASE_ID,
            serving_revision_id="hpserve_" + "8" * 26,
            pricing_projection_id=PROJECTION_ID,
        )

    monkeypatch.setattr(
        prewarm,
        "resolve_plan_release_serving",
        changed_selection,
    )
    with pytest.raises(ValueError, match="exact current ready projection"):
        await prewarm._exact_ready_selection(
            object(),
            plan_release_id=PLAN_RELEASE_ID,
            serving_revision_id=SERVING_REVISION_ID,
            projection_id=PROJECTION_ID,
        )


@pytest.mark.asyncio
async def test_exact_selection_requires_serving_publication_fence(
    monkeypatch,
) -> None:
    async def legacy_selection(*_args, **_kwargs):
        return SimpleNamespace(
            plan_release_id=PLAN_RELEASE_ID,
            serving_revision_id=SERVING_REVISION_ID,
            serving_revision_published_at=None,
            pricing_projection_id=PROJECTION_ID,
        )

    monkeypatch.setattr(
        prewarm,
        "resolve_plan_release_serving",
        legacy_selection,
    )
    with pytest.raises(ValueError, match="exact current ready projection"):
        await prewarm._exact_ready_selection(
            object(),
            plan_release_id=PLAN_RELEASE_ID,
            serving_revision_id=SERVING_REVISION_ID,
            projection_id=PROJECTION_ID,
        )
