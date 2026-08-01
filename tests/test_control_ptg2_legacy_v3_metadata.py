# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Control-route proof for the bounded legacy PTG V3 metadata repair."""

from __future__ import annotations

import copy
import json
import types
from unittest.mock import AsyncMock

import pytest
from sanic import Blueprint, Sanic
from sanic.exceptions import BadRequest, Forbidden, SanicException

from api import control_ptg2_legacy_v3_metadata as control_module
from api.control_ptg_source_attempt_errors import (
    register_source_attempt_error_handler,
)
from process.ptg_parts import ptg2_legacy_v3_metadata_reconcile as reconcile
from process.ptg_parts.ptg2_legacy_v3_metadata_reconcile import (
    LegacyV3MetadataConflict,
)
from process.ptg_parts.ptg_source_attempt_guard import (
    PTGSourceAttemptFencedError,
)
from tests.test_ptg2_legacy_v3_metadata_contract import (
    _observation,
    _operational_absence,
)


SNAPSHOT_ID = "ptg2:202607:synthetic-route"
INTERNAL_RUN_ID = "ptg2:" + "d" * 32
OUTER_RUN_ID = "run_synthetic_legacy_v3_route"
PLAN_DIGEST = "a" * 64
REQUEST_ID = "request-synthetic-legacy-v3"


class _RouteRecorder:
    def __init__(self) -> None:
        self.route_uris: list[str] = []

    def post(self, route_uri: str):
        def record_route(handler):
            self.route_uris.append(route_uri)
            return handler

        return record_route


def _request(payload, *, authenticated: bool = True):
    return types.SimpleNamespace(
        json=payload,
        headers=(
            {
                "Authorization": "Bearer secret",
                "X-Request-ID": REQUEST_ID,
            }
            if authenticated
            else {}
        ),
    )


def _coordinates(*, include_digest: bool) -> dict[str, str]:
    payload = {
        "snapshot_id": SNAPSHOT_ID,
        "internal_run_id": INTERNAL_RUN_ID,
        "outer_run_id": OUTER_RUN_ID,
    }
    if include_digest:
        payload["expected_plan_digest"] = PLAN_DIGEST
    return payload


def _private_observation() -> tuple[dict, str, str]:
    raw_provider_marker = "RAW-SYNTHETIC-PROVIDER-MATERIAL"
    raw_source_marker = "https://source.invalid/private-synthetic-file"
    observation = copy.deepcopy(_observation())
    observation["snapshot"]["payload"]["manifest"] = {
        "provider_material": raw_provider_marker,
        "source_url": raw_source_marker,
    }
    observation["source_import_rows"][0]["payload"][
        "source_url"
    ] = raw_source_marker
    return observation, raw_provider_marker, raw_source_marker


def _assert_redacted_ineligible_plan(
    route_response,
    private_markers: tuple[str, ...],
) -> None:
    response_payload = json.loads(route_response.body)
    serialized_payload = route_response.body.decode("utf-8")
    assert route_response.status == 200
    assert response_payload["status"] == "ineligible"
    assert response_payload["eligible"] is False
    assert response_payload["plan_digest"] is None
    assert response_payload["planned_effects"]["external_effects"] == 0
    assert set(response_payload) == {
        "attachment_counts", "attachment_digest", "catalog_digest",
        "contract", "eligible", "event_high_water_mark", "idempotent",
        "plan_digest", "planned_effects", "preserved_row_digest",
        "reason_codes", "retained_state_digest", "source_attempt_digest",
        "stale_age_seconds", "stale_policy_seconds", "status",
        "target_digest",
    }
    assert all(marker not in serialized_payload for marker in private_markers)


def test_legacy_v3_metadata_routes_are_registered_as_separate_actions() -> None:
    route_recorder = _RouteRecorder()

    control_module.register_legacy_v3_metadata_routes(route_recorder)

    assert route_recorder.route_uris == [
        "/ptg/v3/stale-metadata/reconcile-plan",
        "/ptg/v3/stale-metadata/reconcile",
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler_name", "dependency_name", "include_digest"),
    (
        (
            "control_legacy_v3_metadata_plan",
            "plan_legacy_v3_metadata_reconcile",
            False,
        ),
        (
            "control_legacy_v3_metadata_execute",
            "reconcile_legacy_v3_metadata",
            True,
        ),
    ),
)
async def test_legacy_v3_metadata_routes_require_control_auth(
    monkeypatch,
    handler_name: str,
    dependency_name: str,
    include_digest: bool,
) -> None:
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    dependency = AsyncMock()
    monkeypatch.setattr(control_module, dependency_name, dependency)

    with pytest.raises(Forbidden) as forbidden:
        await getattr(control_module, handler_name)(
            _request(
                _coordinates(include_digest=include_digest),
                authenticated=False,
            )
        )

    assert forbidden.value.status_code == 403
    dependency.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("handler_name", "dependency_name", "payload", "missing_field"),
    (
        (
            "control_legacy_v3_metadata_plan",
            "plan_legacy_v3_metadata_reconcile",
            [],
            "snapshot_id",
        ),
        (
            "control_legacy_v3_metadata_execute",
            "reconcile_legacy_v3_metadata",
            "not-an-object",
            "snapshot_id",
        ),
        (
            "control_legacy_v3_metadata_plan",
            "plan_legacy_v3_metadata_reconcile",
            {
                "snapshot_id": {"invalid": "object"},
                "internal_run_id": INTERNAL_RUN_ID,
                "outer_run_id": OUTER_RUN_ID,
            },
            "snapshot_id",
        ),
        (
            "control_legacy_v3_metadata_plan",
            "plan_legacy_v3_metadata_reconcile",
            {
                "internal_run_id": INTERNAL_RUN_ID,
                "outer_run_id": OUTER_RUN_ID,
            },
            "snapshot_id",
        ),
        (
            "control_legacy_v3_metadata_plan",
            "plan_legacy_v3_metadata_reconcile",
            {
                "snapshot_id": SNAPSHOT_ID,
                "outer_run_id": OUTER_RUN_ID,
            },
            "internal_run_id",
        ),
        (
            "control_legacy_v3_metadata_plan",
            "plan_legacy_v3_metadata_reconcile",
            {
                "snapshot_id": SNAPSHOT_ID,
                "internal_run_id": INTERNAL_RUN_ID,
            },
            "outer_run_id",
        ),
        (
            "control_legacy_v3_metadata_execute",
            "reconcile_legacy_v3_metadata",
            _coordinates(include_digest=False),
            "expected_plan_digest",
        ),
    ),
)
async def test_legacy_v3_metadata_routes_reject_invalid_payloads(
    monkeypatch,
    handler_name: str,
    dependency_name: str,
    payload,
    missing_field: str,
) -> None:
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    dependency = AsyncMock()
    monkeypatch.setattr(control_module, dependency_name, dependency)

    with pytest.raises(BadRequest, match=missing_field) as invalid_request:
        await getattr(control_module, handler_name)(_request(payload))

    assert invalid_request.value.status_code == 400
    dependency.assert_not_awaited()


@pytest.mark.asyncio
async def test_ineligible_plan_route_is_redacted_and_not_executable(
    monkeypatch,
) -> None:
    """Expose only a non-executable sanitized plan for rejected state."""

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    observation, raw_provider_marker, raw_source_marker = _private_observation()
    monkeypatch.setattr(
        reconcile,
        "_database_observation",
        AsyncMock(return_value=(observation, False)),
    )
    monkeypatch.setattr(
        reconcile,
        "load_exact_operational_absence",
        AsyncMock(return_value=_operational_absence()),
    )

    route_response = await control_module.control_legacy_v3_metadata_plan(
        _request(
            {
                "snapshot_id": "ptg2:202607:synthetic-v3-orphan",
                "internal_run_id": "ptg2:0000000000000000000000000000000g",
                "outer_run_id": "run_synthetic_legacy_v3",
            }
        )
    )
    _assert_redacted_ineligible_plan(
        route_response,
        (
            raw_provider_marker,
            raw_source_marker,
            "synthetic-source-import-v3",
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "conflict_message",
    (
        "legacy V3 state changed after plan review",
        "legacy V3 target is not eligible: eligibility_changed",
    ),
)
async def test_execute_maps_digest_or_eligibility_drift_to_conflict(
    monkeypatch,
    conflict_message: str,
) -> None:
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    execute_call = AsyncMock(
        side_effect=LegacyV3MetadataConflict(conflict_message)
    )
    monkeypatch.setattr(
        control_module,
        "reconcile_legacy_v3_metadata",
        execute_call,
    )

    with pytest.raises(SanicException) as conflict:
        await control_module.control_legacy_v3_metadata_execute(
            _request(_coordinates(include_digest=True))
        )

    assert conflict.value.status_code == 409
    assert str(conflict.value) == conflict_message
    execute_call.assert_awaited_once_with(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        outer_run_id=OUTER_RUN_ID,
        expected_plan_digest=PLAN_DIGEST,
    )


@pytest.mark.asyncio
async def test_plan_maps_missing_attempt_authority_schema_to_conflict(
    monkeypatch,
) -> None:
    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    plan_call = AsyncMock(
        side_effect=LegacyV3MetadataConflict(
            "source-attempt authority schema is not configured correctly"
        )
    )
    monkeypatch.setattr(
        control_module,
        "plan_legacy_v3_metadata_reconcile",
        plan_call,
    )

    with pytest.raises(SanicException) as conflict:
        await control_module.control_legacy_v3_metadata_plan(
            _request(_coordinates(include_digest=False))
        )

    assert conflict.value.status_code == 409
    assert str(conflict.value) == (
        "source-attempt authority schema is not configured correctly"
    )
    plan_call.assert_awaited_once_with(
        snapshot_id=SNAPSHOT_ID,
        internal_run_id=INTERNAL_RUN_ID,
        outer_run_id=OUTER_RUN_ID,
    )


@pytest.mark.asyncio
async def test_execute_capability_disappearance_conflicts_before_writes(
    monkeypatch,
) -> None:
    session = object()
    capability_check = AsyncMock(
        side_effect=RuntimeError(
            "PTG_SOURCE_ATTEMPT_CAPABILITY_UNAVAILABLE"
        )
    )
    monkeypatch.setattr(
        reconcile,
        "require_source_attempt_capabilities",
        capability_check,
    )
    target_lock = AsyncMock()
    observation_load = AsyncMock()
    monkeypatch.setattr(reconcile, "_lock_reconcile_target", target_lock)
    monkeypatch.setattr(
        reconcile,
        "load_legacy_v3_reconcile_observation",
        observation_load,
    )

    with pytest.raises(
        LegacyV3MetadataConflict,
        match="source-attempt authority capability is unavailable",
    ):
        await reconcile._locked_observation(
            session,
            schema_name="source_attempt_test",
            coordinates=reconcile._coordinates(
                SNAPSHOT_ID,
                INTERNAL_RUN_ID,
                OUTER_RUN_ID,
            ),
        )

    capability_check.assert_awaited_once_with(
        session,
        require_attempt_authority=True,
    )
    target_lock.assert_not_awaited()
    observation_load.assert_not_awaited()


@pytest.mark.asyncio
async def test_execute_reports_postcommit_red_as_applied(monkeypatch) -> None:
    """Return durable mutation truth instead of a conflict after commit."""

    monkeypatch.setenv("HLTHPRT_CONTROL_API_TOKEN", "secret")
    applied_report_by_field = {
        "state": "applied_postcheck_red",
        "acceptance": "red",
        "reconciliation_id": "b" * 64,
        "postcheck_exact_external_absence": False,
        "reason_codes": ["postcommit_external_identity_present"],
        "retry_allowed": False,
        "operator_action": "stop_no_retry",
    }
    monkeypatch.setattr(
        control_module,
        "reconcile_legacy_v3_metadata",
        AsyncMock(return_value=applied_report_by_field),
    )

    route_response = await control_module.control_legacy_v3_metadata_execute(
        _request(_coordinates(include_digest=True))
    )

    assert route_response.status == 200
    assert json.loads(route_response.body) == applied_report_by_field


@pytest.mark.asyncio
async def test_global_source_fence_uses_stable_sanitized_409_contract() -> None:
    application = Sanic("test_legacy_v3_source_fence_contract")
    control_blueprint = Blueprint(
        "test_legacy_v3_source_fence",
        url_prefix="/control",
    )
    register_source_attempt_error_handler(control_blueprint)

    @control_blueprint.get("/fenced")
    async def fenced_route(_request):
        raise PTGSourceAttemptFencedError(
            "PTG source attempt is terminally reconciled"
        )

    application.blueprint(control_blueprint)
    _request_value, route_response = await application.asgi_client.get(
        "/control/fenced",
        headers={
            "X-Request-ID": f"  {REQUEST_ID}  ",
            "X-Synthetic-Private-Material": "must-not-appear",
        },
    )
    response_payload = route_response.json

    assert route_response.status == 409
    assert response_payload == {
        "error": {
            "code": "ptg_source_attempt_terminally_reconciled",
            "message": "PTG source attempt is terminally reconciled",
            "detail": {},
            "request_id": REQUEST_ID,
        }
    }
    assert "must-not-appear" not in route_response.text
