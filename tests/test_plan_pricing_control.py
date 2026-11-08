# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pricing keys cannot substitute another durable request or execution parent."""

from copy import deepcopy
from unittest.mock import AsyncMock

import pytest

from api import control_imports
from process.plan_pricing_control import (
    is_managed_pricing_key,
    projection_admission_key,
    projection_attempt_fence_key,
    projection_retry_key,
    require_pricing_replay,
)
from tests.test_plan_pricing_idempotency import _durable_import_request


@pytest.mark.parametrize("field", ("engine", "node_id", "importer", "idempotency_key", "retry_of_run_id", "params"))
def test_pricing_replay_rejects_each_durable_request_difference(field):
    expected_by_field = {
        **_durable_import_request("plan-pricing-projection", "caller-run"),
        "engine": "synthetic-engine",
        "node_id": "synthetic-node",
        "retry_of_run_id": None,
    }
    existing_by_field = {**expected_by_field, field: {"different": True} if field == "params" else "different"}
    with pytest.raises(ValueError, match="different request"):
        require_pricing_replay(existing_by_field, expected_by_field)


@pytest.mark.parametrize("changed", (True, 1.0))
def test_pricing_replay_keeps_native_json_number_and_boolean_types(changed):
    expected_by_field = {"importer": "plan-pricing-projection", "params": {"bounded": 1}}
    with pytest.raises(ValueError, match="different request"):
        require_pricing_replay({**expected_by_field, "params": {"bounded": changed}}, expected_by_field)


def test_pricing_replay_ignores_new_response_run_id_but_keeps_nested_request():
    expected_by_field = _durable_import_request("plan-pricing-projection", "new-response-id")
    existing_by_field = deepcopy(expected_by_field)
    existing_by_field["run_id"] = "original-id"
    require_pricing_replay(existing_by_field, expected_by_field)


@pytest.mark.asyncio
@pytest.mark.parametrize("collision", (False, True))
async def test_changed_request_never_replays_or_enqueues_even_after_unique_race(monkeypatch, collision):
    request = _durable_import_request("plan-pricing-projection", "new-run")
    existing_by_field = {
        **request,
        "run_id": "original-run",
        "engine": control_imports.ENGINE_NAME,
        "node_id": control_imports.os.getenv("HLTHPRT_IMPORT_NODE_ID"),
        "params": {"changed": True},
        "status": "succeeded",
    }
    monkeypatch.setattr(
        control_imports,
        "find_importer_run_by_idempotency_key",
        AsyncMock(side_effect=(None, existing_by_field) if collision else (existing_by_field,)),
    )
    monkeypatch.setattr(control_imports, "find_earliest_active_run_by_importer", AsyncMock(return_value=None))
    monkeypatch.setattr(
        control_imports,
        "_admit_import_row",
        AsyncMock(side_effect=control_imports.IntegrityError("insert", {}, RuntimeError("duplicate"))),
    )
    enqueue = AsyncMock(side_effect=AssertionError("a conflicting request must not enqueue"))
    monkeypatch.setattr(control_imports, "_enqueue_import_start", enqueue)
    with pytest.raises(ValueError, match="different request"):
        await control_imports.create_import_run(request)
    enqueue.assert_not_awaited()


def test_managed_keys_are_exact_and_domain_bound():
    request_by_field = {
        "binding_manifest_digest": "a" * 64,
        "projection_contract": "plan_pricing_factorized_v4",
        "publication_identity": "b" * 64,
        "publication_attempt_identity": "c" * 64,
    }
    key = projection_admission_key(request_by_field)
    retry = projection_retry_key(key, "original-run")
    assert key != retry and is_managed_pricing_key(key) and is_managed_pricing_key(retry)
    assert not is_managed_pricing_key(key + " ")
    assert not is_managed_pricing_key(key.upper())
    assert projection_attempt_fence_key("b" * 64, "c" * 64) == (
        "control-plan-pricing-attempt:v1:" + "b" * 64 + ":" + "c" * 64
    )


@pytest.mark.parametrize("identity", ("", "a" * 63, "A" * 64, True, None))
def test_attempt_fence_refuses_partial_or_changed_identity(identity):
    with pytest.raises(ValueError, match="invalid"):
        projection_attempt_fence_key("a" * 64, identity)
