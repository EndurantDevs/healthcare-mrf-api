# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Publisher observation accepts canonical quantities without accepting changed resources."""

import json
from copy import deepcopy
from unittest.mock import Mock

import pytest

from api import control_workers as workers
from api import scoped_catalog_publisher_worker as launcher
from tests.test_scoped_catalog_publisher import _request, _spec
from tests.test_scoped_catalog_worker_repair import _run


@pytest.mark.parametrize(
    "submitted,canonical",
    [("0.5", "500m"), ("1.5Gi", "1536Mi"), ("1e3", "1k"), ("1E6", "1M"), ("1.0", 1), ("1u", "1000n")],
)
def test_equivalent_quantities_have_exact_numeric_equality(submitted, canonical):
    assert launcher._resource_quantity(submitted) == launcher._resource_quantity(canonical)


@pytest.mark.parametrize("quantity", ["NaN", "Infinity", "0.5junk", "1K", "", True, 0.5, None])
def test_malformed_quantity_never_matches(quantity):
    request = _request()
    expected = launcher.manifest(_spec(request), request, "example/image@sha256:" + "a" * 64)
    actual = deepcopy(expected)
    actual["spec"]["template"]["spec"]["containers"][0]["resources"] = {"requests": {"cpu": quantity}}
    assert not launcher._matches(actual, expected)


@pytest.mark.parametrize("changed", [False, True])
def test_canonical_api_response_supports_ensure_state_and_exact_stop(monkeypatch, changed):
    request = _request()
    request.scoped_catalog_admission = True
    image = "example/image@sha256:" + "a" * 64
    profile_by_section = {"requests": {"cpu": "0.5"}, "limits": {"memory": "1.5Gi"}}
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_RESOURCE_PROFILES_JSON", json.dumps({"process.CodeSets": profile_by_section})
    )
    monkeypatch.setattr(workers, "_launcher_mode", lambda: "kubernetes")
    monkeypatch.setattr(workers, "_configured_worker_job_image", lambda *_: image)
    actual = launcher.manifest(_spec(request), request, image)
    actual["metadata"]["uid"] = "synthetic-publisher"
    actual["status"] = {"active": 1}
    actual["spec"]["template"]["spec"]["containers"][0]["resources"] = {
        "requests": {"cpu": "501m" if changed else "500m", "memory": "1536Mi"},
        "limits": {"memory": "1536Mi"},
    }
    api = Mock(return_value=actual)
    monkeypatch.setattr(workers, "_kubernetes_request", api)
    assert launcher.ensure(_spec(request), request)["status"] == ("failed" if changed else "already_running")
    assert launcher.state(request)["status"] == ("failed" if changed else "ok")
    result = launcher.stop_publisher(_run("code-sets"))
    assert result["deleted"] == int(not changed)
    assert [call.args[0] for call in api.call_args_list] == ["GET"] * 3 + ([] if changed else ["DELETE"])
    if not changed:
        assert api.call_args_list[-1].args[2]["preconditions"] == {"uid": "synthetic-publisher"}
