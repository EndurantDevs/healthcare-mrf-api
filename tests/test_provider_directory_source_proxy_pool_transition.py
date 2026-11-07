# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""A retained pool expansion preserves prior quota state and credential isolation."""

import json
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source
from tests.test_provider_directory_source_proxy_launch import (
    _BINDING,
    _cooldowns,
    _environment,
    _inject_broad_pool,
    _pool_selector,
    _retry_lineage,
    _worker_request,
    configured_runtime,
    enabled_proxy,
)
from tests.test_provider_directory_source_worker_launch import (
    IMAGE,
    RUN_ID,
    SPEC,
    _canceled_route_parent,
    _source_route_config,
    _source_route_run,
)

POOL_TWO = "synthetic-pool-two"
IDS = [f"proxy-{index:02}" for index in range(50)]


def _grant():
    return {
        "from_revision": "pool-one",
        "from_secret_name": _pool_selector()["secretName"],
        "to_revision": "pool-two",
        "to_secret_name": POOL_TWO,
        "from_proxy_ids": IDS[:3],
        "to_proxy_ids": IDS,
    }


@pytest.fixture
def expanded_proxy(monkeypatch, enabled_proxy):
    request, lineage_rows = _retry_lineage(monkeypatch)
    for row in lineage_rows:
        row["params"]["allow_retained_pagination_reset"] = False
    routes = json.loads(workers.os.environ[source.ROUTES_ENV])
    routes[1]["proxy_pool_transition"] = _grant()
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps(routes))
    selectors = json.loads(workers.os.environ["HLTHPRT_WORKER_JOB_SECRET_ENV_JSON"])
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", json.dumps([*selectors, {**_pool_selector(), "secretName": POOL_TWO}])
    )
    recorded_run_by_field = {**_source_route_run(), "params": lineage_rows[0]["params"]}
    request.fhir_source_launch = source.admit_fhir_source_worker_launch(recorded_run_by_field)
    return request, lineage_rows


@pytest.fixture
def promoted_proxy(monkeypatch, expanded_proxy):
    monkeypatch.setenv(source.PROXY_POOL_REVISION_ENV, "pool-two")
    monkeypatch.setenv(source.PROXY_POOL_SECRET_ENV, POOL_TWO)
    return expanded_proxy


def test_pool_transition_cannot_enter_publication_route_configuration(monkeypatch, expanded_proxy):
    from api.provider_directory_source_publication import ROUTES_ENV as PUBLICATION_ROUTES_ENV

    monkeypatch.setenv(PUBLICATION_ROUTES_ENV, workers.os.environ[source.ROUTES_ENV])
    with pytest.raises(ValueError, match="fhir_source_worker_route_invalid"):
        source._configured_routes(setting=PUBLICATION_ROUTES_ENV)


def _snapshot(revision, entries):
    return {**_cooldowns(entries), "pool_revision": revision}


def _parent(request, lineage_rows, run_id, revision, entries):
    parent = lineage_rows[0]["retry_of_run_id"]
    lineage_rows.append(
        {
            "run_id": run_id,
            "status": "failed",
            "retry_of_run_id": parent,
            "params": {**lineage_rows[0]["params"], "retry_of_run_id": parent},
            "source_http_proxy_binding": {**_BINDING, "pool_revision": revision},
            "source_http_proxy_cooldowns": _snapshot(revision, entries),
        }
    )
    lineage_rows[0]["retry_of_run_id"] = run_id
    lineage_rows[0]["params"]["retry_of_run_id"] = run_id
    request.fhir_source_launch = replace(request.fhir_source_launch, retry_of_run_id=run_id)


@pytest.mark.asyncio
async def test_three_to_fifty_routes_resume_two_retries_without_reset(expanded_proxy):
    request, lineage_rows = expanded_proxy
    lineage_rows[1]["source_http_proxy_cooldowns"] = _cooldowns([(IDS[0], None), (IDS[1], "2026-11-01T12:00:00Z")])
    _parent(request, lineage_rows, "run_" + "c" * 32, "pool-one", [(IDS[1], "2026-11-01T13:00:00Z"), (IDS[2], None)])
    _parent(
        request,
        lineage_rows,
        "run_" + "d" * 32,
        "pool-two",
        [(IDS[0], "2026-12-01T00:00:00Z"), (IDS[1], "2026-11-01T11:00:00Z"), (IDS[3], None)],
    )
    request.fhir_source_launch = await source.require_fhir_source_route_ancestry(
        SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), request.fhir_source_launch
    )
    launch = request.fhir_source_launch
    assert (launch.pool_revision, launch.pool_secret_name) == ("pool-two", POOL_TWO)
    assert (
        launch.image == _source_route_config()["image"] and launch.secret_name == _source_route_config()["secret_name"]
    )
    expected = _snapshot("pool-two", [(IDS[0], None), (IDS[1], "2026-11-01T13:00:00Z"), (IDS[2], None), (IDS[3], None)])
    assert json.loads(launch.proxy_cooldowns_json) == expected
    assert len(launch.proxy_pool_transition.to_proxy_ids) == 50
    assert not set(IDS[4:]).intersection(entry["proxy_id"] for entry in expected["cooldowns"])
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    environment = _environment(job)
    assert environment[source.PROXY_POOL_ENV]["valueFrom"]["secretKeyRef"]["name"] == POOL_TWO
    assert json.loads(environment[source.PROXY_BINDING_ENV]["value"])["pool_revision"] == "pool-two"
    assert json.loads(environment[source.PROXY_COOLDOWNS_ENV]["value"]) == expected
    assert source.is_fhir_job_set_matching(SPEC, request, [job])
    environment[source.PROXY_POOL_ENV]["valueFrom"]["secretKeyRef"]["name"] = _pool_selector()["secretName"]
    assert not source.is_fhir_job_set_matching(SPEC, request, [job])


@pytest.mark.asyncio
async def test_expansion_retains_blocks_across_unstarted_cancellation(expanded_proxy):
    request, lineage_rows = expanded_proxy
    lineage_rows[1]["source_http_proxy_cooldowns"] = _cooldowns([(IDS[0], None)])
    _parent(request, lineage_rows, "run_" + "c" * 32, "pool-one", [])
    lineage_rows[-1].update(_canceled_route_parent(lineage_rows[-1]["run_id"]))
    lineage_rows[-1]["source_http_proxy_binding"] = None
    lineage_rows[-1]["source_http_proxy_cooldowns"] = None
    updated = await source.require_fhir_source_route_ancestry(
        SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), request.fhir_source_launch
    )
    assert json.loads(updated.proxy_cooldowns_json) == _snapshot("pool-two", [(IDS[0], None)])


@pytest.mark.parametrize(
    "damage",
    [
        "unknown-old-id",
        "unknown-new-id",
        "wrong-snapshot-revision",
        "downgrade",
        "root-already-target",
        "binding-source",
        "binding-policy",
        "binding-endpoint",
        "binding-revision",
    ],
)
@pytest.mark.asyncio
async def test_transition_refuses_ungranted_or_regressing_ancestry(expanded_proxy, damage):
    request, lineage_rows = expanded_proxy
    _parent(request, lineage_rows, "run_" + "c" * 32, "pool-two", [(IDS[3], None)])
    if damage.startswith("binding-"):
        fields_by_damage = {
            "binding-source": "source_id",
            "binding-policy": "policy_revision",
            "binding-endpoint": "endpoint_scope",
            "binding-revision": "pool_revision",
        }
        lineage_rows[-1]["source_http_proxy_binding"][fields_by_damage[damage]] = "unrelated"
    else:
        changes_by_damage = {
            "unknown-old-id": lambda: lineage_rows[1].__setitem__(
                "source_http_proxy_cooldowns", _cooldowns([(IDS[3], None)])
            ),
            "unknown-new-id": lambda: lineage_rows[-1].__setitem__(
                "source_http_proxy_cooldowns", _snapshot("pool-two", [("unknown", None)])
            ),
            "wrong-snapshot-revision": lambda: lineage_rows[-1]["source_http_proxy_cooldowns"].__setitem__(
                "pool_revision", "unrelated"
            ),
            "downgrade": lambda: _parent(request, lineage_rows, "run_" + "d" * 32, "pool-one", []),
            "root-already-target": lambda: lineage_rows[1].update(
                source_http_proxy_binding={**_BINDING, "pool_revision": "pool-two"},
                source_http_proxy_cooldowns=_snapshot("pool-two", []),
            ),
        }
        changes_by_damage[damage]()
    with pytest.raises(ValueError):
        await source.require_fhir_source_route_ancestry(
            SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), request.fhir_source_launch
        )


@pytest.mark.parametrize(
    "damage",
    [
        "missing",
        "fresh",
        "equal-revisions",
        "equal-secrets",
        "missing-old-secret",
        "invalid-old-secret",
        "invalid-new-secret",
        "duplicate-id",
        "removed-id",
        "not-expanded",
        "too-many",
        "invalid-id",
        "extra-field",
        "runtime-secret",
        "null",
        "empty-ids",
        "nonlist-ids",
    ],
)
def test_complete_route_inventory_refuses_malformed_expansion(monkeypatch, expanded_proxy, damage):
    routes = json.loads(workers.os.environ[source.ROUTES_ENV])
    grant = routes[1]["proxy_pool_transition"]
    changes_by_damage = {
        "missing": lambda: grant.pop("from_revision"),
        "fresh": lambda: routes[0].__setitem__("proxy_pool_transition", grant),
        "equal-revisions": lambda: grant.__setitem__("to_revision", grant["from_revision"]),
        "equal-secrets": lambda: grant.__setitem__("from_secret_name", grant["to_secret_name"]),
        "missing-old-secret": lambda: grant.pop("from_secret_name"),
        "invalid-old-secret": lambda: grant.__setitem__("from_secret_name", "invalid/name"),
        "invalid-new-secret": lambda: grant.__setitem__("to_secret_name", None),
        "duplicate-id": lambda: grant["to_proxy_ids"].append(IDS[0]),
        "removed-id": lambda: grant["to_proxy_ids"].remove(IDS[0]),
        "not-expanded": lambda: grant.__setitem__("to_proxy_ids", grant["from_proxy_ids"]),
        "too-many": lambda: grant.__setitem__("to_proxy_ids", [f"proxy-{index}" for index in range(65)]),
        "invalid-id": lambda: grant["from_proxy_ids"].__setitem__(0, "https://example.invalid"),
        "runtime-secret": lambda: grant.__setitem__("to_secret_name", routes[1]["secret_name"]),
        "null": lambda: routes[1].__setitem__("proxy_pool_transition", None),
        "empty-ids": lambda: grant.__setitem__("from_proxy_ids", []),
        "nonlist-ids": lambda: grant.__setitem__("to_proxy_ids", "proxy-00"),
        "extra-field": lambda: grant.__setitem__("other", True),
    }
    changes_by_damage[damage]()
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps(routes))
    with pytest.raises(ValueError, match="route_invalid"):
        source._configured_routes()


@pytest.mark.parametrize(
    "field,value",
    [
        ("to_revision", "pool-three"),
        ("from_revision", "unrelated"),
        ("from_secret_name", "different-old-pool"),
        ("to_secret_name", "different-pool"),
        ("to_proxy_ids", IDS + ["extra"]),
        ("from_proxy_ids", IDS[:2]),
    ],
)
def test_grant_change_is_rechecked_before_job_creation(monkeypatch, expanded_proxy, field, value):
    request, _rows = expanded_proxy
    routes = json.loads(workers.os.environ[source.ROUTES_ENV])
    routes[1]["proxy_pool_transition"][field] = value
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps(routes))
    with pytest.raises(ValueError):
        workers._worker_job_manifest(SPEC, request, IMAGE)


def test_serialized_grant_is_not_a_launch_capability(expanded_proxy):
    request, _rows = expanded_proxy
    request.fhir_source_launch = replace(
        request.fhir_source_launch, proxy_pool_transition=asdict(request.fhir_source_launch.proxy_pool_transition)
    )
    with pytest.raises(ValueError):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.asyncio
async def test_native_ancestry_refuses_a_serialized_grant(expanded_proxy):
    request, lineage_rows = expanded_proxy
    launch = replace(
        request.fhir_source_launch, proxy_pool_transition=asdict(request.fhir_source_launch.proxy_pool_transition)
    )
    with pytest.raises(ValueError, match="route_changed"):
        await source.require_fhir_source_route_ancestry(
            SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), launch
        )


@pytest.mark.parametrize("value", [None, True, 0, "false", "missing"])
@pytest.mark.parametrize("scope", ["current", "root", "retry"])
@pytest.mark.asyncio
async def test_transition_requires_explicit_no_reset_on_native_lineage(expanded_proxy, scope, value):
    request, lineage_rows = expanded_proxy
    _parent(request, lineage_rows, "run_" + "c" * 32, "pool-one", [])
    row = lineage_rows[0] if scope == "current" else lineage_rows[1] if scope == "root" else lineage_rows[-1]
    if value == "missing":
        del row["params"]["allow_retained_pagination_reset"]
    else:
        row["params"]["allow_retained_pagination_reset"] = value
    with pytest.raises(ValueError, match="route_changed"):
        await source.require_fhir_source_route_ancestry(
            SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), request.fhir_source_launch
        )


@pytest.mark.parametrize("value", [None, True, 0, "false"])
def test_transition_refuses_reset_before_capability_capture(expanded_proxy, value):
    _request, lineage_rows = expanded_proxy
    params_by_name = {**lineage_rows[0]["params"], "allow_retained_pagination_reset": value}
    with pytest.raises(ValueError, match="route_changed"):
        source.admit_fhir_source_worker_launch({**_source_route_run(), "params": params_by_name})


@pytest.mark.parametrize("mode", ["enabled", "unopted", "nonfhir"])
def test_local_pool_cannot_escape_through_broad_environment(monkeypatch, expanded_proxy, mode):
    spec, request = _worker_request(mode)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", POOL_TWO)
    with pytest.raises(ValueError, match="proxy_pool_broad_secret_forbidden"):
        workers._worker_job_manifest(spec, request, IMAGE)


@pytest.mark.parametrize("mode", ["enabled", "unopted", "nonfhir"])
def test_route_local_pool_cannot_enter_a_direct_mount(monkeypatch, expanded_proxy, mode):
    spec, request = _worker_request(mode)
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps([{"name": "pool-copy", "secretName": POOL_TWO, "mountPath": "/synthetic-pool"}]),
    )
    with pytest.raises(ValueError, match="proxy_pool_broad_secret_forbidden"):
        workers._worker_job_manifest(spec, request, IMAGE)


def test_route_local_secret_is_reserved_without_a_pool_key(expanded_proxy):
    request, _rows = expanded_proxy
    selector_by_field = {"name": "OTHER", "key": "OTHER", "secretName": POOL_TWO}
    assert source.requires_source_proxy_admission(selector_by_field)
    with pytest.raises(ValueError, match="proxy pool selector"):
        source._is_launch_secret_selected(selector_by_field, request.fhir_source_launch, allow_shared=True)


@pytest.mark.parametrize("path", ["envfrom", "init-envfrom", "secret-volume", "projected-volume"])
@pytest.mark.parametrize("mode", ["enabled", "unopted", "nonfhir"])
def test_reuse_rejects_route_local_pool_leaks(monkeypatch, expanded_proxy, path, mode):
    spec, request = _worker_request(mode)
    job = workers._worker_job_manifest(spec, request, IMAGE)
    _inject_broad_pool(job, path)

    def replace_secret(value):
        if isinstance(value, dict):
            return {key: replace_secret(item) for key, item in value.items()}
        if isinstance(value, list):
            return [replace_secret(item) for item in value]
        return POOL_TWO if value == _pool_selector()["secretName"] else value

    job = replace_secret(job)
    assert workers._has_broad_proxy_pool_reference(job)
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *_args: {"items": [job]})
    assert workers._kubernetes_worker_state(spec, request)["job_status"] == "purpose_mismatch"


def test_expansion_does_not_change_fresh_or_unopted_launches(expanded_proxy):
    for mode in ("enabled", "unopted"):
        spec, request = _worker_request(mode)
        launch = request.fhir_source_launch
        assert launch.proxy_pool_transition is None
        assert launch.pool_revision == ("pool-one" if mode == "enabled" else "")
        job = workers._worker_job_manifest(spec, request, IMAGE)
        assert POOL_TWO not in json.dumps(job)


@pytest.mark.parametrize("global_revision", ["pool-one", "pool-two"])
@pytest.mark.asyncio
async def test_matching_global_pairs_keep_retained_target(monkeypatch, expanded_proxy, global_revision):
    request, lineage_rows = expanded_proxy
    global_secret = _pool_selector()["secretName"] if global_revision == "pool-one" else POOL_TWO
    monkeypatch.setenv(source.PROXY_POOL_REVISION_ENV, global_revision)
    monkeypatch.setenv(source.PROXY_POOL_SECRET_ENV, global_secret)
    captured = source.admit_fhir_source_worker_launch({**_source_route_run(), "params": lineage_rows[0]["params"]})
    assert captured == request.fhir_source_launch
    lineage_rows[1]["source_http_proxy_cooldowns"] = _cooldowns([(IDS[0], None)])
    request.fhir_source_launch = await source.require_fhir_source_route_ancestry(
        SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), captured
    )
    environment = _environment(workers._worker_job_manifest(SPEC, request, IMAGE))
    assert environment[source.PROXY_POOL_ENV]["valueFrom"]["secretKeyRef"]["name"] == POOL_TWO
    assert json.loads(environment[source.PROXY_COOLDOWNS_ENV]["value"]) == _snapshot("pool-two", [(IDS[0], None)])


@pytest.mark.parametrize(
    "revision,secret",
    [
        ("pool-one", POOL_TWO),
        ("pool-two", _pool_selector()["secretName"]),
        ("unrelated-revision", POOL_TWO),
        ("pool-two", "unrelated-secret"),
    ],
)
def test_global_pair_must_match_one_complete_declared_pair(monkeypatch, expanded_proxy, revision, secret):
    request, lineage_rows = expanded_proxy
    monkeypatch.setenv(source.PROXY_POOL_REVISION_ENV, revision)
    monkeypatch.setenv(source.PROXY_POOL_SECRET_ENV, secret)
    with pytest.raises(ValueError, match="proxy_configuration_invalid"):
        source.admit_fhir_source_worker_launch({**_source_route_run(), "params": lineage_rows[0]["params"]})
    with pytest.raises(ValueError, match="proxy_configuration_invalid"):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.parametrize("field", ["from_secret_name", "to_secret_name"])
@pytest.mark.parametrize("route_index", [0, 1])
def test_neither_pool_may_share_any_runtime_secret(monkeypatch, expanded_proxy, field, route_index):
    routes = json.loads(workers.os.environ[source.ROUTES_ENV])
    routes[0]["secret_name"] = "another-runtime-secret"
    routes[1]["proxy_pool_transition"][field] = routes[route_index]["secret_name"]
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps(routes))
    with pytest.raises(ValueError, match="route_invalid"):
        source.configured_source_proxy_pool_secrets()


@pytest.mark.parametrize("mode", ["enabled", "unopted", "nonfhir"])
@pytest.mark.parametrize("path", ["envfrom", "mount"])
def test_old_pool_stays_reserved_after_global_promotion(monkeypatch, promoted_proxy, mode, path):
    spec, request = _worker_request(mode)
    old_secret = _pool_selector()["secretName"]
    if path == "envfrom":
        monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", old_secret)
    else:
        monkeypatch.setenv(
            "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
            json.dumps([{"name": "old-pool-copy", "secretName": old_secret, "mountPath": "/synthetic-pool"}]),
        )
    with pytest.raises(ValueError, match="proxy_pool_broad_secret_forbidden"):
        workers._worker_job_manifest(spec, request, IMAGE)


@pytest.mark.parametrize("mode", ["enabled", "unopted", "nonfhir"])
@pytest.mark.parametrize("path", ["envfrom", "init-envfrom", "secret-volume", "projected-volume"])
def test_reuse_refuses_old_pool_after_global_promotion(monkeypatch, promoted_proxy, mode, path):
    spec, request = _worker_request(mode)
    job = workers._worker_job_manifest(spec, request, IMAGE)
    _inject_broad_pool(job, path)
    assert workers._has_broad_proxy_pool_reference(job)
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *_args: {"items": [job]})
    assert workers._kubernetes_worker_state(spec, request)["job_status"] == "purpose_mismatch"


def test_promoted_pool_keeps_old_selector_reserved_and_gives_fresh_target(promoted_proxy):
    selector_by_field = {"name": "OTHER", "key": "OTHER", "secretName": _grant()["from_secret_name"]}
    assert source.requires_source_proxy_admission(selector_by_field)
    spec, request = _worker_request("enabled")
    launch = request.fhir_source_launch
    assert launch.proxy_pool_transition is None
    assert (launch.pool_revision, launch.pool_secret_name) == ("pool-two", POOL_TWO)
    environment = _environment(workers._worker_job_manifest(spec, request, IMAGE))
    assert environment[source.PROXY_POOL_ENV]["valueFrom"]["secretKeyRef"]["name"] == POOL_TWO
    assert json.loads(environment[source.PROXY_BINDING_ENV]["value"]) == {**_BINDING, "pool_revision": "pool-two"}
    assert json.loads(environment[source.PROXY_COOLDOWNS_ENV]["value"]) == _snapshot("pool-two", [])
    with pytest.raises(ValueError, match="proxy pool selector"):
        source._is_launch_secret_selected(selector_by_field, launch, allow_shared=True)


def test_promoted_global_does_not_allow_captured_old_secret_drift(monkeypatch, promoted_proxy):
    request, _rows = promoted_proxy
    routes = json.loads(workers.os.environ[source.ROUTES_ENV])
    routes[1]["proxy_pool_transition"]["from_secret_name"] = "different-old-secret"
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps(routes))
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(SPEC, request, IMAGE)
