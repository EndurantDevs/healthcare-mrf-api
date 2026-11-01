# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source proxy credentials require the exact native route and frozen policy."""

import json
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import asdict, replace
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import control_workers as workers
from api import provider_directory_source_worker as source
from process.ext.source_http_proxy import SourceProxyConfig
from tests.test_provider_directory_fresh_source_route import _admitted, _fresh_route, _fresh_run, configured_runtime
from tests.test_provider_directory_source_worker_launch import (
    IMAGE,
    RUN_ID,
    SPEC,
    _source_route_config,
    _source_route_run,
)

_POLICY = {
    "version": 1,
    "revision": "policy-one",
    "sources": [
        {"source_id": "synthetic-source", "endpoint_scope": "https://example.test/fhir", "use_proxy": True},
        {"source_id": "another-source", "endpoint_scope": "https://other.example.test/fhir", "use_proxy": True},
    ],
}
_POOL_SECRET = "synthetic-source-proxy-pool"
_BINDING = {
    "source_id": "synthetic-source",
    "endpoint_scope": "https://example.test/fhir",
    "policy_revision": "policy-one",
    "pool_revision": "pool-one",
}


def _pool_selector():
    return {
        "name": source.PROXY_POOL_ENV,
        "key": source.PROXY_POOL_ENV,
        "secretName": _POOL_SECRET,
        "sourceProxyPool": True,
        "purposes": [source.PURPOSE],
        "importers": ["provider-directory-fhir"],
        "workerClasses": [SPEC.worker_class],
    }


@pytest.fixture
def enabled_proxy(monkeypatch, configured_runtime):
    monkeypatch.setenv(source.PROXY_POLICY_ENV, json.dumps(_POLICY))
    monkeypatch.setenv(source.PROXY_POOL_SECRET_ENV, _POOL_SECRET)
    monkeypatch.setenv(source.PROXY_POOL_REVISION_ENV, "pool-one")
    monkeypatch.setenv(source.PROXY_POOL_ENV, "credential pool must never be parsed by the API")
    monkeypatch.setattr(SourceProxyConfig, "bind", Mock(side_effect=AssertionError("API loaded pool credentials")))
    selectors = json.loads(workers.os.environ["HLTHPRT_WORKER_JOB_SECRET_ENV_JSON"])
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", json.dumps([*selectors, _pool_selector()]))


def _environment(job):
    return {entry["name"]: entry for entry in job["spec"]["template"]["spec"]["containers"][0]["env"]}


def _cooldowns(entries=()):
    return {
        "version": 1,
        "pool_revision": "pool-one",
        "origin": "https://example.test",
        "cooldowns": [{"proxy_id": proxy_id, "retry_not_before": boundary} for proxy_id, boundary in entries],
    }


def _retry_lineage(monkeypatch):
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_fresh_route(), _source_route_config()]))
    recorded = _source_route_run()
    root = _source_route_config()["root_run_id"]
    recorded["params"]["retry_of_run_id"] = root
    request = _admitted(recorded)
    lineage_rows = [
        {"run_id": RUN_ID, "status": "queued", "retry_of_run_id": root, "params": recorded["params"]},
        {
            "run_id": root,
            "status": "failed",
            "retry_of_run_id": None,
            "params": {
                "source_ids": ["synthetic-source"],
                "provider_directory_endpoint_scope": _BINDING["endpoint_scope"],
            },
            "source_http_proxy_binding": deepcopy(_BINDING),
            "source_http_proxy_cooldowns": _cooldowns(),
        },
    ]
    return request, lineage_rows


def _insert_retry_parent(request, lineage_rows, binding, cooldowns):
    child = "run_" + "c" * 32
    root = lineage_rows[1]["run_id"]
    request.fhir_source_launch = replace(request.fhir_source_launch, retry_of_run_id=child)
    lineage_rows[0]["retry_of_run_id"] = child
    lineage_rows[0]["params"]["retry_of_run_id"] = child
    lineage_rows.append(
        {
            "run_id": child,
            "status": "failed",
            "retry_of_run_id": root,
            "params": {**lineage_rows[0]["params"], "retry_of_run_id": root},
            "source_http_proxy_binding": binding,
            "source_http_proxy_cooldowns": cooldowns,
        }
    )


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize(
    "params_by_name",
    [
        {},
        {"source_ids": None},
        {"source_ids": []},
        {"source_ids": "synthetic-source"},
        {"source_ids": "unrelated-source,synthetic-source"},
        {"source_id": "synthetic-source"},
        {"provider_directory_source_ids": ["synthetic-source"]},
        {"provider_directory_source_id": "synthetic-source"},
        {"source_query": "synthetic", "limit": 1},
        {"source_ids": [""]},
        {"source_ids": [" synthetic-source"]},
        {"source_ids": [None]},
        {"probe": False, "import_resources": True},
        {"probe": False, "refresh_preset": "monthly-full"},
        {"provider_directory_dataset_followup_stage": "profile"},
        {"publish_artifacts": True},
        {"publish_after_acquisition": True},
    ],
)
def test_source_http_requires_canonical_scope_before_policy_can_be_omitted(params_by_name):
    recorded = _fresh_run()
    recorded["params"] = params_by_name
    with pytest.raises(ValueError, match="proxy_source_scope_required"):
        source.admit_fhir_source_worker_launch(recorded)
    SourceProxyConfig.bind.assert_not_called()


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize(
    "source_scope",
    [
        {},
        {"source_ids": ["synthetic-source"]},
        {"source_ids": ["synthetic-source"], "provider_directory_endpoint_scope": "https://example.test/fhir"},
        {"source_ids": ["another-source"], "provider_directory_endpoint_scope": "https://other.example.test/fhir"},
    ],
)
@pytest.mark.parametrize(
    "params_by_name",
    [
        {"seed_only": True},
        {"seed_only": True, "probe": True, "import_resources": True},
        {"probe": False, "import_resources": False},
        {"publish_artifacts_only": True},
        {"dataset_rehydrate_only": True},
        {"dataset_followup_only": True},
        {"canonical_backfill_only": True},
        {"contact_backfill_only": True},
    ],
)
def test_local_modes_keep_ordinary_admission_without_proxy_credentials(params_by_name, source_scope):
    recorded = _fresh_run()
    recorded["params"] = {**params_by_name, **source_scope}
    request = _admitted(recorded)
    assert request.fhir_source_launch.image == IMAGE
    assert request.fhir_source_launch.source_id == request.fhir_source_launch.secret_name == ""
    assert request.fhir_source_launch.pool_secret_name == ""
    environment = _environment(workers._worker_job_manifest(SPEC, request, IMAGE))
    assert not {
        source.PROXY_POOL_ENV,
        source.PROXY_POLICY_ENV,
        source.PROXY_BINDING_ENV,
        source.PROXY_COOLDOWNS_ENV,
    }.intersection(environment)


@pytest.mark.usefixtures("enabled_proxy")
def test_disabled_policy_keeps_legacy_unscoped_admission(monkeypatch):
    policy_by_field = deepcopy(_POLICY)
    for entry in policy_by_field["sources"]:
        entry["use_proxy"] = False
    monkeypatch.setenv(source.PROXY_POLICY_ENV, json.dumps(policy_by_field))
    recorded = _fresh_run()
    recorded["params"] = {}
    assert source.admit_fhir_source_worker_launch(recorded).pool_secret_name == ""


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize("source_id", ["unrelated-source", "synthetic-source", "another-source"])
def test_serving_recovery_keeps_its_existing_validator_and_exact_source(source_id):
    from process import provider_directory_serving_recovery as recovery

    recorded = _fresh_run()
    recorded["params"] = {
        "source_ids": [source_id],
        recovery.OPERATION_PARAM: "recover",
        recovery.GENERATION_PARAM: "6b75740a-7c8a-556a-8e3c-21a9718c88f1",
    }
    launch = source.admit_fhir_source_worker_launch(recorded)
    assert launch.image == IMAGE and launch.source_id == launch.secret_name == launch.pool_secret_name == ""
    recorded["params"][recovery.GENERATION_PARAM] = "invalid"
    with pytest.raises(ValueError, match="generation_invalid"):
        source.admit_fhir_source_worker_launch(recorded)


def _worker_request(mode):
    if mode == "nonfhir":
        spec = workers._BY_WORKER_CLASS["process.DrugClaims_finish"]
        return spec, {"run_id": RUN_ID, "importer": spec.importers[0]}
    recorded = _fresh_run()
    if mode == "unopted":
        recorded["params"]["source_ids"] = ["unrelated-source"]
    return SPEC, _admitted(recorded)


@pytest.mark.usefixtures("enabled_proxy")
def test_exact_proxy_route_captures_nonsecret_identity_and_only_selected_pool():
    request = _admitted(_fresh_run())
    launch = request.fhir_source_launch
    assert (launch.policy_revision, launch.pool_revision, launch.pool_secret_name) == (
        "policy-one",
        "pool-one",
        _POOL_SECRET,
    )
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    environment = _environment(job)
    assert json.loads(environment[source.PROXY_POLICY_ENV]["value"]) == {**_POLICY, "sources": _POLICY["sources"][:1]}
    assert json.loads(environment[source.PROXY_BINDING_ENV]["value"]) == _BINDING
    assert json.loads(environment[source.PROXY_COOLDOWNS_ENV]["value"]) == {
        "version": 1,
        "pool_revision": "pool-one",
        "origin": "https://example.test",
        "cooldowns": [],
    }
    assert environment[source.PROXY_POOL_ENV] == {
        "name": source.PROXY_POOL_ENV,
        "valueFrom": {"secretKeyRef": {"name": _POOL_SECRET, "key": source.PROXY_POOL_ENV}},
    }
    assert environment["SOURCE_USER"]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-successor"
    assert environment["SHARED_DATABASE_DSN"]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-database"
    assert job["metadata"]["annotations"]["healthporta.com/source-proxy-policy-revision"] == "policy-one"
    assert job["metadata"]["annotations"]["healthporta.com/source-proxy-pool-revision"] == "pool-one"
    assert source.is_fhir_job_set_matching(SPEC, request, [job])
    assert source.is_fhir_source_job_matching(job, launch)
    SourceProxyConfig.bind.assert_not_called()


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize("source_id", ["unrelated-source-a", "unrelated-source-b", "unrelated-source-c"])
def test_proxy_pool_does_not_change_default_sources(source_id):
    recorded = _fresh_run()
    recorded["params"]["source_ids"] = [source_id]
    request = _admitted(recorded)
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    assert request.fhir_source_launch.pool_secret_name == ""
    assert not {source.PROXY_POLICY_ENV, source.PROXY_BINDING_ENV, source.PROXY_POOL_ENV}.intersection(
        _environment(job)
    )
    assert _environment(job)["SOURCE_USER"]["valueFrom"]["secretKeyRef"]["name"] == "synthetic-default"
    assert source.is_fhir_source_job_matching(job, request.fhir_source_launch)


@pytest.mark.usefixtures("enabled_proxy")
def test_disabled_route_keeps_its_runtime_without_proxy(monkeypatch):
    policy = deepcopy(_POLICY)
    policy["sources"][0]["use_proxy"] = False
    monkeypatch.setenv(source.PROXY_POLICY_ENV, json.dumps(policy))
    request = _admitted(_fresh_run())
    assert request.fhir_source_launch.pool_secret_name == ""
    assert source.PROXY_POOL_ENV not in _environment(workers._worker_job_manifest(SPEC, request, IMAGE))


@pytest.mark.usefixtures("enabled_proxy")
def test_enabled_source_requires_an_explicit_compatible_route(monkeypatch):
    monkeypatch.delenv(source.ROUTES_ENV)
    with pytest.raises(ValueError, match="proxy_route_required"):
        _admitted(_fresh_run())


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize(
    ("setting", "value"),
    [
        (source.PROXY_POOL_SECRET_ENV, ""),
        (source.PROXY_POOL_SECRET_ENV, "INVALID"),
        (source.PROXY_POOL_SECRET_ENV, "synthetic-successor"),
        (source.PROXY_POOL_REVISION_ENV, ""),
        (source.PROXY_POOL_REVISION_ENV, "invalid revision"),
    ],
)
def test_proxy_capture_requires_valid_separate_pool_reference(monkeypatch, setting, value):
    monkeypatch.setenv(setting, value)
    with pytest.raises(ValueError, match="proxy_configuration_invalid"):
        _admitted(_fresh_run())


@pytest.mark.usefixtures("enabled_proxy")
def test_serialized_capability_cannot_select_proxy_pool():
    request = _admitted(_fresh_run())
    request_by_field = {**request, "fhir_source_launch": asdict(request.fhir_source_launch)}
    assert not source.is_fhir_source_secret_selected(_pool_selector(), SPEC.worker_class, request_by_field)
    assert source.source_proxy_annotations(SPEC, request_by_field) == {}
    assert source.source_proxy_environment(SPEC, request_by_field, []) == []
    request.fhir_source_launch = asdict(request.fhir_source_launch)
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("policy_revision", "other-policy"),
        ("pool_revision", "other-pool"),
        ("pool_secret_name", "other-secret"),
        ("pool_secret_name", ""),
    ],
)
def test_mutated_proxy_capability_is_rejected(field, value):
    request = _admitted(_fresh_run())
    request.fhir_source_launch = replace(request.fhir_source_launch, **{field: value})
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize("mode", ["unrouted", "disabled-route", "fresh"])
def test_launch_cannot_invent_prior_proxy_cooldowns(monkeypatch, mode):
    recorded = _fresh_run()
    if mode == "unrouted":
        recorded["params"]["source_ids"] = ["unrelated-source"]
    elif mode == "disabled-route":
        policy_by_field = deepcopy(_POLICY)
        policy_by_field["sources"][0]["use_proxy"] = False
        monkeypatch.setenv(source.PROXY_POLICY_ENV, json.dumps(policy_by_field))
    request = _admitted(recorded)
    request.fhir_source_launch = replace(
        request.fhir_source_launch,
        proxy_cooldowns_json=json.dumps(
            _cooldowns([("synthetic-egress", None)]), sort_keys=True, separators=(",", ":")
        ),
    )
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(SPEC, request, IMAGE)
    SourceProxyConfig.bind.assert_not_called()


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize("encoded_snapshot", [None, " " * 65537, json.dumps(_cooldowns())])
def test_launch_rejects_unbounded_or_noncanonical_cooldowns(encoded_snapshot):
    request = _admitted(_fresh_run())
    request.fhir_source_launch = replace(request.fhir_source_launch, proxy_cooldowns_json=encoded_snapshot)
    with pytest.raises(ValueError, match="proxy_cooldowns_invalid"):
        workers._worker_job_manifest(SPEC, request, IMAGE)
    SourceProxyConfig.bind.assert_not_called()


@pytest.mark.usefixtures("enabled_proxy")
def test_enabled_capability_cannot_be_downgraded_to_direct():
    request = _admitted(_fresh_run())
    request.fhir_source_launch = replace(
        request.fhir_source_launch, policy_revision="", pool_revision="", pool_secret_name=""
    )
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.usefixtures("enabled_proxy")
def test_request_cannot_replace_captured_policy_or_pool_reference():
    request = _admitted(_fresh_run())
    request.update(
        {
            source.PROXY_POLICY_ENV: {},
            source.PROXY_POOL_SECRET_ENV: "other-secret",
            source.PROXY_POOL_REVISION_ENV: "other-pool",
            "params": {"source_ids": ["other-source"]},
        }
    )
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    assert json.loads(_environment(job)[source.PROXY_BINDING_ENV]["value"]) == _BINDING


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize(
    "setting", [source.PROXY_POOL_SECRET_ENV, source.PROXY_POOL_REVISION_ENV, source.PROXY_POLICY_ENV]
)
def test_server_configuration_drift_cannot_silently_replace_capture(monkeypatch, setting):
    request = _admitted(_fresh_run())
    value = "other-pool" if setting != source.PROXY_POLICY_ENV else json.dumps({**_POLICY, "revision": "policy-two"})
    monkeypatch.setenv(setting, value)
    with pytest.raises(ValueError, match="admission_unavailable"):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize(
    "change",
    [
        {"sourceProxyPool": "true"},
        {"sourceProxyPool": 1},
        {"sourceProxyPool": False},
        {"sharedForRoutes": True},
        {"optional": True},
        {"name": "OTHER"},
        {"key": "other"},
        {"purposes": []},
        {"importers": ["other"]},
    ],
)
def test_proxy_selector_cannot_broaden_or_rename_pool(change):
    request = _admitted(_fresh_run())
    with pytest.raises(ValueError, match="selector is invalid"):
        workers._is_admitted_importer_selected({**_pool_selector(), **change}, SPEC.worker_class, request)


@pytest.mark.usefixtures("enabled_proxy")
def test_proxy_marker_cannot_bypass_purpose_scope_or_mount_pool():
    request = _admitted(_fresh_run())
    selector = _pool_selector()
    del selector["purposes"]
    with pytest.raises(ValueError, match="selector is invalid"):
        workers._is_admitted_importer_selected(selector, SPEC.worker_class, request)
    with pytest.raises(ValueError, match="selector is invalid"):
        source.is_fhir_source_volume_selected(_pool_selector(), SPEC.worker_class, request.fhir_source_launch)


@pytest.mark.usefixtures("enabled_proxy")
def test_false_pool_marker_preserves_ordinary_runtime_selector():
    request = _admitted(_fresh_run())
    selector_by_field = {
        **_pool_selector(),
        "sourceProxyPool": False,
        "name": "SOURCE_USER",
        "key": "username",
        "secretName": "synthetic-successor",
    }
    assert source.is_fhir_source_secret_selected(selector_by_field, SPEC.worker_class, request)


@pytest.mark.usefixtures("enabled_proxy")
def test_unmarked_or_different_pool_cannot_supply_credentials():
    request = _admitted(_fresh_run())
    selector_by_field = {**_pool_selector(), "name": "OTHER", "key": "other", "secretName": " " + _POOL_SECRET + " "}
    del selector_by_field["sourceProxyPool"]
    with pytest.raises(ValueError, match="selector is invalid"):
        source.is_fhir_source_secret_selected(selector_by_field, SPEC.worker_class, request)
    assert not source.is_fhir_source_secret_selected(
        {**_pool_selector(), "secretName": "other-pool"}, SPEC.worker_class, request
    )


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize(
    "environment_name",
    [source.PROXY_POLICY_ENV, source.PROXY_BINDING_ENV, source.PROXY_POOL_ENV, source.PROXY_COOLDOWNS_ENV],
)
def test_secret_selectors_cannot_inject_reserved_literals(environment_name):
    request = _admitted(_fresh_run())
    selector_by_field = {
        **_pool_selector(),
        "name": " " + environment_name + " ",
        "key": "other",
        "secretName": "synthetic-successor",
    }
    del selector_by_field["sourceProxyPool"]
    with pytest.raises(ValueError, match="selector is invalid"):
        workers._is_admitted_importer_selected(selector_by_field, SPEC.worker_class, request)


@pytest.mark.usefixtures("enabled_proxy")
def test_missing_pool_selector_refuses_enabled_job(monkeypatch):
    request = _admitted(_fresh_run())
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_SECRET_ENV_JSON", "[]")
    with pytest.raises(ValueError, match="proxy_configuration_invalid"):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize(
    "drift", ["policy", "binding", "pool-secret", "pool-key", "pool-optional", "policy-annotation", "pool-annotation"]
)
def test_proxy_job_reuse_checks_every_binding_field(drift):
    request = _admitted(_fresh_run())
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    environment = _environment(job)
    if drift in {"policy", "binding"}:
        environment[source.PROXY_POLICY_ENV if drift == "policy" else source.PROXY_BINDING_ENV]["value"] = "{}"
    elif drift.startswith("pool-") and drift != "pool-annotation":
        reference = environment[source.PROXY_POOL_ENV]["valueFrom"]["secretKeyRef"]
        reference[{"pool-secret": "name", "pool-key": "key", "pool-optional": "optional"}[drift]] = (
            True if drift == "pool-optional" else "other"
        )
    else:
        name = "policy" if drift == "policy-annotation" else "pool"
        job["metadata"]["annotations"][f"healthporta.com/source-proxy-{name}-revision"] = "other"
    assert not source.is_fhir_job_set_matching(SPEC, request, [job])
    assert not source.is_fhir_source_job_matching(job, request.fhir_source_launch)


@pytest.mark.usefixtures("enabled_proxy")
def test_direct_job_cannot_reuse_injected_pool_or_binding():
    recorded = _fresh_run()
    recorded["params"]["source_ids"] = ["unrelated-source"]
    request = _admitted(recorded)
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    job["spec"]["template"]["spec"]["containers"][0]["env"].append({"name": source.PROXY_POOL_ENV, "value": "injected"})
    assert not source.is_fhir_job_set_matching(SPEC, request, [job])


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
@pytest.mark.parametrize("damage", [None, "missing", "policy_revision", "pool_revision", "source_id", "endpoint_scope"])
async def test_proxy_retry_requires_root_observed_binding(monkeypatch, damage):
    monkeypatch.setenv(source.ROUTES_ENV, json.dumps([_fresh_route(), _source_route_config()]))
    recorded = _source_route_run()
    root = _source_route_config()["root_run_id"]
    recorded["params"]["retry_of_run_id"] = root
    launch = _admitted(recorded).fhir_source_launch
    root_binding = deepcopy(_BINDING)
    if damage not in {None, "missing"}:
        root_binding[damage] = "other"
    lineage_rows = [
        {"run_id": RUN_ID, "status": "queued", "retry_of_run_id": root, "params": recorded["params"]},
        {
            "run_id": root,
            "status": "failed",
            "retry_of_run_id": None,
            "params": {
                "source_ids": ["synthetic-source"],
                "provider_directory_endpoint_scope": _BINDING["endpoint_scope"],
            },
            "source_http_proxy_binding": None if damage == "missing" else root_binding,
            "source_http_proxy_cooldowns": {
                "version": 1,
                "pool_revision": "pool-one",
                "origin": "https://example.test",
                "cooldowns": [],
            },
        },
    ]
    connection = SimpleNamespace(all=AsyncMock(return_value=lineage_rows))
    if damage is None:
        updated = await source.require_fhir_source_route_ancestry(connection, launch)
        assert updated is not launch and json.loads(updated.proxy_cooldowns_json) == _cooldowns()
    else:
        with pytest.raises(ValueError, match="route_changed"):
            await source.require_fhir_source_route_ancestry(connection, launch)
    assert "metrics->'audit'->'source_http_proxy'->'binding' AS source_http_proxy_binding" in str(
        connection.all.await_args.args[0]
    )


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
async def test_retry_merges_all_ancestors_without_shortening_blocks(monkeypatch):
    request, lineage_rows = _retry_lineage(monkeypatch)
    root = lineage_rows[1]["run_id"]
    child = "run_" + "c" * 32
    request.fhir_source_launch = replace(request.fhir_source_launch, retry_of_run_id=child)
    lineage_rows[0]["retry_of_run_id"] = child
    lineage_rows[0]["params"]["retry_of_run_id"] = child
    lineage_rows[1]["source_http_proxy_cooldowns"] = _cooldowns(
        [
            ("first", "2026-11-01T00:00:00Z"),
            ("second", None),
            ("third", "2026-11-01T01:00:00Z"),
            ("fifth", "2026-12-01T00:00:00Z"),
        ]
    )
    lineage_rows.append(
        {
            "run_id": child,
            "status": "failed",
            "retry_of_run_id": root,
            "params": {**lineage_rows[0]["params"], "retry_of_run_id": root},
            "source_http_proxy_binding": deepcopy(_BINDING),
            "source_http_proxy_cooldowns": _cooldowns(
                [
                    ("first", "2026-11-01T00:00:00.5Z"),
                    ("second", "2026-12-01T00:00:00Z"),
                    ("third", "2026-10-31T01:00:00Z"),
                    ("fourth", None),
                    ("fifth", None),
                ]
            ),
        }
    )
    connection = SimpleNamespace(all=AsyncMock(return_value=lineage_rows))
    original = request.fhir_source_launch
    request.fhir_source_launch = await source.require_fhir_source_route_ancestry(connection, original)
    assert request.fhir_source_launch is not original and original.proxy_cooldowns_json == ""
    expected_by_field = _cooldowns(
        [
            ("fifth", None),
            ("first", "2026-11-01T00:00:00.500000Z"),
            ("fourth", None),
            ("second", None),
            ("third", "2026-11-01T01:00:00Z"),
        ]
    )
    assert json.loads(request.fhir_source_launch.proxy_cooldowns_json) == expected_by_field
    job = workers._worker_job_manifest(SPEC, request, IMAGE)
    assert json.loads(_environment(job)[source.PROXY_COOLDOWNS_ENV]["value"]) == expected_by_field
    assert source.is_fhir_job_set_matching(SPEC, request, [job])
    _environment(job)[source.PROXY_COOLDOWNS_ENV]["value"] = json.dumps(_cooldowns())
    assert not source.is_fhir_job_set_matching(SPEC, request, [job])


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
@pytest.mark.parametrize(
    "snapshot",
    [
        None,
        {},
        {**_cooldowns(), "version": True},
        {**_cooldowns(), "pool_revision": "other"},
        {**_cooldowns(), "origin": "https://other.example.test"},
        _cooldowns([("one", "2026-11-01T00:00:00")]),
        _cooldowns([("one", "invalid")]),
        _cooldowns([("one", 17)]),
        _cooldowns([("one", "0" * 65)]),
        _cooldowns([("one", None), ("one", None)]),
        _cooldowns([("https://proxy.example", None)]),
        _cooldowns([(str(index), None) for index in range(65)]),
    ],
)
async def test_proxy_retry_refuses_missing_or_invalid_cooldowns(monkeypatch, snapshot):
    request, lineage_rows = _retry_lineage(monkeypatch)
    lineage_rows[1]["source_http_proxy_cooldowns"] = snapshot
    with pytest.raises(ValueError, match="proxy_cooldowns_invalid"):
        await source.require_fhir_source_route_ancestry(
            SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), request.fhir_source_launch
        )


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
@pytest.mark.parametrize("binding", [None, {**_BINDING, "pool_revision": "other-pool"}])
async def test_intermediate_retry_cannot_replace_proxy_binding(monkeypatch, binding):
    request, lineage_rows = _retry_lineage(monkeypatch)
    _insert_retry_parent(request, lineage_rows, binding, _cooldowns())
    with pytest.raises(ValueError, match="route_changed"):
        await source.require_fhir_source_route_ancestry(
            SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), request.fhir_source_launch
        )
    SourceProxyConfig.bind.assert_not_called()


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
@pytest.mark.parametrize("combined_count", [64, 65])
async def test_retry_bounds_combined_ancestor_cooldowns(monkeypatch, combined_count):
    request, lineage_rows = _retry_lineage(monkeypatch)
    lineage_rows[1]["source_http_proxy_cooldowns"] = _cooldowns([(f"synthetic-{index}", None) for index in range(32)])
    _insert_retry_parent(
        request,
        lineage_rows,
        deepcopy(_BINDING),
        _cooldowns([(f"synthetic-{index}", None) for index in range(32, combined_count)]),
    )
    original = request.fhir_source_launch
    capture = source.require_fhir_source_route_ancestry(
        SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), original
    )
    if combined_count > 64:
        with pytest.raises(ValueError, match="proxy_cooldowns_invalid"):
            await capture
        assert request.fhir_source_launch is original and original.proxy_cooldowns_json == ""
    else:
        request.fhir_source_launch = await capture
        job = workers._worker_job_manifest(SPEC, request, IMAGE)
        snapshot_by_field = json.loads(_environment(job)[source.PROXY_COOLDOWNS_ENV]["value"])
        assert len(snapshot_by_field["cooldowns"]) == combined_count
        assert all(entry["retry_not_before"] is None for entry in snapshot_by_field["cooldowns"])
        assert source.is_fhir_job_set_matching(SPEC, request, [job])
    SourceProxyConfig.bind.assert_not_called()


@pytest.mark.usefixtures("enabled_proxy")
def test_proxy_retry_cannot_launch_before_cooldown_capture(monkeypatch):
    request, _lineage_rows = _retry_lineage(monkeypatch)
    with pytest.raises(ValueError, match="proxy_cooldowns_invalid"):
        workers._worker_job_manifest(SPEC, request, IMAGE)


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
async def test_nonproxy_ancestry_returns_original_capability(monkeypatch):
    policy_by_field = deepcopy(_POLICY)
    policy_by_field["sources"][0]["use_proxy"] = False
    monkeypatch.setenv(source.PROXY_POLICY_ENV, json.dumps(policy_by_field))
    request, lineage_rows = _retry_lineage(monkeypatch)
    assert request.fhir_source_launch.proxy_cooldowns_json == ""
    for ancestor in lineage_rows:
        ancestor.pop("source_http_proxy_binding", None)
        ancestor.pop("source_http_proxy_cooldowns", None)
    updated = await source.require_fhir_source_route_ancestry(
        SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), request.fhir_source_launch
    )
    assert updated is request.fhir_source_launch


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
async def test_retry_cannot_disable_a_previously_proxied_root(monkeypatch):
    policy_by_field = deepcopy(_POLICY)
    policy_by_field["sources"][0]["use_proxy"] = False
    monkeypatch.setenv(source.PROXY_POLICY_ENV, json.dumps(policy_by_field))
    request, lineage_rows = _retry_lineage(monkeypatch)
    with pytest.raises(ValueError, match="route_changed"):
        await source.require_fhir_source_route_ancestry(
            SimpleNamespace(all=AsyncMock(return_value=lineage_rows)), request.fhir_source_launch
        )


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.asyncio
async def test_native_launch_fence_captures_replacement_before_launch(monkeypatch):
    from api import provider_directory_cms_registry_stop as stop

    request, lineage_rows = _retry_lineage(monkeypatch)
    connection = SimpleNamespace(all=AsyncMock(return_value=lineage_rows))
    original = request.fhir_source_launch
    events = []

    @asynccontextmanager
    async def acquire():
        events.append("fence")
        yield connection
        events.append("released")

    def launch(payload):
        assert events == ["fence"] and payload.fhir_source_launch is not original
        assert json.loads(payload.fhir_source_launch.proxy_cooldowns_json) == _cooldowns()
        events.append("launch")
        return {"status": "started"}

    monkeypatch.setattr(workers.db, "acquire", acquire)
    monkeypatch.setattr(workers, "acquire_control_run_worker_action_lock", AsyncMock())
    monkeypatch.setattr(workers, "_admit_worker_ensure", AsyncMock(return_value=None))
    monkeypatch.setattr(workers, "ensure_worker", launch)
    assert await stop.guarded_ensure(
        request, run_id=RUN_ID, importer="provider-directory-fhir", selected_specs=[SPEC]
    ) == {"status": "started"}
    assert events == ["fence", "launch", "released"]


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize("mode", ["enabled", "unopted", "nonfhir"])
def test_pool_secret_cannot_enter_broad_envfrom(monkeypatch, mode):
    spec, request = _worker_request(mode)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "ordinary-secret, " + _POOL_SECRET + " ")
    with pytest.raises(ValueError, match="proxy_pool_broad_secret_forbidden"):
        workers._worker_job_manifest(spec, request, IMAGE)


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize("mode", ["enabled", "unopted", "nonfhir"])
@pytest.mark.parametrize("secret_field", ["secretName", "secret_name"])
def test_pool_secret_cannot_enter_direct_mount(monkeypatch, mode, secret_field):
    spec, request = _worker_request(mode)
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps([{"name": "broad-pool", secret_field: " " + _POOL_SECRET + " ", "mountPath": "/synthetic-pool"}]),
    )
    with pytest.raises(ValueError, match="proxy_pool_broad_secret_forbidden"):
        workers._worker_job_manifest(spec, request, IMAGE)


def _inject_broad_pool(job, path):
    pod = job["spec"]["template"]["spec"]
    if path == "envfrom":
        pod["containers"][0].setdefault("envFrom", []).append(
            {"prefix": "RENAMED_", "secretRef": {"name": _POOL_SECRET}}
        )
    elif path == "init-envfrom":
        pod["initContainers"] = [{"name": "broad-pool", "envFrom": [{"secretRef": {"name": _POOL_SECRET}}]}]
    else:
        pool_volume = (
            {"secret": {"secretName": _POOL_SECRET}}
            if path == "secret-volume"
            else {"projected": {"sources": [{"secret": {"name": _POOL_SECRET}}]}}
        )
        pod.setdefault("volumes", []).append({"name": "broad-pool", **pool_volume})
        pod["containers"][0].setdefault("volumeMounts", []).append(
            {"name": "broad-pool", "mountPath": "/synthetic-pool"}
        )


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize("mode", ["enabled", "unopted", "nonfhir"])
@pytest.mark.parametrize("path", ["envfrom", "init-envfrom", "secret-volume", "projected-volume"])
def test_existing_job_cannot_hide_broad_pool(monkeypatch, mode, path):
    spec, request = _worker_request(mode)
    monkeypatch.delenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", raising=False)
    job = workers._worker_job_manifest(spec, request, IMAGE)
    _inject_broad_pool(job, path)
    assert workers._has_broad_proxy_pool_reference(job)
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *_args: {"items": [job]})
    assert workers._kubernetes_worker_state(spec, request)["job_status"] == "purpose_mismatch"
    if mode != "nonfhir":
        assert not source.is_fhir_source_job_matching(job, request.fhir_source_launch)


@pytest.mark.usefixtures("enabled_proxy")
@pytest.mark.parametrize("mode", ["enabled", "unopted", "nonfhir"])
def test_unrelated_broad_secrets_remain_available(monkeypatch, mode):
    spec, request = _worker_request(mode)
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_SECRET", "ordinary-secret")
    monkeypatch.setenv("HLTHPRT_WORKER_JOB_ENV_FROM_CONFIGMAP", "ordinary-settings")
    mounts = json.loads(workers.os.environ["HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON"])
    monkeypatch.setenv(
        "HLTHPRT_WORKER_JOB_SECRET_VOLUME_MOUNTS_JSON",
        json.dumps(
            [*mounts, {"name": "ordinary-mount", "secretName": "ordinary-certs", "mountPath": "/ordinary-certs"}]
        ),
    )
    job = workers._worker_job_manifest(spec, request, IMAGE)
    pod = job["spec"]["template"]["spec"]
    assert pod["containers"][0]["envFrom"] == [
        {"configMapRef": {"name": "ordinary-settings"}},
        {"secretRef": {"name": "ordinary-secret"}},
    ]
    assert {"name": "ordinary-mount", "secret": {"secretName": "ordinary-certs"}} in pod["volumes"]
    assert not workers._has_broad_proxy_pool_reference(job)
    monkeypatch.setattr(workers, "_is_kubernetes_configured", lambda: True)
    monkeypatch.setattr(workers, "_kubernetes_request", lambda *_args: {"items": [job]})
    assert workers._kubernetes_worker_state(spec, request)["job_status"] != "purpose_mismatch"
    if mode != "nonfhir":
        assert source.is_fhir_source_job_matching(job, request.fhir_source_launch)
