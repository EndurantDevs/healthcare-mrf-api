# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Select one fixed FHIR source bootstrap from an observed native queue job."""

import json
import os
import re
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from importlib import import_module

from yarl import URL

from api.reference_source_worker import _IMAGE
from api.reference_source_worker import IMAGE_ENV as REFERENCE_IMAGE_ENV

PURPOSE = "provider-directory-source"
COMMAND = ["/usr/local/bin/fhir-source-worker"]
IMAGE_ENV = "HLTHPRT_PROVIDER_DIRECTORY_SOURCE_WORKER_IMAGE"
ROUTES_ENV = "HLTHPRT_PROVIDER_DIRECTORY_SOURCE_WORKER_ROUTES_JSON"
PROXY_POOL_SECRET_ENV = "HLTHPRT_HTTP_PROXY_POOL_SECRET_NAME"
PROXY_POOL_REVISION_ENV = "HLTHPRT_HTTP_PROXY_POOL_REVISION"
PROXY_POLICY_ENV = "HLTHPRT_SOURCE_HTTP_PROXY_POLICY_JSON"
PROXY_POOL_ENV = "HLTHPRT_HTTP_PROXY_POOL_JSON"
PROXY_BINDING_ENV = "HLTHPRT_SOURCE_HTTP_PROXY_BINDING_JSON"
PROXY_COOLDOWNS_ENV = "HLTHPRT_SOURCE_HTTP_PROXY_COOLDOWNS_JSON"
_WORKER_CLASS = "process.ProviderDirectoryFHIR"
_IMPORTER = "provider-directory-fhir"
_QUEUE = "arq:ProviderDirectoryFHIR"
_FUNCTION = "control_single_job_start"


@dataclass(frozen=True)
class FHIRSourceWorkerLaunch:
    """Keep the owner image and observed native job outside serialized input."""

    run_id: str
    job_id: str
    image: str
    root_run_id: str = ""
    secret_name: str = ""
    source_id: str = ""
    endpoint_scope: str = ""
    retry_of_run_id: str = ""
    policy_revision: str = ""
    pool_revision: str = ""
    pool_secret_name: str = ""
    proxy_cooldowns_json: str = ""


def _configured_routes():
    """Validate the complete server-owned route inventory before selection."""
    raw = os.getenv(ROUTES_ENV, "").strip()
    if not raw:
        return []
    try:
        routes = json.loads(raw)
    except json.JSONDecodeError as error:
        raise ValueError("fhir_source_worker_route_invalid") from error
    if type(routes) is not list or len(routes) > 32:
        raise ValueError("fhir_source_worker_route_invalid")
    identities = set()
    for route in routes:
        if (
            type(route) is not dict
            or set(route) != {"root_run_id", "source_id", "endpoint_scope", "image", "secret_name"}
            or not (
                (
                    route["root_run_id"] is None
                    or type(route["root_run_id"]) is str
                    and re.fullmatch(r"run_[0-9a-f]{32}", route["root_run_id"])
                )
                and type(route["source_id"]) is str
                and re.fullmatch(r"[A-Za-z0-9_-]{1,96}", route["source_id"])
                and type(route["endpoint_scope"]) is str
                and route["endpoint_scope"] == route["endpoint_scope"].strip()
                and route["endpoint_scope"]
                and type(route["image"]) is str
                and _IMAGE.fullmatch(route["image"])
                and type(route["secret_name"]) is str
                and re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", route["secret_name"])
            )
        ):
            raise ValueError("fhir_source_worker_route_invalid")
        route_identity_fields = tuple(route[key] for key in ("root_run_id", "source_id", "endpoint_scope"))
        if route_identity_fields in identities:
            raise ValueError("fhir_source_worker_route_invalid")
        identities.add(route_identity_fields)
    return routes


def _source_route(admitted_run):
    """Preserve an exact original route before selecting fresh or retry execution."""
    params = admitted_run.get("params")
    params = params if type(params) is dict else {}
    routes = _configured_routes()
    root = admitted_run.get("run_id")
    root_routes = [route for route in routes if route["root_run_id"] == root]
    if not root_routes:
        root = params.get("provider_directory_pagination_root_run_id")
        if root is None:
            return _fresh_source_route(admitted_run, params, routes)
        root_routes = [route for route in routes if route["root_run_id"] == root]
    if not root_routes:
        return None
    if root == admitted_run.get("run_id"):
        has_route_lineage = (
            admitted_run.get("retry_of_run_id") is None
            and params.get("retry_of_run_id") is None
            and params.get("provider_directory_pagination_root_run_id") in (None, root)
        )
    else:
        has_route_lineage = (
            type(params.get("retry_of_run_id")) is str
            and re.fullmatch(r"run_[0-9a-f]{32}", params["retry_of_run_id"]) is not None
        )
    matches = [
        route
        for route in root_routes
        if params.get("source_ids") == [route["source_id"]]
        and params.get("provider_directory_endpoint_scope") == route["endpoint_scope"]
        and has_route_lineage
    ]
    if len(matches) != 1:
        raise ValueError("fhir_source_worker_route_changed")
    return matches[0]


def _fresh_source_route(admitted_run, params, routes):
    """A source-scoped credential requires an observed fresh single-source root."""
    source_ids = params.get("source_ids")
    candidates = [
        route
        for route in routes
        if route["root_run_id"] is None and type(source_ids) is list and route["source_id"] in source_ids
    ]
    if not candidates:
        return None
    if not _uses_source_http(params):
        return None
    matches = [
        route
        for route in candidates
        if source_ids == [route["source_id"]]
        and params.get("provider_directory_endpoint_scope") == route["endpoint_scope"]
    ]
    if (
        len(matches) != 1
        or "retry_of_run_id" not in admitted_run
        or admitted_run["retry_of_run_id"] is not None
        or {"retry_of_run_id", "provider_directory_pagination_root_run_id"}.intersection(params)
        or admitted_run.get("status") not in {"queued", "starting", "running", "finalizing"}
    ):
        raise ValueError("fhir_source_worker_route_changed")
    return matches[0]


def _is_launch_secret_selected(selection, launch, *, allow_shared=False):
    """Keep routed credentials isolated while allowing explicitly shared env."""
    secret_name = str(selection.get("secretName") or selection.get("secret_name") or "").strip()
    route_secret_names = {route["secret_name"] for route in _configured_routes()}
    shared_for_routes = selection.get("sharedForRoutes", False)
    if type(shared_for_routes) is not bool or (shared_for_routes and not allow_shared):
        raise ValueError("worker purpose shared secret selector is invalid")
    pool_marker = selection.get("sourceProxyPool", False)
    if type(pool_marker) is not bool:
        raise ValueError("worker purpose proxy pool selector is invalid")
    pool_secret = os.getenv(PROXY_POOL_SECRET_ENV, "").strip()
    environment_name = str(selection.get("name") or "").strip()
    secret_key = str(selection.get("key") or "").strip()
    reserved = (
        environment_name in {PROXY_POOL_ENV, PROXY_POLICY_ENV, PROXY_BINDING_ENV, PROXY_COOLDOWNS_ENV}
        or secret_key == PROXY_POOL_ENV
        or bool(pool_secret and secret_name == pool_secret)
    )
    if pool_marker or reserved:
        if (
            not allow_shared
            or not pool_marker
            or shared_for_routes
            or selection.get("name") != PROXY_POOL_ENV
            or selection.get("key") != PROXY_POOL_ENV
            or selection.get("optional", False) is not False
        ):
            raise ValueError("worker purpose proxy pool selector is invalid")
        return bool(launch.pool_secret_name and secret_name == launch.pool_secret_name)
    if shared_for_routes:
        if secret_name in route_secret_names:
            raise ValueError("worker purpose shared secret selector is invalid")
        return True
    if launch.secret_name:
        return secret_name == launch.secret_name
    return secret_name not in route_secret_names


def requires_source_proxy_admission(selection):
    """Reserve the pool's key and configured Secret for exact Source admission."""
    secret = str(selection.get("secretName") or selection.get("secret_name") or "").strip()
    configured_secret = os.getenv(PROXY_POOL_SECRET_ENV, "").strip()
    environment_name = str(selection.get("name") or "").strip()
    secret_key = str(selection.get("key") or "").strip()
    return (
        "sourceProxyPool" in selection
        or environment_name in {PROXY_POOL_ENV, PROXY_POLICY_ENV, PROXY_BINDING_ENV, PROXY_COOLDOWNS_ENV}
        or secret_key == PROXY_POOL_ENV
        or bool(configured_secret and secret == configured_secret)
    )


def _proxy_fields(source_ids, endpoint_scope, route, *, policy=None):
    """Read only trusted policy and nonsecret pool references in the API."""
    from process.ext.source_http_proxy import SourceProxyConfig

    policy = SourceProxyConfig.from_environment() if policy is None else policy
    source_ids = source_ids if type(source_ids) is list else []
    enabled_source_ids = [source_id for source_id in source_ids if policy.use_proxy(source_id, endpoint_scope)]
    if not enabled_source_ids:
        return {}
    if route is None or source_ids != [route["source_id"]] or endpoint_scope != route["endpoint_scope"]:
        raise ValueError("fhir_source_worker_proxy_route_required")
    secret = os.getenv(PROXY_POOL_SECRET_ENV, "")
    revision = os.getenv(PROXY_POOL_REVISION_ENV, "")
    if (
        not re.fullmatch(r"[a-z0-9]([-a-z0-9]*[a-z0-9])?", secret)
        or len(secret) > 253
        or secret in {entry["secret_name"] for entry in _configured_routes()}
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}", revision)
    ):
        raise ValueError("fhir_source_worker_proxy_configuration_invalid")
    return {
        "policy_revision": policy.policy_revision,
        "pool_revision": revision,
        "pool_secret_name": secret,
        "proxy_cooldowns_json": _cooldowns_json(endpoint_scope, revision, []) if route["root_run_id"] is None else "",
    }


def _uses_source_http(params):
    """Keep recovery and local-only modes outside acquisition transport routing."""
    from process import provider_directory_serving_recovery as recovery
    from process.provider_directory_refresh_preset import apply_provider_directory_refresh_preset

    params = params if type(params) is dict else {}
    effective_by_name = apply_provider_directory_refresh_preset(params)
    if recovery.is_recovery_request(_IMPORTER, effective_by_name) or any(
        bool(effective_by_name.get(name))
        for name in (
            "publish_artifacts_only",
            "dataset_rehydrate_only",
            "dataset_followup_only",
            "canonical_backfill_only",
            "contact_backfill_only",
        )
    ):
        return False
    return not bool(effective_by_name.get("seed_only", False)) and (
        bool(effective_by_name.get("probe", True)) or bool(effective_by_name.get("import_resources", False))
    )


def _require_source_proxy_scope(policy, params):
    """Refuse Source HTTP selectors that would omit an enabled worker policy."""
    if not policy.has_enabled_sources or not _uses_source_http(params):
        return
    params = params if type(params) is dict else {}
    source_ids = params.get("source_ids")
    if (
        type(source_ids) is not list
        or not source_ids
        or any(
            type(source_id) is not str or not source_id.strip() or source_id != source_id.strip()
            for source_id in source_ids
        )
    ):
        raise ValueError("fhir_source_worker_proxy_source_scope_required")


def admit_fhir_source_worker_launch(admitted_run):
    """Require the configured immutable source image and original queued identity."""
    if admitted_run.get("importer") != _IMPORTER:
        return None
    metrics = admitted_run.get("metrics")
    image = os.getenv(IMAGE_ENV, os.getenv(REFERENCE_IMAGE_ENV, ""))
    if (
        not isinstance(metrics, dict)
        or metrics.get("queue") != _QUEUE
        or metrics.get("function") != _FUNCTION
        or any(
            type(identity) is not str or not identity or identity != identity.strip()
            for identity in (admitted_run.get("run_id"), metrics.get("job_id"))
        )
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")
    params_by_name = admitted_run.get("params")
    if (
        isinstance(params_by_name, dict)
        and params_by_name.get("provider_directory_dataset_followup_stage") == "address_overlay"
    ):
        from process.provider_directory_generic_source_artifacts import ordinary_overlay_followup

        directory = import_module("process.provider_directory_fhir")
        if ordinary_overlay_followup(directory, params_by_name) is not None:
            if {"test_mode", "run_id"}.intersection(params_by_name):
                raise ValueError("fhir_source_worker_admission_unavailable")
            return None
    from process.ext.source_http_proxy import SourceProxyConfig

    policy = SourceProxyConfig.from_environment()
    _require_source_proxy_scope(policy, params_by_name)
    route = _source_route(admitted_run)
    if route is not None:
        image = route["image"]
    if _IMAGE.fullmatch(image) is None:
        raise ValueError("fhir_source_worker_admission_unavailable")
    return FHIRSourceWorkerLaunch(
        admitted_run["run_id"],
        metrics["job_id"],
        image,
        (route["root_run_id"] or "") if route else "",
        route["secret_name"] if route else "",
        route["source_id"] if route else "",
        route["endpoint_scope"] if route else "",
        (params_by_name.get("retry_of_run_id") or "") if route and route["root_run_id"] else "",
        **(
            _proxy_fields(
                params_by_name.get("source_ids") if type(params_by_name) is dict else None,
                params_by_name.get("provider_directory_endpoint_scope") if type(params_by_name) is dict else None,
                route,
                policy=policy,
            )
            if policy.has_enabled_sources and _uses_source_http(params_by_name)
            else {}
        ),
    )


def _lineage_by_id(lineage_rows):
    """Normalize the bounded native rows before checking the retry chain."""
    return {
        lineage_record["run_id"]: lineage_record
        for lineage_record in (
            dict(raw_record._mapping) if hasattr(raw_record, "_mapping") else dict(raw_record)
            for raw_record in lineage_rows
        )
    }


def _is_admissible_route_root(lineage_record, launch):
    """Keep active originals distinct from failed ancestors of native retries."""
    if lineage_record["retry_of_run_id"] is not None:
        return False
    if launch.run_id != launch.root_run_id:
        return lineage_record["status"] == "failed"
    params = lineage_record["params"]
    return (
        lineage_record["status"] in {"queued", "starting", "running", "finalizing"}
        and not launch.retry_of_run_id
        and params.get("retry_of_run_id") is None
        and params.get("provider_directory_pagination_root_run_id") in (None, launch.root_run_id)
    )


async def _source_route_lineage(connection, root_run_id):
    """Read only native ancestry and its bounded nonsecret transport audit fields."""
    from sqlalchemy import text

    from api.control_imports import ImportRun
    from db.plan_release_storage import storage_relation

    run_relation = storage_relation(str(ImportRun.__table__.schema), "import_run")
    return await connection.all(
        text(
            "SELECT run_id,importer,status,retry_of_run_id,params,"
            "metrics->'audit'->'source_http_proxy'->'binding' AS source_http_proxy_binding,"
            "metrics->'audit'->'source_http_proxy'->'cooldowns' AS source_http_proxy_cooldowns FROM "
            + run_relation
            + " WHERE importer='provider-directory-fhir' AND "
            "(run_id=:root OR params::jsonb->>'provider_directory_pagination_root_run_id'=:root) LIMIT 65"
        ),
        root=root_run_id,
    )


async def require_fhir_source_route_ancestry(connection, launch):
    """Return the immutable launch only after native ancestry and cooldown capture."""
    if type(launch) is not FHIRSourceWorkerLaunch or not launch.root_run_id:
        return launch
    lineage_rows = await _source_route_lineage(connection, launch.root_run_id)
    runs_by_id = _lineage_by_id(lineage_rows)
    if len(lineage_rows) > 64 or len(runs_by_id) != len(lineage_rows):
        raise ValueError("fhir_source_worker_route_changed")
    current_id = launch.run_id
    seen_run_ids = set()
    ancestors = []
    while current_id not in seen_run_ids and len(seen_run_ids) <= 64:
        seen_run_ids.add(current_id)
        lineage_record = runs_by_id.get(current_id)
        if lineage_record is None:
            break
        if current_id != launch.run_id:
            ancestors.append(lineage_record)
        params = lineage_record["params"]
        if not (
            type(params) is dict
            and params.get("source_ids") == [launch.source_id]
            and params.get("provider_directory_endpoint_scope") == launch.endpoint_scope
            and (
                current_id == launch.root_run_id
                or params.get("provider_directory_pagination_root_run_id") == launch.root_run_id
            )
        ):
            break
        if current_id == launch.root_run_id:
            if _is_admissible_route_root(lineage_record, launch):
                return _proxy_lineage_launch(launch, ancestors or [lineage_record])
            break
        parent_id = lineage_record["retry_of_run_id"]
        if not (
            type(parent_id) is str
            and params.get("retry_of_run_id") == parent_id
            and lineage_record["status"]
            in ({"queued", "starting", "running", "finalizing"} if current_id == launch.run_id else {"failed"})
            and (current_id != launch.run_id or parent_id == launch.retry_of_run_id)
        ):
            break
        current_id = parent_id
    raise ValueError("fhir_source_worker_route_changed")


def authorized_fhir_source_worker_launch(spec, launch_request):
    """Accept only the exact server-held capability and closed engine adapter."""
    from api.control_workers import _AdmittedWorkerRequest

    if type(launch_request) is not _AdmittedWorkerRequest:
        return None
    if getattr(launch_request, "cms_registry_launch", None) is not None:
        return None
    launch = getattr(launch_request, "fhir_source_launch", None)
    if launch is None:
        return None
    if type(launch) is not FHIRSourceWorkerLaunch:
        raise ValueError("fhir_source_worker_admission_unavailable")
    _require_fhir_job_identity(spec, launch_request, launch.run_id, launch.job_id)
    _require_proxy_launch(launch)
    return launch


def _require_proxy_launch(launch):
    """A changed capability or server policy cannot silently replace its binding."""
    fields = (launch.policy_revision, launch.pool_revision, launch.pool_secret_name)
    if not launch.source_id and not any(fields):
        if launch.proxy_cooldowns_json:
            raise ValueError("fhir_source_worker_admission_unavailable")
        return
    route = next(
        (
            entry
            for entry in _configured_routes()
            if (
                entry["root_run_id"] or "",
                entry["source_id"],
                entry["endpoint_scope"],
                entry["image"],
                entry["secret_name"],
            )
            == (launch.root_run_id, launch.source_id, launch.endpoint_scope, launch.image, launch.secret_name)
        ),
        None,
    )
    expected = _proxy_fields([launch.source_id], launch.endpoint_scope, route)
    if fields != tuple(expected.get(name, "") for name in ("policy_revision", "pool_revision", "pool_secret_name")):
        raise ValueError("fhir_source_worker_admission_unavailable")
    if launch.pool_secret_name:
        _require_proxy_cooldowns(launch)
        if not launch.root_run_id and launch.proxy_cooldowns_json != expected["proxy_cooldowns_json"]:
            raise ValueError("fhir_source_worker_admission_unavailable")
    elif launch.proxy_cooldowns_json:
        raise ValueError("fhir_source_worker_admission_unavailable")


def _proxy_binding(launch):
    return {
        "source_id": launch.source_id,
        "endpoint_scope": launch.endpoint_scope,
        "policy_revision": launch.policy_revision,
        "pool_revision": launch.pool_revision,
    }


def _cooldowns_json(endpoint_scope, pool_revision, cooldowns):
    return json.dumps(
        {
            "version": 1,
            "pool_revision": pool_revision,
            "origin": str(URL(endpoint_scope).origin()),
            "cooldowns": cooldowns,
        },
        sort_keys=True,
        separators=(",", ":"),
    )


def _validated_cooldowns(snapshot, launch):
    """Copy a closed nonsecret audit envelope without inspecting any pool credential."""
    error = "fhir_source_worker_proxy_cooldowns_invalid"
    if (
        type(snapshot) is not dict
        or set(snapshot) != {"version", "pool_revision", "origin", "cooldowns"}
        or type(snapshot["version"]) is not int
        or snapshot["version"] != 1
        or snapshot["pool_revision"] != launch.pool_revision
        or snapshot["origin"] != str(URL(launch.endpoint_scope).origin())
        or type(snapshot["cooldowns"]) is not list
        or len(snapshot["cooldowns"]) > 64
    ):
        raise ValueError(error)
    boundaries_by_id = {}
    for entry in snapshot["cooldowns"]:
        if (
            type(entry) is not dict
            or set(entry) != {"proxy_id", "retry_not_before"}
            or type(entry["proxy_id"]) is not str
            or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,255}", entry["proxy_id"])
            or entry["proxy_id"] in boundaries_by_id
        ):
            raise ValueError(error)
        boundary = entry["retry_not_before"]
        if boundary is not None:
            try:
                if type(boundary) is not str or len(boundary) > 64:
                    raise ValueError
                deadline = datetime.fromisoformat(boundary)
                if deadline.tzinfo is None or deadline.utcoffset() is None:
                    raise ValueError
                boundary = deadline.astimezone(UTC).isoformat().replace("+00:00", "Z")
            except ValueError, OverflowError:
                raise ValueError(error) from None
        boundaries_by_id[entry["proxy_id"]] = boundary
    return [
        {"proxy_id": proxy_id, "retry_not_before": boundary} for proxy_id, boundary in sorted(boundaries_by_id.items())
    ]


def _require_proxy_cooldowns(launch):
    try:
        if type(launch.proxy_cooldowns_json) is not str or len(launch.proxy_cooldowns_json) > 65536:
            raise ValueError
        snapshot = json.loads(launch.proxy_cooldowns_json)
    except ValueError, TypeError:
        raise ValueError("fhir_source_worker_proxy_cooldowns_invalid") from None
    cooldowns = _validated_cooldowns(snapshot, launch)
    if launch.proxy_cooldowns_json != _cooldowns_json(launch.endpoint_scope, launch.pool_revision, cooldowns):
        raise ValueError("fhir_source_worker_proxy_cooldowns_invalid")


def _proxy_lineage_launch(launch, ancestors):
    if not launch.pool_secret_name:
        if any(
            ancestor.get("source_http_proxy_binding") is not None
            or ancestor.get("source_http_proxy_cooldowns") is not None
            for ancestor in ancestors
        ):
            raise ValueError("fhir_source_worker_route_changed")
        return launch
    if not ancestors:
        raise ValueError("fhir_source_worker_proxy_cooldowns_invalid")
    boundaries_by_id = {}
    for ancestor in ancestors:
        if ancestor.get("source_http_proxy_binding") != _proxy_binding(launch):
            raise ValueError("fhir_source_worker_route_changed")
        for entry in _validated_cooldowns(ancestor.get("source_http_proxy_cooldowns"), launch):
            proxy_id, boundary = entry["proxy_id"], entry["retry_not_before"]
            if proxy_id not in boundaries_by_id:
                boundaries_by_id[proxy_id] = boundary
            elif boundaries_by_id[proxy_id] is not None:
                boundaries_by_id[proxy_id] = (
                    max(boundaries_by_id[proxy_id], boundary, key=datetime.fromisoformat)
                    if boundary is not None
                    else None
                )
            if len(boundaries_by_id) > 64:
                raise ValueError("fhir_source_worker_proxy_cooldowns_invalid")
    cooldowns = [
        {"proxy_id": proxy_id, "retry_not_before": boundary} for proxy_id, boundary in sorted(boundaries_by_id.items())
    ]
    updated = replace(
        launch, proxy_cooldowns_json=_cooldowns_json(launch.endpoint_scope, launch.pool_revision, cooldowns)
    )
    _require_proxy_cooldowns(updated)
    return updated


def _proxy_literal_environment(launch):
    if not launch.pool_secret_name:
        return []
    policy_by_field = {
        "version": 1,
        "revision": launch.policy_revision,
        "sources": [{"source_id": launch.source_id, "endpoint_scope": launch.endpoint_scope, "use_proxy": True}],
    }
    return [
        {"name": name, "value": json.dumps(value, sort_keys=True, separators=(",", ":"))}
        for name, value in ((PROXY_POLICY_ENV, policy_by_field), (PROXY_BINDING_ENV, _proxy_binding(launch)))
    ] + [{"name": PROXY_COOLDOWNS_ENV, "value": launch.proxy_cooldowns_json}]


def source_proxy_environment(spec, launch_request, environment):
    """Emit only captured nonsecret policy, with exactly its selected pool keyref."""
    launch = authorized_fhir_source_worker_launch(spec, launch_request)
    if launch is None or not launch.pool_secret_name:
        return []
    expected_by_field = {
        "name": PROXY_POOL_ENV,
        "valueFrom": {"secretKeyRef": {"name": launch.pool_secret_name, "key": PROXY_POOL_ENV}},
    }
    if [entry for entry in environment if entry["name"] == PROXY_POOL_ENV] != [expected_by_field]:
        raise ValueError("fhir_source_worker_proxy_configuration_invalid")
    return _proxy_literal_environment(launch)


def source_proxy_annotations(spec, launch_request):
    """Bind proxy-enabled Job admission to its captured policy and pool revisions."""
    launch = authorized_fhir_source_worker_launch(spec, launch_request)
    if launch is None or not launch.pool_secret_name:
        return {}
    return {
        "healthporta.com/source-proxy-policy-revision": launch.policy_revision,
        "healthporta.com/source-proxy-pool-revision": launch.pool_revision,
    }


def _is_proxy_job_matching(job, launch):
    annotations_by_key = job["metadata"]["annotations"]
    if any(
        annotations_by_key.get(name, "") != value
        for name, value in (
            ("healthporta.com/source-proxy-policy-revision", launch.policy_revision),
            ("healthporta.com/source-proxy-pool-revision", launch.pool_revision),
        )
    ):
        return False
    expected_entries = _proxy_literal_environment(launch)
    if launch.pool_secret_name:
        expected_entries.append(
            {
                "name": PROXY_POOL_ENV,
                "valueFrom": {"secretKeyRef": {"name": launch.pool_secret_name, "key": PROXY_POOL_ENV}},
            }
        )
    observed_entries = [
        entry
        for entry in job["spec"]["template"]["spec"]["containers"][0]["env"]
        if entry["name"] in {PROXY_POOL_ENV, PROXY_POLICY_ENV, PROXY_BINDING_ENV, PROXY_COOLDOWNS_ENV}
    ]
    return sorted(observed_entries, key=lambda entry: entry["name"]) == sorted(
        expected_entries, key=lambda entry: entry["name"]
    )


def fresh_route_annotations(spec, launch_request):
    """Expose exact fresh-route coordinates only from the admitted capability."""
    launch = authorized_fhir_source_worker_launch(spec, launch_request)
    if launch is None or launch.root_run_id or not launch.secret_name:
        return {}
    if launch.retry_of_run_id or not any(
        route["root_run_id"] is None
        and (route["source_id"], route["endpoint_scope"], route["image"], route["secret_name"])
        == (launch.source_id, launch.endpoint_scope, launch.image, launch.secret_name)
        for route in _configured_routes()
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")
    return {
        "healthporta.com/source-id": launch.source_id,
        "healthporta.com/source-endpoint-scope": launch.endpoint_scope,
    }


def admitted_overlay_job(spec, launch_request):
    """Select ordinary execution only for the overlay classified from the native run."""
    from api.control_workers import _AdmittedWorkerRequest

    if type(launch_request) is not _AdmittedWorkerRequest:
        return None
    identity = launch_request.ordinary_fhir_overlay_job
    if identity is None:
        return None
    if (
        type(identity) is not tuple
        or len(identity) != 5
        or identity != launch_request.admitted_job
        or launch_request.cms_registry_launch is not None
        or launch_request.fhir_source_launch is not None
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")
    _require_fhir_job_identity(spec, launch_request, identity[0], identity[4])
    return identity[4]


def _require_fhir_job_identity(spec, launch_request, run_id, job_id):
    """Keep both runtimes on the same exact native adapter and queue identity."""
    from api.control_imports import _SINGLE_JOB_ADAPTERS

    adapter = _SINGLE_JOB_ADAPTERS.get(_IMPORTER, {})
    if (
        (spec.worker_class, spec.queue, spec.importers, spec.role) != (_WORKER_CLASS, _QUEUE, (_IMPORTER,), "start")
        or launch_request.admitted_job != (run_id, _IMPORTER, _QUEUE, _FUNCTION, job_id)
        or launch_request.get("run_id") != run_id
        or launch_request.get("importer", _IMPORTER) != _IMPORTER
        or launch_request.get("job_id") not in (None, "", job_id)
        or tuple(
            adapter.get(name)
            for name in ("queue", "function", "payload", "target_module", "target_function", "job_prefix")
        )
        != (
            _QUEUE,
            _FUNCTION,
            "control_wrapped",
            "process.provider_directory_fhir",
            "process_data",
            "provider_directory_start",
        )
        or adapter.get("run_shutdown") is not True
        or job_id != "provider_directory_start_" + run_id
    ):
        raise ValueError("fhir_source_worker_admission_unavailable")


def apply_fhir_source_worker_spec(spec, launch_request, labels, pod_spec):
    """Use the fixed non-root source runtime only after native purpose admission."""
    if authorized_fhir_source_worker_launch(spec, launch_request) is not None:
        labels["healthporta.com/worker-purpose"] = PURPOSE
        security = pod_spec["securityContext"]
        # Preserve the shared volume group to avoid rewriting existing file ownership.
        security.update(runAsUser=10001, runAsGroup=10001)


def _is_fhir_secret_scope(selection, worker_class):
    """Keep environment and mount selectors on the same exact Source purpose."""
    classes = selection.get("workerClasses", selection.get("worker_classes"))
    if (
        selection.get("purposes") != [PURPOSE]
        or selection.get("importers") != [_IMPORTER]
        or classes != [_WORKER_CLASS]
        or ("workerClasses" in selection and "worker_classes" in selection)
    ):
        raise ValueError("worker purpose secret selector is invalid")
    return worker_class == _WORKER_CLASS


def is_fhir_source_secret_selected(selection, worker_class, launch_request):
    """Select source credentials only for the exact admitted FHIR queue purpose."""
    if not _is_fhir_secret_scope(selection, worker_class):
        return False
    from api.control_workers import _BY_WORKER_CLASS

    spec = _BY_WORKER_CLASS.get(_WORKER_CLASS)
    launch = authorized_fhir_source_worker_launch(spec, launch_request) if spec is not None else None
    return launch is not None and _is_launch_secret_selected(selection, launch, allow_shared=True)


def is_fhir_source_volume_selected(selection, worker_class, launch):
    """Use the existing Source capability for explicitly purpose-scoped mounts."""
    return (
        _is_fhir_secret_scope(selection, worker_class)
        and type(launch) is FHIRSourceWorkerLaunch
        and _is_launch_secret_selected(selection, launch)
    )


def is_fhir_source_job_matching(job, launch, launch_request=None):
    """Refuse changed bootstrap code, native target or process isolation on reuse."""
    from api.control_workers import (
        _BY_WORKER_CLASS,
        _has_broad_proxy_pool_reference,
        _worker_job_container_security_context,
        _worker_job_manifest,
        _worker_job_pod_security_context,
        _worker_job_pvc_volumes,
        _worker_job_secret_volumes,
    )

    if type(launch) is not FHIRSourceWorkerLaunch or _has_broad_proxy_pool_reference(job):
        return False
    try:
        template = job["spec"]["template"]
        pod = template["spec"]
        containers = pod["containers"]
        security = pod["securityContext"]
        volumes = [*_worker_job_pvc_volumes(), *_worker_job_secret_volumes(_BY_WORKER_CLASS[_WORKER_CLASS], launch)]
        expected_security = _worker_job_pod_security_context(has_group_read_volume="fsGroup" in security)
        expected_security.update(runAsUser=10001, runAsGroup=10001)
        expected_env = None
        if launch_request is not None:
            expected_job = _worker_job_manifest(_BY_WORKER_CLASS[_WORKER_CLASS], launch_request, launch.image)
            expected_env = expected_job["spec"]["template"]["spec"]["containers"][0]["env"]
        return (
            job["metadata"]["labels"]["healthporta.com/worker-purpose"] == PURPOSE
            and template["metadata"]["labels"]["healthporta.com/worker-purpose"] == PURPOSE
            and security == expected_security
            and pod["automountServiceAccountToken"] is False
            and all(not pod.get(key, False) for key in ("hostPID", "hostIPC", "hostNetwork"))
            and not any(pod.get(name) for name in ("initContainers", "ephemeralContainers"))
            and len(containers) == 1
            and containers[0]["securityContext"] == _worker_job_container_security_context()
            and containers[0]["image"] == launch.image
            and (expected_env is None or containers[0]["env"] == expected_env)
            and _is_proxy_job_matching(job, launch)
            and job["metadata"]["annotations"].get("healthporta.com/source-root-run-id", "")
            == (launch.root_run_id if launch.retry_of_run_id else "")
            and job["metadata"]["annotations"].get("healthporta.com/source-id", "")
            == (launch.source_id if launch.secret_name and not launch.root_run_id else "")
            and job["metadata"]["annotations"].get("healthporta.com/source-endpoint-scope", "")
            == (launch.endpoint_scope if launch.secret_name and not launch.root_run_id else "")
            and containers[0]["command"] == COMMAND
            and containers[0]["workingDir"] == "/app"
            and not any(containers[0].get(name) for name in ("args", "lifecycle", "volumeDevices"))
            and containers[0].get("volumeMounts", []) == [volume["volumeMount"] for volume in volumes]
            and _secret_volume_defaults(pod.get("volumes", []))
            == _secret_volume_defaults([volume["volume"] for volume in volumes])
            and all(
                [entry.get("value") for entry in containers[0]["env"] if entry.get("name") == name] == [expected]
                for name, expected in (
                    ("HLTHPRT_CONTROL_RUN_ID", launch.run_id),
                    ("HLTHPRT_WORKER_ONCE_TARGET_JOB_ID", launch.job_id),
                )
            )
        )
    except KeyError, TypeError, IndexError:
        return False


def _secret_volume_defaults(volumes):
    """Retain every binding field while allowing the API's Secret default 0644."""
    return [
        {**volume, "secret": {"defaultMode": 0o644, **volume["secret"]}} if "secret" in volume else volume
        for volume in volumes
    ]


def is_overlay_job_matching(job, expected_job):
    """Refuse reuse of a different runtime or native target for an ordinary overlay."""
    try:
        template_by_field = job["spec"]["template"]
        pod_by_field = template_by_field["spec"]
        expected_pod_by_field = expected_job["spec"]["template"]["spec"]
        containers = pod_by_field["containers"]
        expected_container_by_field = expected_pod_by_field["containers"][0]
        return (
            len(containers) == 1
            and all(
                containers[0].get(name) == expected_container_by_field.get(name)
                for name in (
                    "image",
                    "imagePullPolicy",
                    "workingDir",
                    "command",
                    "env",
                    "envFrom",
                    "securityContext",
                    "volumeMounts",
                )
            )
            and not any(containers[0].get(name) for name in ("args", "lifecycle", "volumeDevices"))
            and not any(
                pod_by_field.get(name)
                for name in (
                    "initContainers",
                    "ephemeralContainers",
                    "hostPID",
                    "hostIPC",
                    "hostNetwork",
                )
            )
            and pod_by_field["automountServiceAccountToken"] is False
            and pod_by_field.get("serviceAccountName", "default")
            == expected_pod_by_field.get("serviceAccountName", "default")
            and pod_by_field["securityContext"] == expected_pod_by_field["securityContext"]
            and _secret_volume_defaults(pod_by_field.get("volumes", []))
            == _secret_volume_defaults(expected_pod_by_field.get("volumes", []))
            and all(
                "healthporta.com/worker-purpose" not in metadata.get("labels", {})
                for metadata in (job["metadata"], template_by_field["metadata"])
            )
            and job["metadata"]["annotations"] == expected_job["metadata"]["annotations"]
        )
    except KeyError, TypeError, IndexError:
        return False


def is_fhir_job_set_matching(spec, launch_request, jobs):
    """Check existing FHIR Jobs against the runtime selected by native admission."""
    launch = authorized_fhir_source_worker_launch(spec, launch_request)
    if launch is not None:
        return all(is_fhir_source_job_matching(job, launch, launch_request) for job in jobs)
    if admitted_overlay_job(spec, launch_request) is None:
        return True
    from api.control_workers import _worker_job_manifest

    expected_job = _worker_job_manifest(spec, launch_request, os.getenv("HLTHPRT_WORKER_JOB_IMAGE", "").strip())
    return all(is_overlay_job_matching(job, expected_job) for job in jobs)
