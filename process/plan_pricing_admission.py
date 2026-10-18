# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Read-only protected admission for the installed bulk pricing boundary."""

import os
from dataclasses import dataclass

from db.plan_release_storage import plan_release_schema, storage_relation
from process.plan_pricing_control import (
    is_managed_pricing_key,
    pricing_control_digest,
    projection_admission_key,
    projection_attempt_fence_key,
    projection_retry_key,
    require_pricing_replay,
)
from process.plan_pricing_control_policy import load_pricing_control_policy, pricing_control_schema
from process.plan_release_storage_custody import _document, storage_roles, verify_storage_family

CONTROL_NAMES = ("catalog_release_preparation", "catalog_release_admission_attempt")
HISTORICAL_REVISIONS = frozenset(
    {
        "202610060002",
        "202610080001",
        "202610080002",
        "202610080003",
        "202610080004",
        "202610080005",
        "202610090001",
        "202610150003",
    }
)
BULK_REVISION = "202610150002"


def require(condition, code):
    """Refuse an incomplete protected control invariant."""
    if not condition:
        raise ValueError("pricing_control_" + code)


def is_reserved_pricing_key(key):
    """Recognize the existing reserved prefix before validating its exact form."""
    return type(key) is str and key.startswith(("plan-pricing-projection:", "plan-pricing-projection-retry:"))


async def is_managed_storage_installed(connection):
    """A historical scalar installation never becomes managed by table inference."""
    schema = pricing_control_schema()
    names = [storage_relation(schema, name) for name in (*CONTROL_NAMES, "alembic_version")]
    present = await connection.fetch("SELECT to_regclass(name)::oid AS oid FROM unnest($1::text[]) names(name)", names)
    controls = [row["oid"] for row in present[:2]]
    require(present[2]["oid"] is not None, "schema_revision_missing")
    revisions = tuple(
        row["version_num"]
        for row in await connection.fetch(
            f"SELECT version_num FROM {storage_relation(schema, 'alembic_version')} ORDER BY version_num"
        )
    )
    require(len(revisions) == 1, "schema_revision_invalid")
    if revisions[0] in HISTORICAL_REVISIONS:
        require(not any(controls), "partial_storage")
        return False
    require(revisions == (BULK_REVISION,) and all(controls), "schema_revision_invalid")
    return True


def _dispatches(record_by_field, request):
    require(
        pricing_control_digest(request) == record_by_field["request_digest"]
        and projection_admission_key(request) == record_by_field["admission_key"]
        and request["publication_identity"] == record_by_field["publication_identity"]
        and request["publication_attempt_identity"] == record_by_field["attempt_identity"],
        "request_identity_changed",
    )
    history = _document(record_by_field["dispatches"])
    require(type(history) is list and 0 < len(history) <= 3, "dispatch_identity_invalid")
    accepted_run_ids = []
    for index, dispatch in enumerate(history):
        parent = history[index - 1]["run_id"] if index else None
        expected = (
            projection_retry_key(record_by_field["admission_key"], parent)
            if index
            else record_by_field["admission_key"]
        )
        require(
            type(dispatch) is dict
            and set(dispatch) == {"key", "retry_of", "run_id"}
            and dispatch["key"] == expected
            and dispatch["retry_of"] == parent
            and (index == 0 or type(parent) is str and bool(parent))
            and (index == len(history) - 1 or bool(dispatch["run_id"]))
            and (dispatch["run_id"] is None or type(dispatch["run_id"]) is str and bool(dispatch["run_id"])),
            "dispatch_identity_invalid",
        )
        if dispatch["run_id"] is not None:
            accepted_run_ids.append(dispatch["run_id"])
    require(len(set(accepted_run_ids)) == len(accepted_run_ids), "dispatch_run_reused")
    require(
        record_by_field["engine_run_id"] == (accepted_run_ids[-1] if accepted_run_ids else None), "dispatch_run_changed"
    )
    require(
        (record_by_field["status"] == "dispatching" and history[-1]["run_id"] is None)
        or (
            record_by_field["status"] in {"accepted", "ready", "unavailable", "failed"}
            and bool(accepted_run_ids)
            and history[-1]["run_id"] is not None
        ),
        "dispatch_state_invalid",
    )
    return history


async def _root_key(connection, run):
    current = run
    seen_run_ids = set()
    lineage_rows = [run]
    relation = storage_relation(plan_release_schema(), "import_run")
    for _index in range(3):
        parent = current.get("retry_of_run_id")
        if parent is None:
            return current.get("idempotency_key"), lineage_rows
        require(type(parent) is str and parent not in seen_run_ids, "retry_lineage_invalid")
        seen_run_ids.add(parent)
        previous_by_field = await connection.fetchrow(f"SELECT * FROM {relation} WHERE run_id=$1", parent)
        require(previous_by_field is not None, "retry_parent_missing")
        previous_by_field = dict(previous_by_field)
        previous_by_field["params"] = _document(previous_by_field["params"])
        lineage_rows.append(previous_by_field)
        current = previous_by_field
    raise ValueError("pricing_control_retry_lineage_invalid")


def _require_lineage(root_key, lineage_rows):
    root = lineage_rows[-1]
    require(
        is_managed_pricing_key(root_key) and not root_key.startswith("plan-pricing-projection-retry:"),
        "managed_key_invalid",
    )
    require(
        all(parent["status"] in {"failed", "canceled", "cancelled", "dead_letter"} for parent in lineage_rows[1:]),
        "retry_parent_not_terminal",
    )
    for run in lineage_rows:
        expected_key = (
            projection_retry_key(root_key, run["retry_of_run_id"]) if run.get("retry_of_run_id") else root_key
        )
        require(run.get("idempotency_key") == expected_key, "retry_key_changed")
        require_pricing_replay(
            run, {**root, "idempotency_key": run["idempotency_key"], "retry_of_run_id": run.get("retry_of_run_id")}
        )


@dataclass(frozen=True)
class PricingAdmission:
    record_by_field: dict
    request: dict
    dispatch: dict
    policy: object
    family: dict


_ADMISSION_RECORD_SQL = (
    "SELECT admission.*,preparation.status AS preparation_status,preparation.scope_digest,"
    "preparation.receipt_digest,preparation.inventory FROM {admissions} admission "
    "JOIN {preparations} preparation USING(preparation_id) WHERE admission.admission_key=$1"
)


def _admission_record_sql():
    schema = pricing_control_schema()
    return _ADMISSION_RECORD_SQL.format(
        admissions=storage_relation(schema, CONTROL_NAMES[1]),
        preparations=storage_relation(schema, CONTROL_NAMES[0]),
    )


async def _fenced_admission_record(connection, root_key):
    record_by_field = await connection.fetchrow(_admission_record_sql(), root_key)
    require(record_by_field is not None, "admission_missing")
    attempt_pair = (record_by_field["publication_identity"], record_by_field["attempt_identity"])
    await connection.execute("SET LOCAL lock_timeout='500ms'; SET LOCAL statement_timeout='5s'")
    await connection.execute(
        "SELECT pg_advisory_xact_lock(pg_catalog.hashtextextended($1,0))", projection_attempt_fence_key(*attempt_pair)
    )
    record_by_field = dict(await connection.fetchrow(_admission_record_sql(), root_key))
    require(
        (record_by_field["publication_identity"], record_by_field["attempt_identity"]) == attempt_pair,
        "attempt_identity_changed",
    )
    return record_by_field


async def _qualified_control_family(connection, policy):
    identity = await connection.fetchrow("SELECT session_user::text AS session,current_user::text AS current")
    require(
        identity["session"] == identity["current"] and identity["current"] in policy.control_reader_roles,
        "genuine_control_required",
    )
    family = await verify_storage_family(connection, plan_release_schema(), **policy.storage_arguments())
    await storage_roles(connection, pricing_control_schema(), **policy.storage_arguments())
    from process.plan_pricing_control_catalog import require_control_catalog

    await require_control_catalog(connection, family)
    return family


async def _managed_pricing_admission(connection, run, *, action):
    """Hold the same native attempt fence through the caller's delivery/claim cut."""
    require(action in {"create", "replay", "launch", "claim", "inspect"}, "action_invalid")
    if run.get("importer") != "plan-pricing-projection":
        return None
    if not (is_reserved_pricing_key(run.get("idempotency_key")) or run.get("retry_of_run_id")):
        return None
    if not await is_managed_storage_installed(connection):
        return None
    root_key, lineage_rows = await _root_key(connection, run)
    if not any(is_reserved_pricing_key(run_row.get("idempotency_key")) for run_row in lineage_rows):
        return None
    _require_lineage(root_key, lineage_rows)
    require(connection.is_in_transaction(), "transaction_required")
    require(
        await connection.fetchval("SHOW transaction_isolation") == "read committed", "fresh_control_snapshot_required"
    )
    policy = load_pricing_control_policy()
    family = await _qualified_control_family(connection, policy)
    record_by_field = (
        await connection.fetchrow(_admission_record_sql(), root_key)
        if action == "inspect"
        else await _fenced_admission_record(connection, root_key)
    )
    require(record_by_field is not None, "admission_missing")
    request = _document(record_by_field["request"])
    history = _dispatches(record_by_field, request)
    require(
        run.get("engine") == "healthcare-mrf-api"
        and run.get("node_id") == os.getenv("HLTHPRT_IMPORT_NODE_ID")
        and run.get("triggered_by") == "release",
        "control_identity_changed",
    )
    params_by_field = {name: request[name] for name in ("binding_manifest_digest", "projection_contract", "bindings")}
    require_pricing_replay(run, {**run, "params": params_by_field, "node_id": record_by_field["node_id"]})
    dispatches = [
        dispatch
        for dispatch in history
        if dispatch["key"] == run["idempotency_key"] and dispatch["retry_of"] == run.get("retry_of_run_id")
    ]
    require(len(dispatches) == 1, "dispatch_not_authorized")
    dispatch = dispatches[0]
    require(dispatch["run_id"] in (None, run["run_id"]), "dispatch_run_changed")
    if action not in {"replay", "inspect"}:
        require(
            record_by_field["preparation_status"] == "admitting"
            and record_by_field["status"] in {"dispatching", "accepted"},
            "admission_closed",
        )
        from process.plan_pricing_retained_inputs import require_retained_pricing_inputs

        await require_retained_pricing_inputs(
            connection, PricingAdmission(record_by_field, request, dispatch, policy, family)
        )
    return PricingAdmission(record_by_field, request, dispatch, policy, family)


async def require_managed_pricing_admission(connection, run, *, action):
    """Malformed protected state is a refusal, never an unmanaged fallback."""
    try:
        return await _managed_pricing_admission(connection, run, action=action)
    except KeyError, TypeError, IndexError, StopIteration:
        raise ValueError("pricing_control_protected_state_invalid") from None
