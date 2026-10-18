# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Exact request replay and producer delivery state for pricing control runs."""

import hashlib
import json
import re

from sqlalchemy import and_, case, cast, func, literal
from sqlalchemy.dialects.postgresql import JSONB

PRICING_IMPORTERS = frozenset({"plan-pricing-projection", "plan-pricing-prewarm", "plan-pricing-em-distance"})
DELIVERY_FIELD = "plan_pricing_enqueue_delivery"
_MANAGED_KEY = re.compile(r"plan-pricing-projection(?:-retry)?:[0-9a-f]{64}")


def pricing_control_digest(payload, *, domain=None):
    """Keep the existing protected journal's canonical JSON and key domains."""
    body = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    return hashlib.sha256((body if domain is None else domain + ":" + body).encode()).hexdigest()


def is_managed_pricing_key(key):
    """Require one complete canonical base or retry key."""
    return type(key) is str and _MANAGED_KEY.fullmatch(key) is not None


def projection_admission_key(request):
    """Derive the existing journal's complete publication-attempt key."""
    return "plan-pricing-projection:" + pricing_control_digest(
        {
            name: request[name]
            for name in (
                "binding_manifest_digest",
                "projection_contract",
                "publication_identity",
                "publication_attempt_identity",
            )
        },
        domain="plan_pricing_projection_admission_v2",
    )


def projection_retry_key(base_key, parent_run_id):
    """Bind an authorized child to its original admission and exact parent."""
    return "plan-pricing-projection-retry:" + pricing_control_digest(
        {"admission_key": base_key, "retry_of_run_id": parent_run_id}, domain="plan_pricing_projection_retry_v1"
    )


def projection_attempt_fence_key(publication_identity, attempt_identity):
    """Share the existing native fence with the protected terminal CAS."""
    if any(
        type(value) is not str or re.fullmatch(r"[0-9a-f]{64}", value) is None
        for value in (publication_identity, attempt_identity)
    ):
        raise ValueError("pricing control attempt identity is invalid")
    return "control-plan-pricing-attempt:v1:" + publication_identity + ":" + attempt_identity


def require_pricing_replay(existing, expected):
    """A durable key owns the complete persisted request, including its parent."""
    if expected["importer"] not in PRICING_IMPORTERS:
        return
    fields = ("engine", "node_id", "importer", "idempotency_key", "retry_of_run_id", "params")
    encoded_requests = [
        json.dumps(
            {name: record.get(name) for name in fields},
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )
        for record in (existing, expected)
    ]
    if encoded_requests[0] != encoded_requests[1]:
        raise ValueError("pricing idempotency key belongs to a different request")


def initial_delivery(run_id):
    """Persist intent before the producer can send anything to Redis."""
    return {"run_id": run_id, "state": "pending"}


def _producer_state(current):
    fields = (DELIVERY_FIELD, "enqueue_adapter", "queue", "function", "job_id")
    return func.jsonb_build_object(*(value for name in fields for value in (name, current[name])))


def preserve_delivery(model, metrics):
    """Worker, cancellation and reconciliation writes cannot erase producer state."""
    current = func.coalesce(cast(model.metrics, JSONB), literal({}, type_=JSONB))
    supplied = cast(literal(metrics, type_=JSONB), JSONB)
    return case(
        (
            and_(model.importer.in_(sorted(PRICING_IMPORTERS)), current.has_key(DELIVERY_FIELD)),
            supplied.op("||")(_producer_state(current)),
        ),
        else_=supplied,
    )


def enqueue_ack_values(model, run):
    """Settle only this producer while preserving newer lifecycle state."""
    initial = and_(
        model.status == "queued",
        model.phase_detail == "created",
        model.finished_at.is_(None),
        model.progress["attempt_id"].as_string().is_(None),
        model.progress["attempt_started_at"].as_string().is_(None),
    )
    values_by_field = {
        field: case((initial, literal(run[field], type_=model.__table__.c[field].type)), else_=model.__table__.c[field])
        for field in ("status", "phase_detail", "heartbeat_at", "progress", "error")
    }
    current = func.coalesce(cast(model.metrics, JSONB), literal({}, type_=JSONB))
    acknowledged = cast(literal(run["metrics"], type_=JSONB), JSONB)
    # An enqueue exception can lose the reply after Redis accepted the job.
    state = "unknown" if (run.get("error") or {}).get("code") == "enqueue_failed" else "settled"
    marker_by_field = {"run_id": run["run_id"], "state": state, "outcome": run["status"]}
    values_by_field["metrics"] = (
        case((initial, current.op("||")(acknowledged)), else_=current)
        .op("||")(_producer_state(current))
        .op("||")(func.jsonb_build_object(DELIVERY_FIELD, cast(literal(marker_by_field, type_=JSONB), JSONB)))
    )
    return values_by_field
