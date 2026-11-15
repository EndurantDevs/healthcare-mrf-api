# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Keep a Source-admitted MRF acquisition inside its native dataset fence."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import json
import os
import re
from contextlib import asynccontextmanager, suppress

from sqlalchemy import text

from db.models import ImportRun, MRFFile, MRFPayer, MRFSource, db

IMPORTER = "mrf-source-discovery"
ENGINE = "healthcare-mrf-api"
MARKER = "mrf_acquisition"
REPLACEMENT = "mrf_acquisition_replacement"
DATASET_KEYS = ("control_relation", "source_relation", "payer_relation", "file_relation")
LEGACY_DATASET_KEYS = frozenset({"database_oid", "control_run_oid", "source_oid", "payer_oid", "file_oid"})
RELATION_PATTERN = r"[A-Za-z_][A-Za-z0-9_]{0,62}\.[A-Za-z_][A-Za-z0-9_]{0,62}"
RUNNABLE = frozenset({"queued", "starting", "running"})
CONTROL_RELATION_ENV = "HLTHPRT_MRF_ACQUISITION_CONTROL_RELATION"


def params_digest(params):
    """Bind the complete acquisition parameters with canonical JSON."""
    return hashlib.sha256(
        json.dumps(params, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode()
    ).hexdigest()


def _require(condition):
    if not condition:
        raise ValueError("mrf_acquisition_not_admitted")


def _json(value):
    return json.loads(value) if isinstance(value, str) else value


def _dataset(value):
    return (
        isinstance(value, dict)
        and set(value) == set(DATASET_KEYS)
        and all(isinstance(item, str) and re.fullmatch(RELATION_PATTERN, item) for item in value.values())
    )


def _dataset_matches(stored, current):
    # Legacy physical fields are archived metadata, never evidence of a separate dataset.
    return (isinstance(stored, dict) and set(stored) == LEGACY_DATASET_KEYS) or (_dataset(stored) and stored == current)


def _relation(model):
    table = model.__table__
    return ".".join('"' + part.replace('"', '""') + '"' for part in (table.schema, table.name) if part)


def _control_relation():
    configured = os.getenv(CONTROL_RELATION_ENV, "")
    _require(re.fullmatch(RELATION_PATTERN, configured) is not None)
    return '"' + configured.replace(".", '"."') + '"'


async def native_dataset_identity(connection, *, control_relation=None):
    """Read configured names using the connection's native PostgreSQL permissions."""
    control_relation = _control_relation() if control_relation is None else control_relation
    relations = (control_relation, _relation(MRFSource), _relation(MRFPayer), _relation(MRFFile))
    dataset_by_name = {key: relation.replace('"', "") for key, relation in zip(DATASET_KEYS, relations)}
    _require(_dataset(dataset_by_name))
    _require(
        all(
            relation in (name, '"' + name.replace(".", '"."') + '"')
            for relation, name in zip(relations, dataset_by_name.values())
        )
    )
    try:
        for relation in dataset_by_name.values():
            quoted = '"' + relation.replace(".", '"."') + '"'
            await connection.fetchval(f"SELECT 1 FROM {quoted} LIMIT 0")
    except Exception as exc:
        raise ValueError("mrf_acquisition_not_admitted") from exc
    return dataset_by_name


async def acquisition_dataset():
    """Expose named scope alongside the deprecated health wire format."""
    async with db.acquire_driver() as connection:
        dataset = await native_dataset_identity(connection)
        # Older clients echo these fields; current admission never compares them.
        legacy = await connection.fetchrow(
            "SELECT (SELECT oid::bigint FROM pg_catalog.pg_database WHERE datname=current_database()) AS database_oid, "
            "to_regclass($1)::oid::bigint AS control_run_oid, to_regclass($2)::oid::bigint AS source_oid, "
            "to_regclass($3)::oid::bigint AS payer_oid, to_regclass($4)::oid::bigint AS file_oid",
            *('"' + dataset[key].replace(".", '"."') + '"' for key in DATASET_KEYS),
        )
        return {
            "mrf_acquisition_dataset": dict(legacy),
            "mrf_acquisition_dataset_v2": dataset if is_named_discovery_worker() else None,
        }


def is_named_discovery_worker():
    """Only advertise named reservations to workers running this implementation."""
    launcher = os.getenv("HLTHPRT_WORKER_LAUNCHER", "process").strip().lower()
    if launcher == "process":
        return True
    image = (
        os.getenv("HLTHPRT_MRF_SOURCE_DISCOVERY_WORKER_IMAGE", "").strip()
        or os.getenv("HLTHPRT_WORKER_JOB_IMAGE", "").strip()
    )
    return (
        launcher == "kubernetes"
        and re.fullmatch(r"[^\s]+@sha256:[0-9a-f]{64}", image) is not None
        and image == os.getenv("HLTHPRT_RUNTIME_IMAGE_IDENTITY", "").strip()
    )


async def database_health(database):
    """Report database liveness and the available discovery dataset identity."""
    try:
        await database.execute(text("SELECT 1"))
        try:
            dataset_fields_by_name = await acquisition_dataset()
        except Exception:
            dataset_fields_by_name = {"mrf_acquisition_dataset": None, "mrf_acquisition_dataset_v2": None}
        return {"ok": True, **dataset_fields_by_name}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}


def _reservation(run, request, dataset):
    _require(isinstance(run, dict) and run.get("status") in RUNNABLE)
    marker = (run.get("metrics") or {}).get(MARKER)
    _require(isinstance(marker, dict) and marker.get("schema") == "mrf-acquisition-v1")
    for field, expected in (("run_id", request.get("run_id")), ("importer", IMPORTER), ("engine", ENGINE)):
        _require(run.get(field) == marker.get(field) == expected)
    _require(_dataset_matches(marker.get("dataset"), dataset))
    _require(marker.get("idempotency_key") == request.get("idempotency_key") and bool(marker.get("idempotency_key")))
    _require(
        marker.get("params_digest")
        == params_digest(run.get("params") or {})
        == params_digest(request.get("params") or {})
    )
    return marker


async def _replacement_owner(connection, marker, dataset, control_relation):
    link = marker.get("replacement_of")
    if link is None:
        return None
    _require(isinstance(link, dict) and type(link.get("attempt")) is int and link["attempt"] > 0)
    _require(
        all(
            isinstance(link.get(key), str) and re.fullmatch(r"[0-9a-f]{64}", link[key])
            for key in ("old_digest", "command_id")
        )
    )
    old = _json(
        await connection.fetchval(f"SELECT to_jsonb(r) FROM {control_relation} r WHERE run_id=$1", link.get("run_id"))
    )
    _require(
        isinstance(old, dict)
        and old.get("importer") == IMPORTER
        and old.get("engine") == ENGINE
        and old.get("status") == "succeeded"
    )
    metrics = old.get("metrics") or {}
    hold = metrics.get(REPLACEMENT)
    _require(metrics.get("catalog_sync_status") == "failed" and metrics.get("catalog_sync_attempt") == link["attempt"])
    _require(isinstance(hold, dict) and hold.get("schema") == "mrf-replacement-v1")
    _require(hold.get("run_id") == marker["run_id"] and _dataset_matches(hold.get("dataset"), dataset))
    _require(all(hold.get(key) == link[key] for key in ("attempt", "old_digest", "command_id")))
    return old["run_id"]


def _foreign_dataset_sql():
    dataset = "(r.metrics->'mrf_acquisition'->'dataset')"
    valid = " AND ".join(
        f"jsonb_typeof({dataset}->'{key}')='string' AND {dataset}->>'{key}' ~ '^{RELATION_PATTERN}$'"
        for key in DATASET_KEYS
    )
    keys = ",".join("'" + key + "'" for key in DATASET_KEYS)
    disjoint = " AND ".join(
        f"{dataset}->'{known}' <> $2::jsonb->'{target}'" for known in DATASET_KEYS[1:] for target in DATASET_KEYS[1:]
    )
    return (
        f"r.metrics->'mrf_acquisition'->>'schema'='mrf-acquisition-v1' AND jsonb_typeof({dataset})='object' "
        f"AND {dataset} ?& ARRAY[{keys}] AND {dataset} - ARRAY[{keys}] = '{{}}'::jsonb AND {valid} "
        f"AND {dataset}->'control_relation' = $2::jsonb->'control_relation' AND {disjoint}"
    )


async def require_reservation(
    connection, request, *, require_local=False, command=None, command_params=None, control_relation=None
):
    """Read the exact reservation and an untruncated pending-consumer existence query."""
    _require(isinstance(request.get("run_id"), str) and bool(request["run_id"]))
    control_relation = _control_relation() if control_relation is None else control_relation
    dataset = await native_dataset_identity(connection, control_relation=control_relation)
    reserved_run = _json(
        await connection.fetchval(f"SELECT to_jsonb(r) FROM {control_relation} r WHERE run_id=$1", request["run_id"])
    )
    marker = _reservation(reserved_run, request, dataset)
    old_id = await _replacement_owner(connection, marker, dataset, control_relation)
    blocker = await connection.fetchval(
        f"SELECT r.run_id FROM {control_relation} r WHERE r.importer=$1 "
        "AND r.run_id<>$3 AND ($4::text IS NULL OR r.run_id<>$4) AND ("
        "r.status IN ('queued','starting','running','finalizing','canceling') OR "
        "(r.status='succeeded' AND r.metrics->>'catalog_export_version' ~ '^[0-9]*[1-9][0-9]*$' AND "
        "lower(btrim(coalesce(r.metrics->>'catalog_sync_status',''))) NOT IN ('succeeded','superseded'))) "
        "AND NOT coalesce((" + _foreign_dataset_sql() + "),false) ORDER BY r.run_id LIMIT 1",
        IMPORTER,
        json.dumps(dataset),
        request["run_id"],
        old_id,
    )
    _require(blocker is None)
    active = await connection.fetchval(
        "SELECT run_id FROM " + _relation(ImportRun) + " WHERE importer=$1 AND run_id<>$2 "
        "AND status IN ('queued','starting','running','finalizing','canceling') "
        "AND coalesce(params->>'dry_run','false')<>'true' ORDER BY run_id LIMIT 1",
        IMPORTER,
        request["run_id"],
    )
    _require(active is None)
    if require_local:
        local = _json(
            await connection.fetchval(
                "SELECT to_jsonb(r) FROM " + _relation(ImportRun) + " r WHERE run_id=$1",
                request["run_id"],
            )
        )
        _require(isinstance(local, dict) and local.get("status") in RUNNABLE and local.get("importer") == IMPORTER)
        _require(local.get("engine") == ENGINE and local.get("idempotency_key") == marker["idempotency_key"])
        _require(params_digest(local.get("params") or {}) == marker["params_digest"])
    if command is not None:
        from process.mrf_source_discovery import discovery_command_params

        signature = inspect.signature(command)
        expected_params_by_name = {
            name: argument
            for name, argument in discovery_command_params(request.get("params") or {}).items()
            if name in signature.parameters and name != "run_id"
        }
        actual_params_by_name = {name: argument for name, argument in command_params.items() if name != "run_id"}
        _require(params_digest(expected_params_by_name) == params_digest(actual_params_by_name))
    return dataset


async def require_acquisition(request, *, require_local=False):
    """Require a Source reservation before admitting mutable MRF discovery."""
    if request.get("importer") != IMPORTER or (request.get("params") or {}).get("dry_run") is True:
        return
    async with db.acquire_driver() as connection:
        await require_reservation(connection, request, require_local=require_local)


async def require_acquisition_owner(run_id):
    """Recheck a subprocess's configured native scope without relocking its parent."""
    async with db.acquire_driver() as connection:
        local = _json(
            await connection.fetchval("SELECT to_jsonb(r) FROM " + _relation(ImportRun) + " r WHERE run_id=$1", run_id)
        )
        _require(isinstance(local, dict))
        await require_reservation(connection, local, require_local=True)


@asynccontextmanager
async def acquisition_command(command, command_params):
    """Keep the native dataset lock until every acquisition stage has settled."""
    if command_params.get("dry_run") is True:
        yield
        return
    run_id = command_params.get("run_id")
    _require(isinstance(run_id, str) and bool(run_id))
    control_relation = _control_relation()
    async with db.acquire_driver(detached=True) as connection:
        dataset = await native_dataset_identity(connection, control_relation=control_relation)
        lock_key = "mrf-discovery:dataset:" + ":".join(str(dataset[key]) for key in DATASET_KEYS)
        locked = await connection.fetchval("SELECT pg_try_advisory_lock(hashtextextended($1,0))", lock_key)
        _require(locked is True)
        try:
            local = _json(
                await connection.fetchval(
                    "SELECT to_jsonb(r) FROM " + _relation(ImportRun) + " r WHERE run_id=$1", run_id
                )
            )
            _require(isinstance(local, dict))
            rechecked_dataset = await require_reservation(
                connection,
                local,
                require_local=True,
                command=command,
                command_params=command_params,
                control_relation=control_relation,
            )
            _require(rechecked_dataset == dataset)
            yield
        finally:
            # Closing the detached driver also releases its session lock.
            with suppress(Exception):
                await asyncio.shield(connection.fetchval("SELECT pg_advisory_unlock(hashtextextended($1,0))", lock_key))
