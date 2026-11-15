# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Source-only acquisition for protected taxonomy snapshots."""

import asyncio
import hashlib
import json
import os
import re
from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError

from process import nucc_protected_control as control
from process import nucc_protected_stage as stages
from process import nucc_snapshot_partition as partition
from process import reference_family_archive as native
from process.control_cancel import ImportCancelledError
from process.control_lifecycle import _is_protected_nucc_claim_parent
from process.entity_address_cutover_contract import postgres_sqlstate

MAX_BATCH_ROWS = stages.MAX_BATCH_ROWS
STAGE_CONTRACT = "nucc-protected-source-stage.v3"
READY_CONTRACT = "nucc-protected-source-ready.v3"
MARKER_FIELDS = frozenset(
    {
        "contract",
        "phase",
        "preparation_id",
        "admission_sha256",
        "node_id",
        "run_id",
        "attempt_id",
        "attempt_started_at",
        "generation_id",
        "publication_fence",
        "source_contract_sha256",
        "database_oid",
        "history_oid",
        "owner_oid",
        "publisher_oid",
        "builder_oid",
        "parent_oid",
        "parent_index_oid",
        "previous_child_oid",
        "source_predecessor",
        "ownership",
        "handoff",
        "marker_sha256",
    }
)
READY_FIELDS = frozenset(
    {
        "contract",
        "preparation_id",
        "admission_sha256",
        "stage_oid",
        "attempt_id",
        "attempt_started_at",
        "source_files",
        "total_rows",
        "ready_sha256",
    }
)
_LEGACY_MARKER_FIELDS = MARKER_FIELDS
_LEGACY_READY_FIELDS = READY_FIELDS
_OLD_LOCATIONS = {
    "database_oid",
    "history_oid",
    "owner_oid",
    "publisher_oid",
    "builder_oid",
    "parent_oid",
    "parent_index_oid",
    "previous_child_oid",
}
MARKER_FIELDS = frozenset((MARKER_FIELDS - _OLD_LOCATIONS) | control._NAMED_FIELDS | {"publisher_role", "builder_role"})
READY_FIELDS = frozenset((READY_FIELDS - {"stage_oid"}) | {"stage"})


def _marker_locations(marker):
    if "parent" in marker:
        return control.named_locations(marker)
    ownership = _stage_ownership(marker["ownership"])
    return {
        "parent": ("mrf", "nucc_taxonomy"),
        "history": ("mrf", "import_run"),
        "child": (ownership.schema_name, partition.CHILD_TABLE),
    }


WAIT_SECONDS = 300
POLL_SECONDS = 0.25


def _require(condition, reason):
    if not condition:
        raise native.ReferenceFamilyArchiveError("NUCC Source " + reason)


def marker_digest(marker):
    """Hash a bounded local wire preimage, without granting native authority."""
    _require(type(marker) is dict, "marker differs")
    return control.receipt_digest({key: value for key, value in marker.items() if key != "marker_sha256"})


def ready_digest(ready):
    """Hash the closed ready input preimage without treating it as publication proof."""
    _require(type(ready) is dict, "ready differs")
    return control.receipt_digest({key: value for key, value in ready.items() if key != "ready_sha256"})


def _stage_ownership(ownership_by_field):
    named_fields = {"importer_id", "dataset_id", "schema_name", "relations", "sequences"}
    legacy_fields = {
        "importer_id",
        "dataset_id",
        "schema_name",
        "schema_oid",
        "relation_oids",
        "sequence_oids",
        "auxiliary_oid",
    }
    _require(
        type(ownership_by_field) is dict and set(ownership_by_field) in (named_fields, legacy_fields),
        "ownership fields differ",
    )
    control._uuid(ownership_by_field["dataset_id"])
    dataset = UUID(ownership_by_field["dataset_id"])
    is_current = set(ownership_by_field) == named_fields
    _require(
        ownership_by_field["importer_id"] == "nucc"
        and ownership_by_field["schema_name"] == native.reference_family_stage_schema(dataset),
        "ownership differs",
    )
    if is_current:
        _require(
            ownership_by_field["relations"] == [{"table_name": "nucc_taxonomy"}]
            and ownership_by_field["sequences"] == [],
            "ownership differs",
        )
    else:
        entries = ownership_by_field["relation_oids"]
        _require(
            type(entries) is list
            and len(entries) == 1
            and type(entries[0]) is list
            and len(entries[0]) == 2
            and entries[0][0] == "nucc_taxonomy"
            and ownership_by_field["sequence_oids"] == []
            and ownership_by_field["auxiliary_oid"] is None,
            "ownership differs",
        )
    return native.ReferenceFamilyStageOwnership(
        "nucc", dataset, ownership_by_field["schema_name"], None, (("nucc_taxonomy", None),)
    )


def _marker_identity(marker):
    for field in ("preparation_id", "generation_id", "publication_fence"):
        control._uuid(marker[field])
    for field in ("node_id", "run_id", "attempt_id"):
        control._bounded_identity(marker[field], 161 if field == "attempt_id" else 128)
    _require(
        re.fullmatch(re.escape(marker["run_id"]) + r":[0-9a-f]{32}", marker["attempt_id"]) is not None,
        "attempt differs",
    )
    control._bounded_identity(marker["attempt_started_at"], 64)
    try:
        started = datetime.fromisoformat(marker["attempt_started_at"].replace("Z", "+00:00"))
    except ValueError:
        raise native.ReferenceFamilyArchiveError("NUCC Source attempt time differs") from None
    _require(started.tzinfo is not None, "attempt time differs")
    control._digests(marker, ("admission_sha256", "source_contract_sha256", "marker_sha256"))
    control._source_generation(marker["source_predecessor"], predecessor=True, legacy_read="parent" not in marker)
    if "parent" in marker:
        control._native_identities(marker)
        for field in ("publisher_role", "builder_role"):
            _require(
                type(marker[field]) is str and native._schema_name(marker[field]) == marker[field],
                "role declaration differs",
            )


def validate_source_stage_marker(marker):
    """Decode logical Source evidence while retaining old raw physical editions unchanged."""
    _require(type(marker) is dict and set(marker) in (MARKER_FIELDS, _LEGACY_MARKER_FIELDS), "marker fields differ")
    _require(marker["contract"] == STAGE_CONTRACT and marker["phase"] in {"copying", "closed"}, "marker phase differs")
    _marker_identity(marker)
    ownership = _stage_ownership(marker["ownership"])
    if "parent" in marker:
        _require(marker_digest(marker) == marker["marker_sha256"], "marker digest differs")
        _require(_marker_locations(marker)["child"] == (ownership.schema_name, "nucc_taxonomy"), "stage name differs")
    if marker["phase"] == "copying":
        _require(marker["handoff"] is None, "copying marker invents a handoff")
    else:
        handoff = control.validate_handoff(marker["handoff"])
        shared = (
            set(handoff) & set(marker) - {"contract", "ownership", "marker_sha256", "phase", "handoff"} - _OLD_LOCATIONS
        )
        _require(all(handoff[key] == marker[key] for key in shared), "closed handoff differs")
        _require(
            control.named_locations(handoff)["child"] == (ownership.schema_name, "nucc_taxonomy"),
            "closed child differs",
        )
    return json.loads(json.dumps(marker))


@dataclass(frozen=True)
class NuccSourceStage:
    """Local wire reconstruction alone never authorizes COPY or publication."""

    marker: dict
    ownership: native.ReferenceFamilyStageOwnership
    ready: dict | None = None

    @classmethod
    def from_wire(cls, marker):
        """Reconstruct the fixed local locator shape without granting any native rights."""
        marker = validate_source_stage_marker(marker)
        return cls(marker, _stage_ownership(marker["ownership"]))


def _is_same_stage_identity(expected, observed):
    changing = {"phase", "handoff", "marker_sha256"} | _OLD_LOCATIONS | {"ownership"}
    return {key: value for key, value in expected.items() if key not in changing} == {
        key: value for key, value in observed.items() if key not in changing
    } and _stage_ownership(expected["ownership"]) == _stage_ownership(observed["ownership"])


async def _native_stage_marker(session, ownership):
    await native.verify_reference_family_stage_ownership(session, ownership)
    observed = (
        (
            await session.execute(
                text("""SELECT
 CASE WHEN octet_length(obj_description(c.oid,'pg_class'))<=131072 THEN obj_description(c.oid,'pg_class') ELSE NULL END AS marker,
 c.relispartition AS attached,c.relkind='r' AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity AS ordinary
 FROM pg_class c WHERE c.oid=to_regclass(:relation)"""),
                {"relation": partition.relation_sql((ownership.schema_name, "nucc_taxonomy"))},
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(
        observed is not None and observed["ordinary"] is True and type(observed["marker"]) is str,
        "native marker unavailable",
    )
    try:
        marker = validate_source_stage_marker(json.loads(observed["marker"]))
    except ValueError, TypeError:
        raise native.ReferenceFamilyArchiveError("NUCC Source native marker encoding differs") from None
    _require(
        json.dumps(marker, sort_keys=True, separators=(",", ":")) == observed["marker"],
        "native marker is not canonical",
    )
    _require(observed["attached"] is False or marker["phase"] == "closed", "writable stage is attached")
    return marker, observed["attached"]


async def _copying_grants(session, stage):
    ownership = stage.ownership
    relation = partition.relation_sql((ownership.schema_name, "nucc_taxonomy"))
    granted = await session.scalar(
        text("""SELECT has_table_privilege(current_user,:relation,'INSERT')
 AND has_schema_privilege(current_user,:schema,'USAGE') AND NOT has_schema_privilege(current_user,:schema,'CREATE')
 AND NOT EXISTS(SELECT FROM pg_index WHERE indrelid=to_regclass(:relation))"""),
        {"relation": relation, "schema": ownership.schema_name},
    )
    _require(granted is True, "COPY grant unavailable or heap is not isolated")
    address = await native._relation_oid(session, ownership.schema_name, "nucc_taxonomy")
    await native.require_native_read_catalog(session, (address,))


async def _source_stage_state(session, stage, *, copying, allow_pending_retry=False):
    actor = await control._source_transaction(session)
    receipt = await control.reconcile_source_publication(session, stage.marker["run_id"])
    run = await control._locked_source_run(session, stage.marker["run_id"])
    marker, attached = await _native_stage_marker(session, stage.ownership)
    _require(_is_same_stage_identity(stage.marker, marker), "Owner stage identity changed")
    _require(
        not attached or receipt is not None and run["status"] == "succeeded", "attached stage lacks its Owner outcome"
    )
    _require(
        marker["node_id"] == actor["node_id"]
        and ("builder_role" not in marker or marker["builder_role"] == actor["role"]),
        "Source actor differs",
    )
    _require(
        all(run[field] == marker[field] for field in ("node_id", "run_id"))
        and control.source_contract_digest(marker, run["params"]) == marker["source_contract_sha256"],
        "original Source attempt changed",
    )
    if allow_pending_retry and marker["phase"] == "copying":
        _source_pending_attempt(NuccSourceStage.from_wire(marker), run)
    else:
        _require(
            all((run["progress"] or {}).get(field) == marker[field] for field in ("attempt_id", "attempt_started_at")),
            "original Source attempt changed",
        )
    if copying:
        _require(
            receipt is None
            and marker["phase"] == "copying"
            and run["status"] == "running"
            and (run["metrics"] or {}).get("nucc_handoff") is None,
            "COPY attempt is not running",
        )
        parent = await partition.capture_nucc_parent(session)
        _require(
            parent["parent"] == _marker_locations(marker)["parent"]
            and ("previous_child" not in marker or parent["child"] == partition.qualified(marker["previous_child"])),
            "Source parent changed",
        )
        from process.reference_family_result_generation import read_reference_family_result_generation_authority

        prior = await read_reference_family_result_generation_authority(session, importer_id="nucc", schema_name="mrf")
        _require(
            prior == control._source_generation(marker["source_predecessor"], predecessor=True)
            and (prior.relations is None or prior.relations == (parent["child"],)),
            "Source predecessor changed",
        )
        await _copying_grants(session, stage)
    await control._recheck_source_transaction(session, actor)
    return run, marker


async def _wait_nucc_source_poll(database, poll):
    """Retry only rolled-back lock contention within the existing Source wait budget."""
    async with asyncio.timeout(WAIT_SECONDS):
        while True:
            try:
                async with database.transaction() as session:
                    result = await poll(session)
                if result is not None:
                    return result
            except DBAPIError as error:
                if postgres_sqlstate(error) != "55P03":
                    raise
            await asyncio.sleep(POLL_SECONDS)


async def wait_nucc_source_stage(database, ctx):
    """Wait for a genuine Owner-created heap without holding a Source transaction between polls."""
    run_id, attempt, started, _suffix = native._nucc_attempt(ctx)

    async def poll(session):
        """Authenticate the current allocation in this polling transaction."""
        actor = await control._source_transaction(session)
        _require(await control.reconcile_source_publication(session, run_id) is None, "Source already published")
        run = await control._locked_source_run(session, run_id)
        _require(
            run["status"] == "running"
            and (run["progress"] or {}).get("attempt_id") == attempt
            and (run["progress"] or {}).get("attempt_started_at") == started,
            "Source wait attempt changed",
        )
        locator = (run["metrics"] or {}).get("nucc_source_stage")
        if locator is not None:
            stage = NuccSourceStage.from_wire(locator)
            _require(stage.marker["run_id"] == run_id, "locator run differs")
            _current, marker = await _source_stage_state(session, stage, copying=True, allow_pending_retry=True)
        if locator is not None and marker["attempt_id"] == attempt and marker["attempt_started_at"] == started:
            return stage
        await control._recheck_source_transaction(session, actor)

    return await _wait_nucc_source_poll(database, poll)


async def copy_nucc_source_batch(session, stage, model_rows):
    """Authenticate one bounded isolated batch and use only native COPY."""
    _require(isinstance(stage, NuccSourceStage), "stage type differs")
    _require(type(model_rows) in (list, tuple) and 0 < len(model_rows) <= MAX_BATCH_ROWS, "COPY batch exceeds bound")
    actor = await _source_copy_state(session, stage)
    columns = tuple(native.reference_family_spec("nucc").model_types[0].__table__.columns.keys())
    count = await stages.copy_nucc_stage(
        session, stage.ownership, [tuple(row.get(name) for name in columns) for row in model_rows]
    )
    await control._recheck_source_transaction(session, actor)
    return count


async def _source_copy_state(session, stage):
    """Authenticate only the isolated target; COPY holds no action, Source-row or shared-parent lock."""
    actor = await control._source_transaction(session)
    control._bounded_identity(stage.marker["run_id"])
    fields = ",".join(control._SOURCE_SELECT)
    run = (
        (
            await session.execute(
                text(
                    f"SELECT {fields} FROM mrf.import_run WHERE run_id=:run_id "
                    "AND octet_length(params::text)<=131072 AND octet_length(metrics::text)<=393216"
                ),
                {"run_id": stage.marker["run_id"]},
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(
        run is not None and run["engine"] == "healthcare-mrf-api" and run["importer"] == "nucc",
        "COPY real Source run differs",
    )
    marker, attached = await _native_stage_marker(session, stage.ownership)
    _require(
        _is_same_stage_identity(stage.marker, marker) and marker["phase"] == "copying" and not attached,
        "COPY Owner target changed",
    )
    _require(
        marker["node_id"] == actor["node_id"]
        and ("builder_role" not in marker or marker["builder_role"] == actor["role"]),
        "COPY Source actor differs",
    )
    _require(
        all(run[field] == marker[field] for field in ("node_id", "run_id"))
        and all((run["progress"] or {}).get(field) == marker[field] for field in ("attempt_id", "attempt_started_at"))
        and control.source_contract_digest(marker, run["params"]) == marker["source_contract_sha256"],
        "COPY original Source attempt changed",
    )
    _require(
        run["status"] == "running"
        and (run["metrics"] or {}).get("nucc_handoff") is None
        and (run["metrics"] or {}).get("nucc_source_ready") is None,
        "COPY Source attempt is closed",
    )
    await _copying_grants(session, stage)
    return actor


def _source_files(source_files):
    _require(type(source_files) is list and 0 < len(source_files) <= 8, "input files differ")
    for evidence in source_files:
        _require(
            type(evidence) is dict and set(evidence) == {"url", "sha256", "bytes", "row_count"}, "input fields differ"
        )
        control._bounded_identity(evidence["url"], 4096)
        control._digests(evidence, ("sha256",))
        _require(
            type(evidence["bytes"]) is int
            and 0 < evidence["bytes"] <= 8 * 1024**2
            and type(evidence["row_count"]) is int
            and 0 < evidence["row_count"] <= 10000,
            "input census differs",
        )
    _require(sum(evidence["row_count"] for evidence in source_files) <= 10000, "input set exceeds bound")
    return json.loads(json.dumps(source_files))


def validate_source_ready(ready, marker):
    """Validate the exact ready-input edition without rewriting historical bytes or tokens."""
    marker = validate_source_stage_marker(marker)
    _require(
        type(ready) is dict
        and set(ready) in (READY_FIELDS, _LEGACY_READY_FIELDS)
        and ready["contract"] == READY_CONTRACT,
        "ready fields differ",
    )
    _require(
        all(
            ready[key] == marker[key]
            for key in ("preparation_id", "admission_sha256", "attempt_id", "attempt_started_at")
        ),
        "ready identity differs",
    )
    if "stage" in ready:
        ownership = _stage_ownership(marker["ownership"])
        _require(partition.qualified(ready["stage"]) == (ownership.schema_name, "nucc_taxonomy"), "ready stage differs")
    files = _source_files(ready["source_files"])
    _require(
        type(ready["total_rows"]) is int and ready["total_rows"] == sum(item["row_count"] for item in files),
        "ready census differs",
    )
    control._digests(ready, ("ready_sha256",))
    if "stage" in ready:
        _require(ready_digest(ready) == ready["ready_sha256"], "ready digest differs")
    return json.loads(json.dumps(ready))


async def record_nucc_source_ready(session, stage, source_files):
    """Record input evidence on the exact running Source attempt; this grants no publication authority."""
    run, marker = await _source_stage_state(session, stage, copying=True)
    source_files = _source_files(source_files)
    ready_by_field = {
        "contract": READY_CONTRACT,
        "preparation_id": marker["preparation_id"],
        "admission_sha256": marker["admission_sha256"],
        "stage": [stage.ownership.schema_name, "nucc_taxonomy"],
        "attempt_id": marker["attempt_id"],
        "attempt_started_at": marker["attempt_started_at"],
        "source_files": source_files,
        "total_rows": sum(evidence["row_count"] for evidence in source_files),
    }
    ready_by_field["ready_sha256"] = ready_digest(ready_by_field)
    ready_by_field = validate_source_ready(ready_by_field, marker)
    previous = (run["metrics"] or {}).get("nucc_source_ready")
    if previous is not None:
        previous = validate_source_ready(previous, marker)
        _require(
            all(
                previous[key] == ready_by_field[key]
                for key in (
                    "preparation_id",
                    "admission_sha256",
                    "attempt_id",
                    "attempt_started_at",
                    "source_files",
                    "total_rows",
                )
            ),
            "ready input changed",
        )
        return previous
    changed = await session.scalar(
        text("""UPDATE mrf.import_run SET
 metrics=(metrics::jsonb||jsonb_build_object('nucc_source_ready',CAST(:ready AS jsonb)))::json,
 heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC',phase_detail='taxonomy input ready'
 WHERE run_id=:run_id AND node_id=:node_id AND engine='healthcare-mrf-api' AND importer='nucc'
 AND status='running' AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at
 AND metrics->'nucc_handoff' IS NULL AND metrics->'nucc_source_ready' IS NULL RETURNING run_id"""),
        {**marker, "ready": json.dumps(ready_by_field, sort_keys=True, separators=(",", ":"))},
    )
    _require(changed == marker["run_id"], "ready attempt changed")
    return ready_by_field


class NuccSourceReadyNotCommittedError(native.ReferenceFamilyArchiveError):
    """A fenced native read proved the ready transition did not commit."""


async def reconcile_nucc_source_handoff(session, run_id):
    """Recover only a genuine Owner-closed handoff before an API action or retry can change its attempt."""
    actor = await control._source_transaction(session)
    receipt = await control.reconcile_source_publication(session, run_id)
    run = await control._locked_source_run(session, run_id)
    _require(run["status"] != "succeeded" or receipt is not None, "success lacks its actual Owner outcome")
    existing = (run["metrics"] or {}).get("nucc_handoff")
    if existing is not None:
        await control._recheck_source_transaction(session, actor)
        return control.validate_handoff(existing)
    _require(receipt is None, "committed Owner outcome lacks its original handoff")
    if run["status"] in {"canceling", "canceled", "cancelled"}:
        raise ImportCancelledError("taxonomy Source cancellation prevents a new handoff")
    locator = (run["metrics"] or {}).get("nucc_source_stage")
    ready = (run["metrics"] or {}).get("nucc_source_ready")
    if locator is None:
        _require(ready is None, "ready input lacks its native locator")
        await control._recheck_source_transaction(session, actor)
        return None
    stage = NuccSourceStage.from_wire(locator)
    _require(stage.marker["run_id"] == run_id, "handoff locator run differs")
    run, marker = await _source_stage_state(session, stage, copying=False, allow_pending_retry=True)
    if marker["phase"] == "copying":
        await control._recheck_source_transaction(session, actor)
        return None
    _require(run["status"] in {"running", "failed", "dead_letter"}, "closed handoff attempt cannot resume")
    ready = validate_source_ready((run["metrics"] or {}).get("nucc_source_ready"), marker)
    handoff = control.validate_handoff(marker["handoff"])
    _require(ready["total_rows"] == handoff["row_count"], "closed ready census differs")
    changed = await session.scalar(
        text("""UPDATE mrf.import_run SET status='finalizing',
 phase_detail=:phase,heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC',error=NULL,finished_at=NULL,
 metrics=(metrics::jsonb||jsonb_build_object('nucc_handoff',CAST(:handoff AS jsonb)))::json
 WHERE run_id=:run_id AND node_id=:node_id AND engine='healthcare-mrf-api' AND importer='nucc'
 AND status=:prior_status AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at
 AND metrics->>'nucc_handoff' IS NULL AND metrics::jsonb->'nucc_source_ready'=CAST(:ready AS jsonb)
 AND metrics::jsonb->'nucc_source_stage'=CAST(:locator AS jsonb) RETURNING run_id"""),
        {
            **marker,
            "phase": control.HANDOFF_PHASE,
            "prior_status": run["status"],
            "handoff": json.dumps(handoff, sort_keys=True, separators=(",", ":")),
            "ready": json.dumps(ready, sort_keys=True, separators=(",", ":")),
            "locator": json.dumps(locator, sort_keys=True, separators=(",", ":")),
        },
    )
    _require(changed == run_id, "closed handoff original attempt changed")
    await control._recheck_source_transaction(session, actor)
    return handoff


async def wait_nucc_source_handoff(database, stage):
    """Return only a committed original Source handoff equal to the real closed Owner marker."""
    _require(isinstance(stage, NuccSourceStage) and type(stage.ready) is dict, "ready stage differs")
    validate_source_ready(stage.ready, stage.marker)

    async def poll(session):
        """Authenticate the committed handoff in this polling transaction."""
        run, marker = await _source_stage_state(session, stage, copying=False)
        actual_ready = (run["metrics"] or {}).get("nucc_source_ready")
        if actual_ready is None and marker["phase"] == "copying" and run["status"] == "running":
            raise NuccSourceReadyNotCommittedError("NUCC Source ready transition did not commit")
        _require(actual_ready == stage.ready, "committed ready input differs")
        handoff = (run["metrics"] or {}).get("nucc_handoff")
        if handoff is not None:
            handoff = control.validate_handoff(handoff)
            _require(
                marker["phase"] == "closed"
                and marker["handoff"] == handoff
                and run["status"] in {"finalizing", "succeeded"},
                "closed Source handoff differs",
            )
            _require(handoff["row_count"] == stage.ready["total_rows"], "closed input census differs")
            return handoff
        if run["status"] in {"canceling", "canceled", "cancelled"}:
            raise ImportCancelledError("taxonomy Source cancellation is pending")
        _require(run["status"] == "running", "Source input failed before handoff")

    return await _wait_nucc_source_poll(database, poll)


def _require_cancel_inputs(stage, run):
    locator = (run["metrics"] or {}).get("nucc_source_stage")
    handoff = (run["metrics"] or {}).get("nucc_handoff")
    ready = (run["metrics"] or {}).get("nucc_source_ready")
    if locator is None:
        # Lost allocation acknowledgement is ordinary cancellation, never publication authority.
        _require(handoff is None and ready is None, "cancellation input lacks its locator")
    else:
        _require(
            _is_same_stage_identity(stage.marker, validate_source_stage_marker(locator)), "cancellation locator differs"
        )
    if handoff is not None:
        handoff = control.validate_handoff(handoff)
        _require(
            stage.marker["phase"] == "closed" and stage.marker["handoff"] == handoff, "cancellation handoff differs"
        )
    if ready is not None:
        validate_source_ready(ready, stage.marker)


def _source_pending_attempt(stage, run):
    """An old copying locator permits only pending allocation or cancellation cleanup."""
    _require(type(run["progress"]) is dict, "cancellation Source progress differs")
    progress = run["progress"] or {}
    _run_id, attempt, started, _suffix = native._nucc_attempt(
        {
            "control_run_id": run["run_id"],
            "context": {
                "_control_attempt_id": progress.get("attempt_id"),
                "_control_attempt_started_at": progress.get("attempt_started_at"),
            },
        }
    )
    current_by_field = {"attempt_id": attempt, "attempt_started_at": started}
    if all(current_by_field[field] == stage.marker[field] for field in current_by_field):
        return current_by_field
    metrics = run["metrics"] or {}
    _require(
        stage.marker["phase"] == "copying"
        and metrics.get("nucc_source_stage") == stage.marker
        and metrics.get("nucc_handoff") is None
        and attempt != stage.marker["attempt_id"]
        and datetime.fromisoformat(started) > datetime.fromisoformat(stage.marker["attempt_started_at"]),
        "cancellation original Source attempt changed",
    )
    _require_cancel_inputs(stage, run)
    return current_by_field


async def consume_nucc_source_cancel(session, stage):
    """Consume only exact cancellation after the real Owner candidate has been removed."""
    _require(isinstance(stage, NuccSourceStage), "cancellation stage differs")
    actor = await control._source_transaction(session)
    receipt = await control.reconcile_source_publication(session, stage.marker["run_id"])
    run = await control._locked_source_run(session, stage.marker["run_id"])
    if receipt is not None:
        await control._recheck_source_transaction(session, actor)
        return run
    expected = NuccSourceStage.from_wire(stage.marker).ownership
    _require(
        all(
            getattr(expected, name) == getattr(stage.ownership, name)
            for name in ("importer_id", "dataset_id", "schema_name", "relation_names", "sequence_names")
        ),
        "cancellation ownership differs",
    )
    _require(run["status"] in {"canceling", "canceled", "cancelled"}, "cancellation attempt differs")
    _require(
        stage.marker["node_id"] == actor["node_id"]
        and ("builder_role" not in stage.marker or stage.marker["builder_role"] == actor["role"]),
        "cancellation actor differs",
    )
    _require(
        all(run[field] == stage.marker[field] for field in ("node_id", "run_id"))
        and control.source_contract_digest(stage.marker, run["params"]) == stage.marker["source_contract_sha256"],
        "cancellation original Source attempt changed",
    )
    _require_cancel_inputs(stage, run)
    attempt = _source_pending_attempt(stage, run)
    removed = await session.scalar(
        text("SELECT to_regclass(:relation) IS NULL"),
        {
            "relation": f'"{stage.ownership.schema_name}"."nucc_taxonomy"',
        },
    )
    _require(removed is True, "Owner candidate cleanup is not committed")
    if run["status"] == "canceling":
        changed = await session.scalar(
            text("""UPDATE mrf.import_run SET status='canceled',
 phase_detail='nucc canceled',heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC',finished_at=clock_timestamp() AT TIME ZONE 'UTC',
 progress=(progress::jsonb||jsonb_build_object('message','canceled','phase','nucc canceled'))::json
 WHERE run_id=:run_id AND node_id=:node_id AND engine='healthcare-mrf-api' AND importer='nucc'
 AND status='canceling' AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:attempt_started_at
 AND progress::jsonb=CAST(:progress AS jsonb)
 AND metrics::jsonb=CAST(:metrics AS jsonb) RETURNING run_id"""),
            {
                **stage.marker,
                **attempt,
                "progress": json.dumps(run["progress"], sort_keys=True, separators=(",", ":")),
                "metrics": json.dumps(run["metrics"], sort_keys=True, separators=(",", ":")),
            },
        )
        _require(changed == stage.marker["run_id"], "cancellation Source fence changed")
    await control._recheck_source_transaction(session, actor)
    return await control._locked_source_run(session, stage.marker["run_id"])


async def is_protected_source(session):
    """Route from the fixed native relation before any legacy writer can run."""
    protected = await _is_protected_nucc_claim_parent(session)
    if protected and (os.getenv("HLTHPRT_DB_SCHEMA") or "mrf") != "mrf":
        raise RuntimeError("protected taxonomy Source schema differs")
    return protected


async def create_managed_source_run(*, test_mode, queue_context=None, task=None):
    """Use the existing managed queue; a partial test cannot replace a serving set."""
    if test_mode:
        raise ValueError("protected taxonomy test mode requires an isolated database")
    from api.control_imports import create_import_run, get_import_run

    payload = {"importer": "nucc", "params": {}, "triggered_by": "manual"}
    if queue_context is not None:
        _require(type(queue_context) is dict and type(task) is dict, "queue admission differs")
        context = queue_context.get("context") or {}
        existing = task.get("run_id") or queue_context.get("control_run_id") or context.get("control_run_id")
        if existing:
            run = await get_import_run(existing)
            _require(type(run) is dict and run.get("importer") == "nucc", "queued run identity differs")
            return run
        job = queue_context.get("job_id")
        if job is not None:
            control._bounded_identity(job, 256)
            payload["idempotency_key"] = "taxonomy-queue-" + hashlib.sha256(job.encode()).hexdigest()
    run, _created = await create_import_run(payload)
    return run
