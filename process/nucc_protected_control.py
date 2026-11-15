# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Named NUCC Source receipts with semantic provenance and native PostgreSQL access."""

from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from uuid import UUID

HANDOFF_CONTRACT = "nucc-native-handoff.v3"
PUBLICATION_CONTRACT = "nucc-native-publication.v3"
HANDOFF_PHASE = "nucc stage awaiting publication"
SOURCE_ACK_EVENT = "hp_snapshot_retention.publication"
SOURCE_ACK_COLUMNS = (
    "fence_token",
    "node_id",
    "importer_id",
    "dataset_key",
    "generation_id",
    "expected_generation_id",
    "expected_fence_token",
    "binding_sha256",
    "nucc_source_ack",
)
PUBLICATION_FIELDS = frozenset(
    {
        "contract",
        "preparation_id",
        "node_id",
        "run_id",
        "attempt_id",
        "database_oid",
        "history_oid",
        "owner_oid",
        "parent_oid",
        "parent_index_oid",
        "child_oid",
        "child_schema",
        "child_schema_oid",
        "child_filenode",
        "previous_child_oid",
        "generation_id",
        "publication_fence",
        "admission_sha256",
        "handoff_sha256",
        "inventory_sha256",
        "validation_sha256",
        "row_count",
        "semantic_sha256",
        "schema_sha256",
        "transaction_id",
        "source_predecessor",
        "source_authority",
        "published_at",
        "purpose",
        "operation_id",
        "request_id",
    }
)
HANDOFF_FIELDS = frozenset(
    {
        "contract",
        "preparation_id",
        "node_id",
        "run_id",
        "attempt_id",
        "attempt_started_at",
        "database_oid",
        "history_oid",
        "source_contract_sha256",
        "source_predecessor",
        "owner_oid",
        "parent_oid",
        "parent_index_oid",
        "previous_child_oid",
        "child_oid",
        "child_schema",
        "child_schema_oid",
        "child_filenode",
        "generation_id",
        "publication_fence",
        "admission_sha256",
        "inventory_sha256",
        "row_count",
        "semantic_sha256",
        "schema_sha256",
        "handoff_sha256",
    }
)
_LEGACY_PUBLICATION_FIELDS = PUBLICATION_FIELDS
_LEGACY_HANDOFF_FIELDS = HANDOFF_FIELDS
_PHYSICAL_FIELDS = {
    "database_oid",
    "history_oid",
    "owner_oid",
    "parent_oid",
    "parent_index_oid",
    "child_oid",
    "child_schema",
    "child_schema_oid",
    "child_filenode",
    "previous_child_oid",
}
_NAMED_FIELDS = {"history", "parent", "parent_index", "child", "previous_child"}
PUBLICATION_FIELDS = frozenset((PUBLICATION_FIELDS - _PHYSICAL_FIELDS - {"transaction_id"}) | _NAMED_FIELDS)
HANDOFF_FIELDS = frozenset((HANDOFF_FIELDS - _PHYSICAL_FIELDS) | _NAMED_FIELDS)


def named_locations(value):
    """Project only recorded names and fixed protocol roots; old IDs never select names."""
    from process.nucc_snapshot_partition import qualified

    if "parent" in value:
        try:
            return {key: qualified(value[key]) for key in _NAMED_FIELDS}
        except (KeyError, TypeError, ValueError, RuntimeError) as error:
            raise NuccProtectedControlError("NUCC protected control relation name differs") from error
    child_schema = value["child_schema"]
    _require(type(child_schema) is str and _CHILD_SCHEMA.fullmatch(child_schema) is not None, "schema differs")
    return {
        "history": ("mrf", "import_run"),
        "parent": ("mrf", "nucc_taxonomy"),
        "child": (child_schema, "nucc_taxonomy"),
    }


_HEX = re.compile(r"[0-9a-f]{64}\Z")
_CHILD_SCHEMA = re.compile(r"reference_family_archive_[0-9a-f]{32}\Z")


class NuccProtectedControlError(ValueError):
    """The exact Source attempt or protected publication evidence is unavailable."""


def _require(condition, reason):
    if not condition:
        raise NuccProtectedControlError("NUCC protected control " + reason)


def _bounded_identity(value, limit=128):
    _require(
        type(value) is str and value == value.strip() and 0 < len(value.encode("utf-8")) <= limit,
        "identity differs",
    )


def _uuid(value):
    _require(type(value) is str, "UUID differs")
    try:
        parsed = UUID(value)
    except ValueError:
        raise NuccProtectedControlError("NUCC protected control UUID differs") from None
    _require(str(parsed) == value, "UUID differs")


def receipt_digest(value):
    """Hash bounded canonical control evidence without adding native authority."""
    try:
        encoded = json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode("ascii")
    except TypeError, ValueError, UnicodeError:
        raise NuccProtectedControlError("NUCC protected control encoding differs") from None
    _require(len(encoded) <= 131072, "evidence exceeds bound")
    return hashlib.sha256(encoded).hexdigest()


def _source_generation(value, *, predecessor=False, declared_schema=None, legacy_read=False):
    from process.reference_family_result_generation import _nucc_result_authority

    try:
        return _nucc_result_authority(
            value, allow_untracked=predecessor, declared_schema=declared_schema, legacy_read=legacy_read
        )
    except (RuntimeError, TypeError, ValueError, KeyError) as error:
        if str(error).startswith("transition-legacy-nucc-generation:"):
            raise NuccProtectedControlError(str(error)) from None
        raise NuccProtectedControlError("NUCC protected control Source generation differs") from None


def _publication_generation_pair(receipt):
    names = named_locations(receipt)
    prior = _source_generation(
        receipt["source_predecessor"],
        predecessor=True,
        declared_schema=names.get("previous_child", (None,))[0],
        legacy_read="parent" not in receipt,
    )
    authority = _source_generation(receipt["source_authority"], declared_schema=names["child"][0])
    _require(
        authority.local_lineage_id == prior.local_lineage_id
        and authority.local_generation == prior.local_generation + 1
        and authority.relations == (names["child"],)
        and (prior.relations is None or prior.relations == (names.get("previous_child"),)),
        "publication Source predecessor differs",
    )
    if receipt["purpose"] == "source":
        _require(
            authority.serving_generation.origin_lineage_id == authority.local_lineage_id
            and authority.serving_generation.origin_generation == authority.local_generation,
            "native Source origin differs",
        )
    _require(type(receipt["published_at"]) is str, "publication time differs")
    try:
        published = datetime.fromisoformat(receipt["published_at"].replace("Z", "+00:00"))
    except ValueError:
        raise NuccProtectedControlError("NUCC protected control publication time differs") from None
    _require(
        published.tzinfo is not None
        and published.astimezone(timezone.utc).isoformat().replace("+00:00", "Z") == receipt["published_at"]
        and (receipt["purpose"] != "source" or authority.serving_generation.published_at == published),
        "publication time differs",
    )


def _native_identities(receipt):
    names = named_locations(receipt)
    _require(
        names["history"] == ("mrf", "import_run")
        and names["parent"] == ("mrf", "nucc_taxonomy")
        and names["child"][1] == "nucc_taxonomy"
        and _CHILD_SCHEMA.fullmatch(names["child"][0]) is not None,
        "relation scope differs",
    )
    if "previous_child" in names:
        _require(
            names["previous_child"][1] == "nucc_taxonomy"
            and _CHILD_SCHEMA.fullmatch(names["previous_child"][0]) is not None
            and names["previous_child"] != names["child"]
            and names["parent_index"][0] == "mrf"
            and names["parent_index"] != names["parent"],
            "objects overlap or scope differs",
        )


def _identities(value):
    for field in ("preparation_id", "generation_id", "publication_fence"):
        _uuid(value[field])
    for field in ("node_id", "run_id", "attempt_id"):
        _bounded_identity(value[field])
    _require(
        re.fullmatch(re.escape(value["run_id"]) + r":[0-9a-f]{32}", value["attempt_id"]) is not None,
        "attempt differs",
    )
    _native_identities(value)
    _require(
        type(value["row_count"]) is int and 0 < value["row_count"] <= 10000,
        "census differs",
    )


def _publication_identities(value):
    purpose = value["purpose"]
    _require(purpose in {"source", "receive", "restore"}, "publication purpose differs")
    if purpose == "source":
        _identities(value)
        _require(
            value["operation_id"] is None and value["request_id"] is None,
            "Source identity differs",
        )
    else:
        _require(
            all(value[key] is None for key in ("preparation_id", "run_id", "attempt_id")),
            "publication invents a Source attempt",
        )
        _uuid(value["operation_id"])
        if purpose == "receive":
            _require(value["request_id"] is None, "receive request differs")
        else:
            _uuid(value["request_id"])
        for field in ("generation_id", "publication_fence"):
            _uuid(value[field])
        _bounded_identity(value["node_id"])
        _native_identities(value)
        _require(
            type(value["row_count"]) is int and 0 < value["row_count"] <= 10000,
            "census differs",
        )


def _digests(value, fields):
    for field in fields:
        _require(
            type(value[field]) is str and _HEX.fullmatch(value[field]) is not None,
            "digest differs",
        )


def validate_publication(value):
    """Validate named publication semantics while returning historical bodies unchanged."""
    _require(
        type(value) is dict and set(value) in (PUBLICATION_FIELDS, _LEGACY_PUBLICATION_FIELDS),
        "publication fields differ",
    )
    _require(value["contract"] == PUBLICATION_CONTRACT, "publication contract differs")
    _publication_identities(value)
    _digests(
        value,
        (
            "admission_sha256",
            "handoff_sha256",
            "inventory_sha256",
            "validation_sha256",
            "semantic_sha256",
            "schema_sha256",
        ),
    )
    _publication_generation_pair(value)
    return json.loads(json.dumps(value))


def validate_handoff(handoff):
    """Validate logical attempt/data fields; old physical tokens remain opaque."""
    _require(
        type(handoff) is dict and set(handoff) in (HANDOFF_FIELDS, _LEGACY_HANDOFF_FIELDS), "handoff fields differ"
    )
    _require(handoff["contract"] == HANDOFF_CONTRACT, "handoff contract differs")
    _identities(handoff)
    _digests(
        handoff,
        (
            "source_contract_sha256",
            "admission_sha256",
            "inventory_sha256",
            "semantic_sha256",
            "schema_sha256",
            "handoff_sha256",
        ),
    )
    _bounded_identity(handoff["attempt_started_at"], 64)
    try:
        started = datetime.fromisoformat(handoff["attempt_started_at"].replace("Z", "+00:00"))
    except ValueError:
        raise NuccProtectedControlError("NUCC protected control attempt time differs") from None
    _require(started.tzinfo is not None, "attempt time differs")
    names = named_locations(handoff)
    prior = _source_generation(
        handoff["source_predecessor"],
        predecessor=True,
        declared_schema=names.get("previous_child", (None,))[0],
        legacy_read="parent" not in handoff,
    )
    _require(
        prior.relations is None or prior.relations == (names.get("previous_child"),),
        "handoff Source predecessor differs",
    )
    if "parent" in handoff:
        _require(
            handoff["handoff_sha256"]
            == receipt_digest({key: field for key, field in handoff.items() if key != "handoff_sha256"}),
            "handoff digest differs",
        )
    return json.loads(json.dumps(handoff))


def source_contract_digest(handoff, params):
    """Keep the existing Source contract preimage, including real unmodified parameters."""
    _require(type(params) is dict, "Source parameters differ")
    return receipt_digest(
        {key: handoff[key] for key in ("run_id", "attempt_id", "attempt_started_at")}
        | {"schema_name": "mrf", "params": params}
    )


_SOURCE_SELECT = (
    "run_id",
    "node_id",
    "engine",
    "importer",
    "status",
    "phase_detail",
    "params",
    "progress",
    "metrics",
    "finished_at",
    "error",
)
_SOURCE_UPDATES = (
    "status",
    "phase_detail",
    "finished_at",
    "heartbeat_at",
    "error",
    "metrics",
    "progress",
)
_GENERATION_UPDATES = (
    "local_generation",
    "origin_lineage_id",
    "origin_generation",
    "published_at",
    "serving_schema",
    "serving_relation_names",
    "source_revision_tracked",
)


async def _source_transaction(session):
    """Materialize the caller transaction; native grants enforce configured Source access."""
    from sqlalchemy import text

    from process.source_profile_result_pins import load_role_policy

    login, node = os.environ.get("HLTHPRT_DB_USER", ""), os.environ.get("HLTHPRT_IMPORT_NODE_ID", "")
    _bounded_identity(node)
    _owner, runtime_roles = load_role_policy()
    _require(login in runtime_roles, "configured Source role differs")
    _require(
        callable(getattr(session, "in_transaction", None))
        and callable(getattr(session, "in_nested_transaction", None))
        and session.in_transaction()
        and not session.in_nested_transaction(),
        "original Source transaction required",
    )
    await session.execute(text("SELECT 1"))
    return {"role": login, "runtime_roles": runtime_roles, "node_id": node, "transaction": session.get_transaction()}


async def _recheck_source_transaction(session, original):
    _require(await _source_transaction(session) == original, "Source transaction changed")


async def _lock_event_custody(session, actor_by_field):
    """Read only the declared acknowledgement columns through PostgreSQL privileges."""
    from sqlalchemy import text

    _require(session.in_transaction(), "Source transaction required")
    await session.execute(
        text("SELECT " + ",".join(SOURCE_ACK_COLUMNS) + " FROM hp_snapshot_retention.publication WHERE false")
    )


def validate_event_ack(event):
    """Cross-check a closed Owner receipt against its exact immutable publication event."""
    _require(
        type(event) is dict and set(event) == set(SOURCE_ACK_COLUMNS),
        "event fields differ",
    )
    receipt = validate_publication(event["nucc_source_ack"])
    _require(
        event["fence_token"] == receipt["publication_fence"]
        and event["generation_id"] == receipt["generation_id"]
        and event["node_id"] == receipt["node_id"]
        and event["importer_id"] == event["dataset_key"] == "nucc",
        "publication event identity differs",
    )
    _require(
        type(event["binding_sha256"]) is str and _HEX.fullmatch(event["binding_sha256"]) is not None,
        "event binding differs",
    )
    expected_generation, expected_fence = (
        event["expected_generation_id"],
        event["expected_fence_token"],
    )
    _require(
        (expected_generation is None) == (expected_fence is None),
        "event predecessor differs",
    )
    if expected_generation is not None:
        _uuid(expected_generation)
        _uuid(expected_fence)
    return receipt


async def _find_owner_event(session, actor_by_field, fence_token):
    from sqlalchemy import text

    _uuid(fence_token)
    await _lock_event_custody(session, actor_by_field)
    event_row = (
        (
            await session.execute(
                text("""SELECT fence_token::text,node_id,importer_id,dataset_key,generation_id::text,
 expected_generation_id::text,expected_fence_token::text,binding_sha256,
 CASE WHEN pg_catalog.octet_length(nucc_source_ack::text)<=131072
 THEN nucc_source_ack ELSE NULL END AS nucc_source_ack
 FROM hp_snapshot_retention.publication WHERE fence_token=CAST(:fence AS uuid)
 AND importer_id='nucc' AND dataset_key='nucc'"""),
                {"fence": fence_token},
            )
        )
        .mappings()
        .one_or_none()
    )
    if event_row is None:
        return None
    receipt = validate_event_ack(dict(event_row))
    _require(
        receipt["node_id"] == actor_by_field["node_id"],
        "Owner publication location differs",
    )
    return receipt


async def _read_owner_event(session, actor_by_field, fence_token):
    receipt = await _find_owner_event(session, actor_by_field, fence_token)
    _require(receipt is not None, "committed Owner event unavailable")
    return receipt


async def _find_run_owner_event(session, actor_by_field, run_id):
    from sqlalchemy import text

    await _lock_event_custody(session, actor_by_field)
    event_rows = (
        (
            await session.execute(
                text("""SELECT fence_token::text,node_id,importer_id,dataset_key,generation_id::text,
 expected_generation_id::text,expected_fence_token::text,binding_sha256,
 CASE WHEN pg_catalog.octet_length(nucc_source_ack::text)<=131072
 THEN nucc_source_ack ELSE NULL END AS nucc_source_ack
 FROM hp_snapshot_retention.publication WHERE importer_id='nucc' AND dataset_key='nucc'
 AND nucc_source_ack->>'run_id'=:run_id ORDER BY fence_token LIMIT 2"""),
                {"run_id": run_id},
            )
        )
        .mappings()
        .all()
    )
    _require(len(event_rows) <= 1, "multiple Owner outcomes for Source run")
    if not event_rows:
        return None
    receipt = validate_event_ack(dict(event_rows[0]))
    _require(
        receipt["node_id"] == actor_by_field["node_id"]
        and receipt["purpose"] == "source"
        and receipt["run_id"] == run_id,
        "Owner Source outcome location differs",
    )
    return receipt


async def reconcile_source_publication(session, run_id):
    """Return None only for authenticated Owner absence; every custody/identity failure refuses action."""
    from sqlalchemy import text

    from db.connection import ConnectionProxy
    from db.models import db
    from process.control_lifecycle import acquire_control_run_worker_action_lock

    actor = await _source_transaction(session)
    await acquire_control_run_worker_action_lock(ConnectionProxy(db, session, None), run_id)
    await _require_source_controls(session, actor)
    run = await _locked_source_run(session, run_id)
    _require(run["node_id"] == actor["node_id"], "Source run location differs")
    metrics = run["metrics"] or {}
    _require(type(metrics) is dict, "Source metrics differ")
    mirrored = metrics.get("nucc_native_publication")
    if mirrored is not None:
        validate_publication(mirrored)
    raw_handoff = metrics.get("nucc_handoff")
    handoff = None if raw_handoff is None else _run_handoff(run)
    receipt = (
        None
        if handoff is None
        else await _find_owner_event(
            session,
            actor,
            handoff["publication_fence"],
        )
    )
    if receipt is None:
        receipt = await _find_run_owner_event(session, actor, run_id)
    if receipt is None:
        _require(mirrored is None, "mirrored Owner outcome unavailable")
        await _recheck_source_transaction(session, actor)
        return None
    _require(
        handoff is not None and receipt["publication_fence"] == handoff["publication_fence"],
        "Owner outcome locator differs",
    )
    return await acknowledge_source_publication(session, run_id, receipt["publication_fence"])


async def _locked_source_run(session, run_id):
    from sqlalchemy import text

    _bounded_identity(run_id)
    fields = ",".join(_SOURCE_SELECT)
    run = (
        (
            await session.execute(
                text(
                    f"SELECT {fields} FROM mrf.import_run WHERE run_id=:run_id "
                    "AND octet_length(params::text)<=131072 AND octet_length(metrics::text)<=393216 FOR UPDATE NOWAIT"
                ),
                {"run_id": run_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(
        run is not None and run["engine"] == "healthcare-mrf-api" and run["importer"] == "nucc",
        "real Source run differs",
    )
    return dict(run)


async def _require_source_controls(session, actor_by_field, history=None):
    from sqlalchemy import text

    for relation, columns, privilege in (
        ("mrf.import_run", _SOURCE_SELECT, "SELECT"),
        ("mrf.import_run", _SOURCE_UPDATES, "UPDATE"),
        ("mrf.reference_family_result_generation", _GENERATION_UPDATES, "UPDATE"),
    ):
        granted = await session.scalar(
            text(
                "SELECT bool_and(has_column_privilege(current_user,:relation,column_name,:privilege)) FROM unnest(CAST(:columns AS text[])) column_name"
            ),
            {"relation": relation, "columns": list(columns), "privilege": privilege},
        )
        _require(granted is True, "Source control privileges differ")


def _run_handoff(run):
    metrics, progress = run["metrics"], run["progress"]
    _require(
        type(metrics) is dict and type(progress) is dict and type(run["params"]) is dict,
        "Source run payload differs",
    )
    handoff = validate_handoff(metrics.get("nucc_handoff"))
    _require(
        all(run[field] == handoff[field] for field in ("node_id", "run_id"))
        and all(progress.get(field) == handoff[field] for field in ("attempt_id", "attempt_started_at"))
        and source_contract_digest(handoff, run["params"]) == handoff["source_contract_sha256"]
        and not run["params"].get("test")
        and not run["params"].get("test_mode"),
        "Source attempt or parameters differ",
    )
    return handoff


async def lock_source_eligibility(session, expected_handoff, *, publisher_session):
    """Keep real Source row locks in the caller's transaction through the short Publisher commit."""
    from process import nucc_snapshot_partition as partition

    handoff = validate_handoff(expected_handoff)
    actor_by_field = await _source_transaction(session)
    _require(
        handoff["node_id"] == actor_by_field["node_id"],
        "Source handoff location differs",
    )
    publisher_actor_by_field = await _publisher_transaction(publisher_session)
    await _require_publisher_fence(
        session,
        actor_by_field,
        publisher_actor_by_field,
        named_locations(handoff)["parent"],
        run_id=handoff["run_id"],
    )
    await _require_source_controls(session, actor_by_field)
    run = await _locked_source_run(session, handoff["run_id"])
    _require(
        _run_handoff(run) == handoff
        and run["status"] == "finalizing"
        and run["phase_detail"] == HANDOFF_PHASE
        and run["finished_at"] is None
        and run["error"] is None,
        "Source publication is ineligible",
    )
    previous_child = named_locations(handoff).get("previous_child")
    if previous_child is None:
        prior = _source_generation(handoff["source_predecessor"], predecessor=True)
        _require(prior.relations is not None, "transition-legacy-nucc-generation: recorded predecessor name required")
        previous_child = prior.relations[0]
    await _lock_generation_eligibility(
        session,
        handoff["source_predecessor"],
        previous_child,
        actor_by_field,
        publisher_actor_by_field,
        publisher_session=publisher_session,
    )
    await _recheck_publisher_transaction(publisher_session, publisher_actor_by_field)
    await _recheck_source_transaction(session, actor_by_field)
    return handoff


async def _project_generation(session, receipt):
    from sqlalchemy import text

    from process.reference_family_result_generation import read_reference_family_result_generation_authority

    current = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name="mrf", lock=True
    )
    next_authority = _source_generation(
        receipt["source_authority"], declared_schema=named_locations(receipt)["child"][0]
    )
    if current == next_authority:
        return
    _require(
        current
        == _source_generation(
            receipt["source_predecessor"],
            predecessor=True,
            declared_schema=named_locations(receipt).get("previous_child", (None,))[0],
        ),
        "Source ACK predecessor changed",
    )
    serving = next_authority.serving_generation
    updated = await session.scalar(
        text("""UPDATE mrf.reference_family_result_generation
 SET local_generation=:next,origin_lineage_id=CAST(:origin AS uuid),origin_generation=:generation,
 published_at=CAST(:published AS timestamptz),serving_schema=:schema,serving_relation_names=CAST(:names AS text[]),source_revision_tracked=FALSE
 WHERE importer_id='nucc' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior RETURNING importer_id"""),
        {
            "next": next_authority.local_generation,
            "origin": serving.origin_lineage_id,
            "generation": serving.origin_generation,
            "published": serving.published_at,
            "schema": next_authority.relations[0][0],
            "names": [next_authority.relations[0][1]],
            "lineage": current.local_lineage_id,
            "prior": current.local_generation,
        },
    )
    _require(updated == "nucc", "Source generation ACK changed")


async def _read_acknowledged_source_outcome(session, run_id, receipt):
    """Read the exact ACKed row while its authenticated transaction still owns the lock."""
    from sqlalchemy import select

    from db.models import ImportRun

    if ImportRun.__table__.schema != "mrf":
        raise RuntimeError("committed NUCC claim schema differs")
    column_names = (
        "run_id engine node_id importer status phase_detail progress metrics error heartbeat_at finished_at snapshot_id"
    ).split()
    statement = select(*(getattr(ImportRun, name) for name in column_names)).where(ImportRun.run_id == run_id)
    run = (await session.execute(statement, {})).mappings().one_or_none()
    identity_names = ("run_id", "importer", "engine", "status", "node_id", "error")
    expected_identity = (run_id, "nucc", "healthcare-mrf-api", "succeeded", receipt["node_id"], None)
    if (
        run is None
        or tuple(run[name] for name in identity_names) != expected_identity
        or not all(isinstance(run[key], dict) for key in ("progress", "metrics"))
    ):
        raise RuntimeError("committed NUCC claim outcome changed")
    progress, metrics = run["progress"], run["metrics"]
    if (
        metrics.get("nucc_native_publication") != receipt
        or progress.get("attempt_id") != receipt["attempt_id"]
        or not isinstance(progress.get("attempt_started_at"), str)
        or not progress["attempt_started_at"]
    ):
        raise RuntimeError("committed NUCC claim attempt changed")
    return dict(run)


async def acknowledge_source_publication(session, run_id, publication_fence):
    """ACK only a freshly read committed Owner event, after the eligibility transaction has ended."""

    from db.connection import ConnectionProxy
    from db.models import db
    from process.control_lifecycle import acquire_control_run_worker_action_lock

    actor_by_field = await _source_transaction(session)
    await acquire_control_run_worker_action_lock(ConnectionProxy(db, session, None), run_id)
    receipt = await _read_owner_event(session, actor_by_field, publication_fence)
    _require(
        receipt["purpose"] == "source" and receipt["run_id"] == run_id,
        "Source ACK purpose differs",
    )
    await _require_source_controls(session, actor_by_field)
    run = await _locked_source_run(session, run_id)
    handoff = _run_handoff(run)
    shared = (HANDOFF_FIELDS & PUBLICATION_FIELDS & receipt.keys() & handoff.keys()) - {
        "contract",
        "source_predecessor",
    }
    _require(
        all(receipt[key] == handoff[key] for key in shared)
        and named_locations(receipt)["child"] == named_locations(handoff)["child"]
        and receipt["source_predecessor"] == handoff["source_predecessor"],
        "Source ACK handoff differs",
    )
    if run["status"] == "succeeded" and (run["metrics"] or {}).get("nucc_native_publication") == receipt:
        await _recheck_source_transaction(session, actor_by_field)
        return receipt
    _require(
        run["status"]
        in {
            "finalizing",
            "canceling",
            "canceled",
            "cancelled",
            "failed",
            "dead_letter",
        },
        "Source ACK run changed",
    )
    await _project_generation(session, receipt)
    await _acknowledge_run(session, run, handoff, receipt)
    await _recheck_source_transaction(session, actor_by_field)
    return receipt


async def acknowledge_generation_publication(session, publication_fence):
    """Project receive/restore provenance without inventing a native Source import run."""
    actor_by_field = await _source_transaction(session)
    receipt = await _read_owner_event(session, actor_by_field, publication_fence)
    _require(receipt["purpose"] in {"receive", "restore"}, "generation ACK purpose differs")
    await _require_source_controls(session, actor_by_field)
    await _project_generation(session, receipt)
    await _recheck_source_transaction(session, actor_by_field)
    return receipt


async def _publisher_transaction(session):
    from sqlalchemy import text

    from process.reference_family_archive import model_family_roles

    _require(
        callable(getattr(session, "in_transaction", None))
        and callable(getattr(session, "in_nested_transaction", None))
        and session.in_transaction()
        and not session.in_nested_transaction(),
        "original Publisher transaction required",
    )
    owner, readers = model_family_roles()
    await session.execute(text("SELECT 1"))
    return {"owner_role": owner, "reader_roles": readers, "transaction": session.get_transaction()}


async def _recheck_publisher_transaction(session, original):
    _require(await _publisher_transaction(session) == original, "Publisher transaction changed")


async def _require_publisher_fence(session, source_actor_by_field, publisher_actor_by_field, parent, *, run_id=None):
    """Require live owning transactions; ordering and native locks govern the short cutover."""
    _require(tuple(parent) == ("mrf", "nucc_taxonomy"), "Publisher parent scope differs")
    _require(
        session.in_transaction()
        and session.get_transaction() is source_actor_by_field["transaction"]
        and publisher_actor_by_field["transaction"].is_active,
        "Publisher pre-cutover transaction unavailable",
    )


async def _require_prior_owner_ack(
    session, actor_by_field, source_authority, node_id, parent_catalog, *, publisher_session
):
    from sqlalchemy import text

    await _lock_event_custody(session, actor_by_field)
    ack_row = (
        (
            await session.execute(
                text("""SELECT fence_token::text,node_id,importer_id,dataset_key,generation_id::text,
 expected_generation_id::text,expected_fence_token::text,binding_sha256,nucc_source_ack
 FROM hp_snapshot_retention.publication WHERE node_id=:node AND importer_id='nucc' AND dataset_key='nucc'
 AND nucc_source_ack IS NOT NULL AND octet_length(nucc_source_ack::text)<=131072
 ORDER BY (nucc_source_ack->'source_authority'->>'local_generation')::bigint DESC LIMIT 1"""),
                {"node": node_id},
            )
        )
        .mappings()
        .one_or_none()
    )
    if ack_row is None:
        await _require_initial_baseline(
            session, actor_by_field, source_authority, parent_catalog, publisher_session=publisher_session
        )
        return
    receipt = validate_event_ack(dict(ack_row))
    _require(
        _source_generation(receipt["source_authority"], declared_schema=named_locations(receipt)["child"][0])
        == _source_generation(source_authority, predecessor=True),
        "prior protected Owner ACK differs",
    )


async def _acknowledge_run(session, run, handoff, receipt):
    from sqlalchemy import text

    updated = await session.scalar(
        text("""UPDATE mrf.import_run SET status='succeeded',phase_detail='nucc published',
 finished_at=CAST(:published AS timestamptz) AT TIME ZONE 'UTC',heartbeat_at=clock_timestamp() AT TIME ZONE 'UTC',error=NULL,
 metrics=(metrics::jsonb||jsonb_build_object('nucc_native_publication',CAST(:receipt AS jsonb)))::json,
 progress=(progress::jsonb||jsonb_build_object('phase','nucc published','pct',100,'unit','rows','done',CAST(:rows AS bigint),
 'total',CAST(:rows AS bigint),'message','succeeded'))::json
 WHERE run_id=:run_id AND engine='healthcare-mrf-api' AND importer='nucc' AND node_id=:node_id
 AND status=:prior_status AND finished_at IS NOT DISTINCT FROM CAST(:prior_finished AS timestamp)
 AND error::jsonb IS NOT DISTINCT FROM CAST(:prior_error AS jsonb)
 AND progress->>'attempt_id'=:attempt_id AND progress->>'attempt_started_at'=:started
 AND metrics::jsonb->'nucc_handoff'=CAST(:handoff AS jsonb) AND params::jsonb=CAST(:params AS jsonb) RETURNING run_id"""),
        {
            **receipt,
            "published": datetime.fromisoformat(receipt["published_at"].replace("Z", "+00:00")),
            "receipt": json.dumps(receipt),
            "rows": receipt["row_count"],
            "started": handoff["attempt_started_at"],
            "handoff": json.dumps(handoff),
            "params": json.dumps(run["params"]),
            "prior_status": run["status"],
            "prior_finished": run["finished_at"],
            "prior_error": None if run["error"] is None else json.dumps(run["error"]),
        },
    )
    _require(updated == receipt["run_id"], "Source run ACK changed")


async def lock_generation_eligibility(session, expected_authority, *, expected_child, publisher_session):
    """Validate and lock Source before Publisher takes the short parent exclusive lock."""
    actor = await _source_transaction(session)
    publisher_actor_by_field = await _publisher_transaction(publisher_session)
    current = await _lock_generation_eligibility(
        session,
        expected_authority,
        expected_child,
        actor,
        publisher_actor_by_field,
        publisher_session=publisher_session,
    )
    await _recheck_publisher_transaction(publisher_session, publisher_actor_by_field)
    await _recheck_source_transaction(session, actor)
    return current


async def _lock_generation_eligibility(
    session, expected_authority, expected_child, actor_by_field, publisher_actor_by_field, *, publisher_session
):
    from sqlalchemy import text

    from process import nucc_snapshot_partition as partition
    from process.reference_family_result_generation import read_reference_family_result_generation_authority

    expected_child = partition.qualified(expected_child)
    expected = _source_generation(expected_authority, predecessor=True, declared_schema=expected_child[0])
    await _require_publisher_fence(session, actor_by_field, publisher_actor_by_field, ("mrf", "nucc_taxonomy"))
    parent_catalog = partition.decode_catalog(await session.scalar(text(partition.CATALOG_SQL)))
    named = partition.validate_parent_catalog(parent_catalog)
    await _require_source_controls(session, actor_by_field)
    current = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name="mrf", lock=True
    )
    _require(current == expected, "Source generation predecessor changed")
    _require(
        named["child"] == expected_child and (current.relations is None or current.relations == (named["child"],)),
        "prior Source projection is not ACKed",
    )
    await _require_prior_owner_ack(
        session,
        actor_by_field,
        current.as_dict(),
        actor_by_field["node_id"],
        parent_catalog,
        publisher_session=publisher_session,
    )
    return current.as_dict()


async def _require_initial_baseline(session, actor_by_field, source_authority, parent_catalog, *, publisher_session):
    """Compare the initial saved logical provenance and complete preserved content."""
    from sqlalchemy import text

    from process import nucc_protected_bootstrap as bootstrap
    from process import nucc_snapshot_partition as partition
    from process import reference_family_archive as native

    named = partition.validate_parent_catalog(parent_catalog)
    _require(
        await session.scalar(
            text(
                "SELECT NOT EXISTS(SELECT FROM hp_snapshot_retention.publication WHERE importer_id='nucc' AND dataset_key='nucc')"
            )
        )
        is True,
        "initial baseline already consumed",
    )
    comment = await session.scalar(
        text("SELECT obj_description(to_regclass(:parent),'pg_class')"), {"parent": partition.PARENT}
    )
    baseline = bootstrap.validate_baseline(comment, named, source_authority)
    content = await native._projected_row_identity(
        publisher_session, *named["child"], row_json_sql="to_jsonb(row_value)"
    )
    _require(content == (baseline["row_count"], baseline["semantic_sha256"]), "initial baseline content differs")
    _require(
        baseline["source_revision_tracked"]
        is await session.scalar(
            text("SELECT source_revision_tracked FROM mrf.reference_family_result_generation WHERE importer_id='nucc'")
        ),
        "initial baseline tracking differs",
    )
    await _recheck_source_transaction(session, actor_by_field)
