# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed NUCC v3 Source receipts; native custody grants authority separately."""

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


def _source_generation(value, *, predecessor=False):
    from process.reference_family_result_generation import _nucc_result_authority

    try:
        return _nucc_result_authority(value, allow_untracked=predecessor)
    except RuntimeError, TypeError, ValueError, KeyError:
        raise NuccProtectedControlError("NUCC protected control Source generation differs") from None


def _publication_generation_pair(receipt):
    prior = _source_generation(receipt["source_predecessor"], predecessor=True)
    authority = _source_generation(receipt["source_authority"])
    _require(
        authority.local_lineage_id == prior.local_lineage_id
        and authority.local_generation == prior.local_generation + 1
        and authority.relation_oids == (receipt["child_oid"],)
        and (prior.relation_oids is None or prior.relation_oids == (receipt["previous_child_oid"],)),
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
    for field in (
        "database_oid",
        "history_oid",
        "owner_oid",
        "parent_oid",
        "parent_index_oid",
        "child_oid",
        "child_schema_oid",
        "child_filenode",
        "previous_child_oid",
    ):
        _require(
            type(receipt[field]) is int and 0 < receipt[field] < 2**32,
            "native identity differs",
        )
    _require(
        len(
            {
                receipt["parent_oid"],
                receipt["parent_index_oid"],
                receipt["child_oid"],
                receipt["previous_child_oid"],
            }
        )
        == 4,
        "objects overlap",
    )
    _require(
        type(receipt["child_schema"]) is str and _CHILD_SCHEMA.fullmatch(receipt["child_schema"]) is not None,
        "schema differs",
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
    """Decode a closed nonsecret receipt; only a genuine Owner ledger makes it authoritative."""
    _require(
        type(value) is dict and set(value) == PUBLICATION_FIELDS,
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
    transaction_id = value["transaction_id"]
    _require(
        type(transaction_id) is str
        and re.fullmatch(r"[1-9][0-9]{0,19}", transaction_id) is not None
        and int(transaction_id) < 2**64,
        "publication transaction differs",
    )
    _publication_generation_pair(value)
    receipt_digest(value)
    return dict(value)


def validate_handoff(handoff):
    """Validate only the closed v3 shape; persisted Source and Owner rows still require native proof."""
    _require(
        type(handoff) is dict and set(handoff) == HANDOFF_FIELDS,
        "handoff fields differ",
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
    prior = _source_generation(handoff["source_predecessor"], predecessor=True)
    _require(
        prior.relation_oids is None or prior.relation_oids == (handoff["previous_child_oid"],),
        "handoff Source predecessor differs",
    )
    _require(
        handoff["handoff_sha256"]
        == receipt_digest({key: field_value for key, field_value in handoff.items() if key != "handoff_sha256"}),
        "handoff digest differs",
    )
    return dict(handoff)


def source_contract_digest(handoff, params):
    """Keep the existing Source contract preimage, including real unmodified parameters."""
    _require(type(params) is dict, "Source parameters differ")
    return receipt_digest(
        {key: handoff[key] for key in ("run_id", "attempt_id", "attempt_started_at")}
        | {"schema_name": "mrf", "params": params}
    )


_EVENT_CUSTODY_SQL = """SELECT c.oid::bigint AS oid,c.relowner::bigint AS owner_oid,
 n.nspowner::bigint AS schema_owner_oid,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(a.grantee::bigint,a.grantor::bigint,a.privilege_type,a.is_grantable))
 FROM aclexplode(COALESCE(c.relacl,acldefault('r',c.relowner))) a),'[]'::jsonb) AS acl,
 COALESCE((SELECT jsonb_agg(jsonb_build_array(a.attname,g.grantee::bigint,g.grantor::bigint,g.privilege_type,g.is_grantable))
 FROM pg_attribute a CROSS JOIN LATERAL aclexplode(a.attacl) g
 WHERE a.attrelid=c.oid AND a.attnum>0 AND NOT a.attisdropped),'[]'::jsonb) AS column_acl
 FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace
 WHERE c.oid=to_regclass('hp_snapshot_retention.publication') AND c.relkind='r'
 AND c.relpersistence='p' AND NOT c.relrowsecurity AND NOT c.relforcerowsecurity"""
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
    "relation_oids",
    "source_revision_tracked",
)


async def _source_transaction(session):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession

    from db.models import db
    from process.source_profile_result_pins import load_role_policy

    login = os.environ.get("HLTHPRT_DB_USER", "")
    node_id = os.environ.get("HLTHPRT_IMPORT_NODE_ID", "")
    _bounded_identity(node_id)
    _unused_owner, runtime_roles = load_role_policy()
    _require(bool(login) and login in runtime_roles, "configured Source login differs")
    _require(
        isinstance(session, AsyncSession)
        and session.in_transaction()
        and not session.in_nested_transaction()
        and db._reader_binding() is None,
        "original Source transaction required",
    )
    binding = db._transaction_binding()
    _require(
        binding is None or binding.session is session,
        "Source transaction binding differs",
    )
    actor_by_field = (
        (
            await session.execute(
                text("""SELECT session_user::text AS session,current_user::text AS current,
 pg_backend_pid() AS pid,pg_current_xact_id()::text AS xid,
 (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,
 caller.oid::bigint AS actor_oid,owner.oid::bigint AS owner_oid,
 current_setting('transaction_isolation')='read committed' AND current_setting('transaction_read_only')='off'
 AND current_setting('session_replication_role')='origin' AND caller.rolcanlogin
 AND NOT (caller.rolsuper OR caller.rolcreaterole OR caller.rolcreatedb OR caller.rolreplication OR caller.rolbypassrls)
 AND NOT (owner.rolcanlogin OR owner.rolsuper OR owner.rolcreaterole OR owner.rolcreatedb OR owner.rolreplication OR owner.rolbypassrls)
 AND NOT pg_has_role(caller.oid,owner.oid,'MEMBER')
 AND NOT EXISTS(SELECT FROM pg_roles reachable WHERE pg_has_role(caller.oid,reachable.oid,'MEMBER')
 AND (reachable.rolsuper OR reachable.rolcreaterole OR reachable.rolcreatedb OR reachable.rolreplication
 OR reachable.rolbypassrls OR reachable.oid=(SELECT datdba FROM pg_database WHERE datname=current_database())
 OR reachable.rolname=ANY(ARRAY['pg_read_server_files','pg_write_server_files','pg_execute_server_program']::name[]))) AS safe
 FROM pg_roles caller,pg_namespace n JOIN pg_roles owner ON owner.oid=n.nspowner
 WHERE caller.rolname=session_user AND n.nspname='hp_snapshot_retention'""")
            )
        )
        .mappings()
        .one_or_none()
    )
    _require(
        actor_by_field is not None
        and actor_by_field["session"] == actor_by_field["current"] == login
        and actor_by_field["safe"] is True,
        "Source actor differs",
    )
    actor_by_field = dict(actor_by_field)
    actor_by_field["runtime_roles"] = runtime_roles
    actor_by_field["node_id"] = node_id
    return actor_by_field


async def _recheck_source_transaction(session, original):
    _require(await _source_transaction(session) == original, "Source transaction changed")


def validate_event_custody(catalog_by_field, *, owner_oid, builder_oid):
    """Require only Owner table custody and this admitted Builder's exact nonsecret column reads."""
    _require(
        type(catalog_by_field) is dict
        and catalog_by_field["owner_oid"] == catalog_by_field["schema_owner_oid"] == owner_oid,
        "event Owner differs",
    )
    owner_privileges = {
        "SELECT",
        "INSERT",
        "UPDATE",
        "DELETE",
        "TRUNCATE",
        "REFERENCES",
        "TRIGGER",
        "MAINTAIN",
    }
    acl = catalog_by_field["acl"]
    _require(
        type(acl) is list
        and len(acl) == len(owner_privileges)
        and {(entry[0], entry[1], entry[2], entry[3]) for entry in acl}
        == {(owner_oid, owner_oid, privilege, False) for privilege in owner_privileges},
        "event table grants differ",
    )
    columns = catalog_by_field["column_acl"]
    _require(
        type(columns) is list
        and len(columns) == len(SOURCE_ACK_COLUMNS)
        and {tuple(entry) for entry in columns}
        == {(name, builder_oid, owner_oid, "SELECT", False) for name in SOURCE_ACK_COLUMNS},
        "event column grants differ",
    )


async def _lock_event_custody(session, actor_by_field):
    from sqlalchemy import text

    from process.reference_family_archive import require_native_read_catalog

    catalog_by_field = dict((await session.execute(text(_EVENT_CUSTODY_SQL))).mappings().one())
    validate_event_custody(
        catalog_by_field,
        owner_oid=actor_by_field["owner_oid"],
        builder_oid=actor_by_field["actor_oid"],
    )
    await require_native_read_catalog(session, (catalog_by_field["oid"],))
    await session.execute(text("SELECT fence_token FROM hp_snapshot_retention.publication WHERE false"))
    _require(
        dict((await session.execute(text(_EVENT_CUSTODY_SQL))).mappings().one()) == catalog_by_field,
        "event custody changed",
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
        receipt["owner_oid"] == actor_by_field["owner_oid"]
        and receipt["database_oid"] == actor_by_field["database_oid"]
        and receipt["node_id"] == actor_by_field["node_id"],
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
        receipt["owner_oid"] == actor_by_field["owner_oid"]
        and receipt["database_oid"] == actor_by_field["database_oid"]
        and receipt["node_id"] == actor_by_field["node_id"]
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
    history_oid = await session.scalar(text("SELECT to_regclass('mrf.import_run')::oid::bigint"))
    await _require_source_controls(session, actor, history_oid)
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


async def _require_source_controls(session, actor_by_field, history_oid):
    from sqlalchemy import text

    from process.reference_family_archive import require_native_read_catalog

    actual = await session.scalar(text("SELECT to_regclass('mrf.import_run')::oid::bigint"))
    generation_oid = await session.scalar(
        text("SELECT to_regclass('mrf.reference_family_result_generation')::oid::bigint")
    )
    _require(
        actual == history_oid and type(generation_oid) is int,
        "Source control identity differs",
    )
    for relation, columns, privilege in (
        (actual, _SOURCE_SELECT, "SELECT"),
        (actual, _SOURCE_UPDATES, "UPDATE"),
        (generation_oid, _GENERATION_UPDATES, "UPDATE"),
    ):
        granted = await session.scalar(
            text(
                "SELECT bool_and(has_column_privilege(CAST(:actor AS oid),"
                "CAST(:relation AS oid),column_name,:privilege)) FROM unnest(CAST(:columns AS text[])) column_name"
            ),
            {
                "actor": actor_by_field["actor_oid"],
                "relation": relation,
                "columns": list(columns),
                "privilege": privilege,
            },
        )
        _require(granted is True, "Source control privileges differ")
    await require_native_read_catalog(session, (actual, generation_oid))


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
    handoff = validate_handoff(expected_handoff)
    actor_by_field = await _source_transaction(session)
    _require(
        all(handoff[field] == actor_by_field[field] for field in ("node_id", "database_oid", "owner_oid")),
        "Source handoff location differs",
    )
    publisher_actor_by_field = await _publisher_transaction(publisher_session)
    await _require_publisher_fence(
        session,
        actor_by_field,
        publisher_actor_by_field,
        handoff["parent_oid"],
        run_id=handoff["run_id"],
    )
    await _require_source_controls(session, actor_by_field, handoff["history_oid"])
    run = await _locked_source_run(session, handoff["run_id"])
    _require(
        _run_handoff(run) == handoff
        and run["status"] == "finalizing"
        and run["phase_detail"] == HANDOFF_PHASE
        and run["finished_at"] is None
        and run["error"] is None,
        "Source publication is ineligible",
    )
    await _lock_generation_eligibility(
        session,
        handoff["source_predecessor"],
        handoff["previous_child_oid"],
        actor_by_field,
        publisher_actor_by_field,
    )
    await _recheck_publisher_transaction(publisher_session, publisher_actor_by_field)
    await _recheck_source_transaction(session, actor_by_field)
    return handoff


async def _project_generation(session, receipt):
    from sqlalchemy import text

    from process.reference_family_result_generation import (
        read_reference_family_result_generation_authority,
    )

    current = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name="mrf", lock=True
    )
    if current.as_dict() == receipt["source_authority"]:
        return
    _require(
        current.as_dict() == receipt["source_predecessor"],
        "Source ACK predecessor changed",
    )
    next_authority = _source_generation(receipt["source_authority"])
    serving = next_authority.serving_generation
    updated = await session.scalar(
        text("""UPDATE mrf.reference_family_result_generation
 SET local_generation=:next,origin_lineage_id=CAST(:origin AS uuid),origin_generation=:generation,
 published_at=CAST(:published AS timestamptz),relation_oids=CAST(:oids AS bigint[]),source_revision_tracked=FALSE
 WHERE importer_id='nucc' AND local_lineage_id=CAST(:lineage AS uuid) AND local_generation=:prior
 RETURNING importer_id"""),
        {
            "next": next_authority.local_generation,
            "origin": serving.origin_lineage_id,
            "generation": serving.origin_generation,
            "published": serving.published_at,
            "oids": list(next_authority.relation_oids),
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
    await _require_source_controls(session, actor_by_field, receipt["history_oid"])
    run = await _locked_source_run(session, run_id)
    handoff = _run_handoff(run)
    _require(
        all(receipt[key] == handoff[key] for key in HANDOFF_FIELDS & PUBLICATION_FIELDS - {"contract"}),
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
    await _require_source_controls(session, actor_by_field, receipt["history_oid"])
    await _project_generation(session, receipt)
    await _recheck_source_transaction(session, actor_by_field)
    return receipt


async def _publisher_transaction(session):
    from sqlalchemy import text
    from sqlalchemy.ext.asyncio import AsyncSession

    from process.reference_family_archive import protected_publisher_owner

    _require(
        isinstance(session, AsyncSession) and session.in_transaction() and not session.in_nested_transaction(),
        "original Publisher transaction required",
    )
    owner_oid = await protected_publisher_owner(session)
    actor_by_field = dict(
        (
            await session.execute(
                text("""SELECT
 pg_backend_pid() AS pid,pg_current_xact_id()::text AS xid,
 (SELECT oid::bigint FROM pg_database WHERE datname=current_database()) AS database_oid,
 (SELECT virtualtransaction FROM pg_locks WHERE pid=pg_backend_pid()
  AND locktype='virtualxid' AND granted AND mode='ExclusiveLock') AS virtualtransaction,
 current_setting('transaction_isolation')='read committed'
 AND current_setting('transaction_read_only')='off' AS writable""")
            )
        )
        .mappings()
        .one()
    )
    _require(actor_by_field.pop("writable") is True, "Publisher transaction differs")
    actor_by_field["owner_oid"] = owner_oid
    return actor_by_field


async def _recheck_publisher_transaction(session, original):
    _require(await _publisher_transaction(session) == original, "Publisher transaction changed")


async def _require_publisher_fence(
    session, source_actor_by_field, publisher_actor_by_field, parent_oid, *, run_id=None
):
    from sqlalchemy import text

    _require(
        publisher_actor_by_field["database_oid"] == source_actor_by_field["database_oid"]
        and publisher_actor_by_field["owner_oid"] == source_actor_by_field["owner_oid"]
        and publisher_actor_by_field["pid"] != source_actor_by_field["pid"],
        "Publisher location differs",
    )
    held = await session.scalar(
        text("""SELECT EXISTS(SELECT FROM pg_locks transaction_lock
 WHERE transaction_lock.pid=:pid AND transaction_lock.virtualtransaction=:virtualtransaction
 AND transaction_lock.locktype='transactionid' AND transaction_lock.granted
 AND transaction_lock.mode='ExclusiveLock' AND transaction_lock.transactionid=CAST(:xid AS xid)
 AND NOT EXISTS(SELECT FROM pg_locks parent_lock WHERE parent_lock.pid=:pid
 AND parent_lock.locktype='relation' AND parent_lock.granted AND parent_lock.mode='AccessExclusiveLock'
 AND parent_lock.database=CAST(:database AS oid) AND parent_lock.relation=CAST(:parent AS oid))
 AND (CAST(:name AS text) IS NULL OR EXISTS(SELECT FROM pg_locks action_lock
 WHERE action_lock.pid=:pid AND action_lock.virtualtransaction=:virtualtransaction
 AND action_lock.locktype='advisory' AND action_lock.granted AND action_lock.mode='ExclusiveLock'
 AND action_lock.database=CAST(:database AS oid) AND action_lock.objsubid=1
 AND action_lock.classid=CAST((hashtextextended(CAST(:name AS text),0)>>32)&4294967295 AS oid)
 AND action_lock.objid=CAST(hashtextextended(CAST(:name AS text),0)&4294967295 AS oid))))"""),
        {
            "pid": publisher_actor_by_field["pid"],
            "virtualtransaction": publisher_actor_by_field["virtualtransaction"],
            "xid": int(publisher_actor_by_field["xid"]) % 2**32,
            "database": source_actor_by_field["database_oid"],
            "parent": parent_oid,
            "name": None if run_id is None else "control-run-worker-action:v1:" + run_id,
        },
    )
    _require(held is True, "Publisher pre-cutover fence unavailable")


async def _require_prior_owner_ack(session, actor_by_field, source_authority, node_id, parent_catalog):
    from sqlalchemy import text

    await _lock_event_custody(session, actor_by_field)
    row = (
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
    if row is None:
        await _require_initial_baseline(session, actor_by_field, source_authority, parent_catalog)
        return
    receipt = validate_event_ack(dict(row))
    _require(
        receipt["owner_oid"] == actor_by_field["owner_oid"]
        and receipt["database_oid"] == actor_by_field["database_oid"]
        and receipt["source_authority"] == source_authority,
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


async def lock_generation_eligibility(session, expected_authority, *, expected_child_oid, publisher_session):
    """Validate and lock Source before Publisher takes the short parent exclusive lock."""
    actor = await _source_transaction(session)
    publisher_actor_by_field = await _publisher_transaction(publisher_session)
    current = await _lock_generation_eligibility(
        session,
        expected_authority,
        expected_child_oid,
        actor,
        publisher_actor_by_field,
    )
    await _recheck_publisher_transaction(publisher_session, publisher_actor_by_field)
    await _recheck_source_transaction(session, actor)
    return current


async def _lock_generation_eligibility(
    session, expected_authority, expected_child_oid, actor_by_field, publisher_actor_by_field
):
    from sqlalchemy import text

    from process import nucc_snapshot_partition as partition
    from process.reference_family_result_generation import (
        read_reference_family_result_generation_authority,
    )

    _source_generation(expected_authority, predecessor=True)
    _require(type(expected_child_oid) is int and 0 < expected_child_oid < 2**32, "expected child differs")
    # Native ruleutils deparsers can acquire relation locks: never invoke them behind our Publisher AX.
    parent_oid = await session.scalar(text("SELECT to_regclass('mrf.nucc_taxonomy')::oid::bigint"))
    _require(type(parent_oid) is int, "protected parent unavailable")
    await _require_publisher_fence(session, actor_by_field, publisher_actor_by_field, parent_oid)
    parent_catalog = partition.decode_catalog(await session.scalar(text(partition.CATALOG_SQL)))
    physical = partition.validate_parent_catalog(parent_catalog)
    await _require_publisher_fence(session, actor_by_field, publisher_actor_by_field, physical["parent_oid"])
    history_oid = await session.scalar(text("SELECT to_regclass('mrf.import_run')::oid::bigint"))
    await _require_source_controls(session, actor_by_field, history_oid)
    current = await read_reference_family_result_generation_authority(
        session, importer_id="nucc", schema_name="mrf", lock=True
    )
    _require(current.as_dict() == expected_authority, "Source generation predecessor changed")
    _require(
        physical["child_oid"] == expected_child_oid
        and (current.relation_oids is None or current.relation_oids == (expected_child_oid,)),
        "prior Source projection is not ACKed",
    )
    await _require_prior_owner_ack(
        session, actor_by_field, current.as_dict(), actor_by_field["node_id"], parent_catalog
    )
    return current.as_dict()


async def _require_initial_baseline(session, actor_by_field, source_authority, parent_catalog):
    """Only original-admin native adoption with zero actual Owner events can precede the first outcome."""
    from sqlalchemy import text

    from process import nucc_protected_bootstrap as bootstrap
    from process import nucc_snapshot_partition as partition

    _require(await _source_transaction(session) == actor_by_field, "Source transaction changed")
    physical = partition.validate_parent_catalog(parent_catalog)
    _require(
        await session.scalar(
            text(
                "SELECT NOT EXISTS(SELECT FROM hp_snapshot_retention.publication "
                "WHERE importer_id='nucc' AND dataset_key='nucc')"
            )
        )
        is True,
        "initial Owner baseline already consumed",
    )
    comment = await session.scalar(
        text("SELECT obj_description(CAST(:oid AS oid),'pg_class')"), {"oid": physical["parent_oid"]}
    )
    baseline = bootstrap.validate_baseline(comment, physical, source_authority)
    _require(
        baseline["database_oid"] == actor_by_field["database_oid"]
        and baseline["protected_native_sha256"] == partition.catalog_digest(parent_catalog)
        and baseline["child_index_oids"]
        == sorted(index["oid"] for index in parent_catalog["children"][0]["relation"]["indexes"])
        and baseline["source_revision_tracked"]
        is await session.scalar(
            text("SELECT source_revision_tracked FROM mrf.reference_family_result_generation WHERE importer_id='nucc'")
        ),
        "initial Owner baseline differs",
    )
    _require(
        partition.decode_catalog(await session.scalar(text(partition.CATALOG_SQL))) == parent_catalog
        and await _source_transaction(session) == actor_by_field,
        "initial Owner baseline changed",
    )
