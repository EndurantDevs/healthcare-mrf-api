# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Approve explicit manual record versions in a caller-owned transaction."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from uuid import uuid4

import asyncpg

from process.registry_approved_company_groups import approved_company_group_diagnostics
from process.registry_approved_membership import approved_membership_diagnostics
from process.registry_benefit_plan_metadata import validate_benefit_plan_ids
from process.registry_company_approval_fence import lock_registry_company_approval_writer
from process.registry_record_store import (
    RegistryActor,
    RegistryRecordConflict,
    _bounded_text,
    _company_record_json_sql,
    _validated_actor,
)
from process.registry_source_observation_store import _namespace


class RegistryApprovalConflict(RegistryRecordConflict):
    """A stale selection, revision or conflicting retry cannot approve records."""


@dataclass(frozen=True)
class RegistryApprovalCommand:
    expected_draft_revision: int
    expected_approved_revision: int
    selection: tuple[dict, ...]
    reason: str
    idempotency_key: str


def _validated_command(command):
    if not isinstance(command, RegistryApprovalCommand):
        raise ValueError("registry_approval_command_invalid")
    for revision in (command.expected_draft_revision, command.expected_approved_revision):
        if type(revision) is not int or not 0 <= revision < 9223372036854775807:
            raise ValueError("registry_approval_revision_invalid")
    if command.expected_approved_revision > command.expected_draft_revision:
        raise ValueError("registry_approval_revision_invalid")
    reason = _bounded_text(command.reason, 1000, "reason")
    if _bounded_text(command.idempotency_key, 128, "idempotency_key") != command.idempotency_key:
        raise ValueError("registry_idempotency_key_invalid")
    if type(command.selection) is not tuple or not 1 <= len(command.selection) <= 5000:
        raise ValueError("registry_approval_selection_invalid")
    try:
        selection_json = json.dumps(command.selection, sort_keys=True, separators=(",", ":"), allow_nan=False)
    except (TypeError, ValueError) as error:
        raise ValueError("registry_approval_selection_invalid") from error
    if len(selection_json.encode()) > 1048576:
        raise ValueError("registry_approval_selection_invalid")
    return reason, selection_json


async def _canonical_selection(connection, selection_json):
    try:
        selection_status = await connection.fetchrow(
            """WITH items AS (SELECT value AS item FROM jsonb_array_elements($1::jsonb))
        SELECT bool_and(CASE WHEN jsonb_typeof(item)='object' THEN
          item ?& ARRAY['record_kind','record_id','revision']
          AND item-'record_kind'-'record_id'-'revision'='{}'::jsonb
          AND jsonb_typeof(item->'record_kind')='string'
          AND item->>'record_kind' IN ('group','company','network','company_links','provider','location','membership','site_binding','network_binding')
          AND jsonb_typeof(item->'revision')='number'
          AND CASE WHEN item->>'revision' ~ '^[1-9][0-9]*$'
            THEN (item->>'revision')::numeric <= 9223372036854775807 ELSE false END
          AND CASE WHEN item->>'record_kind' IN ('network','membership') THEN
            jsonb_typeof(item->'record_id')='number'
            AND CASE WHEN item->>'record_id' ~ '^[1-9][0-9]*$'
              THEN (item->>'record_id')::numeric <= 2147483647 ELSE false END
          ELSE jsonb_typeof(item->'record_id')='string'
            AND item->>'record_id' ~ '^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$'
            AND item->>'record_id'<>'00000000-0000-0000-0000-000000000000' END
          ELSE false END) AS valid,
          count(*)=count(DISTINCT (item->>'record_kind',item->>'record_id')) AS unique_records,
          jsonb_agg(item ORDER BY item->>'record_kind',item->>'record_id')::text AS selection_json
        FROM items""",
            selection_json,
        )
    except asyncpg.DataError as error:
        raise ValueError("registry_approval_selection_invalid") from error
    if not selection_status["valid"] or not selection_status["unique_records"]:
        raise ValueError("registry_approval_selection_invalid")
    return selection_status["selection_json"]


async def _stage_selection(connection, selection_json):
    staging = '"registry_approval_' + uuid4().hex + '"'
    await connection.execute(
        f"""CREATE TEMP TABLE {staging} ON COMMIT DROP AS
        SELECT record_kind #>> '{{}}' AS record_kind, record_id #>> '{{}}' AS record_key,
               (revision #>> '{{}}')::bigint AS revision
        FROM jsonb_to_recordset($1::jsonb) AS selection(record_kind jsonb, record_id jsonb, revision jsonb)""",
        selection_json,
    )
    return staging


def _selected_history_json_sql():
    """Compare product fields with legacy defaults, leaving retained audit metadata intact."""
    return """CASE WHEN selected.record_kind='company' THEN
            jsonb_build_object('role_assertions','[]'::jsonb,'identifier_assertions','[]'::jsonb,
              'group_assertions','[]'::jsonb,'display_name_origin','unknown')||(history.record_json-'correction_predecessors')
            WHEN selected.record_kind='company_links' AND NOT history.record_json ? 'network_assertions'
            THEN history.record_json||jsonb_build_object('network_assertions','[]'::jsonb)
            WHEN selected.record_kind='network' AND NOT history.record_json ? 'catalog_evidence_json'
            THEN history.record_json||jsonb_build_object('catalog_evidence_json',NULL)
            ELSE history.record_json END"""


async def _validate_selected_history(connection, namespace, staging, draft_revision):
    """Require the selected immutable snapshot to equal its current draft head."""
    valid = await connection.fetchval(
        f"""WITH heads AS (
          SELECT 'group' AS record_kind, selected.record_key, to_jsonb(head) AS record_json, head.created_at
          FROM {staging} selected JOIN {namespace}.company_group_registry head
            ON head.group_id=CASE WHEN selected.record_kind='group' THEN selected.record_key::uuid END
          UNION ALL
          SELECT 'company', selected.record_key, {_company_record_json_sql(namespace)}, head.created_at
          FROM {staging} selected JOIN {namespace}.company_registry head
            ON head.company_id=CASE WHEN selected.record_kind='company' THEN selected.record_key::uuid END
          UNION ALL
          SELECT 'company_links', selected.record_key, to_jsonb(head), head.created_at
          FROM {staging} selected JOIN {namespace}.company_registry_links head
            ON head.company_id=CASE WHEN selected.record_kind='company_links' THEN selected.record_key::uuid END
          UNION ALL
          SELECT 'provider', selected.record_key, to_jsonb(head), head.created_at
          FROM {staging} selected JOIN {namespace}.manual_provider_registry head
            ON head.provider_id=CASE WHEN selected.record_kind='provider' THEN selected.record_key::uuid END
          UNION ALL
          SELECT 'location', selected.record_key, to_jsonb(head), head.created_at
          FROM {staging} selected JOIN {namespace}.manual_location_registry head
            ON head.location_id=CASE WHEN selected.record_kind='location' THEN selected.record_key::uuid END
          UNION ALL
          SELECT 'site_binding', selected.record_key, to_jsonb(head), head.created_at
          FROM {staging} selected JOIN {namespace}.registry_site_binding head
            ON head.binding_id=CASE WHEN selected.record_kind='site_binding' THEN selected.record_key::uuid END
          UNION ALL
          SELECT 'network_binding', selected.record_key, to_jsonb(head), head.created_at
          FROM {staging} selected JOIN {namespace}.registry_network_binding head
            ON head.binding_id=CASE WHEN selected.record_kind='network_binding' THEN selected.record_key::uuid END
          UNION ALL
          SELECT 'membership', selected.record_key, to_jsonb(head), head.created_at
          FROM {staging} selected JOIN {namespace}.network_membership_draft head
            ON head.network_id=CASE WHEN selected.record_kind='membership' THEN selected.record_key::int END
          UNION ALL
          SELECT 'network', selected.record_key, to_jsonb(head), head.created_at
          FROM {staging} selected JOIN {namespace}.network_registry_record head
            ON head.network_id=CASE WHEN selected.record_kind='network' THEN selected.record_key::int END
        )
        SELECT bool_and(coalesce(history.revision IS NOT NULL AND heads.record_json IS NOT NULL
          AND history.custom_revision <= $1
          AND heads.record_json->>'revision'=selected.revision::text
          AND ({_selected_history_json_sql()})-'created_at'=heads.record_json-'created_at'
          AND CASE WHEN pg_input_is_valid(history.record_json->>'created_at','timestamptz')
            THEN (history.record_json->>'created_at')::timestamptz=heads.created_at ELSE false END,false))
        FROM {staging} selected
        LEFT JOIN {namespace}.registry_record_history history
          ON (history.record_kind,history.record_key,history.revision)=
             (selected.record_kind,selected.record_key,selected.revision)
        LEFT JOIN heads ON (heads.record_kind,heads.record_key)=(selected.record_kind,selected.record_key)""",
        draft_revision,
    )
    if not valid:
        raise RegistryApprovalConflict("registry_approval_selection_conflict")


def _approval_metadata(command, actor_document, selection_json, reason):
    actor_json = json.dumps(actor_document, sort_keys=True, separators=(",", ":"))
    request = json.dumps(
        {
            "expected_draft_revision": command.expected_draft_revision,
            "expected_approved_revision": command.expected_approved_revision,
            "selection": json.loads(selection_json),
            "reason": command.reason,
            "idempotency_key": command.idempotency_key,
            "actor": actor_document,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return {
        "actor_json": actor_json,
        "actor_key": hashlib.sha256(actor_json.encode()).hexdigest(),
        "request_sha256": hashlib.sha256(request.encode()).hexdigest(),
        "selection_json": selection_json,
        "reason": reason,
    }


async def _write_approval(connection, namespace, staging, command, approval_metadata):
    approved_revision = command.expected_draft_revision + 1
    await connection.execute(
        f"""INSERT INTO {namespace}.registry_approval_history
        (approved_revision,previous_approved_revision,expected_draft_revision,actor_key,actor_json,
         selection_json,reason,idempotency_key,request_sha256)
        VALUES ($1,$2,$3,$4,$5::jsonb,$6::jsonb,$7,$8,$9)""",
        approved_revision,
        command.expected_approved_revision,
        command.expected_draft_revision,
        approval_metadata["actor_key"],
        approval_metadata["actor_json"],
        approval_metadata["selection_json"],
        approval_metadata["reason"],
        command.idempotency_key,
        approval_metadata["request_sha256"],
    )
    await connection.execute(
        f"""INSERT INTO {namespace}.registry_approved_record
        (approved_revision,record_kind,record_key,record_revision,custom_revision,record_json)
        SELECT $1::bigint,previous.record_kind,previous.record_key,previous.record_revision,
               previous.custom_revision,previous.record_json
        FROM {namespace}.registry_approved_record previous WHERE previous.approved_revision=$2
          AND NOT EXISTS (SELECT 1 FROM {staging} selected
            WHERE (selected.record_kind,selected.record_key)=(previous.record_kind,previous.record_key))
        UNION ALL
        SELECT $1::bigint,history.record_kind,history.record_key,history.revision,history.custom_revision,history.record_json
        FROM {staging} selected JOIN {namespace}.registry_record_history history
          ON (history.record_kind,history.record_key,history.revision)=
             (selected.record_kind,selected.record_key,selected.revision)""",
        approved_revision,
        command.expected_approved_revision,
    )
    await connection.execute(
        f"UPDATE {namespace}.registry_revision_control SET draft_revision=$1,approved_revision=$1 WHERE id=1",
        approved_revision,
    )
    return {
        "approved_revision": approved_revision,
        "previous_approved_revision": command.expected_approved_revision,
        "selected_count": len(command.selection),
        "replayed": False,
    }


async def _validate_company_groups(connection, namespace, staging, previous_revision):
    diagnostics = await approved_company_group_diagnostics(connection, namespace, staging, previous_revision)
    if any(
        diagnostics[key]
        for key in ("group_family_invalid_count", "group_target_conflict_count", "group_source_conflict_count")
    ):
        raise RegistryApprovalConflict("registry_company_assertion_target_conflict")
    if diagnostics["group_review_count"]:
        raise RegistryApprovalConflict("registry_approval_company_group_unresolved")


async def _approval_replay(connection, namespace, command, metadata):
    replay = await connection.fetchrow(
        f"SELECT approved_revision,previous_approved_revision,jsonb_array_length(selection_json) AS selected_count,request_sha256 "
        f"FROM {namespace}.registry_approval_history WHERE actor_key=$1 AND idempotency_key=$2",
        metadata["actor_key"],
        command.idempotency_key,
    )
    if replay is None:
        return None
    if replay["request_sha256"] != metadata["request_sha256"]:
        raise RegistryApprovalConflict("registry_approval_idempotency_conflict")
    return {key: replay[key] for key in ("approved_revision", "previous_approved_revision", "selected_count")} | {
        "replayed": True
    }


async def _approval_benefit_ids(connection, namespace, selection_json):
    return tuple(
        json.loads(
            await connection.fetchval(
                f"""WITH selected AS (SELECT * FROM jsonb_to_recordset($1::jsonb)
          AS selection(record_kind text,record_id text,revision bigint)), identities AS (
          SELECT DISTINCT plan_id #>> '{{}}' AS plan_id FROM selected
          JOIN {namespace}.registry_record_history history ON history.record_kind='company_links'
            AND selected.record_kind='company_links' AND history.record_key=selected.record_id
            AND history.revision=selected.revision
          CROSS JOIN LATERAL jsonb_array_elements(coalesce(history.record_json->'network_assertions','[]'::jsonb)) assertion
          CROSS JOIN LATERAL jsonb_array_elements(coalesce(assertion->'benefit_plan_ids','[]'::jsonb)) plan_id
          WHERE history.record_json->'archived'='false'::jsonb LIMIT 5001)
          SELECT coalesce(jsonb_agg(plan_id ORDER BY plan_id),'[]'::jsonb)::text FROM identities""",
                selection_json,
            )
        )
    )


async def approve_registry_records(
    connection, command, actor: RegistryActor, *, control_schema=None, plan_metadata_client=None, final_authorize=None
):
    """Retain an explicit approved map; the caller authenticates live grants first.

    Actor identity and a replay key do not grant approval permission. The caller
    must recheck live authorization on every invocation, including exact retries.
    A savepoint preserves earlier caller work and never commits the transaction.
    A trusted final_authorize callback runs after metadata and approval locks;
    exact retries still recheck authority without another metadata request.
    """
    if final_authorize is not None and not callable(final_authorize):
        raise ValueError("registry_approval_authority_invalid")
    reason, selection_json = _validated_command(command)
    actor_document = _validated_actor(actor)
    namespace = _namespace(control_schema)
    if not connection.is_in_transaction():
        raise ValueError("registry_approval_requires_caller_transaction")
    async with connection.transaction():
        if any(
            type(entry) is dict and entry.get("record_kind") == "company_links" for entry in json.loads(selection_json)
        ):
            selection_json = await _canonical_selection(connection, selection_json)
            approval_metadata = _approval_metadata(command, actor_document, selection_json, reason)
            if await _approval_replay(connection, namespace, command, approval_metadata) is None:
                await validate_benefit_plan_ids(
                    await _approval_benefit_ids(connection, namespace, selection_json), plan_metadata_client, actor
                )
        await lock_registry_company_approval_writer(connection, namespace[1:-1])
        selection_json = await _canonical_selection(connection, selection_json)
        approval_metadata = _approval_metadata(command, actor_document, selection_json, reason)
        # shortcut: a global lock serializes bounded approvals; partition only if measured contention requires it.
        control = await connection.fetchrow(
            f"SELECT draft_revision,approved_revision FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE"
        )
        if final_authorize is not None:
            await final_authorize()
        if control is None:
            raise RegistryApprovalConflict("registry_control_unavailable")
        replay = await _approval_replay(connection, namespace, command, approval_metadata)
        if replay is not None:
            return replay
        if (control["draft_revision"], control["approved_revision"]) != (
            command.expected_draft_revision,
            command.expected_approved_revision,
        ):
            raise RegistryApprovalConflict("registry_approval_revision_conflict")
        staging = await _stage_selection(connection, selection_json)
        await _validate_selected_history(connection, namespace, staging, control["draft_revision"])
        diagnostics = await approved_membership_diagnostics(
            connection, namespace, staging, command.expected_approved_revision
        )
        if diagnostics["source_binding_unresolved_count"]:
            raise RegistryApprovalConflict("registry_approval_source_binding_unresolved")
        if diagnostics["unresolved_count"]:
            raise RegistryApprovalConflict("registry_approval_membership_unresolved")
        await _validate_company_groups(connection, namespace, staging, command.expected_approved_revision)
        receipt = await _write_approval(connection, namespace, staging, command, approval_metadata)
        await connection.execute(f"DROP TABLE {staging}")
        return receipt
