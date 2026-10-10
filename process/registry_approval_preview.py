# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded comparisons of explicit manual selections with the approved map."""

from __future__ import annotations

import json

from process.registry_approval_store import (
    RegistryApprovalCommand,
    RegistryApprovalConflict,
    _canonical_selection,
    _stage_selection,
    _validate_selected_history,
    _validated_command,
)
from process.registry_approved_membership import approved_membership_diagnostics
from process.registry_imported_selection_preview import MAX_PREVIEW_COUNT, preview_imported_registry_selection
from process.registry_record_store import RegistryActor, _validated_actor
from process.registry_source_observation_store import _namespace


async def _preview_document(connection, namespace, staging, command):
    return await connection.fetchval(
        f"""WITH changes AS MATERIALIZED (
          SELECT selected.record_kind,selected.record_key,
            CASE WHEN previous.record_json IS NULL THEN 'addition'
              WHEN previous.record_json->'archived'='false'::jsonb AND history.record_json->'archived'='true'::jsonb
                THEN 'archive'
              WHEN previous.record_json->'archived'='true'::jsonb AND history.record_json->'archived'='false'::jsonb
                THEN 'restore'
              ELSE 'correction' END AS operation,
            jsonb_build_object('record_kind',selected.record_kind,
              'record_id',history.record_json->CASE selected.record_kind
                WHEN 'group' THEN 'group_id' WHEN 'company' THEN 'company_id'
                WHEN 'company_links' THEN 'company_id' WHEN 'provider' THEN 'provider_id'
                WHEN 'location' THEN 'location_id' WHEN 'site_binding' THEN 'binding_id'
                WHEN 'network_binding' THEN 'binding_id' ELSE 'network_id' END,
              'record_revision',selected.revision,'before',previous.record_json,'after',history.record_json) AS record_json
          FROM {staging} selected JOIN {namespace}.registry_record_history history
            ON (history.record_kind,history.record_key,history.revision)=
               (selected.record_kind,selected.record_key,selected.revision)
          LEFT JOIN {namespace}.registry_approved_record previous
            ON previous.approved_revision=$2
              AND (previous.record_kind,previous.record_key)=(selected.record_kind,selected.record_key)
        ), summary AS (
          SELECT jsonb_build_object('expected_draft_revision',$1::bigint,'expected_approved_revision',$2::bigint,
            'selected_count',count(*),'additions_count',count(*) FILTER(WHERE operation='addition'),
            'corrections_count',count(*) FILTER(WHERE operation='correction'),
            'archives_count',count(*) FILTER(WHERE operation='archive'),
            'restores_count',count(*) FILTER(WHERE operation='restore'),'unresolved_count',0,
            'records','[]'::jsonb) AS summary_json,
            sum(octet_length(record_json::text))+2*(count(*)-1) AS records_bytes
          FROM changes
        )
        SELECT CASE WHEN octet_length(summary_json::text)+records_bytes<=1048576
          THEN jsonb_set(summary_json,'{{records}}',
            (SELECT jsonb_agg(record_json ORDER BY record_kind,record_key) FROM changes))::text
          ELSE NULL END FROM summary""",
        command.expected_draft_revision,
        command.expected_approved_revision,
    )


async def preview_registry_approval(
    connection, command: RegistryApprovalCommand, actor: RegistryActor, *, control_schema=None
):
    """Compare exact selected versions without changing durable registry data.

    The caller verifies live authorization on each request and owns the outer
    transaction. Identity-only previews do not infer bindings or membership.
    """
    _, selection_json = _validated_command(command)
    _validated_actor(actor)
    namespace = _namespace(control_schema)
    if not connection.is_in_transaction():
        raise ValueError("registry_approval_requires_caller_transaction")
    async with connection.transaction():
        selection_json = await _canonical_selection(connection, selection_json)
        control = await connection.fetchrow(
            f"SELECT draft_revision,approved_revision,coalesce((SELECT generation_id FROM {namespace}.network_serving_control WHERE id=1),0) AS serving_generation FROM {namespace}.registry_revision_control WHERE id=1 FOR SHARE"
        )
        if control is None:
            raise RegistryApprovalConflict("registry_control_unavailable")
        if (control["draft_revision"], control["approved_revision"]) != (
            command.expected_draft_revision,
            command.expected_approved_revision,
        ):
            raise RegistryApprovalConflict("registry_approval_revision_conflict")
        staging = await _stage_selection(connection, selection_json)
        await _validate_selected_history(connection, namespace, staging, control["draft_revision"])
        preview_json = await _preview_document(connection, namespace, staging, command)
        diagnostics = await approved_membership_diagnostics(
            connection, namespace, staging, command.expected_approved_revision
        )
        source_selection = await preview_imported_registry_selection(
            connection, command, staging, control["serving_generation"], control_schema=control_schema
        )
        await connection.execute(f"DROP TABLE {staging}")
        if preview_json is None:
            raise ValueError("registry_approval_preview_too_large")
        preview = json.loads(preview_json)
        _merge_preview_diagnostics(preview, diagnostics, source_selection)
        if len(json.dumps(preview, sort_keys=True).encode()) > 1048576:
            raise ValueError("registry_approval_preview_too_large")
        return preview


def _merge_preview_diagnostics(preview, diagnostics, source_selection):
    source_binding_rows = diagnostics.pop("source_binding_rows")
    source_binding_unresolved = diagnostics.pop("source_binding_unresolved_count")
    preview["unresolved_count"] = diagnostics["unresolved_count"] + source_binding_unresolved
    if diagnostics["membership_rows"]:
        preview["membership_diagnostics"] = diagnostics
    if source_binding_rows:
        preview["source_binding_diagnostics"] = {
            "binding_rows": source_binding_rows,
            "unresolved_count": source_binding_unresolved,
        }
    if source_selection is not None:
        preview["source_selection"] = source_selection
        if source_selection["status"] == "available":
            unresolved = preview["unresolved_count"] + source_selection["unresolved_rows"]
            if not 0 <= unresolved <= MAX_PREVIEW_COUNT:
                raise ValueError("registry_approval_preview_too_large")
            preview["unresolved_count"] = unresolved
