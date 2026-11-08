# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded internal lifecycle audit over retained drafts and explicit decisions."""

import json

from process.registry_record_store import _RECORD_MODELS, _record_identity
from process.registry_source_observation_store import _namespace

MAX_AUDIT_BYTES = 1048576

_HISTORY_SQL = """WITH subject_history AS (
  SELECT * FROM {namespace}.registry_record_history WHERE record_kind=$1 AND record_key=$2
), events AS (
  SELECT history.custom_revision AS event_revision,0 AS lifecycle_rank,
    history.revision AS record_revision,history.custom_revision,
    'draft:'||history.revision::text AS event_key,'draft' AS lifecycle,
    history.event_json->>'operation' AS operation,history.event_json,
    history.record_json,history.actor_json,history.reason,history.idempotency_key,
    history.request_sha256,history.created_at,NULL::jsonb AS approval,NULL::jsonb AS publication
  FROM subject_history history
  UNION ALL
  SELECT decision.approved_revision,1,selected.record_revision,selected.custom_revision,
    'approved:'||decision.approved_revision::text,'approved','approve',history.event_json,
    selected.record_json,decision.actor_json,decision.reason,decision.idempotency_key,
    decision.request_sha256,decision.created_at,
    jsonb_build_object('approved_revision',decision.approved_revision,
      'previous_approved_revision',decision.previous_approved_revision),NULL::jsonb
  FROM {namespace}.registry_approved_record selected
  JOIN {namespace}.registry_approval_history decision USING(approved_revision)
  JOIN subject_history history ON history.revision=selected.record_revision AND history.custom_revision=selected.custom_revision
  WHERE selected.record_kind=$1 AND selected.record_key=$2
    AND decision.selection_json @> jsonb_build_array(jsonb_build_object(
      'record_kind',$1::text,'record_id',$3::jsonb,'revision',selected.record_revision))
  UNION ALL
  SELECT manifest.approved_custom_revision,2,selected.record_revision,selected.custom_revision,
    'publication:'||request.request_id::text,'publication',
    coalesce(request.command_json->>'operation','publish'),history.event_json,
    selected.record_json,request.actor_json,request.command_json->>'reason',request.idempotency_key,
    request.request_sha256,request.updated_at,NULL::jsonb,
    jsonb_build_object('request_id',request.request_id,'generation_id',manifest.generation_id,
      'manifest_sha256',manifest.manifest_sha256,'approved_custom_revision',manifest.approved_custom_revision)
  FROM {namespace}.registry_publication_request request
  JOIN {namespace}.network_serving_manifest manifest ON
    (manifest.generation_id::text=request.result_json->>'serving_generation'
      AND manifest.approved_custom_revision::text=request.result_json->>'approved_revision')
    OR (manifest.generation_id::text=request.result_json->>'generation_id'
      AND manifest.approved_custom_revision::text=request.result_json->>'approved_custom_revision')
  JOIN {namespace}.registry_approved_record selected
    ON selected.approved_revision=manifest.approved_custom_revision
    AND selected.record_kind=$1 AND selected.record_key=$2
  JOIN subject_history history ON history.revision=selected.record_revision AND history.custom_revision=selected.custom_revision
  WHERE request.state='completed' AND jsonb_typeof(request.result_json)='object' AND (
    (NOT request.command_json ? 'operation'
      AND jsonb_typeof(request.result_json->'serving_generation')='number'
      AND jsonb_typeof(request.result_json->'approved_revision')='number'
      AND EXISTS(SELECT 1 FROM {namespace}.registry_approval_history decision
        WHERE decision.approved_revision=manifest.approved_custom_revision
          AND decision.previous_approved_revision::text=request.command_json->>'expected_approved_revision'
          AND decision.expected_draft_revision::text=request.command_json->>'expected_draft_revision'
          AND decision.actor_json=request.actor_json AND decision.idempotency_key=request.idempotency_key
          AND decision.selection_json=request.command_json->'selection'
          AND decision.reason=request.command_json->>'reason'
          AND decision.previous_approved_revision::text=request.result_json->>'previous_approved_revision'))
    OR (request.command_json->>'operation' IN ('reactivate_generation','publish_source_rollback')
      AND request.result_json->>'operation'=request.command_json->>'operation'
      AND jsonb_typeof(request.result_json->'generation_id')='number'
      AND jsonb_typeof(request.result_json->'approved_custom_revision')='number'
      AND request.result_json->>'previous_generation_id'=request.command_json->>'expected_serving_generation'
      AND request.result_json->>'approved_custom_revision'=request.command_json->>'expected_approved_revision'
      AND request.result_json->>'manifest_sha256'=manifest.manifest_sha256
      AND ((request.command_json->>'operation'='reactivate_generation'
        AND request.command_json->>'target_generation'=manifest.generation_id::text)
        OR (request.command_json->>'operation'='publish_source_rollback'
          AND request.result_json->>'candidate_id'=manifest.candidate_id::text
          AND request.result_json->>'preview_request_id'=request.command_json->>'preview_request_id'
          AND request.result_json->>'candidate_sha256'=request.command_json->>'candidate_sha256'))))
), page AS (
  SELECT * FROM events WHERE event_revision BETWEEN $4 AND $5
  ORDER BY event_revision DESC,lifecycle_rank DESC,record_revision DESC,event_key
  LIMIT $6+1 OFFSET $7
), documents AS (
  SELECT page.*,jsonb_build_object('lifecycle',page.lifecycle,'event_revision',page.event_revision,
    'event_key',page.event_key,'record_revision',page.record_revision,'custom_revision',page.custom_revision,
    'operation',page.operation,'draft_event',page.event_json,'before_snapshot_retained',
      CASE WHEN page.event_json IS NULL OR page.event_json->'before_revision'='null'::jsonb THEN NULL
        ELSE previous.revision IS NOT NULL END,
    'record',page.record_json,'actor',page.actor_json,'reason',page.reason,'idempotency_key',page.idempotency_key,
    'request_sha256',page.request_sha256,'created_at',page.created_at,
    'approval',page.approval,'publication',page.publication) AS document,
    row_number() OVER(ORDER BY page.event_revision DESC,lifecycle_rank DESC,page.record_revision DESC,page.event_key) AS ordinal
  FROM page LEFT JOIN subject_history previous ON previous.revision::text=page.event_json->>'before_revision'
), bounded AS (
  SELECT *,sum(octet_length(document::text)) FILTER(WHERE ordinal<=$6) OVER() AS page_bytes FROM documents
)
SELECT CASE WHEN ordinal<=$6 AND page_bytes<=$8 THEN document::text ELSE NULL END AS document,
  ordinal,page_bytes FROM bounded ORDER BY ordinal"""


def _audit_event_document(encoded, kind, identity):
    """Refuse a retained snapshot that contradicts its exact typed history subject."""
    event_by_field = json.loads(encoded)
    snapshot_by_field = event_by_field["record"]
    identity_column = _RECORD_MODELS[kind][1]
    if (
        type(snapshot_by_field) is not dict
        or type(snapshot_by_field.get(identity_column)) is not type(identity)
        or snapshot_by_field.get(identity_column) != identity
        or type(snapshot_by_field.get("revision")) is not int
        or snapshot_by_field["revision"] != event_by_field["record_revision"]
    ):
        raise ValueError("registry_custom_history_snapshot_invalid")
    return event_by_field


async def read_registry_custom_history(
    connection,
    kind,
    record_id,
    *,
    minimum_revision=0,
    maximum_revision=9223372036854775807,
    limit=50,
    offset=0,
    control_schema=None,
):
    """Read exact lifecycle evidence in the caller's pinned transaction, granting no authority."""
    if type(kind) is not str or kind not in _RECORD_MODELS:
        raise ValueError("registry_custom_history_request_invalid")
    _record_identity(kind, record_id)
    if (
        type(minimum_revision) is not int
        or type(maximum_revision) is not int
        or not 0 <= minimum_revision <= maximum_revision <= 9223372036854775807
        or type(limit) is not int
        or not 1 <= limit <= 50
        or type(offset) is not int
        or not 0 <= offset <= 1000000
        or not connection.is_in_transaction()
    ):
        raise ValueError("registry_custom_history_request_invalid")
    identity = record_id if type(record_id) is int else str(record_id)
    audit_rows = await connection.fetch(
        _HISTORY_SQL.format(namespace=_namespace(control_schema)),
        kind,
        str(record_id),
        json.dumps(identity),
        minimum_revision,
        maximum_revision,
        limit,
        offset,
        MAX_AUDIT_BYTES,
    )
    selected_rows = audit_rows[:limit]
    if any(audit_row["document"] is None for audit_row in selected_rows):
        raise ValueError("registry_custom_history_output_limit")
    history_by_field = {
        "record_kind": kind,
        "record_id": identity,
        "limit": limit,
        "offset": offset,
        "has_more": len(audit_rows) > limit,
        "events": [_audit_event_document(audit_row["document"], kind, identity) for audit_row in selected_rows],
    }
    if len(json.dumps(history_by_field, ensure_ascii=False, separators=(",", ":")).encode()) > MAX_AUDIT_BYTES:
        raise ValueError("registry_custom_history_output_limit")
    return history_by_field
