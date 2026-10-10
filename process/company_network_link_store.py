# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Atomic explicit company relationship drafts with immutable whole-batch replay."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass, field
from uuid import UUID, uuid4

from process.ext.address_fast import _fast_module
from process.registry_record_store import (
    RegistryActor,
    RegistryAddressUnavailable,
    RegistryRecordConflict,
    _bounded_text,
    _uuid,
    _validated_actor,
)
from process.registry_source_observation_store import _namespace

_MAX_REVISION = 9223372036854775805
_MAX_INPUT_BYTES = 8 * 1024 * 1024
_MAX_RECEIPT_BYTES = 1024 * 1024


@dataclass(frozen=True)
class CompanyLinkBatchTarget:
    """One exact companion head; group context never selects other companies."""

    company_id: UUID
    expected_revision: int
    network_ids: tuple[int, ...]
    group_id: UUID | None = None
    network_assertions: list[dict] = field(default_factory=list)


@dataclass(frozen=True)
class CompanyLinkBatchCommand:
    """Freeze the selected companies, fields and retry key before sending a write."""

    targets: tuple[CompanyLinkBatchTarget, ...]
    reason: str
    idempotency_key: str
    selection_group_id: UUID | None = None


def _json(document):
    """Canonical finite JSON binds all command fields without coercing identities."""
    try:
        return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    except TypeError, ValueError, RecursionError:
        raise ValueError("registry_company_link_batch_json_invalid") from None


def _validated_command(command):
    """Bound low-volume target control fields before any transaction changes."""
    if (
        not isinstance(command, CompanyLinkBatchCommand)
        or type(command.targets) is not tuple
        or not 1 <= len(command.targets) <= 100
    ):
        raise ValueError("registry_company_link_batch_targets_invalid")
    reason = _bounded_text(command.reason, 1000, "reason")
    if (
        _bounded_text(command.idempotency_key, 128, "idempotency_key") != command.idempotency_key
        or not command.idempotency_key.isprintable()
    ):
        raise ValueError("registry_idempotency_key_invalid")
    group_context = str(_uuid(command.selection_group_id)) if command.selection_group_id is not None else None
    selected_targets = []
    assertion_count = 0
    for batch_target in command.targets:
        if not isinstance(batch_target, CompanyLinkBatchTarget):
            raise ValueError("registry_company_link_batch_targets_invalid")
        company_id = str(_uuid(batch_target.company_id))
        if type(batch_target.expected_revision) is not int or not 0 <= batch_target.expected_revision <= _MAX_REVISION:
            raise ValueError("registry_company_link_batch_revision_invalid")
        if (
            type(batch_target.network_ids) is not tuple
            or len(batch_target.network_ids) > 5000
            or any(
                type(network_id) is not int or not 0 < network_id <= 2147483647
                for network_id in batch_target.network_ids
            )
            or any(left >= right for left, right in zip(batch_target.network_ids, batch_target.network_ids[1:]))
        ):
            raise ValueError("registry_company_links_networks_invalid")
        if type(batch_target.network_assertions) is not list:
            raise ValueError("registry_company_network_assertions_invalid")
        assertion_count += len(batch_target.network_assertions)
        if assertion_count > 5000:
            raise ValueError("registry_company_network_assertions_invalid")
        selected_targets.append(
            {
                "company_id": company_id,
                "expected_revision": batch_target.expected_revision,
                "network_ids": batch_target.network_ids,
                "group_id": str(_uuid(batch_target.group_id)) if batch_target.group_id is not None else None,
                "network_assertions": sorted(batch_target.network_assertions, key=_json),
            }
        )
    selected_targets.sort(key=lambda selected_target: selected_target["company_id"])
    if len({selected_target["company_id"] for selected_target in selected_targets}) != len(selected_targets):
        raise ValueError("registry_company_link_batch_targets_invalid")
    target_json = _json(selected_targets)
    if len(target_json.encode()) > _MAX_INPUT_BYTES:
        raise ValueError("registry_company_link_batch_input_limit")
    return json.loads(target_json), target_json, reason, group_context


def _native_assertions(input_bytes):
    """Require the prepared native validator; overlaps need explicit review."""
    native = _fast_module()
    if native is None or not callable(getattr(native, "validate_company_network_assertions", None)):
        raise RegistryAddressUnavailable("registry_company_network_assertions_native_unavailable")
    try:
        encoded = native.validate_company_network_assertions(input_bytes)
    except ValueError:
        raise ValueError("registry_company_network_assertions_invalid") from None
    except AttributeError, RuntimeError, TypeError:
        raise RegistryAddressUnavailable("registry_company_network_assertions_native_unavailable") from None
    try:
        result = json.loads(encoded)
    except ValueError, TypeError:
        raise RegistryAddressUnavailable("registry_company_network_assertions_native_unavailable") from None
    if (
        type(result) is not dict
        or set(result) != {"assertions", "conflicts"}
        or type(result["assertions"]) is not list
        or type(result["conflicts"]) is not list
    ):
        raise RegistryAddressUnavailable("registry_company_network_assertions_native_unavailable")
    if result["conflicts"]:
        raise RegistryRecordConflict("registry_company_link_batch_review_required")
    return _json(result["assertions"])


async def _stage_targets(connection, target_json, assertion_json):
    """Stage exact targets and attach their natively validated assertion documents."""
    staging = '"registry_company_links_' + uuid4().hex + '"'
    await connection.execute(
        f"""CREATE TEMP TABLE {staging} ON COMMIT DROP AS
      WITH assertions AS (SELECT value AS assertion,ordinality FROM jsonb_array_elements($2::jsonb) WITH ORDINALITY)
      SELECT target.*,COALESCE((SELECT jsonb_agg(assertion ORDER BY ordinality) FROM assertions
        WHERE (assertion->>'company_id')::uuid=target.company_id),'[]'::jsonb) AS validated_assertions
      FROM jsonb_to_recordset($1::jsonb) AS target(company_id uuid,expected_revision bigint,
        network_ids integer[],group_id uuid,network_assertions jsonb)""",
        target_json,
        assertion_json,
    )
    return staging


_VALIDATION_SQL = """WITH referenced_groups AS (
  SELECT group_id FROM {staging} WHERE group_id IS NOT NULL UNION SELECT $1::uuid WHERE $1::uuid IS NOT NULL
), checked AS (
  SELECT target.*,(company.company_id IS NOT NULL AND NOT company.archived) AS company_valid,
    ((target.expected_revision=0 AND links.company_id IS NULL) OR
      (target.expected_revision>0 AND links.revision=target.expected_revision AND NOT links.archived)) AS revision_valid
  FROM {staging} target LEFT JOIN {namespace}.company_registry company USING(company_id)
    LEFT JOIN {namespace}.company_registry_links links USING(company_id)
)
SELECT COALESCE(bool_and(company_valid),false)
  AND NOT EXISTS(SELECT 1 FROM referenced_groups reference LEFT JOIN {namespace}.company_group_registry groups
    ON groups.group_id=reference.group_id WHERE groups.group_id IS NULL OR groups.archived)
  AND NOT EXISTS(SELECT 1 FROM {staging} target CROSS JOIN LATERAL unnest(target.network_ids) network(network_id)
    LEFT JOIN {namespace}.network_registry_identity identity USING(network_id)
    LEFT JOIN {namespace}.network_registry_record head USING(network_id)
    WHERE identity.network_id IS NULL OR head.network_id IS NULL OR head.archived)
  AND NOT EXISTS(SELECT 1 FROM {staging} target CROSS JOIN LATERAL jsonb_array_elements(target.network_assertions) assertion
    WHERE (assertion->>'company_id')::uuid<>target.company_id OR NOT ((assertion->>'network_id')::integer=ANY(target.network_ids))) AS targets_valid,
  COALESCE(bool_and(COALESCE(revision_valid,false)),false) AS revisions_valid FROM checked"""


_WRITE_SQL = """WITH written AS (
  INSERT INTO {namespace}.company_registry_links AS head(company_id,network_ids,group_id,network_assertions,archived,revision)
  SELECT company_id,network_ids,group_id,validated_assertions,false,expected_revision+1 FROM {staging}
  ON CONFLICT(company_id) DO UPDATE SET network_ids=excluded.network_ids,group_id=excluded.group_id,
    network_assertions=excluded.network_assertions,revision=excluded.revision
  WHERE head.revision=excluded.revision-1 AND NOT head.archived RETURNING head.*
), history AS (
  INSERT INTO {namespace}.registry_record_history(record_kind,record_key,revision,custom_revision,record_json,
    actor_json,reason,idempotency_key,request_sha256)
  SELECT 'company_links',company_id::text,revision,$1,to_jsonb(written),$2::jsonb,$3,$4,$5 FROM written
  RETURNING record_kind,record_key,revision,custom_revision,record_json
), receipt AS (
  SELECT count(*) AS written_count,jsonb_build_object('batch_id',$6::uuid,'custom_revision',$1::bigint,'records',
    COALESCE(jsonb_agg(jsonb_build_object('record_kind',record_kind,'record_id',record_key,'revision',revision,
      'custom_revision',custom_revision,'record',record_json) ORDER BY record_key),'[]'::jsonb)) AS document FROM history
)
SELECT written_count,document::text AS receipt_json,octet_length(document::text) AS receipt_bytes FROM receipt"""


def _request_metadata(command, selected_targets, actor, reason, group_context):
    """Seal canonical targets and the complete server actor into a scoped retry."""
    actor_json = _json(_validated_actor(actor))
    actor_sha256 = hashlib.sha256(actor_json.encode()).hexdigest()
    request_json = _json(
        {
            "targets": selected_targets,
            "reason": reason,
            "idempotency_key": command.idempotency_key,
            "selection_group_id": group_context,
            "actor": json.loads(actor_json),
        }
    )
    return {
        "actor_json": actor_json,
        "actor_sha256": actor_sha256,
        "request_sha256": hashlib.sha256(request_json.encode()).hexdigest(),
        "reason": reason,
        "idempotency_key": command.idempotency_key,
    }


async def _save_batch(connection, namespace, staging, selected_count, custom_revision, metadata):
    """Retain set-written heads, history and a bounded receipt with one draft step."""
    batch_id = uuid4()
    written = await connection.fetchrow(
        _WRITE_SQL.format(staging=staging, namespace=namespace),
        custom_revision,
        metadata["actor_json"],
        metadata["reason"],
        "batch:" + str(batch_id),
        metadata["request_sha256"],
        batch_id,
    )
    if written["written_count"] != selected_count:
        raise RegistryRecordConflict("registry_company_link_batch_revision_conflict")
    if written["receipt_bytes"] > _MAX_RECEIPT_BYTES:
        raise ValueError("registry_company_link_batch_receipt_limit")
    await connection.execute(
        f"UPDATE {namespace}.registry_revision_control SET draft_revision=$1 WHERE id=1", custom_revision
    )
    await connection.execute(
        f"INSERT INTO {namespace}.registry_company_link_batch(batch_id,actor_sha256,idempotency_key,request_sha256,custom_revision,result_json) VALUES($1,$2,$3,$4,$5,$6::jsonb)",
        batch_id,
        metadata["actor_sha256"],
        metadata["idempotency_key"],
        metadata["request_sha256"],
        custom_revision,
        written["receipt_json"],
    )
    await connection.execute(f"DROP TABLE {staging}")
    return json.loads(written["receipt_json"])


async def apply_company_network_link_batch(connection, command, actor: RegistryActor, *, control_schema=None):
    """Save one selected batch; trusted callers recheck every live grant on retries.

    The caller owns the transaction. A savepoint preserves earlier caller work,
    while all heads, histories and the exact replay receipt succeed together.
    Approved records and serving manifests are never changed here.
    """
    selected_targets, target_json, reason, group_context = _validated_command(command)
    metadata = _request_metadata(command, selected_targets, actor, reason, group_context)
    namespace = _namespace(control_schema)
    if not connection.is_in_transaction():
        raise ValueError("registry_company_link_batch_requires_caller_transaction")
    async with connection.transaction():
        control = await connection.fetchrow(
            f"SELECT draft_revision FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE"
        )
        if control is None:
            raise RegistryRecordConflict("registry_control_unavailable")
        replay = await connection.fetchrow(
            f"SELECT request_sha256,result_json::text AS receipt_json FROM {namespace}.registry_company_link_batch WHERE actor_sha256=$1 AND idempotency_key=$2",
            metadata["actor_sha256"],
            metadata["idempotency_key"],
        )
        if replay is not None:
            if replay["request_sha256"] != metadata["request_sha256"]:
                raise RegistryRecordConflict("registry_company_link_batch_idempotency_conflict")
            return json.loads(replay["receipt_json"])
        if control["draft_revision"] > _MAX_REVISION:
            raise RegistryRecordConflict("registry_company_link_batch_revision_exhausted")
        assertion_bytes = _json(
            [assertion for selected_target in selected_targets for assertion in selected_target["network_assertions"]]
        ).encode()
        assertion_json = await asyncio.to_thread(_native_assertions, assertion_bytes)
        staging = await _stage_targets(connection, target_json, assertion_json)
        status = await connection.fetchrow(_VALIDATION_SQL.format(staging=staging, namespace=namespace), group_context)
        if not status["targets_valid"]:
            raise ValueError("registry_company_links_target_invalid")
        if not status["revisions_valid"]:
            raise RegistryRecordConflict("registry_company_link_batch_revision_conflict")
        return await _save_batch(
            connection, namespace, staging, len(selected_targets), control["draft_revision"] + 1, metadata
        )


async def read_group_company_links(connection, group_id, *, company_ids=None, limit=50, offset=0, control_schema=None):
    """Read one statement's bounded group page; supplied companies only narrow it."""
    group_id = _uuid(group_id)
    if type(limit) is not int or not 1 <= limit <= 100 or type(offset) is not int or not 0 <= offset <= 1000000:
        raise ValueError("registry_company_link_group_page_invalid")
    if company_ids is not None:
        if type(company_ids) not in {list, tuple} or len(company_ids) > 5000:
            raise ValueError("registry_company_link_group_scope_invalid")
        company_ids = tuple(_uuid(company_id) for company_id in company_ids)
        if len(set(company_ids)) != len(company_ids):
            raise ValueError("registry_company_link_group_scope_invalid")
    namespace = _namespace(control_schema)
    group_snapshot = await connection.fetchrow(
        f"""WITH page AS MATERIALIZED (
          SELECT company.company_id,to_jsonb(company) AS company,to_jsonb(links) AS links
          FROM {namespace}.company_registry_links links
          JOIN {namespace}.company_registry company USING(company_id)
          WHERE links.group_id=$1 AND NOT links.archived
            AND ($2::uuid[] IS NULL OR company.company_id=ANY($2::uuid[]))
          ORDER BY company.company_id LIMIT $3 OFFSET $4
        ), accounting AS (
          SELECT COALESCE(sum(octet_length(company::text)+octet_length(links::text)),0) AS bytes FROM page
        ) SELECT control.draft_revision,to_jsonb(groups)::text AS group_json,
          CASE WHEN accounting.bytes <= $5 THEN
            COALESCE((SELECT jsonb_agg(jsonb_build_object('company',company,'links',links) ORDER BY company_id)
              FROM page),'[]'::jsonb)::text END AS records_json
          FROM {namespace}.registry_revision_control control
          JOIN {namespace}.company_group_registry groups ON groups.group_id=$1 AND NOT groups.archived
          CROSS JOIN accounting WHERE control.id=1""",
        group_id,
        company_ids,
        limit,
        offset,
        _MAX_INPUT_BYTES - 131072,
    )
    if group_snapshot is None:
        return None
    if group_snapshot["records_json"] is None:
        raise RegistryAddressUnavailable("registry_company_link_group_response_limit")
    return {
        "group": json.loads(group_snapshot["group_json"]),
        "draft_revision": group_snapshot["draft_revision"],
        "records": json.loads(group_snapshot["records_json"]),
        "limit": limit,
        "offset": offset,
    }
