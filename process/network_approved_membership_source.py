# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded membership pages from an exact, immutable approved registry map."""

from __future__ import annotations

import asyncio
import hashlib
import json
import re
from dataclasses import dataclass
from uuid import UUID, uuid5

import asyncpg

from process.network_membership_candidate_lifecycle import (
    _control_namespace,
    _locked_candidate,
    _require_transaction,
    admit_network_membership_batch,
)
from process.network_membership_copy import (
    MAX_INPUT_BYTES,
    MAX_ROWS,
    MembershipCopyError,
    _encode,
    partition_network_membership_commands,
)
from process.uhc_flex_practitioner_async_safety import drain_operation


class ApprovedMembershipSourceError(MembershipCopyError):
    """The pinned approved map, exact references or bounded input was rejected."""


def _counter(value):
    if type(value) is not int or not 0 <= value <= 9_223_372_036_854_775_807:
        raise ApprovedMembershipSourceError("Approved source counts must be nonnegative bigint values")
    return value


@dataclass(frozen=True)
class ApprovedMembershipSource:
    approved_revision: int
    generation_id: str
    total_rows: int = 0

    def __post_init__(self):
        _counter(self.approved_revision)
        _counter(self.total_rows)
        if type(self.generation_id) is not str or re.fullmatch(r"[0-9a-f]{64}", self.generation_id) is None:
            raise ApprovedMembershipSourceError("Approved source generation must be a lowercase SHA256")


@dataclass(frozen=True)
class ApprovedMembershipBatch:
    source: ApprovedMembershipSource
    offset: int
    row_count: int
    input_bytes: bytes
    input_sha256: str

    @property
    def total_rows(self):
        """Report the complete custom row count retained by the source pin."""
        return self.source.total_rows


@dataclass(frozen=True)
class ApprovedMembershipCommandBatch:
    """One complete approved document, preserving separate native add/remove streams."""

    source: ApprovedMembershipSource
    network_key: str
    command_count: int
    input_bytes: bytes
    input_sha256: str
    additions_copy_bytes: bytes
    additions_count: int
    removals_copy_bytes: bytes
    removals_count: int


_MAP_SQL = """
WITH selected AS MATERIALIZED (
  SELECT * FROM {namespace}.registry_approved_record WHERE approved_revision=$1
), active AS MATERIALIZED (
  SELECT * FROM selected WHERE record_json->'archived'='false'::jsonb
), documents AS MATERIALIZED (
  SELECT head.record_key AS network_key,head.record_json->'memberships_json' AS document,
    EXISTS(SELECT 1 FROM jsonb_array_elements(CASE
      WHEN jsonb_typeof(head.record_json->'memberships_json')='array'
      THEN head.record_json->'memberships_json' ELSE '[]'::jsonb END) entry
      WHERE entry ? 'operation') AS command_mode
  FROM active head WHERE head.record_kind='membership'
), members AS (
  SELECT document.network_key,item.member,item.ordinal,document.command_mode
  FROM documents document CROSS JOIN LATERAL jsonb_array_elements(
    CASE WHEN jsonb_typeof(document.document)='array' THEN document.document ELSE '[]'::jsonb END
  ) WITH ORDINALITY item(member,ordinal)
)
"""

_SCOPE_SQL = (
    _MAP_SQL
    + """
, fingerprints AS MATERIALIZED (
  SELECT record_kind,record_key,
    octet_length(jsonb_build_array(record_kind,record_key,record_revision,
      custom_revision,record_json)::text) AS document_bytes,
    CASE WHEN octet_length(jsonb_build_array(record_kind,record_key,record_revision,
      custom_revision,record_json)::text)<=$2 THEN
      encode(sha256(convert_to(jsonb_build_array(record_kind,record_key,record_revision,
        custom_revision,record_json)::text,'UTF8')),'hex') END AS fingerprint
  FROM selected
), bounds AS (
  SELECT coalesce(max(document_bytes),0)<=$2 AND count(*)*64+
    octet_length(jsonb_build_array($3::text,
      (SELECT oid FROM pg_namespace WHERE nspname=$3),$1::bigint,'')::text)<=$2 AS is_bounded
  FROM fingerprints
), bounded AS (
  SELECT fingerprints.* FROM fingerprints,bounds WHERE bounds.is_bounded
), diagnostics AS (
  SELECT count(*)::bigint AS total_rows,
    count(*) FILTER(WHERE jsonb_typeof(member)<>'object'
      OR (SELECT array_agg(key ORDER BY key) FROM jsonb_object_keys(
        CASE WHEN jsonb_typeof(member)='object' THEN member-'operation' ELSE '{{}}'::jsonb END) key)
        IS DISTINCT FROM ARRAY['evidence_id','location_id','network_id','provider_id','provider_system']::text[]
      OR (member ? 'operation' AND member->'operation' NOT IN ('"add"'::jsonb,'"remove"'::jsonb))
      OR jsonb_typeof(member->'evidence_id') IS DISTINCT FROM 'string'
      OR btrim(member->>'evidence_id')='' OR btrim(member->>'evidence_id')<>member->>'evidence_id'
      OR member->>'evidence_id' ~ '[[:cntrl:]]' OR octet_length(member->>'evidence_id')>1024
      OR member->>'network_id' IS DISTINCT FROM network_key
      OR network.record_key IS NULL
      OR (member->>'provider_system'='manual' AND provider.record_key IS NULL)
      OR (member->>'provider_system'='provider_directory' AND source_binding.count<>1)
      OR (location.record_key IS NULL AND source_binding.count<>1)
      OR (location.record_key IS NOT NULL AND source_binding.count<>0))::bigint AS unresolved_rows
  FROM members
  LEFT JOIN active network ON network.record_kind='network' AND network.record_key=network_key
  LEFT JOIN active provider ON provider.record_kind='provider' AND provider.record_key=member->>'provider_id'
  LEFT JOIN active location ON location.record_kind='location' AND location.record_key=member->>'location_id'
  LEFT JOIN LATERAL (
    SELECT count(*) AS count FROM active binding WHERE binding.record_kind='site_binding'
      AND (binding.record_json->>'provider_system',binding.record_json->>'provider_id',binding.record_json->>'location_id')
        =(member->>'provider_system',member->>'provider_id',member->>'location_id')
  ) source_binding ON true
)
SELECT current_setting('transaction_isolation') AS isolation,
  control.approved_revision,diagnostics.*,bounds.is_bounded,
  (SELECT count(*) FROM documents WHERE command_mode) AS command_documents,
  (SELECT count(*) FROM active WHERE record_kind='membership'
    AND jsonb_typeof(record_json->'memberships_json') IS DISTINCT FROM 'array') AS invalid_documents,
  encode(sha256(convert_to(jsonb_build_array($3::text,namespace.oid,$1::bigint,
    coalesce((SELECT string_agg(fingerprint,'' ORDER BY record_kind COLLATE "C",record_key COLLATE "C") FROM bounded),''))::text,
    'UTF8')),'hex') AS generation_id
FROM {control_source} control
CROSS JOIN pg_namespace namespace CROSS JOIN diagnostics CROSS JOIN bounds
WHERE control.id=1 AND namespace.nspname=$3
"""
)

_PAGE_SQL = (
    _MAP_SQL
    + """
, page AS MATERIALIZED (
  SELECT jsonb_build_object('network_id',member->'network_id',
    'provider_system',member->'provider_system','provider_id',member->'provider_id',
    'location_id',member->'location_id','evidence_id',
    'approved-custom:'||$1::text||':'||encode(sha256(convert_to(member::text,'UTF8')),'hex')) AS row_json,
    network_key,ordinal
  FROM members ORDER BY network_key COLLATE "C",ordinal OFFSET $2 LIMIT $3
), bounds AS (
  SELECT count(*)::bigint AS row_count,
    coalesce(sum(octet_length(row_json::text)+2),0)+2<=$4 AS is_bounded FROM page
), bounded AS (SELECT page.* FROM page,bounds WHERE bounds.is_bounded)
SELECT bounds.*,coalesce((SELECT jsonb_agg(row_json ORDER BY network_key COLLATE "C",ordinal)
  FROM bounded),'[]'::jsonb)::text AS input_json FROM bounds
"""
)


_COMMAND_DOCUMENT_SQL = """WITH chosen AS MATERIALIZED (
 SELECT record_key,record_json->'memberships_json' AS document
 FROM {namespace}.registry_approved_record
 WHERE approved_revision=$1 AND record_kind='membership' AND record_json->'archived'='false'::jsonb
   AND ($2::text IS NULL OR record_key COLLATE "C">$2::text COLLATE "C")
 ORDER BY record_key COLLATE "C" LIMIT 1
), bounded AS (
 SELECT *,CASE WHEN jsonb_typeof(document)='array' THEN jsonb_array_length(document) END AS command_count
 FROM chosen WHERE octet_length(document::text)<=$3
), encoded AS (
 SELECT bounded.*,(SELECT jsonb_agg(member||jsonb_build_object('evidence_id',
   'approved-custom:'||$1::text||':'||encode(sha256(convert_to(member::text,'UTF8')),'hex')) ORDER BY ordinal)
 FROM jsonb_array_elements(CASE WHEN command_count<=$4 THEN document ELSE '[]'::jsonb END)
 WITH ORDINALITY entry(member,ordinal)) AS commands FROM bounded
)
SELECT current_setting('transaction_isolation') AS isolation,control.approved_revision,
 chosen.record_key,bounded.command_count,
 CASE WHEN bounded.command_count<=$4 AND octet_length(coalesce(encoded.commands,'[]'::jsonb)::text)<=$3
 THEN coalesce(encoded.commands,'[]'::jsonb)::text END AS input_json
FROM {namespace}.registry_revision_control control LEFT JOIN chosen ON true
 LEFT JOIN bounded USING(record_key) LEFT JOIN encoded USING(record_key) WHERE control.id=1"""


async def read_approved_membership_commands(
    connection, approved_source, *, after_network_key=None, control_schema=None
):
    """Compile one full document from a pin verified in the caller's current transaction.

    Approved records are immutable. After the caller pins the full map once,
    bounded document seeks do not repeat its whole-map fingerprint scan.
    """
    if type(approved_source) is not ApprovedMembershipSource or not connection.is_in_transaction():
        raise ApprovedMembershipSourceError("Exact approved source and caller transaction are required")
    if after_network_key is not None and (
        type(after_network_key) is not str
        or re.fullmatch(r"[1-9][0-9]{0,9}", after_network_key) is None
        or int(after_network_key) > 2147483647
    ):
        raise ApprovedMembershipSourceError("Approved membership document seek key is invalid")
    try:
        page = await connection.fetchrow(
            _COMMAND_DOCUMENT_SQL.format(namespace=_control_namespace(control_schema)),
            approved_source.approved_revision,
            after_network_key,
            MAX_INPUT_BYTES,
            MAX_ROWS,
        )
    except asyncpg.PostgresError as error:
        raise ApprovedMembershipSourceError("Approved membership document is unavailable") from error
    if (
        page is None
        or page["approved_revision"] != approved_source.approved_revision
        or (page["isolation"] not in ("repeatable read", "serializable"))
    ):
        raise ApprovedMembershipSourceError("Approved document transaction differs from the pin")
    if page["record_key"] is None:
        return None
    if page["input_json"] is None:
        raise ApprovedMembershipSourceError("Approved membership document exceeds native bounds")
    input_bytes = page["input_json"].encode("utf8")
    try:
        partition = await drain_operation(
            asyncio.to_thread(partition_network_membership_commands, input_bytes), preserve_cancellation=True
        )
    except MembershipCopyError, TypeError, ValueError:
        raise ApprovedMembershipSourceError("Approved command document failed native validation") from None
    if partition.additions_count + partition.removals_count != page["command_count"]:
        raise ApprovedMembershipSourceError("Approved command document accounting mismatch")
    return ApprovedMembershipCommandBatch(
        approved_source,
        page["record_key"],
        page["command_count"],
        input_bytes,
        hashlib.sha256(input_bytes).hexdigest(),
        partition.additions_copy_bytes,
        partition.additions_count,
        partition.removals_copy_bytes,
        partition.removals_count,
    )


async def _scope(connection, approved_revision, namespace, *, retained=False, legacy=False):
    _counter(approved_revision)
    if not connection.is_in_transaction():
        raise ApprovedMembershipSourceError("Approved source requires a caller-owned repeatable-read transaction")
    try:
        control_source = (
            "(SELECT $1::bigint AS approved_revision,1 AS id)" if retained else f"{namespace}.registry_revision_control"
        )
        scope = await connection.fetchrow(
            _SCOPE_SQL.format(namespace=namespace, control_source=control_source),
            approved_revision,
            MAX_INPUT_BYTES,
            namespace[1:-1],
        )
    except asyncpg.PostgresError as error:
        raise ApprovedMembershipSourceError("Approved source prerequisites are unavailable") from error
    if scope is None or scope["approved_revision"] != approved_revision:
        raise ApprovedMembershipSourceError("Approved source revision no longer matches the control pin")
    if scope["isolation"] not in ("repeatable read", "serializable"):
        raise ApprovedMembershipSourceError("Approved source requires repeatable read or serializable isolation")
    if not scope["is_bounded"]:
        raise ApprovedMembershipSourceError("Approved source document or fingerprint aggregate exceeds 8 MiB")
    if scope["invalid_documents"] or scope["unresolved_rows"]:
        raise ApprovedMembershipSourceError("Approved source has unresolved or malformed membership references")
    if legacy and scope["command_documents"]:
        raise ApprovedMembershipSourceError("Command documents require the separate add/remove reader")
    return ApprovedMembershipSource(approved_revision, scope["generation_id"], scope["total_rows"])


async def pin_approved_membership_source(connection, *, approved_revision, control_schema=None):
    """Pin the current full approved map without exporting its documents to Python."""
    return await _scope(connection, approved_revision, _control_namespace(control_schema))


async def pin_retained_approved_membership_source(connection, *, approved_revision, control_schema=None):
    """Fingerprint the exact approved revision retained by a verified serving manifest."""
    return await _scope(connection, approved_revision, _control_namespace(control_schema), retained=True)


async def _read_batch(connection, source, offset, limit, namespace):
    if type(source) is not ApprovedMembershipSource:
        raise ApprovedMembershipSourceError("Exact approved membership source is required")
    _counter(offset)
    if type(limit) is not int or not 1 <= limit <= MAX_ROWS:
        raise ApprovedMembershipSourceError("Approved membership page limit must be 1 through 5000")
    if await _scope(connection, source.approved_revision, namespace, legacy=True) != source:
        raise ApprovedMembershipSourceError("Approved source map identity changed")
    try:
        page = await connection.fetchrow(
            _PAGE_SQL.format(namespace=namespace), source.approved_revision, offset, limit, MAX_INPUT_BYTES
        )
    except asyncpg.PostgresError as error:
        raise ApprovedMembershipSourceError("Approved membership page is unavailable") from error
    if not page["is_bounded"]:
        raise ApprovedMembershipSourceError("Approved membership batch exceeds 8 MiB")
    input_bytes = page["input_json"].encode("utf8")
    return ApprovedMembershipBatch(
        source, offset, page["row_count"], input_bytes, hashlib.sha256(input_bytes).hexdigest()
    )


async def read_approved_membership_batch(connection, source, *, offset=0, limit=MAX_ROWS, control_schema=None):
    """Read two set-based statements, then validate the entire page with the native codec."""
    batch = await _read_batch(connection, source, offset, limit, _control_namespace(control_schema))
    try:
        encoded = await drain_operation(asyncio.to_thread(_encode, batch.input_bytes), preserve_cancellation=True)
    except MembershipCopyError, TypeError, ValueError:
        raise ApprovedMembershipSourceError("Approved membership page failed native validation") from None
    if encoded[1] != batch.row_count:
        raise ApprovedMembershipSourceError("Approved membership page accounting mismatch")
    return batch


async def copy_approved_membership_batch(
    connection, source, copy_target, *, offset=0, limit=MAX_ROWS, control_schema=None
):
    """Admit once; exact receipt replay reaches no encoder or native COPY."""
    _require_transaction(connection, copy_target)
    namespace = _control_namespace(control_schema)
    batch = await _read_batch(connection, source, offset, limit, namespace)
    candidate = await _locked_candidate(connection, copy_target, namespace)
    source_generations = candidate["source_generations"]
    if type(source_generations) is str:
        source_generations = json.loads(source_generations)
    if candidate["approved_custom_revision"] != source.approved_revision or (
        source_generations.get("custom_membership") != source.generation_id
    ):
        raise ApprovedMembershipSourceError("Candidate approved custom source does not match the pin")
    batch_id = uuid5(
        UUID(copy_target.candidate_id), f"approved-custom:{source.approved_revision}:{source.generation_id}:{offset}"
    )
    return await admit_network_membership_batch(
        connection,
        copy_target,
        batch_id=batch_id,
        input_bytes=batch.input_bytes,
        expected_input_sha256=batch.input_sha256,
        control_schema=control_schema,
    )
