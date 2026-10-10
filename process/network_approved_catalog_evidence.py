# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded reference export from an exact retained approved network map.

The caller owns the stable transaction, authorization and map retention. This
reader validates syntax and custody; it does not verify pricing or benefits.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import asyncpg

from process.network_approved_membership_source import (
    ApprovedMembershipSource,
    ApprovedMembershipSourceError,
    pin_retained_approved_membership_source,
)
from process.network_membership_candidate_lifecycle import _control_namespace
from process.registry_record_store import RegistryAddressUnavailable, _validated_catalog_evidence

MAX_NETWORKS = 5000
MAX_REFERENCES = 4096
MAX_BINDINGS = 16
MAX_REQUEST_BYTES = 1_048_576
MAX_EVIDENCE_BYTES = 16384
MAX_STORED_EVIDENCE_BYTES = 32768


class ApprovedNetworkCatalogEvidenceError(ValueError):
    """Value-free refusal of changed custody or malformed bounded references."""

    def __init__(self):
        super().__init__("registry_approved_catalog_evidence_unavailable")


class ApprovedNetworkCatalogEvidenceResourceLimit(ApprovedNetworkCatalogEvidenceError):
    """Fully validated retained references exceed an unchanged export bound."""


@dataclass(frozen=True)
class ApprovedNetworkCatalogEvidenceRecord:
    network_id: int
    approved_record_revision: int
    authored_record_revision: int | None
    evidence_origin: str


@dataclass(frozen=True)
class ApprovedNetworkCatalogEvidenceBatch:
    source: ApprovedMembershipSource
    offset: int
    total_networks: int
    records: tuple[ApprovedNetworkCatalogEvidenceRecord, ...]
    reference_count: int
    request_bytes: bytes | None
    request_sha256: str | None

    @property
    def approved_revision(self):
        """Return the immutable revision of the retained approved membership source."""
        return self.source.approved_revision

    @property
    def approved_map_sha256(self):
        """Return the retained map fingerprint, without establishing capabilities."""
        return self.source.generation_id


_PAGE_SQL = """WITH networks AS MATERIALIZED (
  SELECT record_key,record_revision,record_json->'archived' AS archived,
    record_json->'network_id' AS identity_json,record_json->'revision' AS revision_json,
    record_json ? 'catalog_evidence_json' AS evidence_present,
    record_json->'catalog_evidence_json' AS evidence,
    CASE WHEN record_key ~ '^[1-9][0-9]{{0,9}}$' THEN
      CASE WHEN record_key::numeric<=2147483647 THEN record_key::integer END END AS network_id
  FROM {namespace}.registry_approved_record
  WHERE approved_revision=$1 AND record_kind='network'
), diagnostics AS (
  SELECT count(*) FILTER(WHERE NOT COALESCE(
    jsonb_typeof(archived)='boolean' AND network_id IS NOT NULL AND record_revision>0
    AND jsonb_typeof(identity_json)='number' AND identity_json#>>'{{}}'=record_key
    AND jsonb_typeof(revision_json)='number' AND revision_json#>>'{{}}'=record_revision::text,false))
    AS invalid_networks,
    count(*) FILTER(WHERE archived='false'::jsonb)::bigint AS total_networks
  FROM networks
), page AS MATERIALIZED (
  SELECT * FROM networks WHERE archived='false'::jsonb
    AND ($2::integer[] IS NULL OR network_id=ANY($2::integer[]))
  ORDER BY network_id OFFSET $3 LIMIT $4
), documents AS MATERIALIZED (
  SELECT *,jsonb_build_object('network_id',network_id,'record_revision',record_revision,
    'evidence_present',evidence_present,'evidence',evidence) AS row_json,
    CASE WHEN jsonb_typeof(evidence->'pricing_refs')='array'
      THEN jsonb_array_length(evidence->'pricing_refs') ELSE 0 END AS pricing_count,
    CASE WHEN jsonb_typeof(evidence->'benefit_refs')='array'
      THEN jsonb_array_length(evidence->'benefit_refs') ELSE 0 END AS benefit_count
  FROM page
), bounds AS (
  SELECT count(*)::bigint AS row_count,
    coalesce(sum(pricing_count+benefit_count),0)::bigint AS reference_count,
    coalesce(bool_and(evidence IS NULL OR evidence='null'::jsonb OR
      (jsonb_typeof(evidence)='object' AND octet_length(evidence::text)<=$5
        AND pricing_count<=$8 AND benefit_count<=$8)),true)
      AND coalesce(sum(octet_length(row_json::text)+2),0)+2<=$6
      AND coalesce(sum(pricing_count+benefit_count),0)<=$7 AS bounded
  FROM documents
), bounded AS (
  SELECT documents.* FROM documents CROSS JOIN bounds CROSS JOIN diagnostics
  WHERE bounds.bounded AND diagnostics.invalid_networks=0
    AND ($2::integer[] IS NULL OR bounds.row_count=cardinality($2::integer[]))
    AND current_setting('transaction_read_only')='on'
    AND current_setting('transaction_isolation') IN ('repeatable read','serializable')
)
SELECT current_setting('transaction_isolation') AS isolation,
  current_setting('transaction_read_only') AS read_only,diagnostics.*,bounds.*,
  coalesce((SELECT jsonb_agg(row_json ORDER BY network_id) FROM bounded),'[]'::jsonb)::text AS rows_json
FROM diagnostics CROSS JOIN bounds
"""


_REFUSED_PAGE_SQL = """WITH selected AS MATERIALIZED (
  SELECT record_key::integer AS network_id,record_revision,
    record_json ? 'catalog_evidence_json' AS evidence_present,
    record_json->'catalog_evidence_json' AS evidence
  FROM {namespace}.registry_approved_record
  WHERE approved_revision=$1 AND record_kind='network' AND record_json->'archived'='false'::jsonb
    AND ($2::integer[] IS NULL OR record_key::integer=ANY($2::integer[]))
  ORDER BY record_key::integer OFFSET $3 LIMIT $4
), guarded AS MATERIALIZED (
  SELECT *,evidence IS NULL OR evidence='null'::jsonb OR octet_length(evidence::text)<=$5 AS stored_bounded
  FROM selected
), documents AS (
  SELECT network_id,stored_bounded,CASE WHEN stored_bounded THEN
    jsonb_build_object('network_id',network_id,'record_revision',record_revision,
      'evidence_present',evidence_present,'evidence',evidence)::text END AS row_json
  FROM guarded
)
SELECT stored_bounded,octet_length(row_json)+2 AS row_bytes,row_json FROM documents ORDER BY network_id
"""


def _require(condition):
    if not condition:
        raise ApprovedNetworkCatalogEvidenceError()


def _integer(value, maximum, minimum=0):
    _require(type(value) is int and minimum <= value <= maximum)
    return value


def _json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def _unique(items):
    object_by_key = {}
    for key, value in items:
        _require(key not in object_by_key)
        object_by_key[key] = value
    return object_by_key


def _validated_network_evidence(retained_network_by_field, previous_network_id):
    """Validate one exact approved record and rebind its authored evidence base."""
    _require(
        type(retained_network_by_field) is dict
        and set(retained_network_by_field) == {"network_id", "record_revision", "evidence_present", "evidence"}
    )
    network_id = _integer(retained_network_by_field["network_id"], 2147483647, 1)
    _require(network_id > previous_network_id)
    approved_record_revision = _integer(retained_network_by_field["record_revision"], 9223372036854775807, 1)
    _require(type(retained_network_by_field["evidence_present"]) is bool)
    retained_evidence_by_field = retained_network_by_field["evidence"]
    authored_record_revision = None
    if retained_evidence_by_field is None:
        evidence_origin = "null" if retained_network_by_field["evidence_present"] else "absent"
        request_by_field = {
            "network_id": network_id,
            "expected_record_revision": approved_record_revision,
            "pricing_refs": None,
            "benefit_refs": None,
        }
    else:
        _require(retained_network_by_field["evidence_present"] is True and type(retained_evidence_by_field) is dict)
        _require(
            type(retained_evidence_by_field.get("network_id")) is int
            and retained_evidence_by_field["network_id"] == network_id
        )
        authored_record_revision = _integer(
            retained_evidence_by_field.get("expected_record_revision"), approved_record_revision - 1, 1
        )
        request_by_field = {**retained_evidence_by_field, "expected_record_revision": approved_record_revision}
        evidence_origin = "retained"
    canonical_evidence_by_field = _validated_catalog_evidence(request_by_field)
    _require(
        type(canonical_evidence_by_field) is dict and _json(canonical_evidence_by_field) == _json(request_by_field)
    )
    _require(len(_json(canonical_evidence_by_field)) <= MAX_EVIDENCE_BYTES)
    approved_record = ApprovedNetworkCatalogEvidenceRecord(
        network_id, approved_record_revision, authored_record_revision, evidence_origin
    )
    return approved_record, canonical_evidence_by_field


def _validated_page(retained_source, offset, limit, page, selected_ids):
    """Validate page accounting and bounded references before encoding the export."""
    _require(page is not None)
    _require(page["isolation"] in {"repeatable read", "serializable"} and page["read_only"] == "on")
    _require(_integer(page["invalid_networks"], 9223372036854775807) == 0 and page["bounded"] is True)
    total_networks = _integer(page["total_networks"], 9223372036854775807)
    row_count = _integer(page["row_count"], MAX_NETWORKS)
    reference_count = _integer(page["reference_count"], MAX_REFERENCES)
    expected_rows = min(limit, max(total_networks - offset, 0)) if selected_ids is None else len(selected_ids)
    _require(row_count <= total_networks and row_count == expected_rows)
    rows_json = page["rows_json"]
    _require(type(rows_json) is str and len(rows_json) <= MAX_REQUEST_BYTES)
    _require(len(rows_json.encode()) <= MAX_REQUEST_BYTES)
    retained_networks = json.loads(rows_json, object_pairs_hook=_unique)
    _require(type(retained_networks) is list and len(retained_networks) == row_count)
    approved_records = []
    network_requests = []
    previous_network_id = seen_references = 0
    for retained_network_by_field in retained_networks:
        approved_record, canonical_evidence_by_field = _validated_network_evidence(
            retained_network_by_field, previous_network_id
        )
        previous_network_id = approved_record.network_id
        for field in ("pricing_refs", "benefit_refs"):
            binding_references = canonical_evidence_by_field[field]
            _require(
                binding_references is None
                or type(binding_references) is list
                and len(binding_references) <= MAX_BINDINGS
            )
            seen_references += 0 if binding_references is None else len(binding_references)
            _require(seen_references <= MAX_REFERENCES)
        network_requests.append(canonical_evidence_by_field)
        approved_records.append(approved_record)
    _require(seen_references == reference_count)
    if selected_ids is not None:
        _require(tuple(approved_record.network_id for approved_record in approved_records) == selected_ids)
    request_bytes = _json({"networks": network_requests}) if network_requests else None
    _require(request_bytes is None or len(request_bytes) <= MAX_REQUEST_BYTES)
    digest = hashlib.sha256(request_bytes).hexdigest() if request_bytes is not None else None
    return ApprovedNetworkCatalogEvidenceBatch(
        retained_source, offset, total_networks, tuple(approved_records), reference_count, request_bytes, digest
    )


async def read_approved_network_catalog_evidence(
    connection, source, *, offset=0, limit=MAX_NETWORKS, network_ids=None, control_schema=None
):
    """Revalidate the full retained pin, then export one bounded native set.

    The caller must own a read-only repeatable-read/serializable transaction.
    Two set-based statements include the existing complete fingerprint check.
    No mutable draft/control head is consulted. Empty pages have no controller
    request. Pins and exported reference bytes need caller-owned retention
    through physical proof, candidate publication and subsequent serving.
    """
    _require(type(source) is ApprovedMembershipSource)
    _integer(offset, 9223372036854775807)
    _integer(limit, MAX_NETWORKS, 1)
    if network_ids is not None:
        _require(type(network_ids) is tuple and 1 <= len(network_ids) <= limit and offset == 0)
        previous = 0
        for identity in network_ids:
            _integer(identity, 2147483647, 1)
            _require(identity > previous)
            previous = identity
    return await _read_retained_evidence_page(
        connection, source, offset=offset, limit=limit, network_ids=network_ids, control_schema=control_schema
    )


async def _validate_refused_cursor(connection, retained_source, page, *, offset, limit, selected_ids, namespace):
    """Reuse the native grammar for every bounded row; retain no aggregate."""
    network_count = reference_count = stored_page_bytes = 0
    previous_network_id = 0
    request_bytes = len(_json({"networks": []}))
    cursor = connection.cursor(
        _REFUSED_PAGE_SQL.format(namespace=namespace),
        retained_source.approved_revision,
        None if selected_ids is None else list(selected_ids),
        offset,
        limit,
        MAX_STORED_EVIDENCE_BYTES,
        prefetch=16,
    )
    async for stored_row in cursor:
        _require(network_count < page["row_count"] and stored_row["stored_bounded"] is True)
        encoded_row = stored_row["row_json"]
        _require(type(encoded_row) is str and len(encoded_row.encode()) <= MAX_REQUEST_BYTES)
        row_bytes = _integer(stored_row["row_bytes"], MAX_REQUEST_BYTES)
        _require(row_bytes == len(encoded_row.encode()) + 2)
        retained_network_by_field = json.loads(encoded_row, object_pairs_hook=_unique)
        approved_record, canonical_evidence_by_field = _validated_network_evidence(
            retained_network_by_field,
            previous_network_id,
        )
        if selected_ids is not None:
            _require(approved_record.network_id == selected_ids[network_count])
        previous_network_id = approved_record.network_id
        for field in ("pricing_refs", "benefit_refs"):
            binding_references = canonical_evidence_by_field[field]
            _require(
                binding_references is None
                or type(binding_references) is list
                and len(binding_references) <= MAX_BINDINGS
            )
            reference_count += 0 if binding_references is None else len(binding_references)
        stored_page_bytes += row_bytes
        request_bytes += len(_json(canonical_evidence_by_field)) + (1 if network_count else 0)
        network_count += 1
    _require(connection.is_in_transaction())
    _require(network_count == page["row_count"] and reference_count == page["reference_count"])
    _require(
        stored_page_bytes + 2 > MAX_REQUEST_BYTES
        or request_bytes > MAX_REQUEST_BYTES
        or reference_count > MAX_REFERENCES
    )


async def _require_resource_only_refusal(connection, source, page, *, offset, limit, selected_ids, namespace):
    """Distinguish resources only after exact custody and complete native EOF."""
    _require(page["isolation"] in {"repeatable read", "serializable"} and page["read_only"] == "on")
    _require(_integer(page["invalid_networks"], 9223372036854775807) == 0)
    total_networks = _integer(page["total_networks"], 9223372036854775807)
    row_count = _integer(page["row_count"], MAX_NETWORKS)
    _integer(page["reference_count"], 9223372036854775807)
    expected_rows = min(limit, max(total_networks - offset, 0)) if selected_ids is None else len(selected_ids)
    _require(row_count <= total_networks and row_count == expected_rows)
    await _validate_refused_cursor(
        connection,
        source,
        page,
        offset=offset,
        limit=limit,
        selected_ids=selected_ids,
        namespace=namespace,
    )
    raise ApprovedNetworkCatalogEvidenceResourceLimit()


async def _read_retained_evidence_page(connection, retained_source, *, offset, limit, network_ids, control_schema):
    """Read the exact retained pin and sanitize only the existing refusal classes."""
    try:
        namespace = _control_namespace(control_schema)
        retained = await pin_retained_approved_membership_source(
            connection, approved_revision=retained_source.approved_revision, control_schema=control_schema
        )
        _require(retained == retained_source)
        page = await connection.fetchrow(
            _PAGE_SQL.format(namespace=namespace),
            retained_source.approved_revision,
            None if network_ids is None else list(network_ids),
            offset,
            limit,
            MAX_STORED_EVIDENCE_BYTES,
            MAX_REQUEST_BYTES,
            MAX_REFERENCES,
            MAX_BINDINGS,
        )
        if page is not None and page["bounded"] is False:
            await _require_resource_only_refusal(
                connection,
                retained_source,
                page,
                offset=offset,
                limit=limit,
                selected_ids=network_ids,
                namespace=namespace,
            )
        return _validated_page(retained_source, offset, limit, page, network_ids)
    except ApprovedNetworkCatalogEvidenceResourceLimit:
        raise
    except (
        ApprovedMembershipSourceError,
        asyncpg.PostgresError,
        RegistryAddressUnavailable,
        ValueError,
        TypeError,
        KeyError,
        AttributeError,
        UnicodeError,
        RecursionError,
    ):
        raise ApprovedNetworkCatalogEvidenceError() from None
