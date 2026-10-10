# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded source-period issuer evidence, independent of editable draft names."""

from __future__ import annotations

import json
import re
from typing import Any, Sequence

import asyncpg

from process.registry_source_observation_store import RegistryObservationError, _namespace

MAX_ISSUERS = 200
MAX_EVIDENCE_ROWS = 5_000
MAX_RESULT_BYTES = 8 * 1024 * 1024


class RegistryIssuerResolutionError(RegistryObservationError):
    """The issuer selector, transaction or bounded evidence was rejected."""


class RegistryIssuerResolutionUnavailable(RuntimeError):
    """Native source-evidence prerequisites could not be read."""


_READ_SQL = """
WITH requested AS (
  SELECT hios,ordinal FROM unnest($1::text[]) WITH ORDINALITY input(hios,ordinal)
), periods AS (
  SELECT a.hios_issuer_id,max(s.reporting_year) AS latest_year
  FROM {namespace}.registry_issuer_company_assertion a
  JOIN {namespace}.registry_source_snapshot s USING(snapshot_id)
  WHERE a.hios_issuer_id=ANY($1::text[]) GROUP BY a.hios_issuer_id
), chosen AS (
  SELECT r.*,h.business_state,coalesce($2::smallint,p.latest_year) AS reporting_year
  FROM requested r LEFT JOIN periods p ON p.hios_issuer_id=r.hios
  LEFT JOIN {namespace}.hios_issuer_registry h ON h.hios_issuer_id=r.hios
), evidence AS MATERIALIZED (
  SELECT r.hios,a.snapshot_id,a.source_record_key,a.state,a.company_id,a.resolution_status,
    a.valid_from,a.valid_to,r.business_state,r.reporting_year,
    s.source_system,s.source_id,s.edition_id,s.published_at,s.retrieved_at,s.input_sha256,s.parser_version,
    o.status AS observation_status,o.source_record_key IS NOT NULL AS observation_present,
    o.observation_json->'raw_fields'->>'company_name' AS company_label,
    o.observation_json->'raw_fields'->>'group_affiliation' AS group_label,
    c.company_id IS NOT NULL AS company_present,
    g.source_record_key IS NOT NULL AS group_assertion_present,g.group_id,
    g.resolution_status AS group_status,g.relationship_kind,g.valid_from AS group_valid_from,g.valid_to AS group_valid_to,
    head.group_id IS NOT NULL AS group_present,head.group_kind
  FROM chosen r JOIN {namespace}.registry_issuer_company_assertion a ON a.hios_issuer_id=r.hios
  JOIN {namespace}.registry_source_snapshot s ON s.snapshot_id=a.snapshot_id
    AND s.reporting_year IS NOT DISTINCT FROM r.reporting_year
  LEFT JOIN {namespace}.registry_source_observation o ON o.snapshot_id=a.snapshot_id AND o.source_record_key=a.source_record_key
  LEFT JOIN {namespace}.company_registry c ON c.company_id=a.company_id
  LEFT JOIN {namespace}.registry_company_group_assertion g ON g.snapshot_id=a.snapshot_id
    AND g.source_record_key=a.source_record_key AND g.company_id=a.company_id
  LEFT JOIN {namespace}.company_group_registry head ON head.group_id=g.group_id
), row_bounds AS (
  SELECT count(*)<=$3 AS bounded FROM evidence
), bounded_evidence AS (
  SELECT evidence.* FROM evidence,row_bounds WHERE row_bounds.bounded
), counts AS (
  SELECT hios,count(*) AS rows,count(DISTINCT state) AS states,count(DISTINCT company_id) AS companies,
    bool_or(resolution_status='conflicting' OR state IS DISTINCT FROM business_state AND business_state IS NOT NULL) AS company_conflict,
    bool_or(resolution_status<>'resolved' OR NOT company_present OR NOT observation_present OR observation_status='rejected') AS company_gap,
    bool_or(company_id IS NOT NULL AND NOT company_present) AS missing_company,
    count(*) FILTER(WHERE group_assertion_present) AS group_rows,count(DISTINCT group_id) AS groups,
    count(DISTINCT relationship_kind) AS relationship_kinds,
    bool_or(group_status='conflicting') AS group_conflict,
    bool_or(NOT group_assertion_present OR group_status IS DISTINCT FROM 'resolved' OR NOT group_present) AS group_gap,
    bool_or(group_id IS NOT NULL AND NOT group_present) AS missing_group,
    bool_or(NOT observation_present OR observation_status='rejected') AS observation_gap,
    min(company_id::text) AS company_id,min(group_id::text) AS group_id,min(relationship_kind) AS relationship_kind,
    coalesce(jsonb_agg(DISTINCT company_label COLLATE "C" ORDER BY company_label COLLATE "C") FILTER(WHERE nullif(btrim(company_label),'') IS NOT NULL),'[]'::jsonb) AS company_labels,
    coalesce(jsonb_agg(DISTINCT group_label COLLATE "C" ORDER BY group_label COLLATE "C") FILTER(WHERE nullif(btrim(group_label),'') IS NOT NULL),'[]'::jsonb) AS group_labels,
    jsonb_agg(jsonb_build_object(
      'snapshot_id',snapshot_id,'source_record_key',source_record_key,'state',state,'company_id',company_id,
      'resolution_status',resolution_status,'valid_from',valid_from,'valid_to',valid_to,
      'source_system',source_system,'source_id',source_id,'edition_id',edition_id,'reporting_year',reporting_year,
      'published_at',published_at,'retrieved_at',retrieved_at,'input_sha256',input_sha256,'parser_version',parser_version,
      'observation_status',observation_status,'company_label',company_label,'group_label',group_label,'company_present',company_present,
      'group_assertion',CASE WHEN group_assertion_present THEN jsonb_build_object('group_id',group_id,
        'resolution_status',group_status,'relationship_kind',relationship_kind,'valid_from',group_valid_from,
        'valid_to',group_valid_to,'group_present',group_present,'group_kind',group_kind) END
    ) ORDER BY snapshot_id,source_record_key) AS evidence_json
  FROM bounded_evidence GROUP BY hios
), statuses AS (
  SELECT *,CASE WHEN c.rows IS NULL THEN 'missing'
    WHEN c.company_conflict OR c.states>1 OR c.companies>1 THEN 'conflicting'
    WHEN r.reporting_year IS NULL OR r.business_state IS NULL OR c.company_gap THEN 'unresolved' ELSE 'resolved' END AS company_status,
    CASE WHEN c.group_rows IS NULL OR c.group_rows=0 THEN 'missing'
      WHEN c.group_conflict OR c.groups>1 OR c.relationship_kinds>1 THEN 'conflicting'
      WHEN c.group_gap THEN 'unresolved' ELSE 'resolved' END AS group_resolution_status
  FROM chosen r LEFT JOIN counts c USING(hios)
), documents AS MATERIALIZED (
  SELECT r.ordinal,jsonb_build_object('issuer_id',r.hios::integer,'hios_issuer_id',r.hios,'business_state',r.business_state,
    'reporting_year',r.reporting_year,'relationship_scope','source_reporting_period',
    'company_resolution_status',r.company_status,'group_resolution_status',r.group_resolution_status,
    'resolution_status',CASE WHEN r.company_status='missing' THEN 'missing'
      WHEN r.company_status='conflicting' OR r.group_resolution_status='conflicting' THEN 'conflicting'
      WHEN r.company_status<>'resolved' OR r.group_resolution_status<>'resolved' THEN 'unresolved' ELSE 'resolved' END,
    'legal_company',CASE WHEN r.company_status='resolved' THEN jsonb_build_object('company_id',r.company_id,'source_labels',r.company_labels) END,
    'reported_group',CASE WHEN r.company_status='resolved' AND r.group_resolution_status='resolved'
      THEN jsonb_build_object('group_id',r.group_id,'source_labels',r.group_labels,'relationship_kind',r.relationship_kind) END,
    'issues',to_jsonb(array_remove(ARRAY[
      CASE WHEN r.rows IS NULL THEN 'period_evidence_missing' END,
      CASE WHEN r.reporting_year IS NULL THEN 'reporting_period_missing' END,
      CASE WHEN r.business_state IS NULL THEN 'issuer_registry_missing' END,
      CASE WHEN r.company_conflict OR r.states>1 OR r.companies>1 THEN 'company_assertions_conflicting' END,
      CASE WHEN r.missing_company THEN 'company_reference_missing' END,
      CASE WHEN r.company_gap THEN 'company_evidence_unresolved' END,
      CASE WHEN r.observation_gap THEN 'source_observation_missing_or_rejected' END,
      CASE WHEN r.group_resolution_status='missing' THEN 'group_assertion_missing' END,
      CASE WHEN r.group_resolution_status='conflicting' THEN 'group_assertions_conflicting' END,
      CASE WHEN r.missing_group THEN 'group_reference_missing' END,
      CASE WHEN r.group_resolution_status='unresolved' THEN 'group_evidence_unresolved' END
    ]::text[],NULL)),
    'evidence',coalesce(r.evidence_json,'[]'::jsonb)) AS document
  FROM statuses r
), bounds AS (
  SELECT (SELECT bounded FROM row_bounds) AND coalesce(sum(octet_length(document::text)+2),0)+2<=$4 AS bounded FROM documents
)
SELECT current_setting('transaction_isolation') AS isolation,bounds.bounded,
  CASE WHEN bounds.bounded THEN (SELECT jsonb_agg(document ORDER BY ordinal)::text FROM documents) END AS result
FROM bounds
"""


def _selectors(issuer_ids: Sequence[int | str], reporting_year: int | None) -> tuple[str, ...]:
    if type(issuer_ids) not in (list, tuple) or not 1 <= len(issuer_ids) <= MAX_ISSUERS:
        raise RegistryIssuerResolutionError("Select 1–200 exact issuer identities")
    hios_ids = []
    for issuer in issuer_ids:
        if type(issuer) is int and 0 < issuer <= 99_999:
            issuer = f"{issuer:05d}"
        if type(issuer) is not str or re.fullmatch(r"[0-9]{5}", issuer) is None or issuer == "00000":
            raise RegistryIssuerResolutionError(
                "Issuer identity must be canonical HIOS text or a positive legacy integer"
            )
        hios_ids.append(issuer)
    if len(set(hios_ids)) != len(hios_ids):
        raise RegistryIssuerResolutionError("Issuer identities must be unique after canonicalization")
    if reporting_year is not None and (type(reporting_year) is not int or not 2010 <= reporting_year <= 2100):
        raise RegistryIssuerResolutionError("Reporting year must match the supported source period")
    return tuple(hios_ids)


async def read_registry_issuer_resolutions(
    connection: Any,
    issuer_ids: Sequence[int | str],
    *,
    reporting_year: int | None = None,
    control_schema: str | None = None,
) -> tuple[dict, ...]:
    """Read exact source-period relationships in one native statement.

    The caller owns a repeatable-read or serializable transaction. Latest means
    highest reporting year for each issuer, including unresolved/conflicting
    evidence; no older period supplies a fallback. Source labels and reported
    affiliations describe that period, never current ownership or draft names.
    The complete response is capped at 5000 evidence rows and 8 MiB, without
    truncating conflicting evidence. This function never locks or commits.
    """
    hios_ids = _selectors(issuer_ids, reporting_year)
    if not connection.is_in_transaction():
        raise RegistryIssuerResolutionError("Issuer resolution requires a caller-owned pinned transaction")
    namespace = _namespace(control_schema)
    try:
        receipt = await connection.fetchrow(
            _READ_SQL.format(namespace=namespace), hios_ids, reporting_year, MAX_EVIDENCE_ROWS, MAX_RESULT_BYTES
        )
    except asyncpg.PostgresError:
        raise RegistryIssuerResolutionUnavailable("Issuer source evidence is unavailable") from None
    if receipt["isolation"] not in ("repeatable read", "serializable"):
        raise RegistryIssuerResolutionError("Issuer resolution requires repeatable read or serializable isolation")
    if not receipt["bounded"]:
        raise RegistryIssuerResolutionError("Issuer period evidence exceeds the complete-response bound")
    return tuple(json.loads(receipt["result"]))
