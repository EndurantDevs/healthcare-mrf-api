# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded group diagnostics under the caller's existing approval writer fence."""

import json

_MAX_ROWS = 5000
_MAX_BYTES = 8 * 1024 * 1024

_DIAGNOSTICS_SQL = """
WITH prospective AS MATERIALIZED (
  SELECT previous.record_kind,previous.record_key,previous.record_json,false AS selected
  FROM {namespace}.registry_approved_record previous WHERE previous.approved_revision=$1
    AND NOT EXISTS(SELECT 1 FROM {staging} selected
      WHERE (selected.record_kind,selected.record_key)=(previous.record_kind,previous.record_key))
  UNION ALL
  SELECT history.record_kind,history.record_key,history.record_json,true
  FROM {staging} selected JOIN {namespace}.registry_record_history history
    ON (history.record_kind,history.record_key,history.revision)=
      (selected.record_kind,selected.record_key,selected.revision)
), companies AS MATERIALIZED (
  SELECT record_key,selected,
    CASE WHEN NOT record_json ? 'group_assertions' THEN '[]'::jsonb
      WHEN jsonb_typeof(record_json->'group_assertions')='array' THEN
        CASE WHEN jsonb_array_length(record_json->'group_assertions')<=128
          AND octet_length((record_json->'group_assertions')::text)<={max_bytes}-2
        THEN record_json->'group_assertions' END END AS family
  FROM prospective WHERE record_kind='company' AND record_json->'archived'='false'::jsonb
), groups AS MATERIALIZED (
  SELECT company.record_key,assertion_ordinal,assertion FROM companies company
  CROSS JOIN LATERAL jsonb_array_elements(company.family) WITH ORDINALITY entries(assertion,assertion_ordinal)
), invalid AS (
  SELECT DISTINCT g.record_key FROM groups g WHERE NOT coalesce(
    jsonb_typeof(assertion)='object'
    AND assertion ?& ARRAY['assertion_id','company_id','group_id','group_kind','relationship_kind',
      'valid_from','valid_to','provenance']
    AND assertion-'assertion_id'-'company_id'-'group_id'-'group_kind'-'relationship_kind'
      -'valid_from'-'valid_to'-'provenance'='{{}}'::jsonb
    AND jsonb_typeof(assertion->'assertion_id')='string'
    AND jsonb_typeof(assertion->'company_id')='string'
    AND jsonb_typeof(assertion->'group_id') IN ('null','string')
    AND jsonb_typeof(assertion->'valid_from') IN ('null','string')
    AND jsonb_typeof(assertion->'valid_to') IN ('null','string')
    AND (assertion->>'group_id' IS NULL OR pg_input_is_valid(assertion->>'group_id','uuid'))
    AND (assertion->>'valid_from' IS NULL OR pg_input_is_valid(assertion->>'valid_from','date'))
    AND (assertion->>'valid_to' IS NULL OR pg_input_is_valid(assertion->>'valid_to','date'))
    AND assertion->>'company_id'=g.record_key
    AND assertion->>'group_kind' IN ('corporate_parent','naic_group')
    AND assertion->>'relationship_kind' IN ('reported_affiliation','verified_ownership')
    AND jsonb_typeof(assertion->'provenance')='object'
    AND assertion->'provenance' ?& ARRAY['kind','evidence_ref','snapshot_id','source_record_key']
    AND (assertion->'provenance')-'kind'-'evidence_ref'-'snapshot_id'-'source_record_key'='{{}}'::jsonb
    AND jsonb_typeof(assertion->'provenance'->'evidence_ref')='string'
    AND jsonb_typeof(assertion->'provenance'->'snapshot_id') IN ('null','string')
    AND (assertion->'provenance'->>'snapshot_id' IS NULL
      OR pg_input_is_valid(assertion->'provenance'->>'snapshot_id','uuid'))
    AND jsonb_typeof(assertion->'provenance'->'source_record_key') IN ('null','string')
    AND assertion->'provenance'->>'kind' IN ('manual_reference','source_reference'),false)
), targets AS (
  SELECT g.record_key,g.assertion_ordinal FROM groups g WHERE assertion->>'group_id' IS NOT NULL AND NOT EXISTS (
    SELECT 1 FROM prospective target
    WHERE target.record_kind='group' AND target.record_key=g.assertion->>'group_id'
      AND target.record_json->'archived'='false'::jsonb
      AND target.record_json->>'group_kind'=g.assertion->>'group_kind')
), sources AS (
  SELECT g.record_key,g.assertion_ordinal FROM groups g
  WHERE assertion->'provenance'->>'kind'='source_reference' AND NOT EXISTS (
    SELECT 1 FROM {namespace}.registry_source_snapshot snapshot
    JOIN {namespace}.registry_source_observation observation USING(snapshot_id)
    JOIN {namespace}.registry_company_group_assertion edge
      ON edge.snapshot_id=observation.snapshot_id AND edge.source_record_key=observation.source_record_key
    WHERE snapshot.snapshot_id=CASE WHEN pg_input_is_valid(g.assertion->'provenance'->>'snapshot_id','uuid')
      THEN (g.assertion->'provenance'->>'snapshot_id')::uuid END
      AND observation.source_record_key=g.assertion->'provenance'->>'source_record_key'
      AND observation.status IN ('accepted','unresolved')
      AND edge.resolution_status IN ('resolved','unresolved')
      AND edge.company_id::text=g.record_key AND edge.company_id::text=g.assertion->>'company_id'
      AND edge.group_id IS NOT DISTINCT FROM CASE WHEN pg_input_is_valid(g.assertion->>'group_id','uuid')
        THEN (g.assertion->>'group_id')::uuid END
      AND edge.relationship_kind=g.assertion->>'relationship_kind'
      AND edge.valid_from IS NOT DISTINCT FROM CASE WHEN pg_input_is_valid(g.assertion->>'valid_from','date')
        THEN (g.assertion->>'valid_from')::date END
      AND edge.valid_to IS NOT DISTINCT FROM CASE WHEN pg_input_is_valid(g.assertion->>'valid_to','date')
        THEN (g.assertion->>'valid_to')::date END
      AND (observation.observation_json->>'group_kind' IS NULL
        OR observation.observation_json->>'group_kind'=g.assertion->>'group_kind'))
)
SELECT NULL::text AS company_key,NULL::text AS family_json,jsonb_build_object(
  'group_family_invalid_count',(SELECT count(*) FROM companies WHERE family IS NULL)+(SELECT count(*) FROM invalid),
  'group_target_conflict_count',(SELECT count(*) FROM targets),
  'group_source_conflict_count',(SELECT count(*) FROM sources),
  'group_assertion_rows',(SELECT count(*) FROM groups),
  'group_unresolved_count',(SELECT count(*) FROM companies WHERE family IS NULL)+(SELECT count(*) FROM invalid)
    +(SELECT count(*) FROM (SELECT * FROM targets UNION SELECT * FROM sources) unresolved))::text AS diagnostics_json
UNION ALL
SELECT record_key,family::text,NULL FROM companies WHERE selected AND family<>'[]'::jsonb
  AND NOT EXISTS(SELECT 1 FROM companies WHERE family IS NULL)
  AND NOT EXISTS(SELECT 1 FROM invalid) AND NOT EXISTS(SELECT 1 FROM targets) AND NOT EXISTS(SELECT 1 FROM sources)
ORDER BY company_key NULLS FIRST
"""


def _reduce_batch(parts, diagnostics):
    from ptg2_address_canon import validate_company_group_assertions

    reduced = json.loads(validate_company_group_assertions(b"[" + b",".join(parts) + b"]"))
    diagnostics["group_review_count"] += len(reduced["reviews"])
    diagnostics["group_review_rows"] += reduced["counts"]["review_rows"]
    for review in reduced["reviews"]:
        code = review["code"]
        diagnostics["group_reviews_by_code"][code] = diagnostics["group_reviews_by_code"].get(code, 0) + 1


async def approved_company_group_diagnostics(connection, namespace, staging, previous_revision):
    """Check the full prospective map, reducing only newly selected active families.

    Requires the same live authorization, protected immutable SOURCE custody and
    company/control writer fence as approval. Counts and digests grant no authority.
    The caller owns the transaction; this helper never writes or commits.
    """
    diagnostics = None
    parts, identities = [], set()
    row_count, byte_count = 0, 2
    query = _DIAGNOSTICS_SQL.format(namespace=namespace, staging=staging, max_bytes=_MAX_BYTES)
    # Eight capped families bound prefetched JSON at 64 MiB without per-company fetches.
    async for cursor_row in connection.cursor(query, previous_revision, prefetch=8):
        if cursor_row["company_key"] is None:
            diagnostics = json.loads(cursor_row["diagnostics_json"]) | {
                "group_review_count": 0,
                "group_review_rows": 0,
                "group_reviews_by_code": {},
            }
            continue
        family = json.loads(cursor_row["family_json"])
        encoded = json.dumps(family, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
        family_ids = {assertion["assertion_id"] for assertion in family}
        # IDs are company-scoped; the native batch uses a global uniqueness set.
        if parts and (
            row_count + len(family) > _MAX_ROWS or byte_count + len(encoded) - 1 > _MAX_BYTES or identities & family_ids
        ):
            _reduce_batch(parts, diagnostics)
            parts, identities = [], set()
            row_count, byte_count = 0, 2
        if not 1 <= len(family) <= 128 or len(encoded) > _MAX_BYTES:
            raise ValueError("registry_company_group_assertions_invalid")
        parts.append(encoded[1:-1])
        identities.update(family_ids)
        row_count += len(family)
        byte_count += len(encoded) - 1
    if diagnostics is None:
        raise ValueError("registry_company_group_diagnostics_invalid")
    if parts:
        _reduce_batch(parts, diagnostics)
    if not any(
        diagnostics[key]
        for key in ("group_family_invalid_count", "group_target_conflict_count", "group_source_conflict_count")
    ):
        diagnostics["group_unresolved_count"] = diagnostics["group_review_rows"]
    return diagnostics
