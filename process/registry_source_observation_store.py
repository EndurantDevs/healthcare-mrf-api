# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain sealed native source observations with set-based identity assertions."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

from db.registry_schema import registry_schema

MAX_ROWS = 5_000
MAX_PLANFINDER_ROWS = 100_000
_FIELDS = ("snapshot_id", "source_record_key", "source_row_number", "status", "observation_json", "issues_json")
_TRIM_CHARACTERS = "\t\n\v\f\r \u0085\u00a0\u1680\u2000\u2001\u2002\u2003\u2004\u2005\u2006\u2007\u2008\u2009\u200a\u2028\u2029\u202f\u205f\u3000"
_NORMALIZED = (
    ("normalized_ein", "^[0-9]{9}$", "000000000"),
    ("normalized_naic_company", "^[0-9]{5}$", "00000"),
    ("normalized_naic_group", "^[1-9][0-9]{0,4}$", "0"),
    ("hios", "^[0-9]{5}$", "00000"),
    ("state", "^[A-Z]{2}$", ""),
)


class RegistryObservationError(ValueError):
    """Source scope, landing contents or immutable replay was rejected."""


def _identifier(identifier: str) -> str:
    if type(identifier) is not str or re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", identifier) is None:
        raise RegistryObservationError("Registry relation identifiers must be bounded native names")
    return f'"{identifier}"'


def _namespace(control_schema: str | None) -> str:
    return _identifier(control_schema if control_schema is not None else registry_schema())


@dataclass(frozen=True)
class RegistryObservationLanding:
    schema_name: str
    table_name: str
    relation_oid: int
    snapshot_id: UUID
    source_system: str
    source_id: str
    edition_id: str
    input_sha256: str
    parser_version: str
    expected_rows: int

    def __post_init__(self):
        _identifier(self.schema_name)
        _identifier(self.table_name)
        if type(self.relation_oid) is not int or not 0 < self.relation_oid <= 4_294_967_295:
            raise RegistryObservationError("Landing requires a positive native relation OID")
        if type(self.snapshot_id) is not UUID or self.snapshot_id.int == 0:
            raise RegistryObservationError("Source snapshot identity must be a nonzero UUID")
        maximum = MAX_PLANFINDER_ROWS if (self.source_system, self.source_id) == ("cms", "plan-finder") else MAX_ROWS
        if type(self.expected_rows) is not int or not 0 <= self.expected_rows <= maximum:
            raise RegistryObservationError("Source landing exceeds its row bound")
        for field, maximum in (("source_system", 64), ("source_id", 128), ("edition_id", 128), ("parser_version", 128)):
            field_value = getattr(self, field)
            if type(field_value) is not str or not field_value.strip() or len(field_value.encode()) > maximum:
                raise RegistryObservationError("Expected source metadata must be bounded nonblank text")
        if type(self.input_sha256) is not str or re.fullmatch(r"[0-9a-f]{64}", self.input_sha256) is None:
            raise RegistryObservationError("Expected source input digest is invalid")


async def _verify_landing(connection: Any, landing: RegistryObservationLanding) -> str:
    relation = await connection.fetchrow(
        "SELECT c.oid,n.nspname,c.relname,c.relkind::text,c.relpersistence::text,c.relnamespace=pg_my_temp_schema() AS owned "
        "FROM pg_class c JOIN pg_namespace n ON n.oid=c.relnamespace WHERE c.oid=$1::oid",
        landing.relation_oid,
    )
    if relation is None or (
        relation["nspname"],
        relation["relname"],
        relation["relkind"],
        relation["relpersistence"],
        relation["owned"],
    ) != (landing.schema_name, landing.table_name, "r", "t", True):
        raise RegistryObservationError("Landing is not the exact session-owned temporary relation")
    columns = await connection.fetch(
        "SELECT attname,atttypid::regtype::text AS type_name FROM pg_attribute "
        "WHERE attrelid=$1::oid AND attnum>0 AND NOT attisdropped ORDER BY attnum",
        landing.relation_oid,
    )
    if [tuple(column) for column in columns] != list(
        zip(_FIELDS, ("uuid", "character varying", "integer", "character varying", "jsonb", "jsonb"))
    ):
        raise RegistryObservationError("Landing columns do not match the native observation contract")
    return f"{_identifier(landing.schema_name)}.{_identifier(landing.table_name)}"


async def _lock_snapshot(connection: Any, landing: RegistryObservationLanding, namespace: str) -> None:
    snapshot_record = await connection.fetchrow(
        f"SELECT * FROM {namespace}.registry_source_snapshot WHERE snapshot_id=$1 FOR UPDATE", landing.snapshot_id
    )
    if snapshot_record is None or any(
        snapshot_record[field] != getattr(landing, field)
        for field in ("source_system", "source_id", "edition_id", "input_sha256", "parser_version")
    ):
        raise RegistryObservationError("Source snapshot metadata does not match the registered edition")


def _legal_name_field(landing: RegistryObservationLanding) -> str:
    return "issr_lgl_name" if (landing.source_system, landing.source_id) == ("cms", "plan-finder") else "company_name"


def _planfinder_content_checks(landing: RegistryObservationLanding, namespace: str) -> list[str]:
    """Keep exact workbook layout, coordinates and edition evidence in set validation."""
    from process.cms_planfinder_workbook_input import HEADERS, LAYOUT

    headers = "ARRAY[" + ",".join("'" + field + "'" for field in HEADERS) + "]::text[]"
    evidence = "observation_json->'source_evidence'"
    checks = [
        "source_row_number>=2",
        "observation_json->>'submission_id'=btrim(source_record_key,$2::text)",
        "observation_json->>'row_kind'='issuer_filing'",
        "observation_json->'normalized_naic_company'='null'::jsonb",
        "observation_json->'normalized_naic_group'='null'::jsonb",
        "observation_json->'group_kind'='null'::jsonb",
        f"observation_json->'raw_fields' ?& {headers}",
        f"(observation_json->'raw_fields')-{headers}='{{}}'::jsonb",
        f"{evidence}->>'component'='cms_planfinder_workbook_input'",
        f"{evidence}->'revision'='1'::jsonb",
        f"{evidence}->>'layout'='{LAYOUT}'",
        f"{evidence}->>'sheet'='ISSUER_1'",
        f"{evidence}->'headers'=to_jsonb({headers})",
        f"{evidence}->>'source_row'=source_row_number::text",
        f"{evidence}->>'workbook_sha256'='{landing.input_sha256}'",
        f"{evidence}->>'artifact_sha256'=(SELECT artifact_sha256 FROM {namespace}.registry_source_snapshot WHERE snapshot_id=$1)",
        f"{evidence}->>'batch_sha256' ~ '^[0-9a-f]{{64}}$'",
    ]
    for field in ("values", "raw_values", "cell_types", "style_ids"):
        checks.append(
            f"CASE WHEN jsonb_typeof({evidence}->'{field}')='array' "
            f"THEN jsonb_array_length({evidence}->'{field}')=19 ELSE false END"
        )
    return checks


def _valid_content_sql(landing: RegistryObservationLanding, namespace: str) -> str:
    checks = [
        "snapshot_id=$1",
        "source_row_number>0",
        "source_record_key='row:'||source_row_number::text",
        "status IN ('accepted','unresolved','rejected')",
        "jsonb_typeof(observation_json)='object'",
        "jsonb_typeof(issues_json)='array'",
        "observation_json->>'status'=status",
        "observation_json->>'source_row_number'=source_row_number::text",
        "observation_json->'issues'=issues_json",
        "jsonb_typeof(observation_json->'raw_fields')='object'",
        "jsonb_typeof(observation_json->'submission_id')='string'",
        "observation_json->>'row_kind' IN ('issuer_filing','grand_total')",
        "CASE WHEN jsonb_typeof(issues_json)='array' THEN CASE status "
        "WHEN 'accepted' THEN jsonb_array_length(issues_json)=0 "
        "WHEN 'unresolved' THEN jsonb_array_length(issues_json)>0 AND NOT jsonb_path_exists(issues_json,'$[*] ? (@.rejecting == true)') "
        "WHEN 'rejected' THEN jsonb_path_exists(issues_json,'$[*] ? (@.rejecting == true)') ELSE false END ELSE false END",
    ]
    if (landing.source_system, landing.source_id) == ("cms", "plan-finder"):
        checks.extend(_planfinder_content_checks(landing, namespace))
    else:
        checks.extend(
            (
                "jsonb_typeof(observation_json->'raw_fields'->'mr_submission_template_id')='string'",
                "observation_json->>'submission_id'=btrim(observation_json->'raw_fields'->>'mr_submission_template_id', $2::text)",
            )
        )
    for field, pattern, zero in _NORMALIZED:
        checks.append(
            f"(observation_json->'{field}'='null'::jsonb OR (jsonb_typeof(observation_json->'{field}')='string' "
            f"AND observation_json->>'{field}' ~ '{pattern}' AND observation_json->>'{field}'<>'{zero}'))"
        )
    for normalized, raw in (
        ("normalized_ein", "federal_ein"),
        ("normalized_naic_company", "naic_company_code"),
        ("normalized_naic_group", "naic_group_code"),
    ):
        checks.append(
            f"(observation_json->'{normalized}'='null'::jsonb OR (jsonb_typeof(observation_json->'raw_fields'->'{raw}')='string' "
            f"AND btrim(observation_json->'raw_fields'->>'{raw}')<>'' AND octet_length(observation_json->'raw_fields'->>'{raw}')<=128))"
        )
    return " AND ".join(f"COALESCE(({check}),false)" for check in checks)


async def _validate_contents(
    connection: Any, landing: RegistryObservationLanding, relation: str, namespace: str
) -> None:
    accounting = await connection.fetchrow(
        f"SELECT COUNT(*) AS observed,COUNT(DISTINCT source_record_key) AS distinct_keys, "
        f"COUNT(*) FILTER (WHERE NOT ({_valid_content_sql(landing, namespace)})) AS invalid, "
        f"MIN(source_row_number) AS first_row,MAX(source_row_number) AS last_row FROM {relation}",
        landing.snapshot_id,
        _TRIM_CHARACTERS,
    )
    if (
        accounting["observed"] != landing.expected_rows
        or accounting["distinct_keys"] != accounting["observed"]
        or accounting["invalid"]
    ):
        raise RegistryObservationError("Landing content, row identities or source accounting is invalid")
    if (landing.source_system, landing.source_id) == ("cms", "plan-finder") and (
        landing.expected_rows == 0
        or accounting["first_row"] != 2
        or accounting["last_row"] != landing.expected_rows + 1
    ):
        raise RegistryObservationError("Plan Finder edition rows must be complete and contiguous")


async def _is_exact_replay(connection: Any, landing: RegistryObservationLanding, namespace: str, relation: str) -> bool:
    comparison = await connection.fetchrow(
        f"""WITH existing AS (SELECT * FROM {namespace}.registry_source_observation WHERE snapshot_id=$1)
        SELECT (SELECT COUNT(*) FROM existing) AS retained,
        EXISTS(SELECT 1 FROM existing e FULL JOIN {relation} l USING(snapshot_id,source_record_key)
          WHERE ROW(e.source_row_number,e.status,e.observation_json,e.issues_json)
          IS DISTINCT FROM ROW(l.source_row_number,l.status,l.observation_json,l.issues_json)) AS different""",
        landing.snapshot_id,
    )
    if comparison["retained"] and (comparison["retained"] != landing.expected_rows or comparison["different"]):
        raise RegistryObservationError("Previously retained source edition differs from this landing")
    return comparison["retained"] > 0 or landing.expected_rows == 0


def _company_resolution_sql(namespace: str, relation: str, landing: RegistryObservationLanding) -> str:
    legal_name_field = _legal_name_field(landing)
    return f"""WITH decoded AS (
      SELECT l.*,observation_json->>'hios' AS hios,observation_json->>'state' AS state,
        observation_json->>'normalized_ein' AS ein,observation_json->>'normalized_naic_company' AS naic_company,
        NULLIF(regexp_replace(btrim(translate(observation_json->'raw_fields'->>'{legal_name_field}',
          $1,repeat(' ',char_length($1)))),' +',' ','g'),'') AS legal_name,
        jsonb_path_exists(issues_json,'$[*] ? (@.code == "conflicting_company_names_for_ein" || @.code == "conflicting_naic_codes_for_ein" || @.code == "conflicting_eins_for_naic_code")') AS declared_company_conflict,
        jsonb_path_exists(issues_json,'$[*] ? (@.code == "conflicting_issuer_identity")') AS declared_issuer_conflict
      FROM {relation} l),
    bound AS (
      SELECT d.*,e.entity_id AS ein_company_id,n.entity_id AS naic_binding_id,g.entity_id AS group_id
      FROM decoded d LEFT JOIN {namespace}.registry_identifier_binding e
        ON e.entity_kind='company' AND e.identifier_system='ein' AND e.identifier_value=d.ein
      LEFT JOIN {namespace}.registry_identifier_binding n
        ON n.entity_kind='company' AND n.identifier_system='naic_company' AND n.identifier_value=d.naic_company
      LEFT JOIN {namespace}.registry_identifier_binding g
        ON g.entity_kind='group' AND g.identifier_system='naic_group'
        AND g.identifier_value=d.observation_json->>'normalized_naic_group'),
    naic_conflicts AS (
      SELECT naic_company FROM bound WHERE naic_company IS NOT NULL
      GROUP BY naic_company HAVING COUNT(DISTINCT ein)>1 OR bool_or(declared_company_conflict)),
    ein_conflicts AS (
      SELECT ein FROM bound WHERE ein IS NOT NULL GROUP BY ein
      HAVING COUNT(DISTINCT legal_name)>1 OR COUNT(DISTINCT naic_company)>1
        OR bool_or(declared_company_conflict) OR bool_or(naic_company IN (SELECT naic_company FROM naic_conflicts))
        OR bool_or(ein_company_id<>naic_binding_id)),
    affected_naic AS (
      SELECT naic_company FROM naic_conflicts UNION SELECT naic_company FROM bound
        WHERE naic_company IS NOT NULL AND (ein IN (SELECT ein FROM ein_conflicts) OR declared_company_conflict)),
    flagged AS (
      SELECT b.*,COALESCE(ein IN (SELECT ein FROM ein_conflicts),false)
        OR COALESCE(naic_company IN (SELECT naic_company FROM affected_naic),false) AS company_conflicting FROM bound b)
    SELECT f.*,CASE WHEN status<>'rejected' AND NOT company_conflicting THEN ein_company_id END AS company_id,
      CASE WHEN status<>'rejected' AND NOT company_conflicting AND ein_company_id=naic_binding_id
        THEN ein_company_id END AS naic_company_id FROM flagged f"""


async def _resolved_landing(connection: Any, namespace: str, relation: str, landing: RegistryObservationLanding) -> str:
    staging = _identifier("registry_resolution_" + uuid4().hex)
    await connection.execute(
        f"CREATE TEMP TABLE {staging} ON COMMIT DROP AS " + _company_resolution_sql(namespace, relation, landing),
        _TRIM_CHARACTERS,
    )
    invalid_bindings = await connection.fetchval(f"""SELECT EXISTS(
      SELECT 1 FROM {staging} s WHERE
        (ein_company_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM {namespace}.company_registry c WHERE c.company_id=s.ein_company_id))
        OR (naic_binding_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM {namespace}.company_registry c WHERE c.company_id=s.naic_binding_id))
        OR (group_id IS NOT NULL AND NOT EXISTS(SELECT 1 FROM {namespace}.company_group_registry g
          WHERE g.group_id=s.group_id AND g.group_kind='naic_group')))""")
    if invalid_bindings:
        raise RegistryObservationError(
            "Reviewed source binding points to a missing durable or incompatible organization"
        )
    return staging


async def _persist_identifiers(connection: Any, namespace: str, staging: str) -> None:
    await connection.execute(f"""INSERT INTO {namespace}.registry_identifier_observation
      (snapshot_id,source_record_key,identifier_system,identifier_value,entity_kind,raw_value,entity_id,resolution_status)
      SELECT s.snapshot_id,s.source_record_key,i.system,i.identifier,i.kind,i.raw_value,
        CASE WHEN s.status<>'rejected' THEN i.entity_id END,
        CASE WHEN s.status='rejected' OR (i.kind='company' AND s.company_conflicting) THEN 'conflicting'
          WHEN i.entity_id IS NOT NULL THEN 'resolved' ELSE 'unresolved' END
      FROM {staging} s CROSS JOIN LATERAL (VALUES
        ('ein',s.observation_json->>'normalized_ein','company',s.observation_json->'raw_fields'->>'federal_ein',s.company_id),
        ('naic_company',s.observation_json->>'normalized_naic_company','company',s.observation_json->'raw_fields'->>'naic_company_code',s.naic_company_id),
        ('naic_group',s.observation_json->>'normalized_naic_group','group',s.observation_json->'raw_fields'->>'naic_group_code',s.group_id)
      ) i(system,identifier,kind,raw_value,entity_id) WHERE i.identifier IS NOT NULL""")


async def _persist_relationships(
    connection: Any, namespace: str, staging: str, landing: RegistryObservationLanding
) -> None:
    # Plan Finder discovers valid issuer fields independently of company/EIN validity.
    issuer_eligible = (
        "true" if (landing.source_system, landing.source_id) == ("cms", "plan-finder") else "status<>'rejected'"
    )
    await connection.execute(f"""INSERT INTO {namespace}.hios_issuer_registry(hios_issuer_id,business_state)
      SELECT hios,MIN(state) FROM {staging} WHERE {issuer_eligible} AND hios IS NOT NULL AND state IS NOT NULL
      GROUP BY hios HAVING COUNT(DISTINCT state)=1 ORDER BY hios ON CONFLICT(hios_issuer_id) DO NOTHING""")
    await connection.execute(f"""WITH state_counts AS (
      SELECT hios,COUNT(DISTINCT state) AS state_count,COUNT(DISTINCT ein) AS company_identity_count,
        bool_or(declared_issuer_conflict) AS declared_conflict FROM {staging}
      WHERE {issuer_eligible} AND hios IS NOT NULL AND state IS NOT NULL GROUP BY hios)
      INSERT INTO {namespace}.registry_issuer_company_assertion
        (snapshot_id,source_record_key,hios_issuer_id,state,company_id,resolution_status)
      SELECT s.snapshot_id,s.source_record_key,s.hios,s.state,
        CASE WHEN sc.state_count=1 AND sc.company_identity_count<=1 AND h.business_state=s.state
          AND NOT sc.declared_conflict THEN s.company_id END,
        CASE WHEN sc.state_count<>1 OR h.business_state IS DISTINCT FROM s.state OR s.company_conflicting
          OR sc.company_identity_count>1 OR sc.declared_conflict THEN 'conflicting'
          WHEN s.company_id IS NULL THEN 'unresolved' ELSE 'resolved' END
      FROM {staging} s JOIN state_counts sc ON sc.hios=s.hios
      LEFT JOIN {namespace}.hios_issuer_registry h ON h.hios_issuer_id=s.hios
      WHERE {issuer_eligible.replace("status", "s.status")} AND s.hios IS NOT NULL AND s.state IS NOT NULL""")
    await connection.execute(f"""INSERT INTO {namespace}.registry_company_group_assertion
      (snapshot_id,source_record_key,company_id,group_id,relationship_kind,resolution_status)
      SELECT snapshot_id,source_record_key,company_id,group_id,'reported_affiliation',
        CASE WHEN group_id IS NULL THEN 'unresolved' ELSE 'resolved' END
      FROM {staging} WHERE status<>'rejected' AND company_id IS NOT NULL
        AND observation_json->'raw_fields' ? 'naic_group_code'""")


async def _accounting(connection: Any, namespace: str, snapshot_id: UUID, is_replay: bool) -> dict:
    totals = await connection.fetchrow(
        f"""SELECT COUNT(*) AS observations,
      COUNT(*) FILTER(WHERE status='accepted') AS accepted,COUNT(*) FILTER(WHERE status='unresolved') AS unresolved,
      COUNT(*) FILTER(WHERE status='rejected') AS rejected,
      (SELECT COUNT(*) FROM {namespace}.registry_identifier_observation WHERE snapshot_id=$1) AS identifiers,
      (SELECT COUNT(*) FROM {namespace}.registry_identifier_observation WHERE snapshot_id=$1 AND resolution_status='resolved') AS resolved_identifiers,
      (SELECT COUNT(*) FROM {namespace}.registry_issuer_company_assertion WHERE snapshot_id=$1) AS issuer_assertions,
      (SELECT COUNT(*) FROM {namespace}.registry_issuer_company_assertion WHERE snapshot_id=$1 AND resolution_status='resolved') AS resolved_issuers,
      (SELECT COUNT(*) FROM {namespace}.registry_issuer_company_assertion WHERE snapshot_id=$1 AND resolution_status='conflicting') AS conflicting_issuers,
      (SELECT COUNT(*) FROM {namespace}.registry_company_group_assertion WHERE snapshot_id=$1) AS group_assertions,
      (SELECT COUNT(*) FROM {namespace}.registry_company_group_assertion WHERE snapshot_id=$1 AND resolution_status='resolved') AS resolved_groups
      FROM {namespace}.registry_source_observation WHERE snapshot_id=$1""",
        snapshot_id,
    )
    return dict(totals) | {"replayed": is_replay}


async def persist_registry_source_observations(
    connection: Any, landing: RegistryObservationLanding, *, control_schema: str | None = None
) -> dict:
    """Persist a sealed temporary landing in the caller's transaction.

    The admission path owns loading, closing writes and authenticating the
    producer. This trusted DTO is not an authority token for external callers.
    Savepoint failure retains earlier caller work; successful results do not commit.
    """
    if type(landing) is not RegistryObservationLanding or not connection.is_in_transaction():
        raise RegistryObservationError("Source persistence requires a trusted landing and caller-owned transaction")
    namespace = _namespace(control_schema)
    async with connection.transaction():
        relation = await _verify_landing(connection, landing)
        await _lock_snapshot(connection, landing, namespace)
        await _validate_contents(connection, landing, relation, namespace)
        if await _is_exact_replay(connection, landing, namespace, relation):
            return await _accounting(connection, namespace, landing.snapshot_id, True)
        staging = await _resolved_landing(connection, namespace, relation, landing)
        await connection.execute(f"INSERT INTO {namespace}.registry_source_observation SELECT * FROM {relation}")
        await _persist_identifiers(connection, namespace, staging)
        await _persist_relationships(connection, namespace, staging, landing)
        await connection.execute(f"DROP TABLE {staging}")
        return await _accounting(connection, namespace, landing.snapshot_id, False)


async def read_registry_issuer_evidence(
    connection: Any, hios_issuer_id: str | int, *, limit: int = 100, control_schema: str | None = None
) -> tuple[dict, ...]:
    """Return bounded historical evidence, with additive legacy integer issuer IDs."""
    if type(hios_issuer_id) is int and 0 < hios_issuer_id <= 99_999:
        hios_issuer_id = f"{hios_issuer_id:05d}"
    if (
        type(hios_issuer_id) is not str
        or re.fullmatch(r"[0-9]{5}", hios_issuer_id) is None
        or hios_issuer_id == "00000"
    ):
        raise RegistryObservationError("Issuer identity must be canonical HIOS text or a positive legacy integer")
    if type(limit) is not int or not 1 <= limit <= MAX_ROWS:
        raise RegistryObservationError("Issuer evidence read limit is invalid")
    namespace = _namespace(control_schema)
    evidence_rows = await connection.fetch(
        f"""SELECT a.*,a.hios_issuer_id::integer AS issuer_id,
      s.source_system,s.source_id,s.edition_id,s.reporting_year,s.published_at,s.retrieved_at,
      s.input_sha256,s.parser_version FROM {namespace}.registry_issuer_company_assertion a
      JOIN {namespace}.registry_source_snapshot s USING(snapshot_id) WHERE a.hios_issuer_id=$1
      ORDER BY s.reporting_year DESC NULLS LAST,s.snapshot_id,a.source_record_key LIMIT $2""",
        hios_issuer_id,
        limit,
    )
    return tuple(dict(evidence) for evidence in evidence_rows)
