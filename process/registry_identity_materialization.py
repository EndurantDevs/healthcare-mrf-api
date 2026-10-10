# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Create source-reported organization identities through consistent strong IDs."""

from __future__ import annotations

from typing import Any
from uuid import uuid4

from process.registry_source_observation_store import (
    _TRIM_CHARACTERS,
    RegistryObservationError,
    RegistryObservationLanding,
    _identifier,
    _is_exact_replay,
    _legal_name_field,
    _lock_snapshot,
    _namespace,
    _validate_contents,
    _verify_landing,
)


class RegistryMaterializationError(RegistryObservationError):
    """A reviewed identifier ledger has an invalid durable reference."""


async def _source_candidates(connection: Any, relation: str, landing: RegistryObservationLanding) -> str:
    staging = _identifier("registry_identity_source_" + uuid4().hex)
    legal_name_field = _legal_name_field(landing)
    await connection.execute(
        f"""CREATE TEMP TABLE {staging} ON COMMIT DROP AS
        SELECT snapshot_id,source_record_key,source_row_number,status,issues_json,
          observation_json->>'normalized_ein' AS ein,
          observation_json->>'normalized_naic_company' AS naic_company,
          observation_json->>'normalized_naic_group' AS group_code,
          CASE WHEN jsonb_typeof(observation_json->'raw_fields'->'{legal_name_field}')='string'
            THEN regexp_replace(btrim(translate(observation_json->'raw_fields'->>'{legal_name_field}',
              $1,repeat(' ',char_length($1)))),' +',' ','g') END AS legal_name,
          CASE WHEN jsonb_typeof(observation_json->'raw_fields'->'group_affiliation')='string'
            THEN regexp_replace(btrim(translate(observation_json->'raw_fields'->>'group_affiliation',
              $1,repeat(' ',char_length($1)))),' +',' ','g') ELSE '' END AS group_label
        FROM {relation}""",
        _TRIM_CHARACTERS,
    )
    return staging


async def _validate_binding_references(connection: Any, namespace: str, staging: str) -> None:
    invalid_references = await connection.fetchval(f"""SELECT EXISTS(
      SELECT 1 FROM {staging} s CROSS JOIN LATERAL (VALUES
        ('company','ein',s.ein),('company','naic_company',s.naic_company),('group','naic_group',s.group_code)
      ) i(kind,system,identifier) JOIN {namespace}.registry_identifier_binding b
        ON b.entity_kind=i.kind AND b.identifier_system=i.system AND b.identifier_value=i.identifier
      WHERE (b.entity_kind='company' AND NOT EXISTS(
          SELECT 1 FROM {namespace}.company_registry c WHERE c.company_id=b.entity_id))
        OR (b.entity_kind='group' AND NOT EXISTS(
          SELECT 1 FROM {namespace}.company_group_registry g WHERE g.group_id=b.entity_id AND g.group_kind='naic_group')))""")
    if invalid_references:
        raise RegistryMaterializationError("Identifier ledger references an absent or incompatible durable identity")


async def _company_candidates(connection: Any, namespace: str, staging: str) -> str:
    companies = _identifier("registry_company_candidates_" + uuid4().hex)
    await connection.execute(f"""CREATE TEMP TABLE {companies} ON COMMIT DROP AS
      WITH naic_conflicts AS (
        SELECT naic_company FROM {staging} WHERE status<>'rejected' AND ein IS NOT NULL AND naic_company IS NOT NULL
        GROUP BY naic_company HAVING COUNT(DISTINCT ein)>1),
      grouped AS (
        SELECT ein,MIN(legal_name) AS legal_name,MIN(naic_company) AS naic_company,
          (array_agg(source_record_key ORDER BY source_row_number))[1] AS evidence_key,
          COUNT(DISTINCT legal_name)=1 AND COUNT(DISTINCT naic_company)<=1
            AND NOT COALESCE(bool_or(naic_company IN (SELECT naic_company FROM naic_conflicts)),false)
            AND NOT bool_or(jsonb_path_exists(issues_json,'$[*] ? (@.code == "conflicting_company_names_for_ein" || @.code == "conflicting_naic_codes_for_ein" || @.code == "conflicting_eins_for_naic_code")')) AS consistent,
          bool_and(legal_name IS NOT NULL AND legal_name<>'' AND char_length(legal_name)<=512) AS supported_name
        FROM {staging} WHERE status<>'rejected' AND ein IS NOT NULL GROUP BY ein)
      SELECT c.*,e.entity_id AS company_id,false AS is_new,
        c.consistent AND (n.entity_id IS NULL OR n.entity_id=e.entity_id) AS compatible
      FROM grouped c LEFT JOIN {namespace}.registry_identifier_binding e
        ON e.entity_kind='company' AND e.identifier_system='ein' AND e.identifier_value=c.ein
      LEFT JOIN {namespace}.registry_identifier_binding n
        ON n.entity_kind='company' AND n.identifier_system='naic_company' AND n.identifier_value=c.naic_company""")
    await connection.execute(
        f"UPDATE {companies} SET company_id=gen_random_uuid(),is_new=true WHERE compatible AND supported_name AND company_id IS NULL"
    )
    return companies


async def _group_candidates(connection: Any, namespace: str, staging: str) -> str:
    groups = _identifier("registry_group_candidates_" + uuid4().hex)
    await connection.execute(f"""CREATE TEMP TABLE {groups} ON COMMIT DROP AS
      WITH grouped AS (
        SELECT group_code,(array_agg(source_record_key ORDER BY source_row_number))[1] AS evidence_key,
          COALESCE(array_agg(DISTINCT group_label ORDER BY group_label) FILTER(WHERE group_label<>''),ARRAY[]::text[]) AS labels,
          bool_and(char_length(group_label)<=512) AND COUNT(DISTINCT group_label) FILTER(WHERE group_label<>'')<=100 AS supported_labels
        FROM {staging} WHERE status<>'rejected' AND group_code IS NOT NULL GROUP BY group_code)
      SELECT c.*,b.entity_id AS group_id,false AS is_new,
        CASE WHEN cardinality(labels)=1 THEN labels[1] ELSE 'NAIC group '||group_code END AS display_name,
        CASE WHEN cardinality(labels)>1 THEN to_jsonb(labels) ELSE '[]'::jsonb END AS aliases
      FROM grouped c LEFT JOIN {namespace}.registry_identifier_binding b
        ON b.entity_kind='group' AND b.identifier_system='naic_group' AND b.identifier_value=c.group_code""")
    await connection.execute(
        f"UPDATE {groups} SET group_id=gen_random_uuid(),is_new=true WHERE supported_labels AND group_id IS NULL"
    )
    return groups


async def _insert_heads(connection: Any, namespace: str, companies: str, groups: str) -> tuple[int, int]:
    new_companies = await connection.fetchval(f"""WITH inserted AS (
      INSERT INTO {namespace}.company_registry(company_id,display_name,roles,aliases)
      SELECT company_id,legal_name,ARRAY['insurer']::varchar[],'[]'::jsonb FROM {companies} WHERE is_new RETURNING 1)
      SELECT COUNT(*) FROM inserted""")
    new_groups = await connection.fetchval(f"""WITH inserted AS (
      INSERT INTO {namespace}.company_group_registry(group_id,group_kind,display_name,aliases)
      SELECT group_id,'naic_group',display_name,aliases FROM {groups} WHERE is_new RETURNING 1)
      SELECT COUNT(*) FROM inserted""")
    return new_companies, new_groups


async def _insert_bindings(
    connection: Any, namespace: str, companies: str, groups: str, landing: RegistryObservationLanding
) -> int:
    return await connection.fetchval(
        f"""WITH candidates AS (
      SELECT 'company' AS kind,'ein' AS system,ein AS identifier,company_id AS entity_id,evidence_key
        FROM {companies} WHERE compatible AND company_id IS NOT NULL
      UNION ALL SELECT 'company','naic_company',naic_company,company_id,evidence_key FROM {companies}
        WHERE compatible AND company_id IS NOT NULL AND naic_company IS NOT NULL
      UNION ALL SELECT 'group','naic_group',group_code,group_id,evidence_key FROM {groups} WHERE group_id IS NOT NULL),
      inserted AS (INSERT INTO {namespace}.registry_identifier_binding
        (entity_kind,identifier_system,identifier_value,entity_id,snapshot_id,evidence_key)
        SELECT kind,system,identifier,entity_id,$1,evidence_key FROM candidates ORDER BY kind,system,identifier
        ON CONFLICT(entity_kind,identifier_system,identifier_value) DO NOTHING RETURNING 1)
      SELECT COUNT(*) FROM inserted""",
        landing.snapshot_id,
    )


async def _verify_inserted_sets(connection: Any, namespace: str, companies: str, groups: str) -> None:
    invalid_sets = await connection.fetchval(f"""SELECT EXISTS(
      SELECT 1 FROM {companies} c WHERE compatible AND company_id IS NOT NULL AND (
        NOT EXISTS(SELECT 1 FROM {namespace}.company_registry h WHERE h.company_id=c.company_id)
        OR NOT EXISTS(SELECT 1 FROM {namespace}.registry_identifier_binding b WHERE
          b.entity_kind='company' AND b.identifier_system='ein' AND b.identifier_value=c.ein AND b.entity_id=c.company_id)
        OR (naic_company IS NOT NULL AND NOT EXISTS(SELECT 1 FROM {namespace}.registry_identifier_binding b WHERE
          b.entity_kind='company' AND b.identifier_system='naic_company' AND b.identifier_value=c.naic_company AND b.entity_id=c.company_id)))
      UNION ALL SELECT 1 FROM {groups} g WHERE group_id IS NOT NULL AND (
        NOT EXISTS(SELECT 1 FROM {namespace}.company_group_registry h WHERE h.group_id=g.group_id AND h.group_kind='naic_group')
        OR NOT EXISTS(SELECT 1 FROM {namespace}.registry_identifier_binding b WHERE
          b.entity_kind='group' AND b.identifier_system='naic_group' AND b.identifier_value=g.group_code AND b.entity_id=g.group_id)))""")
    if invalid_sets:
        raise RegistryMaterializationError("Materialized identities and identifier bindings do not reconcile")


async def _candidate_totals(connection: Any, companies: str, groups: str) -> dict:
    totals = await connection.fetchrow(f"""SELECT
      (SELECT COUNT(*) FROM {companies}) AS company_candidates,
      (SELECT COUNT(*) FROM {companies} WHERE NOT COALESCE(compatible,false)) AS company_conflicts,
      (SELECT COUNT(*) FROM {companies} WHERE company_id IS NULL OR NOT COALESCE(compatible,false)) AS company_gaps,
      (SELECT COUNT(*) FROM {companies} WHERE NOT supported_name) AS unsupported_company_names,
      (SELECT COUNT(*) FROM {groups}) AS group_candidates,
      (SELECT COUNT(*) FROM {groups} WHERE group_id IS NULL) AS group_gaps,
      (SELECT COUNT(*) FROM {groups} WHERE NOT supported_labels) AS unsupported_group_labels""")
    return dict(totals)


async def materialize_registry_source_identities(
    connection: Any, landing: RegistryObservationLanding, *, control_schema: str | None = None
) -> dict:
    """Materialize reported insurer/group roles, without verifying current licensing.

    The caller owns the sealed native landing, producer authority and transaction.
    Existing editable heads, evidence bindings and custom revision controls stay
    unchanged. A later persistence step retains all raw assertions and gaps.
    """
    if type(landing) is not RegistryObservationLanding or not connection.is_in_transaction():
        raise RegistryMaterializationError(
            "Identity materialization requires a trusted landing and caller-owned transaction"
        )
    namespace = _namespace(control_schema)
    async with connection.transaction():
        relation = await _verify_landing(connection, landing)
        await _lock_snapshot(connection, landing, namespace)
        await _validate_contents(connection, landing, relation, namespace)
        if await _is_exact_replay(connection, landing, namespace, relation):
            return dict(
                source_rows=landing.expected_rows,
                companies_created=0,
                groups_created=0,
                bindings_created=0,
                replayed=True,
            )
        # ponytail: ledger-wide allocation serializes sources; partition the ledger
        # only if measured concurrent imports justify an equivalent identity guard.
        await connection.execute(f"LOCK TABLE {namespace}.registry_identifier_binding IN SHARE ROW EXCLUSIVE MODE")
        staging = await _source_candidates(connection, relation, landing)
        await _validate_binding_references(connection, namespace, staging)
        companies = await _company_candidates(connection, namespace, staging)
        groups = await _group_candidates(connection, namespace, staging)
        companies_created, groups_created = await _insert_heads(connection, namespace, companies, groups)
        bindings_created = await _insert_bindings(connection, namespace, companies, groups, landing)
        await _verify_inserted_sets(connection, namespace, companies, groups)
        totals = await _candidate_totals(connection, companies, groups)
        await connection.execute(f"DROP TABLE {staging},{companies},{groups}")
    return totals | dict(
        source_rows=landing.expected_rows,
        companies_created=companies_created,
        groups_created=groups_created,
        bindings_created=bindings_created,
        replayed=False,
    )
