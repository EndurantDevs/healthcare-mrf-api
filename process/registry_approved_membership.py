# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Validate membership references against the prospective approved map in one query."""

from process.registry_record_store import RegistryRecordConflict

_DIAGNOSTICS_SQL = """
      WITH prospective AS MATERIALIZED (
        SELECT previous.record_kind,previous.record_key,previous.record_json
        FROM {namespace}.registry_approved_record previous WHERE previous.approved_revision=$1
          AND NOT EXISTS(SELECT 1 FROM {staging} selected
            WHERE (selected.record_kind,selected.record_key)=(previous.record_kind,previous.record_key))
        UNION ALL
        SELECT history.record_kind,history.record_key,history.record_json
        FROM {staging} selected JOIN {namespace}.registry_record_history history
          ON (history.record_kind,history.record_key,history.revision)=
             (selected.record_kind,selected.record_key,selected.revision)
      ), active AS MATERIALIZED (
        SELECT * FROM prospective WHERE record_json->'archived'='false'::jsonb
      ), binding_conflicts AS (
        SELECT binding.record_json->>'provider_system',binding.record_json->>'provider_id',
          binding.record_json->>'location_id'
        FROM active binding LEFT JOIN active location ON location.record_kind='location'
          AND location.record_key=binding.record_json->>'location_id'
        WHERE binding.record_kind='site_binding'
        GROUP BY binding.record_json->>'provider_system',binding.record_json->>'provider_id',
          binding.record_json->>'location_id'
        HAVING count(*)>1 OR bool_or(location.record_key IS NOT NULL)
      ), source_bindings AS MATERIALIZED (
        SELECT binding.record_key,network.record_key IS NULL AS network_missing
        FROM active binding LEFT JOIN active network ON network.record_kind='network'
          AND network.record_key=binding.record_json->>'network_id'
        WHERE binding.record_kind='network_binding'
      ), members AS MATERIALIZED (
        SELECT head.record_key AS network_key, member.*
        FROM active head CROSS JOIN LATERAL jsonb_to_recordset(head.record_json->'memberships_json')
          AS member(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
        WHERE head.record_kind='membership'
      ), unresolved AS (
        SELECT member.*, network.record_key IS NULL AS network_missing,
          (member.provider_system='manual' AND provider.record_key IS NULL)
            OR (member.provider_system='provider_directory' AND source_binding.count<>1) AS provider_missing,
          (location.record_key IS NULL AND source_binding.count<>1)
            OR (location.record_key IS NOT NULL AND source_binding.count<>0) AS location_missing
        FROM members member
        LEFT JOIN active network ON network.record_kind='network' AND network.record_key=member.network_key
        LEFT JOIN active provider ON provider.record_kind='provider' AND provider.record_key=member.provider_id
        LEFT JOIN active location ON location.record_kind='location' AND location.record_key=member.location_id::text
        LEFT JOIN LATERAL (
          SELECT count(*) AS count FROM active binding WHERE binding.record_kind='site_binding'
            AND (binding.record_json->>'provider_system',binding.record_json->>'provider_id',binding.record_json->>'location_id')
              =(member.provider_system,member.provider_id,member.location_id::text)
        ) source_binding ON true
      )
      SELECT count(*)::bigint AS membership_rows,
        (SELECT count(*) FROM source_bindings)::bigint AS source_binding_rows,
        (SELECT count(*) FROM source_bindings WHERE network_missing)::bigint AS source_binding_unresolved_count,
        EXISTS(SELECT 1 FROM binding_conflicts) AS binding_conflict,
        count(*) FILTER(WHERE network_missing OR provider_missing OR location_missing
          OR network_id::text<>network_key)::bigint AS unresolved_count,
        count(*) FILTER(WHERE network_missing)::bigint AS unapproved_network_count,
        count(*) FILTER(WHERE provider_missing)::bigint AS unapproved_provider_count,
        count(*) FILTER(WHERE location_missing)::bigint AS unapproved_location_count
      FROM unresolved
    """


async def approved_membership_diagnostics(connection, namespace, staging, previous_revision):
    """Pending identity corrections cannot silently enter an approved membership."""
    diagnostics_by_name = dict(
        await connection.fetchrow(_DIAGNOSTICS_SQL.format(namespace=namespace, staging=staging), previous_revision)
    )
    if diagnostics_by_name.pop("binding_conflict"):
        raise RegistryRecordConflict("registry_approval_site_binding_conflict")
    return diagnostics_by_name
