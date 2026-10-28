# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Set-based enduring network allocation from exact source Organization identities."""

import json
import re
from dataclasses import dataclass
from uuid import UUID, uuid4, uuid5

from db.plan_release_storage import storage_identifier

_ALLOCATION_DOMAIN = UUID("f2e18149-9c63-54e9-997d-cef910be6311")
ALIAS_SCOPE = "fhir-catalog/v1"
MAX_ROWS = 1000
MAX_BYTES = 8 * 1024 * 1024
_IDENTITY_COLUMNS = (
    "ordinal",
    "organization_id",
    "endpoint_id",
    "source_aliases",
    "evidence_id",
    "legacy_uuid",
    "legacy_scope",
    "allocation_key",
)


@dataclass(frozen=True)
class FHIRNetworkIdentityInput:
    organization_id: str
    endpoint_id: str
    source_aliases: tuple[str, ...]
    evidence_id: str
    legacy_uuid: str | None = None
    legacy_scope: str | None = None


def _validate_identity_source_aliases(source_aliases):
    """Require bounded, unique, sorted source aliases before preparing identity rows."""
    if type(source_aliases) is not tuple or not 1 <= len(source_aliases) <= 100:
        raise ValueError("registry_fhir_catalog_identity_aliases_invalid")
    if tuple(sorted(set(source_aliases))) != source_aliases or any(
        type(identifier) is not str
        or not identifier
        or identifier.strip() != identifier
        or len(identifier.encode()) > 128
        for identifier in source_aliases
    ):
        raise ValueError("registry_fhir_catalog_identity_aliases_invalid")


def _prepared(identity_inputs):
    """Validate identity inputs and prepare their bounded COPY records without database writes."""
    if type(identity_inputs) is not tuple or not 0 < len(identity_inputs) <= MAX_ROWS:
        raise ValueError("registry_fhir_catalog_identity_bounds")
    identity_rows = []
    for ordinal, network_identity in enumerate(identity_inputs):
        if (
            type(network_identity) is not FHIRNetworkIdentityInput
            or type(network_identity.organization_id) is not str
            or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", network_identity.organization_id) is None
        ):
            raise ValueError("registry_fhir_catalog_identity_invalid")
        for identifier, maximum in ((network_identity.endpoint_id, 64), (network_identity.evidence_id, 512)):
            if (
                type(identifier) is not str
                or not identifier
                or identifier.strip() != identifier
                or len(identifier.encode()) > maximum
            ):
                raise ValueError("registry_fhir_catalog_identity_invalid")
        _validate_identity_source_aliases(network_identity.source_aliases)
        legacy = None if network_identity.legacy_uuid is None else UUID(network_identity.legacy_uuid)
        if legacy is not None and (not legacy.int or str(legacy) != network_identity.legacy_uuid):
            raise ValueError("registry_fhir_catalog_identity_legacy_invalid")
        if (
            (legacy is None) != (network_identity.legacy_scope is None)
            or network_identity.legacy_scope is not None
            and (
                type(network_identity.legacy_scope) is not str
                or not network_identity.legacy_scope
                or len(network_identity.legacy_scope.encode()) > 512
            )
        ):
            raise ValueError("registry_fhir_catalog_identity_legacy_invalid")
        allocation = uuid5(
            _ALLOCATION_DOMAIN,
            json.dumps([network_identity.endpoint_id, network_identity.organization_id], separators=(",", ":")),
        )
        identity_rows.append(
            (
                ordinal,
                network_identity.organization_id,
                network_identity.endpoint_id,
                network_identity.source_aliases,
                network_identity.evidence_id,
                legacy,
                network_identity.legacy_scope,
                allocation,
            )
        )
    if len(json.dumps(identity_rows, default=str).encode()) > MAX_BYTES:
        raise ValueError("registry_fhir_catalog_identity_bounds")
    return identity_rows


_RESOLVE_SQL = """WITH candidates AS (
  SELECT s.ordinal,a.network_id FROM {stage} s JOIN {control}.network_registry_alias a
    ON a.source_system='fhir' AND a.source_id=ANY(s.source_aliases)
      AND ((a.alias_type='fhir_organization' AND a.alias_value=s.organization_id AND a.scope_key=$2)
        OR (a.alias_type='legacy_fhir_uuid' AND a.alias_value=s.legacy_uuid::text AND a.scope_key=s.legacy_scope))
  UNION
  SELECT s.ordinal,(b.record_json->>'network_id')::integer FROM {stage} s
    JOIN {control}.registry_approved_record b ON b.approved_revision=$1 AND b.record_kind='network_binding'
    AND b.record_json->>'source_system'='fhir' AND b.record_json->>'source_id'=ANY(s.source_aliases)
    AND b.record_json->>'source_key'=s.organization_id AND b.record_json->'archived'='false'::jsonb
  UNION
  SELECT s.ordinal,i.network_id FROM {stage} s JOIN {control}.network_registry_identity i USING(allocation_key)
), resolved AS (
  SELECT ordinal,min(network_id) AS network_id,count(DISTINCT network_id)>1 AS conflict
  FROM candidates GROUP BY ordinal
) UPDATE {stage} s SET network_id=r.network_id,conflict=r.conflict FROM resolved r WHERE r.ordinal=s.ordinal"""


async def allocate_fhir_catalog_network_ids(connection, rows, *, approved_revision, control_schema):
    """COPY once, resolve aliases together, allocate only missing keys, append exact aliases."""
    if not connection.is_in_transaction() or type(approved_revision) is not int or approved_revision < 0:
        raise ValueError("registry_fhir_catalog_identity_transaction_required")
    identity_rows = _prepared(rows)
    control = storage_identifier(control_schema)
    name = "fhir_catalog_identity_" + uuid4().hex
    stage = "pg_temp." + storage_identifier(name)
    await connection.execute(f"""CREATE TEMP TABLE {stage}(
      ordinal integer PRIMARY KEY,organization_id text NOT NULL,endpoint_id text NOT NULL,
      source_aliases text[] NOT NULL,evidence_id text NOT NULL,legacy_uuid uuid,legacy_scope text,allocation_key uuid NOT NULL,
      network_id integer,conflict boolean NOT NULL DEFAULT false) ON COMMIT DROP""")
    await connection.copy_records_to_table(
        name,
        schema_name="pg_temp",
        records=identity_rows,
        columns=_IDENTITY_COLUMNS,
    )
    await connection.execute(_RESOLVE_SQL.format(stage=stage, control=control), approved_revision, ALIAS_SCOPE)
    created = await _allocate_missing(connection, stage, control)
    await _append_aliases(connection, stage, control)
    resolved_identities = await connection.fetch(
        f"SELECT organization_id,network_id,conflict FROM {stage} ORDER BY ordinal"
    )
    await connection.execute(f"DROP TABLE {stage}")
    return {
        "created_networks": created,
        "rows": tuple(dict(resolved_identity) for resolved_identity in resolved_identities),
    }


async def _allocate_missing(connection, stage, control):
    created = await connection.fetchval(f"""WITH inserted AS (
      INSERT INTO {control}.network_registry_identity(allocation_key)
      SELECT DISTINCT allocation_key FROM {stage} WHERE network_id IS NULL AND NOT conflict
      ON CONFLICT(allocation_key) DO NOTHING RETURNING network_id)
      SELECT count(*) FROM inserted""")
    await connection.execute(f"""UPDATE {stage} s SET network_id=i.network_id
      FROM {control}.network_registry_identity i WHERE s.network_id IS NULL AND NOT s.conflict
      AND i.allocation_key=s.allocation_key""")
    return created


async def _append_aliases(connection, stage, control):
    await connection.execute(
        f"""INSERT INTO {control}.network_registry_alias
      (source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id)
      SELECT DISTINCT 'fhir',alias,'fhir_organization',s.organization_id,$1,s.network_id,s.evidence_id
      FROM {stage} s CROSS JOIN LATERAL unnest(s.source_aliases) alias
      WHERE s.network_id IS NOT NULL AND NOT s.conflict
      ON CONFLICT(source_system,source_id,alias_type,alias_value,scope_key) DO NOTHING""",
        ALIAS_SCOPE,
    )
    invalid = await connection.fetchval(
        f"""SELECT EXISTS(SELECT FROM {stage} s
      CROSS JOIN LATERAL unnest(s.source_aliases) alias_name
      LEFT JOIN {control}.network_registry_alias a ON a.source_system='fhir' AND a.source_id=alias_name
        AND a.alias_type='fhir_organization' AND a.alias_value=s.organization_id AND a.scope_key=$1
      WHERE NOT s.conflict AND a.network_id IS DISTINCT FROM s.network_id)""",
        ALIAS_SCOPE,
    )
    if invalid:
        raise ValueError("registry_fhir_catalog_identity_conflict")
