# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Retain explicit manual-office pairs in the existing membership transaction."""

import io
from uuid import uuid4

import asyncpg

from process.network_membership_copy import COPY_COLUMNS, MembershipCopyError


class ManualLocationBindingConflict(ValueError):
    """An immutable pair already names another location identity."""


_PAIRS_SQL = """WITH pairs AS MATERIALIZED (
 SELECT member.provider_system,member.provider_id,member.location_id,
   encode(sha256(convert_to(jsonb_build_array('network_custom',member.provider_system,
     member.provider_id,member.location_id)::text,'UTF8')),'hex') AS location_key
 FROM {stage} member JOIN {locations} location USING(location_id)
 WHERE NOT location.archived AND member.provider_system IN ('manual','npi')
   AND NOT EXISTS (SELECT 1 FROM {source_bindings} source
     WHERE NOT source.archived AND source.provider_system=member.provider_system
       AND source.provider_id=member.provider_id AND source.location_id=member.location_id)
)
"""
_INSERT_SQL = """INSERT INTO {bindings}
 (provider_system,provider_id,location_id,location_key,entity_type,entity_id,archived,revision)
 SELECT provider_system,provider_id,location_id,location_key,provider_system,provider_id,false,1 FROM pairs
 ON CONFLICT(provider_system,provider_id,location_id) DO NOTHING
"""
_CONFLICT_SQL = """SELECT EXISTS (
 SELECT 1 FROM pairs pair LEFT JOIN {bindings} binding
   USING(provider_system,provider_id,location_id)
 WHERE binding.location_key IS DISTINCT FROM pair.location_key
   OR binding.entity_type IS DISTINCT FROM pair.provider_system
   OR binding.entity_id IS DISTINCT FROM pair.provider_id OR binding.archived IS DISTINCT FROM false)
"""


async def retain_manual_location_bindings(connection, copy_bytes, row_count, relations_by_name):
    """COPY one already validated batch and retain exact pairs without changing prior bindings."""
    if not connection.is_in_transaction():
        raise ValueError("registry_requires_caller_transaction")
    if not row_count:
        return
    stage_name = "registry_manual_pairs_" + uuid4().hex
    stage = f'pg_temp."{stage_name}"'
    stage_relations_by_name = {**relations_by_name, "stage": stage}
    await connection.execute(f"""CREATE TEMP TABLE {stage} (
      network_id integer NOT NULL,provider_system text NOT NULL,provider_id text NOT NULL,
      location_id uuid NOT NULL,evidence_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id)) ON COMMIT DROP""")
    with io.BytesIO(copy_bytes) as source:
        status = await connection.copy_to_table(
            stage_name, schema_name="pg_temp", columns=COPY_COLUMNS, format="binary", source=source
        )
        if status != f"COPY {row_count}" or source.tell() != len(copy_bytes):
            raise MembershipCopyError("Native membership COPY consumption differs")
    try:
        await connection.execute((_PAIRS_SQL + _INSERT_SQL).format(**stage_relations_by_name))
    except asyncpg.UniqueViolationError:
        raise ManualLocationBindingConflict("registry_manual_location_binding_conflict") from None
    if await connection.fetchval((_PAIRS_SQL + _CONFLICT_SQL).format(**stage_relations_by_name)):
        raise ManualLocationBindingConflict("registry_manual_location_binding_conflict")
    await connection.execute(f"DROP TABLE {stage}")
