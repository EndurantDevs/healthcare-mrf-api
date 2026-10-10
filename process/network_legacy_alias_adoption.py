"""Adopt scoped legacy UUID aliases using durable bulk integer allocation."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from uuid import UUID, uuid4

from sqlalchemy import text

from process.network_registry_identity import allocate_network_ids

MAX_ADOPTION_ROWS = 5000


class LegacyNetworkAdoptionError(ValueError):
    """The complete adoption batch was rejected without changing its bindings."""


@dataclass(frozen=True)
class LegacyNetworkAdoptionRow:
    source_system: str
    source_id: str
    scope_key: str
    evidence_id: str
    legacy_uuid: str
    reviewed_network_id: int | None = None

    def __post_init__(self):
        for field_name, maximum in (
            ("source_system", 64),
            ("source_id", 128),
            ("scope_key", 512),
            ("evidence_id", 512),
        ):
            field_value = getattr(self, field_name)
            if (
                type(field_value) is not str
                or not field_value
                or len(field_value.encode()) > maximum
                or field_value.strip() != field_value
                or any(ord(character) < 32 or ord(character) == 127 for character in field_value)
            ):
                raise LegacyNetworkAdoptionError("Legacy alias scope and evidence fields must be nonempty bounded text")
        try:
            parsed_uuid = UUID(self.legacy_uuid) if type(self.legacy_uuid) is str else None
        except ValueError:
            parsed_uuid = None
        if parsed_uuid is None or parsed_uuid.int == 0 or str(parsed_uuid) != self.legacy_uuid:
            raise LegacyNetworkAdoptionError("Legacy alias UUID must be canonical and nonzero")
        if self.reviewed_network_id is not None and (
            type(self.reviewed_network_id) is not int or not 0 < self.reviewed_network_id <= 2147483647
        ):
            raise LegacyNetworkAdoptionError("Reviewed network ID must be a positive int32")


@dataclass(frozen=True)
class AdoptedNetworkAlias:
    source_system: str
    source_id: str
    scope_key: str
    legacy_uuid: str
    network_id: int


def _stage_payload(adoption_rows):
    if not isinstance(adoption_rows, (list, tuple)) or len(adoption_rows) > MAX_ADOPTION_ROWS:
        raise LegacyNetworkAdoptionError("Legacy alias adoption batch exceeds row limit")
    payload_rows = []
    for row_ordinal, adoption_row in enumerate(adoption_rows):
        if type(adoption_row) is not LegacyNetworkAdoptionRow:
            raise LegacyNetworkAdoptionError("Legacy adoption requires validated rows")
        payload_rows.append(
            {
                "row_ordinal": row_ordinal,
                "source_system": adoption_row.source_system,
                "source_id": adoption_row.source_id,
                "scope_key": adoption_row.scope_key,
                "evidence_id": adoption_row.evidence_id,
                "legacy_uuid": adoption_row.legacy_uuid,
                "reviewed_network_id": adoption_row.reviewed_network_id,
            }
        )
    return json.dumps(payload_rows, ensure_ascii=False, separators=(",", ":"))


_ALIAS_MATCH_SQL = (
    "existing.source_system=staged.source_system AND existing.source_id=staged.source_id "
    "AND existing.alias_type='legacy_fhir_uuid' AND existing.alias_value=staged.legacy_uuid::text "
    "AND existing.scope_key=staged.scope_key"
)


async def _prepare_adoption_stage(session, stage_relation, adoption_payload):
    await session.execute(
        text(f"""CREATE TEMP TABLE {stage_relation} (
        row_ordinal integer NOT NULL, source_system text NOT NULL, source_id text NOT NULL,
        scope_key text NOT NULL, evidence_id text NOT NULL, legacy_uuid uuid NOT NULL,
        reviewed_network_id integer, resolved_network_id integer
    ) ON COMMIT DROP""")
    )
    await session.execute(
        text(f"""INSERT INTO {stage_relation}
        (row_ordinal,source_system,source_id,scope_key,evidence_id,legacy_uuid,reviewed_network_id)
        SELECT row_ordinal,source_system,source_id,scope_key,evidence_id,legacy_uuid,reviewed_network_id
        FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS batch(
            row_ordinal integer,source_system text,source_id text,scope_key text,
            evidence_id text,legacy_uuid uuid,reviewed_network_id integer)
    """),
        {"payload": adoption_payload},
    )


async def _check_reviewed_aliases(session, stage_relation, aliases_relation, identity_relation):
    has_invalid_review = await session.scalar(
        text(f"""SELECT EXISTS (
        SELECT 1 FROM {stage_relation}
        GROUP BY source_system,source_id,legacy_uuid,scope_key
        HAVING count(DISTINCT reviewed_network_id)>1
    ) OR EXISTS (
        SELECT 1 FROM {stage_relation} staged
        LEFT JOIN {identity_relation} identity ON identity.network_id=staged.reviewed_network_id
        WHERE staged.reviewed_network_id IS NOT NULL AND identity.network_id IS NULL
    ) OR EXISTS (
        SELECT 1 FROM {stage_relation} staged JOIN {aliases_relation} existing ON {_ALIAS_MATCH_SQL}
        WHERE staged.reviewed_network_id IS NOT NULL AND staged.reviewed_network_id<>existing.network_id
    )""")
    )
    if has_invalid_review:
        raise LegacyNetworkAdoptionError("Reviewed legacy alias binding conflicts or refers to an unknown network")


async def _resolve_existing_aliases(session, stage_relation, aliases_relation):
    await session.execute(
        text(f"""UPDATE {stage_relation} staged
        SET resolved_network_id=existing.network_id
        FROM {aliases_relation} existing WHERE {_ALIAS_MATCH_SQL}
    """)
    )
    await session.execute(
        text(f"""UPDATE {stage_relation} staged
        SET resolved_network_id=reviewed.network_id
        FROM (SELECT source_system,source_id,legacy_uuid,scope_key,max(reviewed_network_id) AS network_id
              FROM {stage_relation} GROUP BY source_system,source_id,legacy_uuid,scope_key) reviewed
        WHERE staged.resolved_network_id IS NULL AND reviewed.network_id IS NOT NULL
          AND staged.source_system=reviewed.source_system AND staged.source_id=reviewed.source_id
          AND staged.legacy_uuid=reviewed.legacy_uuid AND staged.scope_key=reviewed.scope_key
    """)
    )


async def _allocate_unresolved_aliases(session, stage_relation, schema):
    unresolved_uuids = (
        (
            await session.execute(
                text(f"""SELECT DISTINCT legacy_uuid FROM {stage_relation}
        WHERE resolved_network_id IS NULL ORDER BY legacy_uuid""")
            )
        )
        .scalars()
        .all()
    )
    if unresolved_uuids:
        allocated_by_uuid = await allocate_network_ids(session, unresolved_uuids, schema=schema)
        allocation_payload = json.dumps(
            [
                {"legacy_uuid": str(legacy_uuid), "network_id": network_id}
                for legacy_uuid, network_id in allocated_by_uuid.items()
            ],
            separators=(",", ":"),
        )
        await session.execute(
            text(f"""UPDATE {stage_relation} staged SET resolved_network_id=allocated.network_id
            FROM jsonb_to_recordset(CAST(:payload AS jsonb)) AS allocated(legacy_uuid uuid,network_id integer)
            WHERE staged.resolved_network_id IS NULL AND staged.legacy_uuid=allocated.legacy_uuid
        """),
            {"payload": allocation_payload},
        )


async def _persist_adoption_aliases(session, stage_relation, aliases_relation, identity_relation, expected_rows):
    await session.execute(
        text(f"""INSERT INTO {aliases_relation}
        (source_system,source_id,alias_type,alias_value,scope_key,network_id,evidence_id)
        SELECT DISTINCT ON (source_system,source_id,legacy_uuid,scope_key)
            source_system,source_id,'legacy_fhir_uuid',legacy_uuid::text,scope_key,resolved_network_id,evidence_id
        FROM {stage_relation} ORDER BY source_system,source_id,legacy_uuid,scope_key,evidence_id
        ON CONFLICT (source_system,source_id,alias_type,alias_value,scope_key) DO NOTHING
    """)
    )
    invalid_binding = await session.scalar(
        text(f"""SELECT EXISTS (
        SELECT 1 FROM {stage_relation} staged LEFT JOIN {aliases_relation} existing ON {_ALIAS_MATCH_SQL}
        LEFT JOIN {identity_relation} identity ON identity.network_id=existing.network_id
        WHERE existing.network_id IS DISTINCT FROM staged.resolved_network_id OR identity.network_id IS NULL
    )""")
    )
    if invalid_binding:
        raise LegacyNetworkAdoptionError("Legacy alias binding changed during concurrent adoption")
    resolved_rows = (
        await session.execute(
            text(f"""SELECT staged.source_system,staged.source_id,
        staged.scope_key,staged.legacy_uuid::text,existing.network_id
        FROM {stage_relation} staged JOIN {aliases_relation} existing ON {_ALIAS_MATCH_SQL}
        ORDER BY staged.row_ordinal
    """)
        )
    ).all()
    if len(resolved_rows) != expected_rows:
        raise LegacyNetworkAdoptionError("Legacy alias adoption did not account for the complete batch")
    await session.execute(text(f"DROP TABLE {stage_relation}"))
    return tuple(AdoptedNetworkAlias(*resolved_binding) for resolved_binding in resolved_rows)


async def adopt_legacy_network_aliases(session, adoption_rows, *, schema):
    """Resolve the whole batch in a savepoint under the caller's transaction.

    Reviewed disagreements reject the batch. Legacy UUIDs are continuity keys;
    names, checksums and numeric source IDs never derive canonical integer IDs.
    """
    adoption_payload = _stage_payload(adoption_rows)
    if type(schema) is not str or re.fullmatch(r"[a-z_][a-z0-9_]{0,62}", schema) is None:
        raise LegacyNetworkAdoptionError("Registry schema identifier is invalid")
    if not session.in_transaction():
        raise LegacyNetworkAdoptionError("Legacy adoption requires a caller-owned transaction")
    if not adoption_rows:
        return ()
    stage_relation = f'pg_temp."network_alias_adoption_{uuid4().hex}"'
    aliases_relation = f'"{schema}".network_registry_alias'
    identity_relation = f'"{schema}".network_registry_identity'
    async with session.begin_nested():
        await _prepare_adoption_stage(session, stage_relation, adoption_payload)
        await _check_reviewed_aliases(session, stage_relation, aliases_relation, identity_relation)
        await _resolve_existing_aliases(session, stage_relation, aliases_relation)
        await _allocate_unresolved_aliases(session, stage_relation, schema)
        adopted_aliases = await _persist_adoption_aliases(
            session, stage_relation, aliases_relation, identity_relation, len(adoption_rows)
        )
    return adopted_aliases
