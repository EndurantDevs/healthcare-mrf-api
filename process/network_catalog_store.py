# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seed missing named network drafts from exact, already adopted source aliases."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID, uuid4

from process.network_address_projection import _identifier
from process.network_legacy_alias_adoption import MAX_ADOPTION_ROWS, LegacyNetworkAdoptionRow
from process.registry_management_permissions import (
    assume_registry_source_owner,
    verify_registry_management_permissions,
)
from process.registry_record_store import _bounded_text, _record_identity, _validated_actor, _validated_fields

MAX_CATALOG_BYTES = 8 * 1024 * 1024


class NetworkCatalogSeedError(ValueError):
    """Reject the complete catalog batch without changing existing drafts."""


@dataclass(frozen=True)
class NetworkCatalogDraftRow:
    alias: LegacyNetworkAdoptionRow
    network_id: int
    display_name: str
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class NetworkCatalogSourceContext:
    source_system: str
    source_id: str
    alias_scope: str
    reason: str
    idempotency_key: str


def _json(document):
    return json.dumps(document, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _prepare_rows(draft_rows, source_system, source_id, alias_scope):
    if type(draft_rows) not in {list, tuple} or not 0 < len(draft_rows) <= MAX_ADOPTION_ROWS:
        raise NetworkCatalogSeedError("Network catalog batch exceeds row limit")
    prepared_rows = []
    for row in draft_rows:
        if type(row) is not NetworkCatalogDraftRow or type(row.alias) is not LegacyNetworkAdoptionRow:
            raise NetworkCatalogSeedError("Network catalog requires validated alias rows")
        alias = row.alias
        if (alias.source_system, alias.source_id, alias.scope_key) != (source_system, source_id, alias_scope):
            raise NetworkCatalogSeedError("Network catalog source context differs from its alias")
        _record_identity("network", row.network_id)
        if alias.reviewed_network_id is not None and alias.reviewed_network_id != row.network_id:
            raise NetworkCatalogSeedError("Network catalog differs from the reviewed integer binding")
        if type(row.aliases) not in {tuple, list}:
            raise NetworkCatalogSeedError("Network catalog aliases must be a bounded sequence")
        fields = _validated_fields("network", {"display_name": row.display_name, "aliases": list(row.aliases)})
        prepared_rows.append(
            {"legacy_uuid": alias.legacy_uuid, "network_id": row.network_id, "evidence_id": alias.evidence_id, **fields}
        )
    return sorted(prepared_rows, key=_json)


async def _stage_rows(connection, draft_rows):
    stage_name = "network_catalog_seed_" + uuid4().hex
    staging = "pg_temp." + _identifier(stage_name)
    await connection.execute(f"""CREATE TEMP TABLE {staging} (
        legacy_uuid uuid NOT NULL, network_id integer NOT NULL, evidence_id text NOT NULL,
        display_name text NOT NULL, aliases jsonb NOT NULL
    ) ON COMMIT DROP""")
    await connection.copy_records_to_table(
        stage_name,
        schema_name="pg_temp",
        columns=("legacy_uuid", "network_id", "evidence_id", "display_name", "aliases"),
        records=[
            (
                UUID(row["legacy_uuid"]),
                row["network_id"],
                row["evidence_id"],
                row["display_name"],
                _json(row["aliases"]),
            )
            for row in draft_rows
        ],
    )
    return staging


async def _validate_bindings(connection, namespace, staging, source_context, key, digest):
    await connection.execute(
        f"LOCK TABLE {namespace}.network_registry_identity,{namespace}.network_registry_alias IN SHARE MODE"
    )
    invalid = await connection.fetchval(
        f"""SELECT EXISTS (
          SELECT 1 FROM {staging} staged
          LEFT JOIN {namespace}.network_registry_alias alias
            ON alias.source_system=$1 AND alias.source_id=$2 AND alias.scope_key=$3
            AND alias.alias_type='legacy_fhir_uuid' AND alias.alias_value=staged.legacy_uuid::text
          LEFT JOIN {namespace}.network_registry_identity identity ON identity.network_id=staged.network_id
          WHERE alias.network_id IS DISTINCT FROM staged.network_id
            OR alias.evidence_id IS DISTINCT FROM staged.evidence_id OR identity.network_id IS NULL
        ) OR EXISTS (
          SELECT 1 FROM {staging} GROUP BY network_id
          HAVING count(DISTINCT (display_name,aliases))>1
        ) OR EXISTS (
          SELECT 1 FROM {staging} staged JOIN {namespace}.registry_record_history history
            ON history.record_kind='network' AND history.record_key=staged.network_id::text
            AND history.idempotency_key=$4 WHERE history.request_sha256<>$5
        ) OR EXISTS (
          SELECT 1 FROM {staging} staged JOIN {namespace}.registry_record_history history
            ON history.record_kind='network' AND history.record_key=staged.network_id::text
          LEFT JOIN {namespace}.network_registry_record head ON head.network_id=staged.network_id
          WHERE head.network_id IS NULL
        )""",
        *source_context,
        key,
        digest,
    )
    if invalid:
        raise NetworkCatalogSeedError("Network catalog binding, evidence, names or retained request conflicts")


async def _insert_missing(connection, namespace, staging, actor_json, reason, key, digest):
    seed_receipt = await connection.fetchrow(
        f"""WITH inserted AS (
          INSERT INTO {namespace}.network_registry_record(network_id,display_name,aliases)
          SELECT DISTINCT network_id,display_name,aliases FROM {staging}
          ON CONFLICT (network_id) DO NOTHING RETURNING *
        ), revision AS (
          UPDATE {namespace}.registry_revision_control SET draft_revision=draft_revision+1
          WHERE id=1 AND EXISTS(SELECT 1 FROM inserted) RETURNING draft_revision,approved_revision
        ), history AS (
          INSERT INTO {namespace}.registry_record_history
            (record_kind,record_key,revision,custom_revision,record_json,actor_json,reason,idempotency_key,request_sha256,event_json)
          SELECT 'network',inserted.network_id::text,inserted.revision,revision.draft_revision,
            to_jsonb(inserted),$1::jsonb,$2,$3,$4,
            jsonb_build_object('operation','catalog_seed','before_revision',0,'operation_context','{{}}'::jsonb)
            FROM inserted CROSS JOIN revision RETURNING record_key
        ) SELECT (SELECT count(*) FROM history) AS created_records,
          (SELECT count(DISTINCT network_id) FROM {staging}) AS requested_records,
          coalesce((SELECT draft_revision FROM revision),control.draft_revision) AS draft_revision,
          control.approved_revision FROM {namespace}.registry_revision_control control WHERE id=1""",
        actor_json,
        reason,
        key,
        digest,
    )
    return {
        **dict(seed_receipt),
        "preserved_records": seed_receipt["requested_records"] - seed_receipt["created_records"],
    }


async def seed_network_catalog_drafts(
    connection,
    draft_rows,
    actor,
    source_context,
    *,
    api_role,
    owner_role,
    control_schema,
):
    """Create only missing heads and exact initial history in one bounded owner batch.

    The trusted operator supplies its explicit actor and exact adopted source
    scope. IDs and aliases must already exist. Names are seed values, never
    recurring-import updates. The caller owns commit/rollback; this service
    leaves approval, serving state and every existing head/history untouched.
    """
    if type(source_context) is not NetworkCatalogSourceContext:
        raise NetworkCatalogSeedError("Network catalog requires an explicit source context")
    alias_context = (source_context.source_system, source_context.source_id, source_context.alias_scope)
    prepared_rows = _prepare_rows(draft_rows, *alias_context)
    actor_document = _validated_actor(actor)
    if actor_document["kind"] != "platform_admin":
        raise NetworkCatalogSeedError("Network catalog requires a platform administrator source operator")
    reason = _bounded_text(source_context.reason, 1000, "reason")
    key = "catalog-seed:" + _bounded_text(source_context.idempotency_key, 115, "idempotency_key")
    namespace = _identifier(control_schema)
    request = _json(
        {"rows": prepared_rows, "source": alias_context, "actor": actor_document, "reason": reason, "key": key}
    )
    if len(request.encode()) > MAX_CATALOG_BYTES:
        raise NetworkCatalogSeedError("Network catalog batch exceeds byte limit")
    digest = hashlib.sha256(request.encode()).hexdigest()
    if not connection.is_in_transaction():
        raise NetworkCatalogSeedError("Network catalog requires a caller-owned transaction")
    caller = await connection.fetchval("SELECT quote_ident(current_user)")
    try:
        async with connection.transaction():
            await verify_registry_management_permissions(
                connection, api_role=api_role, owner_role=owner_role, control_schema=control_schema
            )
            await assume_registry_source_owner(connection, owner_role, control_schema=control_schema)
            control = await connection.fetchrow(
                f"SELECT * FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE"
            )
            if control is None:
                raise NetworkCatalogSeedError("Network catalog revision control is missing")
            staging = await _stage_rows(connection, prepared_rows)
            await _validate_bindings(connection, namespace, staging, alias_context, key, digest)
            seed_receipt = await _insert_missing(
                connection, namespace, staging, _json(actor_document), reason, key, digest
            )
            await connection.execute(f"DROP TABLE {staging}")
    finally:
        await connection.execute(f"SET LOCAL ROLE {caller}")
    return {**seed_receipt, "request_sha256": digest}
