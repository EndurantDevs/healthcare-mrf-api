# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Seed missing named network drafts from exact, already adopted source aliases."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from uuid import UUID, uuid4

from process.network_address_projection import _identifier
from process.network_legacy_alias_adoption import MAX_ADOPTION_ROWS, LegacyNetworkAdoptionRow
from process.registry_fhir_catalog_identity import ALIAS_SCOPE, FHIRNetworkIdentityInput, _allocation_key
from process.registry_management_permissions import assume_registry_source_owner
from process.registry_record_store import _bounded_text, _record_identity, _validated_actor, _validated_fields
from process.uhc_provider_file_source_identity import UHC_PROVIDER_FILE_SOURCE_ID

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
class ExistingPlanTierCatalogDraftRow:
    network_id: int
    allocation_key: UUID
    endpoint_id: str
    network_key_id: str
    evidence_id: str
    display_name: str
    aliases: tuple[str, ...] = ()


@dataclass(frozen=True)
class NetworkCatalogPreimage:
    draft_revision: int
    approved_revision: int
    serving_generation: int
    metadata_generation: str

    def __post_init__(self):
        revisions = (self.draft_revision, self.approved_revision, self.serving_generation)
        if (
            any(type(value) is not int or not 0 <= value < 9223372036854775806 for value in revisions)
            or self.approved_revision > self.draft_revision
            or type(self.metadata_generation) is not str
            or re.fullmatch(r"[0-9a-f]{64}", self.metadata_generation) is None
        ):
            raise NetworkCatalogSeedError("Network catalog preimage is invalid")


@dataclass(frozen=True)
class NetworkCatalogSourceContext:
    source_system: str
    source_id: str
    alias_scope: str
    reason: str
    idempotency_key: str
    expected_preimage: NetworkCatalogPreimage | None = None


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


def _prepare_plan_tier_rows(draft_rows, source_context):
    if (
        type(source_context.expected_preimage) is not NetworkCatalogPreimage
        or (source_context.source_system, source_context.source_id, source_context.alias_scope)
        != ("fhir", UHC_PROVIDER_FILE_SOURCE_ID, ALIAS_SCOPE)
        or type(draft_rows) not in {list, tuple}
        or not 0 < len(draft_rows) <= MAX_ADOPTION_ROWS
    ):
        raise NetworkCatalogSeedError("Network catalog typed source context is invalid")
    prepared_rows, network_ids = [], set()
    for draft_row in draft_rows:
        if type(draft_row) is not ExistingPlanTierCatalogDraftRow:
            raise NetworkCatalogSeedError("Network catalog requires homogeneous typed rows")
        _record_identity("network", draft_row.network_id)
        if draft_row.network_id in network_ids:
            raise NetworkCatalogSeedError("Network catalog typed identity is duplicated")
        network_ids.add(draft_row.network_id)
        if (
            type(draft_row.allocation_key) is not UUID
            or type(draft_row.endpoint_id) is not str
            or re.fullmatch(r"[0-9a-f]{64}", draft_row.endpoint_id) is None
            or type(draft_row.network_key_id) is not str
            or re.fullmatch(r"[0-9a-f]{64}", draft_row.network_key_id) is None
            or type(draft_row.evidence_id) is not str
            or re.fullmatch(r"[0-9a-f]{64}:UHCPlanTier/" + draft_row.network_key_id, draft_row.evidence_id) is None
            or type(draft_row.aliases) not in {tuple, list}
        ):
            raise NetworkCatalogSeedError("Network catalog typed identity is invalid")
        identity = FHIRNetworkIdentityInput(
            draft_row.network_key_id, draft_row.endpoint_id, (source_context.source_id,), draft_row.evidence_id
        )
        if draft_row.allocation_key != _allocation_key(identity, "plan_tier"):
            raise NetworkCatalogSeedError("Network catalog typed allocation key differs")
        fields = _validated_fields(
            "network", {"display_name": draft_row.display_name, "aliases": list(draft_row.aliases)}
        )
        prepared_rows.append(
            {
                "network_id": draft_row.network_id,
                "allocation_key": str(draft_row.allocation_key),
                "alias_value": draft_row.network_key_id,
                "evidence_id": draft_row.evidence_id,
                **fields,
            }
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


async def _stage_plan_tier_rows(connection, draft_rows):
    name = "network_catalog_seed_" + uuid4().hex
    staging = "pg_temp." + _identifier(name)
    await connection.execute(f"""CREATE TEMP TABLE {staging} (
        network_id integer PRIMARY KEY, allocation_key uuid NOT NULL, alias_value text NOT NULL,
        evidence_id text NOT NULL, display_name text NOT NULL, aliases jsonb NOT NULL
    ) ON COMMIT DROP""")
    await connection.copy_records_to_table(
        name,
        schema_name="pg_temp",
        columns=("network_id", "allocation_key", "alias_value", "evidence_id", "display_name", "aliases"),
        records=[
            (
                draft_row["network_id"],
                UUID(draft_row["allocation_key"]),
                draft_row["alias_value"],
                draft_row["evidence_id"],
                draft_row["display_name"],
                _json(draft_row["aliases"]),
            )
            for draft_row in draft_rows
        ],
    )
    return staging


async def _validate_plan_tier_bindings(connection, namespace, staging, source_context, key, digest, control):
    expected = source_context.expected_preimage
    serving = await connection.fetchrow(
        f"SELECT coalesce(generation_id,0) AS generation_id,metadata_generation "
        f"FROM {namespace}.network_serving_control WHERE id=1 FOR SHARE"
    )
    if (
        serving is None
        or control["approved_revision"] != expected.approved_revision
        or serving["generation_id"] != expected.serving_generation
        or serving["metadata_generation"] != expected.metadata_generation
        or control["draft_revision"] not in {expected.draft_revision, expected.draft_revision + 1}
    ):
        raise NetworkCatalogSeedError("Network catalog preimage changed")
    await connection.execute(
        f"LOCK TABLE {namespace}.network_registry_identity,{namespace}.network_registry_alias IN SHARE MODE"
    )
    valid = await connection.fetchval(
        f"""SELECT coalesce(bool_and(
      coalesce(identity.allocation_key=staged.allocation_key
      AND alias.network_id=staged.network_id AND alias.evidence_id=staged.evidence_id,false)
      AND NOT EXISTS(SELECT 1 FROM {namespace}.registry_record_history h
        WHERE h.record_kind='network' AND h.record_key=staged.network_id::text
        AND (head.network_id IS NULL OR (h.idempotency_key=$4 AND h.request_sha256<>$5)))
      AND ($6::bigint=$7::bigint OR EXISTS(
        SELECT 1 FROM {namespace}.registry_record_history h WHERE h.record_kind='network'
        AND h.record_key=staged.network_id::text AND h.revision=1 AND h.custom_revision=$7+1
        AND h.idempotency_key=$4 AND h.request_sha256=$5 AND h.record_json=to_jsonb(head)
      ))),false)
      FROM {staging} staged
      LEFT JOIN {namespace}.network_registry_identity identity USING(network_id)
      LEFT JOIN {namespace}.network_registry_alias alias
        ON alias.source_system=$1 AND alias.source_id=$2 AND alias.scope_key=$3
        AND alias.alias_type='fhir_plan_tier' AND alias.alias_value=staged.alias_value
      LEFT JOIN {namespace}.network_registry_record head ON head.network_id=staged.network_id""",
        source_context.source_system,
        source_context.source_id,
        source_context.alias_scope,
        key,
        digest,
        control["draft_revision"],
        expected.draft_revision,
    )
    if valid is not True:
        raise NetworkCatalogSeedError("Network catalog typed binding or preimage conflicts")


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


def _seed_request(draft_rows, actor, source_context):
    if type(source_context) is not NetworkCatalogSourceContext:
        raise NetworkCatalogSeedError("Network catalog requires an explicit source context")
    alias_context = (source_context.source_system, source_context.source_id, source_context.alias_scope)
    is_plan_tier = source_context.expected_preimage is not None
    prepared_rows = (
        _prepare_plan_tier_rows(draft_rows, source_context)
        if is_plan_tier
        else _prepare_rows(draft_rows, *alias_context)
    )
    actor_document = _validated_actor(actor)
    if actor_document["kind"] != "platform_admin":
        raise NetworkCatalogSeedError("Network catalog requires a platform administrator source operator")
    reason = _bounded_text(source_context.reason, 1000, "reason")
    key = "catalog-seed:" + _bounded_text(source_context.idempotency_key, 115, "idempotency_key")
    request_by_field = {
        "rows": prepared_rows,
        "source": alias_context,
        "actor": actor_document,
        "reason": reason,
        "key": key,
    }
    if is_plan_tier:
        request_by_field["expected_preimage"] = source_context.expected_preimage.__dict__
    request = _json(request_by_field)
    if len(request.encode()) > MAX_CATALOG_BYTES:
        raise NetworkCatalogSeedError("Network catalog batch exceeds byte limit")
    digest = hashlib.sha256(request.encode()).hexdigest()
    return prepared_rows, actor_document, reason, key, digest


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
    prepared_rows, actor_document, reason, key, digest = _seed_request(draft_rows, actor, source_context)
    alias_context = (source_context.source_system, source_context.source_id, source_context.alias_scope)
    is_plan_tier = source_context.expected_preimage is not None
    namespace = _identifier(control_schema)
    if not connection.is_in_transaction():
        raise NetworkCatalogSeedError("Network catalog requires a caller-owned transaction")
    caller = await connection.fetchval("SELECT quote_ident(current_user)")
    try:
        async with connection.transaction():
            await assume_registry_source_owner(connection, owner_role, control_schema=control_schema)
            control = await connection.fetchrow(
                f"SELECT * FROM {namespace}.registry_revision_control WHERE id=1 FOR UPDATE"
            )
            if control is None:
                raise NetworkCatalogSeedError("Network catalog revision control is missing")
            if is_plan_tier:
                staging = await _stage_plan_tier_rows(connection, prepared_rows)
                await _validate_plan_tier_bindings(connection, namespace, staging, source_context, key, digest, control)
            else:
                staging = await _stage_rows(connection, prepared_rows)
                await _validate_bindings(connection, namespace, staging, alias_context, key, digest)
            seed_receipt = await _insert_missing(
                connection, namespace, staging, _json(actor_document), reason, key, digest
            )
            await connection.execute(f"DROP TABLE {staging}")
    finally:
        await connection.execute(f"SET LOCAL ROLE {caller}")
    return {**seed_receipt, "request_sha256": digest}
