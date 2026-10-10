# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify reviewed provider/site tuples against one closed retained generation."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import asdict, dataclass

import asyncpg

from process.network_address_projection import _identifier
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, _encode
from process.network_serving_read import PinnedNetworkServingManifest, resolve_network_serving_manifest


class RetainedSiteAdoptionError(ValueError):
    """The whole reviewed selection failed without producing partial evidence."""


@dataclass(frozen=True)
class RetainedSiteAdoption:
    provider_system: str
    provider_id: str
    location_id: str
    location_key: str
    entity_type: str
    entity_id: str
    address_row_sha256: str


@dataclass(frozen=True)
class RetainedSiteAdoptionReceipt:
    generation_id: int
    candidate_id: str
    schema_name: str
    schema_revision: int
    source_generations: tuple[tuple[str, str], ...]
    approved_custom_revision: int
    manifest_sha256: str
    schema_oid: int
    address_table_oid: int
    binding_table_oid: int
    owner_role_oid: int
    records: tuple[RetainedSiteAdoption, ...]

    def as_dict(self):
        """Return a fresh JSON envelope without exposing mutable receipt state."""
        envelope = asdict(self)
        envelope["source_generations"] = dict(self.source_generations)
        envelope["records"] = [asdict(record) for record in self.records]
        return envelope


def _unique_object(pairs):
    """Reject repeated JSON field names before native identity validation."""
    value_by_field = dict(pairs)
    if len(value_by_field) != len(pairs):
        raise RetainedSiteAdoptionError("Retained site adoption input is invalid")
    return value_by_field


def _input_rows(input_bytes):
    """Check the closed wire shape and project identity fields to the native codec."""
    if type(input_bytes) is not bytes or len(input_bytes) > MAX_INPUT_BYTES:
        raise RetainedSiteAdoptionError("Retained site adoption input is invalid")
    rows = json.loads(input_bytes, object_pairs_hook=_unique_object)
    fields = {"provider_system", "provider_id", "location_id", "location_key", "address_row_sha256"}
    if type(rows) is not list or not 1 <= len(rows) <= MAX_ROWS:
        raise RetainedSiteAdoptionError("Retained site adoption input is invalid")
    projected_rows = []
    for row in rows:
        if (
            type(row) is not dict
            or set(row) != fields
            or row["provider_system"] not in ("npi", "provider_directory")
            or any(
                type(row[field]) is not str or re.fullmatch(r"[0-9a-f]{64}", row[field]) is None
                for field in ("location_key", "address_row_sha256")
            )
        ):
            raise RetainedSiteAdoptionError("Retained site adoption input is invalid")
        projected_rows.append(
            {
                "network_id": 1,
                **{field: row[field] for field in ("provider_system", "provider_id", "location_id")},
                "evidence_id": row["address_row_sha256"],
            }
        )
    return rows, json.dumps(projected_rows, separators=(",", ":")).encode()


async def _selected_rows(connection, namespace, selection_rows):
    """Resolve the entire selection and physical receipt coordinates in one query."""
    return await connection.fetchrow(
        f"""WITH requested AS MATERIALIZED (
          SELECT * FROM jsonb_to_recordset($1::jsonb) AS item(provider_system text,provider_id text,
            location_id uuid,location_key text,address_row_sha256 text)
        ), matched AS MATERIALIZED (
          SELECT item.*,binding.entity_type,binding.entity_id,address.location_key AS matched_key
          FROM requested item LEFT JOIN {namespace}.provider_location_binding binding
            ON (binding.provider_system,binding.provider_id,binding.location_id,binding.location_key)
              =(item.provider_system,item.provider_id,item.location_id,item.location_key)
          LEFT JOIN {namespace}.entity_address_unified address
            ON (address.location_key,address.entity_type,address.entity_id)
              =(binding.location_key,binding.entity_type,binding.entity_id)
            AND encode(sha256(convert_to(to_jsonb(address)::text,'UTF8')),'hex')=item.address_row_sha256
        ) SELECT
          (SELECT count(*) FROM requested) AS requested_count,
          (SELECT count(DISTINCT (provider_system,provider_id,location_id)) FROM requested) AS pair_count,
          count(*) AS matched_count,count(*) FILTER(WHERE matched_key IS NULL) AS unresolved_count,
          jsonb_agg(jsonb_build_object('provider_system',provider_system,'provider_id',provider_id,
            'location_id',location_id::text,'location_key',location_key,'entity_type',entity_type,
            'entity_id',entity_id,'address_row_sha256',address_row_sha256)
            ORDER BY provider_system,provider_id,location_id)::text AS records_json,
          (SELECT oid::bigint FROM pg_namespace WHERE nspname=$2) AS schema_oid,
          (SELECT oid::bigint FROM pg_class WHERE relnamespace=to_regnamespace($2)
            AND relname='entity_address_unified' AND relkind='r') AS address_table_oid,
          (SELECT oid::bigint FROM pg_class WHERE relnamespace=to_regnamespace($2)
            AND relname='provider_location_binding' AND relkind='r') AS binding_table_oid,
          (SELECT nspowner::bigint FROM pg_namespace WHERE nspname=$2) AS owner_role_oid
        FROM matched""",
        json.dumps(selection_rows, separators=(",", ":")),
        namespace[1:-1],
    )


def _receipt(source, selected):
    """Seal verified native coordinates and reject aggregate or response overflow."""
    count = selected["requested_count"]
    oid_fields = ("schema_oid", "address_table_oid", "binding_table_oid", "owner_role_oid")
    if (
        selected["pair_count"] != count
        or selected["matched_count"] != count
        or selected["unresolved_count"] != 0
        or selected["address_table_oid"] != source.address_table_oid
        or any(type(selected[field]) is not int or selected[field] <= 0 for field in oid_fields)
        or len(selected["records_json"].encode()) > MAX_INPUT_BYTES
    ):
        raise RetainedSiteAdoptionError("Retained site adoption selection is unresolved")
    manifest = asdict(source)
    manifest["source_generations"] = tuple(sorted(source.source_generations.items()))
    receipt = RetainedSiteAdoptionReceipt(
        **manifest,
        **{field: selected[field] for field in oid_fields if field != "address_table_oid"},
        records=tuple(RetainedSiteAdoption(**row) for row in json.loads(selected["records_json"])),
    )
    if len(json.dumps(receipt.as_dict(), separators=(",", ":"), sort_keys=True).encode()) > MAX_INPUT_BYTES:
        raise RetainedSiteAdoptionError("Retained site adoption receipt exceeds the byte limit")
    return receipt


async def _verified_source(connection, generation_id, expected_source_json, control_schema):
    """Require a repeatable caller snapshot and the exact closed manifest identity."""
    if not connection.is_in_transaction() or await connection.fetchval(
        "SELECT current_setting('transaction_isolation')"
    ) not in ("repeatable read", "serializable"):
        raise RetainedSiteAdoptionError("Retained site adoption requires a repeatable caller transaction")
    verified = await resolve_network_serving_manifest(
        connection, generation_id=generation_id, control_schema=control_schema
    )
    if json.dumps(asdict(verified), sort_keys=True, separators=(",", ":")) != expected_source_json:
        raise RetainedSiteAdoptionError("Retained site adoption source differs")
    return verified


async def resolve_retained_site_adoptions(
    connection,
    source: PinnedNetworkServingManifest,
    input_bytes: bytes,
    *,
    control_schema=None,
) -> RetainedSiteAdoptionReceipt:
    """Read exact reviewed tuples in the caller's repeatable snapshot without writes.

    The source must remain eligible and physically closed. No mutable binding
    head, provider-wide address expansion or live-source fallback is consulted.
    Hashes cover each complete retained address row in native JSONB text form.
    """
    try:
        if type(source) is not PinnedNetworkServingManifest:
            raise RetainedSiteAdoptionError("Retained site adoption input is invalid")
        expected_source = json.dumps(asdict(source), sort_keys=True, separators=(",", ":"), allow_nan=False)
        generation_id = source.generation_id
        selection_rows, projected_bytes = _input_rows(input_bytes)
        _, row_count = await asyncio.to_thread(_encode, projected_bytes)
        if row_count != len(selection_rows):
            raise RetainedSiteAdoptionError("Retained site adoption input is invalid")
        verified = await _verified_source(connection, generation_id, expected_source, control_schema)
        selected = await _selected_rows(connection, _identifier(verified.schema_name), selection_rows)
        return _receipt(verified, selected)
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, asyncpg.PostgresError) as error:
        if isinstance(error, RetainedSiteAdoptionError):
            raise
        raise RetainedSiteAdoptionError("Retained site adoption verification failed") from error
