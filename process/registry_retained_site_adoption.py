# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Verify reviewed provider/site tuples against one closed retained generation."""

from __future__ import annotations

import asyncio
import json
import re
from dataclasses import asdict, dataclass

import asyncpg

from process.network_address_projection import _identifier
from process.network_compact_layout import CompactAddressRelation
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, _encode
from process.network_serving_read import PinnedNetworkServingManifest, resolve_network_serving_manifest
from process.uhc_flex_practitioner_async_safety import drain_operation


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
    address_relation: CompactAddressRelation
    records: tuple[RetainedSiteAdoption, ...]
    compact_address_relations: tuple[tuple[str, str, str], ...] = ()

    def as_dict(self):
        """Return a fresh JSON envelope without exposing mutable receipt state."""
        envelope = asdict(self)
        envelope["source_generations"] = dict(self.source_generations)
        envelope["records"] = [asdict(record) for record in self.records]
        relations = envelope.pop("compact_address_relations")
        if relations:
            envelope["compact_address_relations"] = [list(relation) for relation in relations]
        return envelope


def retained_site_receipt_document(document):
    """Project the known historical receipt to names while retaining its exact data selection."""
    fields = {
        "generation_id",
        "candidate_id",
        "schema_name",
        "schema_revision",
        "source_generations",
        "approved_custom_revision",
        "manifest_sha256",
        "records",
    }
    retired_fields = {"schema_oid", "address_table_oid", "binding_table_oid", "owner_role_oid"}
    if type(document) is not dict:
        raise RetainedSiteAdoptionError("Retained site receipt is invalid")
    required = set(document) - {"compact_address_relations"}
    if required not in (fields | retired_fields, fields | {"address_relation"}):
        raise RetainedSiteAdoptionError("Retained site receipt is invalid")
    has_legacy_fields = required == fields | retired_fields
    receipt_by_field = {key: document[key] for key in fields}
    _identifier(receipt_by_field["schema_name"])
    relations = []
    for relation in document.get("compact_address_relations", []):
        if type(relation) not in (tuple, list) or len(relation) != (4 if has_legacy_fields else 3):
            raise RetainedSiteAdoptionError("Retained site receipt is invalid")
        role, schema, table = relation[:3]
        if role not in {"base", "custom"} or role in {selected_relation[0] for selected_relation in relations}:
            raise RetainedSiteAdoptionError("Retained site receipt is invalid")
        _identifier(schema)
        _identifier(table)
        relations.append([role, schema, table])
    address_by_field = document.get("address_relation")
    if address_by_field is None:
        address_by_field = {"schema_name": document["schema_name"], "table_name": "entity_address_unified"}
        if relations:
            base = next((relation for relation in relations if relation[0] == "base"), None)
            if base is None:
                raise RetainedSiteAdoptionError("Retained site receipt is invalid")
            address_by_field = {"schema_name": base[1], "table_name": base[2]}
    if type(address_by_field) is not dict or set(address_by_field) != {"schema_name", "table_name"}:
        raise RetainedSiteAdoptionError("Retained site receipt is invalid")
    receipt_by_field["address_relation"] = asdict(CompactAddressRelation(**address_by_field))
    if relations:
        receipt_by_field["compact_address_relations"] = relations
    return receipt_by_field


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


async def _selected_rows(connection, manifest, selection_rows):
    """Resolve every selected binding and complete retained address row in one query."""
    namespace = _identifier(manifest.schema_name)
    addresses, address_arguments = _source_addresses(manifest, namespace)
    return await connection.fetchrow(
        f"""WITH requested AS MATERIALIZED (
          SELECT * FROM jsonb_to_recordset($1::jsonb) AS item(provider_system text,provider_id text,
            location_id uuid,location_key text,address_row_sha256 text)
        ), matched AS MATERIALIZED (
          SELECT item.*,binding.entity_type,binding.entity_id,address.location_key AS matched_key
          FROM requested item LEFT JOIN {namespace}.provider_location_binding binding
            ON (binding.provider_system,binding.provider_id,binding.location_id,binding.location_key)
              =(item.provider_system,item.provider_id,item.location_id,item.location_key)
          LEFT JOIN {addresses} address
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
            ORDER BY provider_system,provider_id,location_id)::text AS records_json
        FROM matched""",
        json.dumps(selection_rows, separators=(",", ":")),
        *address_arguments,
    )


def _source_addresses(source, namespace):
    if source.compact_layout is None:
        return namespace + ".entity_address_unified", ()
    from api.network_compact_address import compact_address_native_sql

    return compact_address_native_sql(source.compact_layout, first_parameter=2, visible_membership_sql="TRUE")


def _compact_relations(source):
    if source.compact_layout is None:
        return ()
    layout = source.compact_layout
    relations = [("base", layout.base.schema_name, layout.base.table_name)]
    if layout.custom is not None:
        relations.append(("custom", layout.custom.schema_name, layout.custom.table_name))
    return tuple(relations)


def _receipt(source_manifest, selected):
    """Bind complete selected data to its named generation and enforce response bounds."""
    count = selected["requested_count"]
    if (
        selected["pair_count"] != count
        or selected["matched_count"] != count
        or selected["unresolved_count"] != 0
        or len(selected["records_json"].encode()) > MAX_INPUT_BYTES
    ):
        raise RetainedSiteAdoptionError("Retained site adoption selection is unresolved")
    manifest_by_field = {
        field: getattr(source_manifest, field)
        for field in (
            "generation_id",
            "candidate_id",
            "schema_name",
            "schema_revision",
            "source_generations",
            "approved_custom_revision",
            "manifest_sha256",
            "address_relation",
        )
    }
    manifest_by_field["source_generations"] = tuple(sorted(source_manifest.source_generations.items()))
    receipt = RetainedSiteAdoptionReceipt(
        **manifest_by_field,
        records=tuple(
            RetainedSiteAdoption(**record_by_field) for record_by_field in json.loads(selected["records_json"])
        ),
        compact_address_relations=_compact_relations(source_manifest),
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

    The source remains eligible in the pinned repeatable snapshot. Hashes cover
    each complete retained address row in native JSONB text form.
    """
    try:
        if type(source) is not PinnedNetworkServingManifest:
            raise RetainedSiteAdoptionError("Retained site adoption input is invalid")
        expected_source = json.dumps(asdict(source), sort_keys=True, separators=(",", ":"), allow_nan=False)
        generation_id = source.generation_id
        selection_rows, projected_bytes = _input_rows(input_bytes)
        _, row_count = await drain_operation(asyncio.to_thread(_encode, projected_bytes), preserve_cancellation=True)
        if row_count != len(selection_rows):
            raise RetainedSiteAdoptionError("Retained site adoption input is invalid")
        verified = await _verified_source(connection, generation_id, expected_source, control_schema)
        selected = await _selected_rows(connection, verified, selection_rows)
        return _receipt(verified, selected)
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, asyncpg.PostgresError) as error:
        if isinstance(error, RetainedSiteAdoptionError):
            raise
        raise RetainedSiteAdoptionError("Retained site adoption verification failed") from error
