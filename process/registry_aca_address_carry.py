# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Carry only approved, native-proven ACA offices into an isolated address artifact."""

import hashlib
import json
from dataclasses import dataclass
from uuid import UUID

from process.network_address_artifact import _locked_table
from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_compact_layout import CompactAddressRelation
from process.network_initial_source_office_bindings import _canonical_bindings, _canonical_source, _office_records
from process.network_legacy_membership_source import ACAMembershipBatch, PinnedACAMembershipSource, _require_archive
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS


@dataclass(frozen=True)
class RegistryACAAddressCarry:
    """Bound the exact source pages and resulting office rows without keeping pages."""

    sha256: str | None = None
    pages: int = 0
    rows: int = 0


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


_SOURCE_SQL = """WITH pairs AS MATERIALIZED (
 SELECT DISTINCT provider_id,location_id FROM jsonb_to_recordset($1::jsonb)
 AS member(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
), evidence AS MATERIALIZED (
 SELECT e.*,{canonical} AS canonical,{location_key} AS location_key
 FROM {source}.mrf_address_evidence e
 JOIN pairs p ON p.provider_id=e.npi::text AND p.location_id=e.address_key
 LEFT JOIN {source}.mrf_canonical_address c ON c.address_key=e.address_key
 WHERE {import_filter} AND e.issuer_id=$3 AND e.year=$4 AND {url_filter}
 AND e.evidence_checksum=ANY($6::bigint[])
), documents AS (
 SELECT jsonb_build_object('provider_id',npi::text,'location_id',address_key::text,
 'location_key',location_key,'canonical',canonical,
 'evidence_address',jsonb_build_array(first_line,second_line,city_name,state_name,postal_code,country_code),
 'address',jsonb_build_array(first_line,second_line,city_name,state_name,postal_code,country_code),
 'source_record_id',source_record_id,
 'evidence_sha256',encode(sha256(convert_to(to_jsonb(evidence)::text,'UTF8')),'hex')) AS document,
 canonical IS NULL OR canonical->>'address_key' IS DISTINCT FROM address_key::text
 OR canonical->>'precision' IS DISTINCT FROM 'street' OR canonical->>'merged_into' IS NOT NULL
 OR canonical->'source_bits' IS NULL OR ((canonical->>'source_bits')::integer & 16)<>16 AS invalid
 FROM evidence
)
SELECT (SELECT count(*) FROM pairs) AS pairs,
 (SELECT count(DISTINCT(npi,address_key)) FROM evidence) AS covered_pairs,
 coalesce(bool_or(invalid),false) AS invalid,
 CASE WHEN count(*)<=5000 AND coalesce(sum(octet_length(document::text)),0)+2*count(*)+2<=8388608
 THEN coalesce(jsonb_agg(document ORDER BY document::text),'[]'::jsonb)::text END AS records_json FROM documents"""


def _physical_selection(source_pin):
    """Only a concrete admitted global epoch may omit row import/URL filters."""
    from process.network_legacy_membership_source import ACAGlobalEpochReadScope, aca_physical_source
    from process.registry_aca_global_epoch import PinnedACAGlobalEpoch

    physical = aca_physical_source(source_pin)
    if type(physical) is ACAGlobalEpochReadScope:
        epoch = getattr(source_pin, "retained_epoch", None)
        if (
            type(source_pin) is not PinnedACAMembershipSource
            or type(epoch) is not PinnedACAGlobalEpoch
            or source_pin.retained_successor is not None
            or physical.epoch is not epoch
            or physical.prepared != epoch.prepared
            or physical.validation != epoch.validation
            or physical.runtime_roles != epoch.runtime_roles
            or (physical.issuer_id, physical.year) != (source_pin.issuer_id, source_pin.year)
            or physical.import_id is not None
            or physical.source_url is not None
        ):
            raise ValueError("registry_aca_address_carry_global_scope_invalid")
        return physical, True
    if (
        type(physical) is not PinnedACAMembershipSource
        or type(physical.import_id) is not str
        or not physical.import_id
        or type(physical.source_url) is not str
        or not physical.source_url
    ):
        raise ValueError("registry_aca_address_carry_physical_scope_invalid")
    return physical, False


async def _source_documents(connection, batch):
    from process.entity_address_unified import _location_key_expr

    physical, global_scope = _physical_selection(batch.source)
    if global_scope:
        await _require_archive(connection, batch.source)
    location_key = _location_key_expr(
        dict(
            entity_type="'npi'::text",
            entity_id="e.npi::text",
            npi="e.npi",
            inferred_npi="NULL::bigint",
            address_role_id="4",
            row_origin="'base'::text",
            address_key="e.address_key",
            source_id="2",
            source_record_id="e.source_record_id",
            zip5="NULL::text",
            state_code="NULL::text",
            city_norm="NULL::text",
        )
    )
    summary = await connection.fetchrow(
        _SOURCE_SQL.format(
            source=_identifier(physical.prepared.ownership.schema_name),
            canonical=_canonical_source(physical)[1],
            location_key=location_key,
            import_filter="($2::text IS NULL OR e.import_id=$2)" if global_scope else "e.import_id=$2",
            url_filter="($5::text IS NULL OR e.source_url=$5)" if global_scope else "e.source_url=$5",
        ),
        batch.input_bytes.decode(),
        physical.import_id,
        physical.issuer_id,
        physical.year,
        physical.source_url,
        list(batch.accepted_evidence_checksums),
    )
    if summary["invalid"] or summary["pairs"] != summary["covered_pairs"] or summary["records_json"] is None:
        raise ValueError("registry_aca_address_carry_source_invalid")
    return json.loads(summary["records_json"])


_INSERT_SQL = """WITH offices AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset($1::jsonb) AS office(provider_id text,location_id uuid,location_key text,
 canonical jsonb,address jsonb,source_record_id text)
), selected AS (SELECT DISTINCT ON(provider_id,location_id) * FROM offices ORDER BY provider_id,location_id)
INSERT INTO {target}.entity_address_unified
(entity_type,entity_id,npi,entity_name,location_key,row_origin,archive_identity_version,address_precision,
 checksum,type,first_line,second_line,city_name,state_name,postal_code,country_code,address_key,premise_key,
 zip5,state_code,city_norm,source_mask,address_source_mask,source_count,independent_source_count,
 address_sources,source_record_ids,taxonomy_array,plans_network_array,procedures_array,medications_array,canonical_network_ids)
SELECT 'npi',provider_id,provider_id::bigint,'NPI '||provider_id,location_key,'base',
 'v'||(canonical->>'identity_version'),'street',('x'||substr(location_key,1,16))::bit(64)::bigint,'practice',
 address->>0,address->>1,address->>2,address->>3,address->>4,address->>5,location_id,
 (canonical->>'premise_key')::uuid,canonical->>'zip5',canonical->>'state_code',canonical->>'city_norm',
 2,2,1,1,ARRAY['mrf']::varchar[],ARRAY(SELECT DISTINCT record.source_record_id FROM offices record
 WHERE record.provider_id=office.provider_id AND record.location_id=office.location_id ORDER BY record.source_record_id),
 '{{}}'::integer[],'{{}}'::integer[],'{{}}'::integer[],'{{}}'::integer[],'{{}}'::integer[] FROM selected office
WHERE NOT EXISTS(SELECT 1 FROM {target}.entity_address_unified existing
 WHERE existing.npi=office.provider_id::bigint AND existing.address_key=office.location_id)
"""
_BINDINGS_CTE = """WITH selected AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset($1::jsonb) AS binding(provider_system text,provider_id text,location_id uuid,
 location_key text,entity_type text,entity_id text)) """
_BINDING_COLUMNS = ("provider_system", "provider_id", "location_id", "location_key", "entity_type", "entity_id")
_EXPECTED_BINDINGS_SQL = """WITH offices AS MATERIALIZED (
 SELECT value AS office FROM jsonb_array_elements($1::jsonb)
), bindings AS MATERIALIZED (
 SELECT DISTINCT 'npi'::text AS provider_system,office->>'provider_id' AS provider_id,
 (office->>'location_id')::uuid AS location_id,office->>'location_key' AS location_key,
 'npi'::text AS entity_type,office->>'provider_id' AS entity_id FROM offices
)
SELECT count(*)<=5000 AND count(*)=count(DISTINCT(provider_id,location_id))
 AND count(*)=count(DISTINCT location_key)
 AND coalesce(bool_and((provider_id ~ '^[0-9]{10}$') IS TRUE AND location_id IS NOT NULL
 AND (location_key ~ '^[0-9a-f]{64}$') IS TRUE),true) AS valid,
 coalesce(jsonb_agg(to_jsonb(bindings) ORDER BY provider_id,location_id),'[]'::jsonb)::text AS bindings_json
FROM bindings"""


async def _expected_bindings(connection, records):
    """Derive the same closed native-output tuples set-wise from exact SQL offices."""
    result = await connection.fetchrow(_EXPECTED_BINDINGS_SQL, _canonical(records).decode())
    encoded = result["bindings_json"]
    if result["valid"] is not True or type(encoded) is not str or len(encoded.encode()) > MAX_INPUT_BYTES:
        raise ValueError("registry_aca_address_carry_binding_conflict")
    documents = json.loads(encoded)
    tuples = [
        tuple(UUID(binding[name]) if name == "location_id" else binding[name] for name in _BINDING_COLUMNS)
        for binding in documents
    ]
    return encoded, documents, tuples


async def _bindings(connection, target_schema, encoded, *, writing):
    target = _identifier(target_schema)
    if writing:
        await connection.execute(
            _BINDINGS_CTE
            + f"INSERT INTO {target}.provider_location_binding SELECT * FROM selected ON CONFLICT DO NOTHING",
            encoded,
        )
    invalid = await connection.fetchval(
        _BINDINGS_CTE
        + f"""SELECT EXISTS(SELECT 1 FROM selected expected
        LEFT JOIN {target}.provider_location_binding actual USING(provider_system,provider_id,location_id)
        WHERE actual.location_key IS NULL OR (actual.location_key,actual.entity_type,actual.entity_id)
        IS DISTINCT FROM (expected.location_key,expected.entity_type,expected.entity_id))""",
        encoded,
    )
    if invalid is not False:
        raise ValueError("registry_aca_address_carry_binding_conflict")


async def _copy_existing_offices(connection, batch, target_schema, existing_source):
    """Keep selected native base rows, including enrichment, before constructing missing carriers."""
    if type(existing_source) is not CompactAddressRelation or not connection.is_in_transaction():
        raise ValueError("registry_aca_address_carry_existing_source_invalid")
    physical = await _locked_table(connection, existing_source.schema_name, existing_source.table_oid)
    if CompactAddressRelation(physical["schema_name"], physical["relname"], physical["oid"]) != existing_source:
        raise ValueError("registry_aca_address_carry_existing_source_invalid")
    source_relation = _identifier(existing_source.schema_name) + "." + _identifier(existing_source.table_name)
    target_relation = _identifier(target_schema) + ".entity_address_unified"
    summary = await connection.fetchrow(
        f"""WITH pairs AS MATERIALIZED (
          SELECT DISTINCT provider_id::bigint AS npi,location_id FROM jsonb_to_recordset($1::jsonb)
          AS member(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
        ), selected AS MATERIALIZED (
          SELECT address.* FROM pairs CROSS JOIN LATERAL (
            SELECT address.* FROM {source_relation} address WHERE address.npi=pairs.npi
              AND address.entity_type='npi' AND address.entity_id=pairs.npi::text
              AND address.address_key=pairs.location_id OFFSET 0
          ) address
        ), bounds AS MATERIALIZED (
          SELECT count(*)<=5000 AND coalesce(sum(octet_length(to_jsonb(selected)::text)),0)<=8388608 AS bounded
          FROM selected
        ), copied AS (
          INSERT INTO {target_relation} SELECT selected.* FROM selected CROSS JOIN bounds WHERE bounds.bounded
          ON CONFLICT(location_key) DO NOTHING RETURNING 1
        ) SELECT bounded,(SELECT count(*) FROM copied) AS copied_rows FROM bounds""",
        batch.input_bytes.decode(),
    )
    if summary["bounded"] is not True:
        raise ValueError("registry_aca_address_carry_existing_source_bound")


async def _page(connection, batch, target_schema, *, writing, existing_source=None):
    documents = await _source_documents(connection, batch)
    if writing:
        if existing_source is not None:
            await _copy_existing_offices(connection, batch, target_schema, existing_source)
        await connection.execute(_INSERT_SQL.format(target=_identifier(target_schema)), _canonical(documents).decode())
    records = await _office_records(
        connection, batch, PinnedAddressSource(target_schema, "entity_address_unified", "aca-carry")
    )
    encoded, bindings, expected_bindings = await _expected_bindings(connection, records)
    if writing and await _canonical_bindings(connection, records) != expected_bindings:
        raise ValueError("registry_aca_address_carry_native_binding_differs")
    await _bindings(connection, target_schema, encoded, writing=writing)
    return {
        "generation": batch.generation_id,
        "input_sha256": batch.input_sha256,
        "source_rows": batch.source_rows,
        "accepted_source_rows": batch.accepted_source_rows,
        "omitted_source_rows": batch.omitted_source_rows,
        "source": documents,
        "offices": records,
        "bindings": bindings,
    }, len(bindings)


async def _stream_recipe(
    connection,
    recipe,
    approved,
    control_schema,
    destination,
    digest,
    aca_reads_by_oid=None,
):
    """Stream validated ACA pages into the same destination and ordered carry digest."""
    target_schema, writing, existing_source = destination
    from process.registry_source_recipe_composition import _read_recipe_page, _require_recipe_custody

    verified = await _require_recipe_custody(
        connection, recipe, approved, control_schema, aca_reads_by_oid=aca_reads_by_oid
    )
    if verified is None:
        await _require_archive(connection, recipe.source_pin)
    cursor, limit, generation = None, 1000, None
    pages = office_rows = 0
    while True:
        batch, next_cursor, limit = await _read_recipe_page(
            connection, recipe, approved, cursor, control_schema, limit, verified
        )
        _require_carry_batch(batch, recipe, approved, limit, generation)
        generation = batch.generation_id
        if not batch.source_rows:
            if batch.membership_rows or batch.input_bytes != b"[]":
                raise ValueError("registry_aca_address_carry_accounting_invalid")
            return pages, office_rows
        if next_cursor is None or next_cursor == cursor:
            raise ValueError("registry_aca_address_carry_cursor_invalid")
        source_options = {} if existing_source is None else {"existing_source": existing_source}
        document, row_count = await _page(connection, batch, target_schema, writing=writing, **source_options)
        encoded = _canonical(document)
        digest.update(len(encoded).to_bytes(8, "big"))
        digest.update(encoded)
        pages += 1
        office_rows += row_count
        cursor = next_cursor


async def prepare_aca_address_carry(
    connection,
    recipes,
    approved,
    control_schema,
    target_schema,
    *,
    writing=False,
    existing_source=None,
    inventory=False,
):
    """Read immutable source pages; only the explicit pre-freeze path adds offices.

    The caller owns the repeatable transaction and native candidate custody.
    Loading canonicalizes source and actual offices once before freeze. Replay
    verifies custody and exact SQL correspondence, then reproduces the complete
    source/office/binding proof bound by the immutable artifact receipt. It does
    not repeat native canonicalization or encode its canonical-address output.
    """
    from process.registry_source_recipe_store import canonical_registry_source_recipes, decode_registry_source_recipes

    _identifier(target_schema)
    if type(inventory) is not bool:
        raise ValueError("registry_aca_address_carry_inventory_invalid")
    if inventory:
        from process.registry_source_inventory_preparation import SourceInventoryPreparation

        recipes = SourceInventoryPreparation(recipes).recipes
    else:
        canonical = canonical_registry_source_recipes(recipes)
        recipes = decode_registry_source_recipes(canonical)
    selected_recipes = tuple(recipe for recipe in recipes if type(recipe.source_pin) is PinnedACAMembershipSource)
    if not selected_recipes:
        return RegistryACAAddressCarry()
    if inventory:
        from process.registry_source_recipe_store import _source_inventory_recipe_digest

        digest = _source_inventory_recipe_digest(selected_recipes)
    else:
        digest = hashlib.sha256(canonical_registry_source_recipes(selected_recipes).encode())
    digest.update(_canonical({"approved_generation": approved.generation_id}))
    pages = office_rows = 0
    aca_reads_by_oid = {}
    for recipe in selected_recipes:
        page_count, row_count = await _stream_recipe(
            connection,
            recipe,
            approved,
            control_schema,
            (target_schema, writing, existing_source),
            digest,
            aca_reads_by_oid,
        )
        pages += page_count
        office_rows += row_count
    digest.update(_canonical({"pages": pages, "rows": office_rows}))
    return RegistryACAAddressCarry(digest.hexdigest(), pages, office_rows)


def _require_carry_batch(batch, recipe, approved, limit, generation):
    if (
        type(batch) is not ACAMembershipBatch
        or batch.source != recipe.source_pin
        or batch.approved_source != approved
        or batch.binding_coordinates != recipe.binding_coordinates
        or batch.approved_only is not True
        or any(
            type(count) is not int or count < 0
            for count in (
                batch.source_rows,
                batch.membership_rows,
                batch.accepted_source_rows,
                batch.omitted_source_rows,
            )
        )
        or not 0 <= batch.source_rows <= limit <= 1000
        or batch.membership_rows > MAX_ROWS
        or batch.unresolved_rows
        or batch.unresolved_source_rows
        or batch.complete_raw_facts is not True
        or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256
        or len(batch.input_bytes) > MAX_INPUT_BYTES
        or batch.source_rows != batch.accepted_source_rows + batch.omitted_source_rows
        or (generation is not None and generation != batch.generation_id)
    ):
        raise ValueError("registry_aca_address_carry_accounting_invalid")
