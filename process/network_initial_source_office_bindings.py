# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bind exact ACA offices across closed sources, without claiming ingestion lineage."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
from dataclasses import asdict, dataclass
from uuid import UUID, uuid4

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_custom_address_source import _require_transaction, _source_identity
from process.network_legacy_membership_source import (
    ACAMembershipBatch,
    PinnedACAMembershipSource,
    read_aca_membership_batch,
)
from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate, _require_open
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, MembershipCopyTarget, _encode


class InitialACAOfficeBindingError(ValueError):
    """A complete office correspondence failed without retaining partial writes."""


@dataclass(frozen=True)
class PinnedACAOfficeAddressSource:
    """Trusted native authority captured before candidate construction."""

    address_source: PinnedAddressSource
    aca_generation_id: str
    address_identity: tuple[int, int, int, str]
    membership_generation_id: str
    membership_input_sha256: str
    selected_offices_sha256: str
    after_evidence_checksum: int | None
    office_count: int


async def pin_aca_office_address_source(
    connection,
    batch,
    address_source,
    *,
    runtime_roles,
    after_evidence_checksum=None,
    control_schema=None,
):
    """Pin closed native identities and actual selected office hashes before COPY."""
    _validate_batch(batch)
    if type(address_source) is not PinnedAddressSource:
        raise InitialACAOfficeBindingError("initial_aca_office_source_pin_invalid")
    if address_source.table_name != "entity_address_unified":
        raise InitialACAOfficeBindingError("initial_aca_office_source_pin_invalid")
    _validate_roles(runtime_roles)
    await _require_transaction(connection)
    await _verify_batch(connection, batch, _control_namespace(control_schema)[1:-1], after_evidence_checksum)
    identity = await _source_identity(connection, address_source, runtime_roles, address=True)
    office_records = await _office_records(connection, batch, address_source, after_evidence_checksum)
    bindings = await _canonical_bindings(connection, office_records)
    return PinnedACAOfficeAddressSource(
        address_source,
        batch.source.generation_id,
        identity,
        batch.generation_id,
        batch.input_sha256,
        _digest(office_records),
        after_evidence_checksum,
        len(bindings),
    )


_COLUMNS = ("provider_system", "provider_id", "location_id", "location_key", "entity_type", "entity_id")
_OFFICES_SQL = """WITH pairs AS MATERIALIZED (
 SELECT DISTINCT provider_id,location_id FROM jsonb_to_recordset($1::jsonb)
 AS item(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
), evidence AS MATERIALIZED (
 SELECT e.*,c.payload AS canonical FROM {source}.mrf_address_evidence e
 JOIN pairs p ON p.provider_id=e.npi::text AND p.location_id=e.address_key
 LEFT JOIN {source}.mrf_canonical_address c ON c.address_key=e.address_key
 WHERE e.import_id=$2 AND e.issuer_id=$3 AND e.year=$4 AND e.source_url=$5
   AND ($6::bigint IS NULL OR e.evidence_checksum>$6) AND e.evidence_checksum<=$7
), matched AS MATERIALIZED (
 SELECT e.*,a.location_key,a.entity_type,a.entity_id,a.address_precision,a.row_origin,
   a.type AS address_type,a.inferred_npi,to_jsonb(a) AS address,to_jsonb(e) AS evidence_json,
   encode(sha256(convert_to(to_jsonb(a)::text,'UTF8')),'hex') AS address_row_sha256,
   count(a.location_key) OVER(PARTITION BY e.evidence_checksum) AS matches
 FROM evidence e LEFT JOIN {address} a ON a.entity_type='npi' AND a.entity_id=e.npi::text
   AND a.npi=e.npi AND a.address_key=e.address_key
), documents AS (
 SELECT jsonb_build_object('provider_id',npi::text,'location_id',address_key::text,
   'location_key',location_key,'canonical',canonical,
   'evidence_address',jsonb_build_array(first_line,second_line,city_name,state_name,postal_code,country_code),
   'address',jsonb_build_array(address->'first_line',address->'second_line',address->'city_name',
     address->'state_name',address->'postal_code',address->'country_code'),
   'evidence_sha256',encode(sha256(convert_to(evidence_json::text,'UTF8')),'hex'),
   'address_row_sha256',address_row_sha256) AS document,
   matches<>1 OR canonical IS NULL OR canonical->>'address_key' IS DISTINCT FROM address_key::text
     OR canonical->>'precision' IS DISTINCT FROM 'street' OR canonical->>'merged_into' IS NOT NULL
     OR canonical->'source_bits' IS NULL OR ((canonical->>'source_bits')::integer & 16)<>16
     OR address->>'archive_identity_version' IS DISTINCT FROM ('v'||(canonical->>'identity_version'))
     OR address_precision IS DISTINCT FROM 'street' OR row_origin IS DISTINCT FROM 'base'
     OR address_type IS DISTINCT FROM 'practice' OR inferred_npi IS NOT NULL AS invalid
 FROM matched
)
SELECT (SELECT count(*) FROM pairs) AS pairs,
 (SELECT count(DISTINCT (npi,address_key)) FROM evidence) AS covered_pairs,
 count(*) AS evidence_rows,coalesce(bool_or(invalid),false) AS invalid,
 CASE WHEN count(*)<=5000 AND coalesce(sum(octet_length(document::text)),0)+2*count(*)+2<=8388608
 THEN coalesce(jsonb_agg(document ORDER BY document::text),'[]'::jsonb)::text END AS records_json
FROM documents"""


def _digest(document):
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


def _validate_roles(runtime_roles):
    if (
        type(runtime_roles) is not tuple
        or not 1 <= len(runtime_roles) <= 32
        or tuple(sorted(set(runtime_roles))) != runtime_roles
    ):
        raise InitialACAOfficeBindingError("initial_aca_office_roles_invalid")
    for role in runtime_roles:
        _identifier(role)


def _validate_batch(batch):
    if (
        type(batch) is not ACAMembershipBatch
        or batch.unresolved_rows
        or type(batch.source) is not PinnedACAMembershipSource
        or batch.source.prepared.manifest.importer_id != "mrf"
        or batch.source.prepared.ownership.auxiliary_oid is None
        or type(batch.input_bytes) is not bytes
        or len(batch.input_bytes) > MAX_INPUT_BYTES
        or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256
        or not 0 <= batch.source_rows <= MAX_ROWS
    ):
        raise InitialACAOfficeBindingError("initial_aca_office_input_invalid")


def _validate_input(batch, target, address_pin, runtime_roles):
    _validate_batch(batch)
    if type(target) is not MembershipCopyTarget or type(address_pin) is not PinnedACAOfficeAddressSource:
        raise InitialACAOfficeBindingError("initial_aca_office_input_invalid")
    address_source = address_pin.address_source
    if (
        type(address_source) is not PinnedAddressSource
        or address_source.table_name != "entity_address_unified"
        or address_pin.aca_generation_id != batch.source.generation_id
        or address_pin.membership_generation_id != batch.generation_id
        or address_pin.membership_input_sha256 != batch.input_sha256
        or type(address_pin.office_count) is not int
        or not 0 <= address_pin.office_count <= MAX_ROWS
    ):
        raise InitialACAOfficeBindingError("initial_aca_office_source_pin_differs")
    _validate_roles(runtime_roles)
    if target.schema_name in {address_source.schema_name, batch.source.prepared.ownership.schema_name}:
        raise InitialACAOfficeBindingError("initial_aca_office_source_not_external")


async def _verify_batch(connection, batch, control_schema, after):
    replay = await read_aca_membership_batch(
        connection,
        batch.source,
        registry_schema=control_schema,
        after_evidence_checksum=after,
        limit=max(1, batch.source_rows),
        approved_source=batch.approved_source,
        binding_coordinates=batch.binding_coordinates,
        approved_only=batch.approved_only,
    )
    if replay != batch:
        raise InitialACAOfficeBindingError("initial_aca_office_batch_differs")
    _, rows = await asyncio.to_thread(_encode, batch.input_bytes)
    if rows != batch.membership_rows:
        raise InitialACAOfficeBindingError("initial_aca_office_batch_accounting_invalid")


async def _candidate(connection, target, batch, address_source, control):
    candidate = await _locked_candidate(connection, target, control)
    _require_open(candidate)
    generations = candidate["source_generations"]
    if isinstance(generations, str):
        generations = json.loads(generations)
    if (
        generations.get("unified_address") != address_source.generation_id
        or batch.generation_id not in generations.values()
        or (
            batch.approved_source is not None
            and (
                candidate["approved_custom_revision"] != batch.approved_source.approved_revision
                or generations.get("custom_membership") != batch.approved_source.generation_id
            )
        )
    ):
        raise InitialACAOfficeBindingError("initial_aca_office_candidate_pin_differs")


async def _office_records(connection, batch, address_source, after):
    source_pin = batch.source
    result = await connection.fetchrow(
        _OFFICES_SQL.format(
            source=_identifier(source_pin.prepared.ownership.schema_name),
            address=f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}",
        ),
        batch.input_bytes.decode(),
        source_pin.import_id,
        source_pin.issuer_id,
        source_pin.year,
        source_pin.source_url,
        after,
        batch.next_evidence_checksum,
    )
    if result["invalid"] or result["pairs"] != result["covered_pairs"] or result["records_json"] is None:
        raise InitialACAOfficeBindingError("initial_aca_office_unresolved")
    return json.loads(result["records_json"])


_CANONICAL_BINDINGS_SQL = """WITH offices AS MATERIALIZED (
 SELECT office,ordinal FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY item(office,ordinal)
), canonical AS MATERIALIZED (
 SELECT office,($2::jsonb)->(ordinal::integer-1) AS source_address,
   ($2::jsonb)->(ordinal::integer-1+(SELECT count(*) FROM offices)::integer) AS retained_address
 FROM offices
), checked AS MATERIALIZED (
 SELECT *,source_address=retained_address
   AND source_address->>'identity_key' LIKE '%|street'
   AND source_address->>'address_key'=office->>'location_id'
   AND (source_address-'premise_identity_key') <@ (office->'canonical')
   AND office->'canonical'->'identity_version'=to_jsonb($3::integer)
   AND office->>'location_key' ~ '^[0-9a-f]{64}$' AS valid FROM canonical
), bindings AS MATERIALIZED (
 SELECT DISTINCT 'npi'::text AS provider_system,office->>'provider_id' AS provider_id,
   office->>'location_id' AS location_id,office->>'location_key' AS location_key,
   'npi'::text AS entity_type,office->>'provider_id' AS entity_id FROM checked
)
SELECT NOT EXISTS(SELECT 1 FROM checked WHERE valid IS DISTINCT FROM true)
  AND (SELECT count(*) FROM bindings)=(SELECT count(DISTINCT (provider_id,location_id)) FROM bindings)
  AND jsonb_array_length($2::jsonb)=2*(SELECT count(*) FROM offices) AS valid,
  coalesce(jsonb_agg(to_jsonb(bindings) ORDER BY provider_id,location_id),'[]'::jsonb)::text AS bindings_json
FROM bindings"""


async def _canonical_bindings(connection, office_records):
    native = importlib.import_module("ptg2_address_canon")
    source_addresses = [tuple(office["evidence_address"]) for office in office_records]
    retained_addresses = [tuple(office["address"]) for office in office_records]
    canonical_addresses = await asyncio.to_thread(native.canonicalize_batch, source_addresses + retained_addresses)
    canonical_json = json.dumps(canonical_addresses, separators=(",", ":"), allow_nan=False)
    if len(canonical_json.encode()) > 2 * MAX_INPUT_BYTES:
        raise InitialACAOfficeBindingError("initial_aca_office_canonical_bound_exceeded")
    result = await connection.fetchrow(
        _CANONICAL_BINDINGS_SQL,
        json.dumps(office_records, separators=(",", ":")),
        canonical_json,
        native.canon_version()["identity_version"],
    )
    if result["valid"] is not True:
        raise InitialACAOfficeBindingError("initial_aca_office_canonical_identity_differs")
    return [
        (
            binding["provider_system"],
            binding["provider_id"],
            UUID(binding["location_id"]),
            binding["location_key"],
            binding["entity_type"],
            binding["entity_id"],
        )
        for binding in json.loads(result["bindings_json"])
    ]


async def _copy_bindings(connection, target, bindings):
    namespace = _identifier(target.schema_name)
    columns = ",".join(_COLUMNS)
    await connection.execute(f"""CREATE TABLE IF NOT EXISTS {namespace}.provider_location_binding(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))""")
    temporary = "initial_aca_offices_" + uuid4().hex
    await connection.execute(f"CREATE TEMP TABLE {_identifier(temporary)} (LIKE {namespace}.provider_location_binding)")
    status = await connection.copy_records_to_table(
        temporary, records=bindings, columns=_COLUMNS, schema_name="pg_temp"
    )
    if status != f"COPY {len(bindings)}":
        raise InitialACAOfficeBindingError("initial_aca_office_copy_accounting_invalid")
    await connection.execute(
        f"INSERT INTO {namespace}.provider_location_binding({columns}) "
        f"SELECT {columns} FROM {_identifier(temporary)} ON CONFLICT DO NOTHING"
    )
    differs = await connection.fetchval(f"""SELECT EXISTS(SELECT 1 FROM {_identifier(temporary)} expected
      LEFT JOIN {namespace}.provider_location_binding actual USING(provider_system,provider_id,location_id)
      WHERE actual.location_key IS NULL OR (actual.location_key,actual.entity_type,actual.entity_id)
        IS DISTINCT FROM (expected.location_key,expected.entity_type,expected.entity_id))""")
    if differs:
        raise InitialACAOfficeBindingError("initial_aca_office_binding_conflict")
    await connection.execute(f"DROP TABLE {_identifier(temporary)}")


async def copy_initial_aca_office_bindings(
    connection,
    batch,
    copy_target,
    address_pin,
    *,
    runtime_roles,
    after_evidence_checksum=None,
    control_schema=None,
):
    """COPY bounded exact offices under a caller RR transaction and local savepoint.

    The receipt proves correspondence between admitted immutable ACA evidence
    and selected immutable EUA offices. It does not claim original EUA ingestion
    from that edition. Generation names alone never authorize a correspondence.
    Candidate metadata must retain both pins; the integrating publisher persists
    the receipt and closes all candidate writers before validation/publication.
    """
    _validate_input(batch, copy_target, address_pin, runtime_roles)
    if after_evidence_checksum != address_pin.after_evidence_checksum:
        raise InitialACAOfficeBindingError("initial_aca_office_cursor_differs")
    address_source = address_pin.address_source
    await _require_transaction(connection)
    control = _control_namespace(control_schema)
    async with connection.transaction():
        await _candidate(connection, copy_target, batch, address_source, control)
        await _verify_batch(connection, batch, control[1:-1], after_evidence_checksum)
        address_identity = await _source_identity(connection, address_source, runtime_roles, address=True)
        if address_identity != address_pin.address_identity:
            raise InitialACAOfficeBindingError("initial_aca_office_native_identity_differs")
        office_records = await _office_records(connection, batch, address_source, after_evidence_checksum)
        if _digest(office_records) != address_pin.selected_offices_sha256:
            raise InitialACAOfficeBindingError("initial_aca_office_rows_differ")
        bindings = await _canonical_bindings(connection, office_records)
        if len(bindings) != address_pin.office_count:
            raise InitialACAOfficeBindingError("initial_aca_office_count_differs")
        await _copy_bindings(connection, copy_target, bindings)
        await _candidate(connection, copy_target, batch, address_source, control)
        return _correspondence_receipt(copy_target, batch, address_source, address_identity, bindings, office_records)


def _correspondence_receipt(copy_target, batch, address_source, address_identity, bindings, office_records):
    source_pin = batch.source
    ownership = source_pin.prepared.ownership
    receipt_by_field = {
        "component": "initial_aca_office_correspondence",
        "revision": 1,
        "candidate": asdict(copy_target),
        "source_generation": batch.generation_id,
        "aca_source": {
            "generation": source_pin.generation_id,
            "coordinates": source_pin.coordinates,
            "manifest_sha256": source_pin.validation.manifest_sha256,
            "validation_sha256": source_pin.validation.validation_sha256,
            "schema_oid": ownership.schema_oid,
            "relation_oids": list(ownership.relation_oids),
            "canonical_address_oid": ownership.auxiliary_oid,
            "owner_oid": source_pin.validation.sealed_owner_oid,
        },
        "address_source": asdict(address_source),
        "address_identity": list(address_identity),
        "membership_input_sha256": batch.input_sha256,
        "office_count": len(bindings),
        "selected_offices_sha256": _digest(office_records),
    }
    receipt_by_field["correspondence_sha256"] = _digest(receipt_by_field)
    return receipt_by_field
