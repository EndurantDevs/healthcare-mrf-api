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
from process.network_custom_address_source import _artifact_dependency_identity, _require_transaction
from process.network_legacy_membership_source import (
    ACAGlobalEpochReadScope,
    ACAMembershipBatch,
    ACARawFactPosition,
    PinnedACAMembershipSource,
    _read_aca_batch,
    aca_physical_source,
    read_aca_membership_batch,
    read_aca_raw_fact_batch,
)
from process.network_membership_candidate_lifecycle import _control_namespace, _locked_candidate, _require_open
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, MembershipCopyTarget, _encode
from process.reference_family_archive import CONTRACT, MODEL_CLOSURE_CONTRACTS, TYPED_MRF_CONTRACT
from process.registry_source_office_origin import resolve_source_office_origin
from process.uhc_flex_practitioner_async_safety import drain_operation


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
    after_source_position: ACARawFactPosition | None = None
    origin_manifest_sha256: str | None = None


async def capture_aca_office_address_source(
    connection,
    batch,
    address_source,
    *,
    runtime_roles,
    after_evidence_checksum=None,
    after_source_position=None,
    control_schema=None,
    origin_manifest_sha256=None,
):
    """Pin closed native identities and actual selected office hashes before COPY."""
    _validate_batch(batch)
    if type(address_source) is not PinnedAddressSource:
        raise InitialACAOfficeBindingError("initial_aca_office_source_pin_invalid")
    if address_source.table_name != "entity_address_unified":
        raise InitialACAOfficeBindingError("initial_aca_office_source_pin_invalid")
    _validate_roles(runtime_roles)
    await _require_transaction(connection)
    await _verify_batch(
        connection, batch, _control_namespace(control_schema)[1:-1], after_evidence_checksum, after_source_position
    )
    origin = await resolve_source_office_origin(
        connection,
        address_source,
        runtime_roles=runtime_roles,
        control_schema=_control_namespace(control_schema)[1:-1],
        origin_manifest_sha256=origin_manifest_sha256,
        first_parameter=7,
    )
    office_records = await _office_records(
        connection, batch, address_source, address_relation=origin.relation_sql, address_arguments=origin.arguments
    )
    bindings = await _canonical_bindings(connection, office_records)
    pin = PinnedACAOfficeAddressSource(
        address_source,
        batch.source.generation_id,
        origin.address_identity,
        batch.generation_id,
        batch.input_sha256,
        _digest(office_records),
        after_evidence_checksum,
        len(bindings),
        after_source_position,
        origin_manifest_sha256,
    )
    return pin, bindings


async def pin_aca_office_address_source(
    connection,
    batch,
    address_source,
    *,
    runtime_roles,
    after_evidence_checksum=None,
    after_source_position=None,
    control_schema=None,
    origin_manifest_sha256=None,
):
    """Pin exact native address correspondence without retaining the selected rows."""
    pin, _ = await capture_aca_office_address_source(
        connection,
        batch,
        address_source,
        runtime_roles=runtime_roles,
        after_evidence_checksum=after_evidence_checksum,
        after_source_position=after_source_position,
        control_schema=control_schema,
        origin_manifest_sha256=origin_manifest_sha256,
    )
    return pin


_COLUMNS = ("provider_system", "provider_id", "location_id", "location_key", "entity_type", "entity_id")
_OFFICES_SQL = """WITH pairs AS MATERIALIZED (
 SELECT DISTINCT provider_id,location_id FROM jsonb_to_recordset($1::jsonb)
 AS item(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
), evidence AS MATERIALIZED (
 SELECT e.*,{canonical} AS canonical FROM {source}.mrf_address_evidence e
 JOIN pairs p ON p.provider_id=e.npi::text AND p.location_id=e.address_key
 LEFT JOIN {source}.mrf_canonical_address c ON c.address_key=e.address_key
 WHERE e.import_id=$2 AND e.issuer_id=$3 AND e.year=$4 AND e.source_url=$5
   AND e.evidence_checksum=ANY($6::bigint[])
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


_TYPED_CANONICAL_FIELDS = (
    "address_key",
    "identity_key",
    "premise_key",
    "line1_norm",
    "unit_norm",
    "city_norm",
    "state_code",
    "zip5",
    "zip4",
    "country_code",
    "identity_version",
    "precision",
    "source_bits",
    "merged_into",
)
_TYPED_CANONICAL_SQL = (
    "jsonb_build_object(" + ",".join("'" + name + "',c." + name for name in _TYPED_CANONICAL_FIELDS) + ")"
)


def _canonical_source(source_pin):
    """Choose the exact closed model contract and its recorded native relation."""
    if type(source_pin) is PinnedACAMembershipSource:
        source_pin = aca_physical_source(source_pin)
    manifest = source_pin.prepared.manifest
    ownership = source_pin.prepared.ownership
    contract = manifest.as_dict()["contract"]
    relation_oid_by_name = dict(ownership.relation_oids)
    model_contract = (manifest.auxiliary or {}).get("contract")
    if contract == TYPED_MRF_CONTRACT and model_contract in MODEL_CLOSURE_CONTRACTS:
        canonical_oid = relation_oid_by_name.get("mrf_canonical_address")
        canonical_sql = _TYPED_CANONICAL_SQL
        has_valid_layout = ownership.auxiliary_oid is None
    elif contract == CONTRACT and model_contract is None:
        canonical_oid = ownership.auxiliary_oid
        canonical_sql = "c.payload"
        has_valid_layout = "mrf_canonical_address" not in relation_oid_by_name
    else:
        raise InitialACAOfficeBindingError("initial_aca_office_input_invalid")
    if not has_valid_layout or type(canonical_oid) is not int or not 0 < canonical_oid < 2**32:
        raise InitialACAOfficeBindingError("initial_aca_office_input_invalid")
    return canonical_oid, canonical_sql


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
        or type(batch.input_bytes) is not bytes
        or len(batch.input_bytes) > MAX_INPUT_BYTES
        or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256
        or not 0 <= batch.source_rows <= MAX_ROWS
    ):
        raise InitialACAOfficeBindingError("initial_aca_office_input_invalid")
    _canonical_source(batch.source)


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
    if target.schema_name in {
        address_source.schema_name,
        batch.source.prepared.ownership.schema_name,
        aca_physical_source(batch.source).prepared.ownership.schema_name,
    }:
        raise InitialACAOfficeBindingError("initial_aca_office_source_not_external")


async def _verify_batch(connection, batch, control_schema, after, after_source_position=None):
    reader = read_aca_raw_fact_batch if batch.complete_raw_facts else read_aca_membership_batch
    cursor_options = (
        {"after_source_position": after_source_position}
        if batch.complete_raw_facts
        else {"after_evidence_checksum": after}
    )
    if batch.complete_raw_facts and after is not None:
        raise InitialACAOfficeBindingError("initial_aca_office_cursor_differs")
    if batch.verified_source is None:
        replay = await reader(
            connection,
            batch.source,
            registry_schema=control_schema,
            **cursor_options,
            limit=max(1, batch.source_rows),
            approved_source=batch.approved_source,
            binding_coordinates=batch.binding_coordinates,
            approved_only=batch.approved_only,
        )
    else:
        replay = await _read_aca_batch(
            connection,
            batch.source,
            control_schema,
            batch.approved_source,
            batch.binding_coordinates,
            batch.approved_only,
            (after, max(1, batch.source_rows), after_source_position, batch.complete_raw_facts),
            batch.verified_source,
        )
    if replay != batch:
        raise InitialACAOfficeBindingError("initial_aca_office_batch_differs")
    _, encoded_rows = await drain_operation(asyncio.to_thread(_encode, batch.input_bytes), preserve_cancellation=True)
    if encoded_rows != batch.membership_rows:
        raise InitialACAOfficeBindingError("initial_aca_office_batch_accounting_invalid")


async def _candidate(connection, target, batch, address_source, control, *, origin_manifest_sha256=None):
    candidate = await _locked_candidate(connection, target, control)
    _require_open(candidate)
    generations = candidate["source_generations"]
    if isinstance(generations, str):
        generations = json.loads(generations)
    if (
        generations.get("unified_address") != address_source.generation_id
        or (origin_manifest_sha256 is not None and generations.get("retained_network_source") != origin_manifest_sha256)
        or (
            batch.approved_source is not None
            and (
                candidate["approved_custom_revision"] != batch.approved_source.approved_revision
                or generations.get("custom_membership") != batch.approved_source.generation_id
            )
        )
    ):
        raise InitialACAOfficeBindingError("initial_aca_office_candidate_pin_differs")
    recipes = candidate.get("source_recipes_json")
    has_inventory = type(recipes) is dict or type(recipes) is str and recipes.lstrip().startswith("{")
    matches = (
        await _is_inventory_pin_registered(connection, target, candidate, batch, control)
        if has_inventory
        else batch.generation_id in generations.values()
    )
    if not matches:
        raise InitialACAOfficeBindingError("initial_aca_office_candidate_pin_differs")


def _office_recipe(batch):
    from process.network_approved_membership_source import ApprovedMembershipSource
    from process.network_fhir_membership_source import FHIRMembershipBatch
    from process.registry_ptg_office_membership import PTGOfficeMembershipBatch
    from process.registry_source_recipe_store import RegistrySourceMembershipRecipe

    if type(batch.approved_source) is not ApprovedMembershipSource:
        raise InitialACAOfficeBindingError("initial_aca_office_candidate_pin_differs")
    if type(batch) is PTGOfficeMembershipBatch:
        coordinates = batch.source.coordinates
    elif type(batch) in {ACAMembershipBatch, FHIRMembershipBatch} and batch.approved_only is True:
        coordinates = batch.binding_coordinates
    else:
        raise InitialACAOfficeBindingError("initial_aca_office_candidate_pin_differs")
    return RegistrySourceMembershipRecipe(batch.source, coordinates)


_REGISTERED_PIN_SQL = """SELECT count(*) AS entry_count,max(ordinal) AS ordinal,
 max(octet_length(recipe_json)) AS recipe_bytes,
 coalesce(bool_and(CASE WHEN octet_length(recipe_json) BETWEEN 1 AND $11::integer THEN
   convert_to(recipe_json,'UTF8')=convert_to($2::text,'UTF8') AND
   office_derivation_json IS NOT NULL AND (contribution_json IS NOT NULL)=($3::text='fhir') AND
   (source_system,source_id,dataset_schema,dataset_id,producer_id,edition_id,fhir_scope_key,fhir_scope_value)
   IS NOT DISTINCT FROM ($3::text,$4::text,$5::text,$6::text,$7::text,$8::text,$9::text,$10::text)
   ELSE false END),false) AS is_exact
 FROM (SELECT ordinal,recipe_json,contribution_json,office_derivation_json,source_system,source_id,dataset_schema,dataset_id,
   producer_id,edition_id,fhir_scope_key,fhir_scope_value FROM {relation}
   WHERE recipe_sha256=$1 ORDER BY ordinal LIMIT 2) selected"""


async def _is_inventory_pin_registered(connection, copy_target, candidate, batch, control):
    """Check one raw registration row, never Source authority or membership in the complete root.

    The producer folds the complete inventory before office COPY in this locked
    preparation transaction. Selected metadata can still be tampered with while
    OPEN; Source admission/page replay remain mandatory below, and independent
    complete-root/Source/office custody must succeed before publication.
    """
    from process.registry_source_inventory_contract import make_source_inventory_entry
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_store import _physical_context, _relation, source_inventory_record
    from process.registry_source_recipe_store import MAX_SOURCE_RECIPE_BYTES

    root = verify_source_recipe_metadata(candidate).inventory.root
    if (
        root.entry_count < 1
        or root.derivation_count != root.entry_count
        or await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}
    ):
        return False
    pin_record = source_inventory_record(make_source_inventory_entry(1, _office_recipe(batch)))
    if (pin_record[5] == "fhir" and root.contribution_count < 1) or (
        pin_record[5] != "fhir" and root.contribution_count >= root.entry_count
    ):
        return False
    oid = await connection.fetchval("SELECT to_regclass($1)::oid::bigint", _relation(copy_target))
    relation = await _physical_context(connection, copy_target, oid, control[1:-1])
    selected = await connection.fetchrow(
        _REGISTERED_PIN_SQL.format(relation=relation),
        pin_record[4],
        pin_record[1],
        *pin_record[5:],
        MAX_SOURCE_RECIPE_BYTES - 2,
    )
    return (
        selected is not None
        and type(selected["entry_count"]) is int
        and selected["entry_count"] == 1
        and type(selected["ordinal"]) is int
        and 1 <= selected["ordinal"] <= root.entry_count
        and type(selected["recipe_bytes"]) is int
        and 1 <= selected["recipe_bytes"] <= MAX_SOURCE_RECIPE_BYTES - 2
        and selected["is_exact"] is True
    )


async def _office_records(connection, batch, address_source, *, address_relation=None, address_arguments=()):
    source_pin = aca_physical_source(batch.source) if type(batch.source) is PinnedACAMembershipSource else batch.source
    source_sql = _OFFICES_SQL
    if type(source_pin) is ACAGlobalEpochReadScope:
        source_sql = source_sql.replace("e.import_id=$2", "($2::text IS NULL OR e.import_id=$2)")
        source_sql = source_sql.replace("e.source_url=$5", "($5::text IS NULL OR e.source_url=$5)")
    result = await connection.fetchrow(
        source_sql.format(
            source=_identifier(source_pin.prepared.ownership.schema_name),
            canonical=_canonical_source(source_pin)[1],
            address=address_relation
            or f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}",
        ),
        batch.input_bytes.decode(),
        source_pin.import_id,
        source_pin.issuer_id,
        source_pin.year,
        source_pin.source_url,
        list(batch.accepted_evidence_checksums),
        *address_arguments,
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
    canonical_addresses = await drain_operation(
        asyncio.to_thread(native.canonicalize_batch, source_addresses + retained_addresses), preserve_cancellation=True
    )
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


async def copy_source_office_binding_rows(connection, schema_name, table_name, bindings):
    """COPY verified page rows; admission authority belongs to each enclosing caller."""
    namespace = _identifier(schema_name)
    relation = namespace + "." + _identifier(table_name)
    columns = ",".join(_COLUMNS)
    temporary_ddl = "TEMP " if schema_name == "pg_temp" else ""
    await connection.execute(f"""CREATE {temporary_ddl}TABLE IF NOT EXISTS {relation}(
      provider_system text NOT NULL,provider_id text NOT NULL,location_id uuid NOT NULL,
      location_key varchar(64) NOT NULL,entity_type text NOT NULL,entity_id text NOT NULL,
      PRIMARY KEY(provider_system,provider_id,location_id))""")
    temporary = "initial_aca_offices_" + uuid4().hex
    await connection.execute(f"CREATE TEMP TABLE {_identifier(temporary)} (LIKE {relation}) ON COMMIT DROP")
    status = await connection.copy_records_to_table(
        temporary, records=bindings, columns=_COLUMNS, schema_name="pg_temp"
    )
    if status != f"COPY {len(bindings)}":
        raise InitialACAOfficeBindingError("initial_aca_office_copy_accounting_invalid")
    await connection.execute(
        f"INSERT INTO {relation}({columns}) SELECT {columns} FROM {_identifier(temporary)} ON CONFLICT DO NOTHING"
    )
    differs = await connection.fetchval(f"""SELECT EXISTS(SELECT 1 FROM {_identifier(temporary)} expected
      LEFT JOIN {relation} actual USING(provider_system,provider_id,location_id)
      WHERE actual.location_key IS NULL OR (actual.location_key,actual.entity_type,actual.entity_id)
        IS DISTINCT FROM (expected.location_key,expected.entity_type,expected.entity_id))""")
    if differs:
        raise InitialACAOfficeBindingError("initial_aca_office_binding_conflict")
    await connection.execute(f"DROP TABLE {_identifier(temporary)}")


async def _copy_bindings(connection, target, bindings):
    await copy_source_office_binding_rows(connection, target.schema_name, "provider_location_binding", bindings)


def _require_matching_aca_office_pin(actual, expected, bindings):
    if actual.address_identity != expected.address_identity:
        raise InitialACAOfficeBindingError("initial_aca_office_native_identity_differs")
    if actual.selected_offices_sha256 != expected.selected_offices_sha256:
        raise InitialACAOfficeBindingError("initial_aca_office_rows_differ")
    if len(bindings) != expected.office_count:
        raise InitialACAOfficeBindingError("initial_aca_office_count_differs")


async def copy_initial_aca_office_bindings(
    connection,
    batch,
    copy_target,
    address_pin,
    *,
    runtime_roles,
    after_evidence_checksum=None,
    after_source_position=None,
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
    if (
        after_evidence_checksum != address_pin.after_evidence_checksum
        or after_source_position != address_pin.after_source_position
    ):
        raise InitialACAOfficeBindingError("initial_aca_office_cursor_differs")
    address_source = address_pin.address_source
    await _require_transaction(connection)
    control = _control_namespace(control_schema)
    async with connection.transaction():
        await _candidate(
            connection,
            copy_target,
            batch,
            address_source,
            control,
            origin_manifest_sha256=address_pin.origin_manifest_sha256,
        )
        actual, bindings = await capture_aca_office_address_source(
            connection,
            batch,
            address_source,
            runtime_roles=runtime_roles,
            control_schema=control[1:-1],
            after_evidence_checksum=after_evidence_checksum,
            after_source_position=after_source_position,
            origin_manifest_sha256=address_pin.origin_manifest_sha256,
        )
        _require_matching_aca_office_pin(actual, address_pin, bindings)
        await _copy_bindings(connection, copy_target, bindings)
        await _candidate(
            connection,
            copy_target,
            batch,
            address_source,
            control,
            origin_manifest_sha256=address_pin.origin_manifest_sha256,
        )
        return _correspondence_receipt(copy_target, batch, actual, bindings)


def _correspondence_receipt(copy_target, batch, address_pin, bindings):
    source_pin = aca_physical_source(batch.source) if type(batch.source) is PinnedACAMembershipSource else batch.source
    ownership = source_pin.prepared.ownership
    receipt_by_field = {
        "component": "initial_aca_office_correspondence",
        "revision": 2 if address_pin.origin_manifest_sha256 is not None else 1,
        "candidate": asdict(copy_target),
        "source_generation": batch.generation_id,
        "aca_source": {
            "generation": batch.source.generation_id,
            "coordinates": source_pin.coordinates,
            "manifest_sha256": source_pin.validation.manifest_sha256,
            "validation_sha256": source_pin.validation.validation_sha256,
            "schema_oid": ownership.schema_oid,
            "relation_oids": list(ownership.relation_oids),
            "canonical_address_oid": _canonical_source(source_pin)[0],
            "owner_oid": source_pin.validation.sealed_owner_oid,
        },
        "address_source": asdict(address_pin.address_source),
        "address_identity": list(address_pin.address_identity),
        "membership_input_sha256": batch.input_sha256,
        "office_count": len(bindings),
        "selected_offices_sha256": address_pin.selected_offices_sha256,
    }
    if address_pin.origin_manifest_sha256 is not None:
        receipt_by_field["origin_manifest_sha256"] = address_pin.origin_manifest_sha256
    receipt_by_field["correspondence_sha256"] = _digest(receipt_by_field)
    return receipt_by_field
