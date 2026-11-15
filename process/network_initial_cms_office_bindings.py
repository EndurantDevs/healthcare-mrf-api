# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prove exact CMS Location correspondence with closed unified offices."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import asdict, dataclass
from uuid import UUID, uuid4

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_custom_address_source import _require_transaction
from process.network_fhir_membership_source import (
    FHIRMembershipBatch,
    PinnedFHIRMembershipSource,
    RetainedFHIRSuccessorReads,
    fhir_normalized_capture,
    fhir_physical_source,
    read_fhir_membership_batch,
    read_retained_fhir_membership_batch,
)
from process.network_initial_source_office_bindings import _candidate, _copy_bindings
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, MembershipCopyTarget, _encode
from process.registry_source_office_errors import SourceOfficeNotReady
from process.registry_source_office_origin import resolve_source_office_origin, source_office_pin_document
from process.registry_source_recipe_composition import _require_recipe_custody
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from process.uhc_flex_practitioner_async_safety import drain_operation


class InitialCMSOfficeBindingError(ValueError):
    """Complete source custody or exact office correspondence is unavailable."""


@dataclass(frozen=True)
class PinnedCMSOfficeAddressSource:
    address_source: PinnedAddressSource
    address_scope: tuple[str, str, str, str | None]
    source_generation_id: str
    membership_generation_id: str
    membership_input_sha256: str
    selected_offices_sha256: str
    after_resource: tuple[str, str] | None
    office_count: int
    origin_manifest_sha256: str | None = None


def _digest(document):
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


async def _verify_batch(connection, batch, after_resource, control_schema, retained_read=None):
    if (
        type(batch) is not FHIRMembershipBatch
        or type(batch.source) is not PinnedFHIRMembershipSource
        or (batch.source.source_id != "cms-npd" and batch.source.retained_capture is None)
        or not batch.approved_only
        or batch.unresolved_rows
        or type(batch.input_bytes) is not bytes
        or len(batch.input_bytes) > MAX_INPUT_BYTES
        or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_input_invalid")
    recipe = RegistrySourceMembershipRecipe(batch.source, batch.binding_coordinates)
    if batch.source.retained_capture is None:
        await _require_recipe_custody(connection, recipe)
    elif retained_read is not None:
        await _verify_retained_office_read(connection, batch.source, retained_read)
    options_by_name = {
        "after_resource": after_resource,
        "limit": max(1, batch.source_rows),
        "registry_schema": control_schema,
        "approved_source": batch.approved_source,
        "binding_coordinates": batch.binding_coordinates,
    }
    if batch.source.retained_capture is not None:
        replay = await read_retained_fhir_membership_batch(connection, batch.source, retained_read, **options_by_name)
    else:
        replay = await read_fhir_membership_batch(connection, batch.source, approved_only=True, **options_by_name)

    if replay != batch:
        raise InitialCMSOfficeBindingError("initial_cms_office_batch_differs")
    _, count = await drain_operation(asyncio.to_thread(_encode, batch.input_bytes), preserve_cancellation=True)
    if count != batch.membership_rows:
        raise InitialCMSOfficeBindingError("initial_cms_office_accounting_invalid")


async def _verify_retained_office_read(connection, source_pin, retained_read):
    """Verify the native Source or Reader capability before reading actual Location payloads."""
    from process.network_fhir_retained_capture import RetainedFHIRRead

    if type(source_pin) is not PinnedFHIRMembershipSource or fhir_normalized_capture(source_pin) is None:
        raise InitialCMSOfficeBindingError("initial_retained_mapping_source_invalid")
    if source_pin.source_witness_capture is not None:
        from process.network_fhir_source_witness_capture import SourceWitnessRead
        from process.network_fhir_source_witness_contract import require_capture_anchor

        if type(retained_read) is not SourceWitnessRead or source_pin.retained_successor is not None:
            raise InitialCMSOfficeBindingError("initial_retained_mapping_source_invalid")
        require_capture_anchor(source_pin.source_witness_capture, source_pin.source_witness_anchor)
        retained_read.require_ready(source_pin.source_witness_capture)
        return
    physical = fhir_physical_source(source_pin)
    if physical is source_pin:
        if type(retained_read) is not RetainedFHIRRead:
            raise InitialCMSOfficeBindingError("initial_retained_mapping_source_invalid")
        retained_read.require_ready(source_pin.retained_capture)
        return
    if (
        type(retained_read) is not RetainedFHIRSuccessorReads
        or type(retained_read.original) is not RetainedFHIRRead
        or type(retained_read.physical) is not RetainedFHIRRead
        or physical.retained_capture is None
    ):
        raise InitialCMSOfficeBindingError("initial_retained_mapping_source_invalid")
    retained_read.original.require_ready(source_pin.retained_capture)
    retained_read.physical.require_ready(physical.retained_capture)


_SOURCE_SQL = """WITH pairs AS MATERIALIZED (
 SELECT DISTINCT provider_system,provider_id,location_id FROM jsonb_to_recordset($1::jsonb)
 AS member(network_id integer,provider_system text,provider_id text,location_id uuid,evidence_id text)
), offices AS MATERIALIZED (
 SELECT pair.*,location.resource_id,location.payload_hash,location.payload_json::jsonb AS document
 FROM pairs pair LEFT JOIN {source}.provider_directory_entity_source_binding binding
 ON binding.source_id=$2 AND binding.resource_type='Location' AND binding.site_id=pair.location_id
 LEFT JOIN {source}.provider_directory_dataset_resource location
 ON location.dataset_id=$3 AND location.resource_type='Location' AND location.resource_id=binding.resource_id
), encoded AS (
 SELECT jsonb_build_object('provider_id',provider_id,'location_id',location_id::text,
 'resource_id',resource_id,'payload_sha256',payload_hash,
 'source_address',jsonb_build_array(document->'first_line',document->'second_line',document->'city_name',
 document->'state_name',document->'postal_code',document->'country_code'))
 ||CASE WHEN provider_system='npi' THEN '{{}}'::jsonb
 ELSE jsonb_build_object('provider_system',provider_system) END AS office,
 resource_id IS NULL OR provider_system NOT IN ('npi','provider_directory') AS invalid FROM offices
)
SELECT (SELECT count(*) FROM pairs) AS pair_count,count(*) AS source_count,
 coalesce(bool_or(invalid),false) AS invalid,
 CASE WHEN count(*)<=5000 AND coalesce(sum(octet_length(office::text)),0)+2*count(*)+2<=8388608
 THEN coalesce(jsonb_agg(office ORDER BY office::text),'[]'::jsonb)::text END AS offices_json FROM encoded"""

_MATCH_BODY = """matched AS MATERIALIZED (
 SELECT {page_column}office,canonical,address.location_key,to_jsonb(address) AS address,
 count(address.location_key) OVER(PARTITION BY {page_column}ordinal) AS matches
 FROM offices LEFT JOIN {address} address ON address.entity_type=coalesce(office->>'provider_system','npi')
 AND address.entity_id=office->>'provider_id'
 AND CASE WHEN coalesce(office->>'provider_system','npi')='npi'
 THEN address.npi=(office->>'provider_id')::bigint ELSE address.npi IS NULL END
 AND address.address_key=(canonical->>'address_key')::uuid
 AND address.address_precision='street' AND address.type='practice' AND address.inferred_npi IS NULL
), encoded AS (
 SELECT {page_column}office||jsonb_build_object('canonical',canonical,'location_key',location_key,
 'address',jsonb_build_array(address->'first_line',address->'second_line',address->'city_name',
 address->'state_name',address->'postal_code',address->'country_code'),
 'address_row_sha256',encode(sha256(convert_to(address::text,'UTF8')),'hex')) AS office,matches,
 matches>1 OR canonical IS NULL OR jsonb_typeof(canonical) IS DISTINCT FROM 'object'
 OR canonical->>'identity_key' IS NULL OR canonical->>'identity_key' NOT LIKE '%|street'
 OR canonical->>'address_key' IS NULL
 OR (matches=1 AND (location_key IS NULL OR location_key !~ '^[0-9a-f]{{64}}$')) AS invalid FROM matched
)"""

_MATCH_SQL = (
    """WITH offices AS MATERIALIZED (
 SELECT office,ordinal,($2::jsonb)->(ordinal::integer-1) AS canonical
 FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY item(office,ordinal)
), """
    + _MATCH_BODY.replace("{page_column}", "")
    + """
SELECT coalesce(bool_or(invalid),false) AS invalid,count(*) FILTER(WHERE matches=0) AS missing_count,
 CASE WHEN count(*)<=5000 AND coalesce(sum(octet_length(office::text)),0)+2*count(*)+2<=8388608
 THEN coalesce(jsonb_agg(office ORDER BY office::text) FILTER(WHERE matches=1),'[]'::jsonb)::text END AS offices_json FROM encoded"""
)


_CHECK_SQL = """WITH offices AS MATERIALIZED (
 SELECT office,ordinal FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY item(office,ordinal)
), checked AS (
 SELECT office,office->'canonical'=($2::jsonb)->(ordinal::integer-1) AS valid FROM offices
), bindings AS (
 SELECT DISTINCT coalesce(office->>'provider_system','npi') AS provider_system,office->>'provider_id' AS provider_id,
 office->>'location_id' AS location_id,office->>'location_key' AS location_key,
 coalesce(office->>'provider_system','npi') AS entity_type,office->>'provider_id' AS entity_id FROM checked
)
SELECT NOT EXISTS(SELECT 1 FROM checked WHERE valid IS DISTINCT FROM true)
 AND jsonb_array_length($2::jsonb)=(SELECT count(*) FROM offices)
 AND (SELECT count(*) FROM bindings)=(SELECT count(DISTINCT (provider_system,provider_id,location_id)) FROM bindings)
 AND NOT EXISTS(SELECT FROM checked GROUP BY office->>'location_key'
   HAVING count(DISTINCT office->'canonical')>1 OR count(DISTINCT office->'address')>1
     OR count(DISTINCT office->>'address_row_sha256')>1)
 AND NOT EXISTS(SELECT FROM bindings GROUP BY location_key
   HAVING count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1) AS valid,
 coalesce(jsonb_agg(to_jsonb(bindings) ORDER BY provider_id,location_id),'[]'::jsonb)::text AS bindings_json
 FROM bindings"""


async def _canonicalize(addresses):
    native = importlib.import_module("ptg2_address_canon")
    canonical = await drain_operation(
        asyncio.to_thread(native.canonicalize_batch, addresses), preserve_cancellation=True
    )
    encoded = json.dumps(canonical, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > 2 * MAX_INPUT_BYTES:
        raise InitialCMSOfficeBindingError("initial_cms_office_canonical_bound_exceeded")
    return encoded


async def _source_offices(connection, batch):
    source_summary = await connection.fetchrow(
        _SOURCE_SQL.format(source=_identifier(batch.source.read_schema_name)),
        batch.input_bytes.decode(),
        batch.source.source_id,
        fhir_physical_source(batch.source).dataset_id,
    )
    if (
        source_summary["invalid"]
        or source_summary["pair_count"] != source_summary["source_count"]
        or source_summary["offices_json"] is None
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_source_unresolved")
    source_offices = json.loads(source_summary["offices_json"])
    canonical = await _canonicalize([tuple(office["source_address"]) for office in source_offices])
    return source_summary["offices_json"], canonical


def _page_key(batch, after_resource):
    return _digest((batch.source.generation_id, batch.generation_id, batch.input_sha256, after_resource))


@dataclass(frozen=True)
class _OfficeMatches:
    table_name: str
    address_source: PinnedAddressSource
    page_proofs_by_key: dict
    recipe_sha256: str | None = None
    approved_generation_id: str | None = None


_ACTIVE_OFFICE_MATCHES = ContextVar("initial_cms_office_matches", default=None)


async def _matched_page(connection, matches, page_key):
    summary = await connection.fetchrow(
        f"""SELECT count(*) AS rows,CASE WHEN count(*)<=5000
        AND coalesce(sum(octet_length(office::text)),0)+2*count(*)+2<=8388608
        THEN coalesce(jsonb_agg(office ORDER BY office::text),'[]'::jsonb)::text END AS offices_json
        FROM pg_temp.{_identifier(matches.table_name)} WHERE page_key=$1""",
        page_key,
    )
    expected = matches.page_proofs_by_key.get(page_key)
    if summary["offices_json"] is None or expected is None or summary["rows"] != expected[0]:
        raise InitialCMSOfficeBindingError("initial_cms_office_correspondence_unresolved")
    return summary["offices_json"]


async def _stage_match_inputs(connection, input_name, pages, control_schema):
    """COPY bounded authenticated pages; retain only bounded counts and digest slots."""
    page_proofs_by_key = {}
    async for batch, cursor, retained_read in pages:
        await _verify_batch(connection, batch, cursor, control_schema, retained_read)
        source_json, canonical_json = await _source_offices(connection, batch)
        key = _page_key(batch, cursor)
        if key in page_proofs_by_key or len(page_proofs_by_key) >= MAX_INPUT_BYTES // 512:
            raise InitialCMSOfficeBindingError("initial_cms_office_page_accounting_invalid")
        offices, canonical = json.loads(source_json), json.loads(canonical_json)
        records = [
            (key, ordinal, json.dumps(office), json.dumps(canonical[ordinal - 1]))
            for ordinal, office in enumerate(offices, 1)
        ]
        copied = await connection.copy_records_to_table(
            input_name, schema_name="pg_temp", columns=("page_key", "ordinal", "office", "canonical"), records=records
        )
        if copied != f"COPY {len(records)}":
            raise InitialCMSOfficeBindingError("initial_cms_office_accounting_invalid")
        page_proofs_by_key[key] = (len(records), None)
    return page_proofs_by_key


async def _match_complete_inputs(connection, input_name, result_name, address_source):
    """Execute the unchanged exact base predicate once for every staged input."""
    complete = (
        "WITH offices AS MATERIALIZED (SELECT * FROM pg_temp."
        + _identifier(input_name)
        + ") , "
        + _MATCH_BODY.replace("{page_column}", "page_key,")
        + " SELECT page_key,office,invalid,matches FROM encoded"
    )
    await connection.execute(
        f"CREATE TEMP TABLE {_identifier(result_name)} ON COMMIT DROP AS "
        + complete.format(address=f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}")
    )
    if await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM pg_temp.{_identifier(result_name)} WHERE invalid IS DISTINCT FROM false)"
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_correspondence_unresolved")
    if await connection.fetchval(f"SELECT EXISTS(SELECT 1 FROM pg_temp.{_identifier(result_name)} WHERE matches=0)"):
        raise SourceOfficeNotReady("registry_source_office_not_ready")
    await connection.execute(f"CREATE INDEX ON {_identifier(result_name)}(page_key)")


@asynccontextmanager
async def prepare_cms_office_matches(connection, address_source, runtime_roles, pages, *, control_schema):
    """Match bounded complete inputs once against the selected named address edition."""
    await _require_transaction(connection)
    address_source.__post_init__()
    input_name, result_name = ("initial_cms_inputs_" + uuid4().hex, "initial_cms_matches_" + uuid4().hex)
    savepoint = connection.transaction()
    await savepoint.start()
    try:
        await connection.execute(f"""CREATE TEMP TABLE {_identifier(input_name)} (
            page_key text NOT NULL,ordinal integer NOT NULL,office jsonb NOT NULL,canonical jsonb NOT NULL,
            PRIMARY KEY(page_key,ordinal)) ON COMMIT DROP""")
        page_proofs_by_key = await _stage_match_inputs(connection, input_name, pages, control_schema)
        await _match_complete_inputs(connection, input_name, result_name, address_source)
        matches = _OfficeMatches(
            result_name,
            address_source,
            page_proofs_by_key,
        )
        for key, (count, _) in page_proofs_by_key.items():
            page_proofs_by_key[key] = (
                count,
                hashlib.sha256((await _matched_page(connection, matches, key)).encode()).hexdigest(),
            )
        token = _ACTIVE_OFFICE_MATCHES.set(matches)
        try:
            yield matches
        finally:
            _ACTIVE_OFFICE_MATCHES.reset(token)
        await connection.execute(f"DROP TABLE pg_temp.{_identifier(result_name)},pg_temp.{_identifier(input_name)}")
    except BaseException:
        await drain_operation(savepoint.rollback(), preserve_cancellation=False)
        raise
    else:
        await drain_operation(savepoint.commit(), preserve_cancellation=True)


async def _office_records(
    connection,
    batch,
    address_source,
    office_matches=None,
    after_resource=None,
    *,
    address_relation=None,
    address_arguments=(),
):
    if office_matches is not None:
        if type(office_matches) is not _OfficeMatches or office_matches.address_source != address_source:
            raise InitialCMSOfficeBindingError("initial_cms_office_match_scope_differs")
        key = _page_key(batch, after_resource)
        matched_json = await _matched_page(connection, office_matches, key)
        if hashlib.sha256(matched_json.encode()).hexdigest() != office_matches.page_proofs_by_key[key][1]:
            raise InitialCMSOfficeBindingError("initial_cms_office_match_scope_differs")
        matched_by_field = {"invalid": False, "offices_json": matched_json}
    else:
        source_summary = await connection.fetchrow(
            _SOURCE_SQL.format(source=_identifier(batch.source.read_schema_name)),
            batch.input_bytes.decode(),
            batch.source.source_id,
            fhir_physical_source(batch.source).dataset_id,
        )
        return await _match_source_office_records(
            connection,
            source_summary,
            address_source,
            address_relation=address_relation,
            address_arguments=address_arguments,
        )
    if matched_by_field["invalid"] or matched_by_field["offices_json"] is None:
        raise InitialCMSOfficeBindingError("initial_cms_office_correspondence_unresolved")
    offices = json.loads(matched_by_field["offices_json"])
    retained = await _canonicalize([tuple(office["address"]) for office in offices])
    checked = await connection.fetchrow(_CHECK_SQL, matched_by_field["offices_json"], retained)
    if checked["valid"] is not True:
        raise InitialCMSOfficeBindingError("initial_cms_office_full_identity_differs")
    bindings = [
        (
            binding["provider_system"],
            binding["provider_id"],
            UUID(binding["location_id"]),
            binding["location_key"],
            binding["entity_type"],
            binding["entity_id"],
        )
        for binding in json.loads(checked["bindings_json"])
    ]
    return offices, bindings


async def _capture_offices(
    connection,
    batch,
    address_source,
    runtime_roles,
    after_resource,
    control_schema,
    origin_manifest_sha256=None,
    *,
    retained_read=None,
):
    if type(address_source) is not PinnedAddressSource or address_source.table_name != "entity_address_unified":
        raise InitialCMSOfficeBindingError("initial_cms_office_address_pin_invalid")
    await _require_transaction(connection)
    if batch.source.retained_capture is None:
        await _verify_batch(connection, batch, after_resource, control_schema)
    else:
        await _verify_batch(connection, batch, after_resource, control_schema, retained_read=retained_read)
    origin = await resolve_source_office_origin(
        connection,
        address_source,
        runtime_roles=runtime_roles,
        control_schema=_control_namespace(control_schema)[1:-1],
        origin_manifest_sha256=origin_manifest_sha256,
        first_parameter=3,
    )
    office_matches = _ACTIVE_OFFICE_MATCHES.get()
    if office_matches is not None and (
        type(office_matches) is not _OfficeMatches
        or origin_manifest_sha256 is not None
        or origin.address_source != office_matches.address_source
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_match_scope_differs")
    offices, bindings = await _office_records(
        connection,
        batch,
        address_source,
        office_matches,
        after_resource,
        address_relation=origin.relation_sql,
        address_arguments=origin.arguments,
    )
    pin = PinnedCMSOfficeAddressSource(
        address_source,
        origin.address_scope,
        batch.source.generation_id,
        batch.generation_id,
        batch.input_sha256,
        _digest(offices),
        after_resource,
        len(bindings),
        origin_manifest_sha256,
    )
    return pin, bindings


async def pin_cms_office_address_source(
    connection,
    batch,
    address_source,
    *,
    runtime_roles,
    after_resource=None,
    control_schema=None,
    origin_manifest_sha256=None,
    retained_read=None,
):
    """Pin full immutable source and selected office correspondence before candidate creation.

    This is a correspondence receipt; it does not assert that the unified office
    was originally ingested from this CMS edition. Source site and canonical
    address UUIDs remain distinct identities.
    """
    pin, _ = await _capture_offices(
        connection,
        batch,
        address_source,
        runtime_roles,
        after_resource,
        control_schema,
        origin_manifest_sha256,
        retained_read=retained_read,
    )
    return pin


async def copy_initial_cms_office_bindings(
    connection,
    batch,
    copy_target,
    address_pin,
    *,
    runtime_roles,
    after_resource=None,
    control_schema=None,
    retained_read=None,
):
    """Copy only fully verified exact CMS offices into an open isolated candidate."""
    if (
        type(batch) is not FHIRMembershipBatch
        or type(batch.source) is not PinnedFHIRMembershipSource
        or type(copy_target) is not MembershipCopyTarget
        or type(address_pin) is not PinnedCMSOfficeAddressSource
        or type(address_pin.address_source) is not PinnedAddressSource
        or type(address_pin.office_count) is not int
        or not 0 <= address_pin.office_count <= MAX_ROWS
        or after_resource != address_pin.after_resource
        or copy_target.schema_name in {address_pin.address_source.schema_name, batch.source.schema_name}
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_input_invalid")
    await _require_transaction(connection)
    async with connection.transaction():
        await _candidate(
            connection,
            copy_target,
            batch,
            address_pin.address_source,
            _control_namespace(control_schema),
            origin_manifest_sha256=address_pin.origin_manifest_sha256,
        )
        actual, bindings = await _capture_offices(
            connection,
            batch,
            address_pin.address_source,
            runtime_roles,
            after_resource,
            control_schema,
            address_pin.origin_manifest_sha256,
            retained_read=retained_read,
        )
        if actual != address_pin:
            raise InitialCMSOfficeBindingError("initial_cms_office_pin_differs")
        await _copy_source_office_records(connection, copy_target, bindings)
        return {
            "component": "initial_retained_fhir_office_correspondence"
            if batch.source.retained_capture is not None
            else "initial_cms_office_correspondence",
            "revision": 2 if actual.origin_manifest_sha256 is not None else 1,
            "office_count": len(bindings),
            "correspondence_sha256": _digest(source_office_pin_document(actual)),
            **(
                {"origin_manifest_sha256": actual.origin_manifest_sha256}
                if actual.origin_manifest_sha256 is not None
                else {}
            ),
        }


_VERIFIED_POLICY = "retained-fhir-exact-office-v1"
_VERIFIED_SOURCE_SQL = (
    _SOURCE_SQL.split(", encoded AS (", 1)[0]
    + """, unique_offices AS (
 SELECT *,count(*) OVER(PARTITION BY provider_system,provider_id,location_id) AS matches FROM offices
), encoded AS (
 SELECT jsonb_build_object('provider_system',provider_system,'provider_id',provider_id,
 'location_id',location_id::text,'resource_id',resource_id,'payload_sha256',payload_hash,
 'source_address',jsonb_build_array(document->'first_line',document->'second_line',document->'city_name',
 document->'state_name',document->'postal_code',document->'country_code')) AS office
 FROM unique_offices WHERE matches=1 AND resource_id IS NOT NULL
)
SELECT CASE WHEN count(*)<=5000 AND coalesce(sum(octet_length(office::text)),0)+2*count(*)+2<=8388608
 THEN coalesce(jsonb_agg(office ORDER BY office::text),'[]'::jsonb)::text END AS offices_json FROM encoded"""
)


def _verified_membership_input(batch):
    """Validate each distinct NPI before invoking the unchanged strict native codec."""
    if (
        type(batch) is not FHIRMembershipBatch
        or batch.source.retained_capture is None
        or not batch.approved_only
        or len(batch.input_bytes) > MAX_INPUT_BYTES
        or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256
    ):
        raise InitialCMSOfficeBindingError("initial_retained_mapping_input_invalid")
    return _verified_member_input(batch.input_bytes, batch.membership_rows)


def _verified_member_input(input_bytes, membership_rows):
    """Apply the same NPI policy to authenticated batch or native mapped rows."""
    from process.provider_directory_projection_fhir_values import is_valid_npi

    if (
        type(input_bytes) is not bytes
        or len(input_bytes) > MAX_INPUT_BYTES
        or type(membership_rows) is not int
        or not 0 <= membership_rows <= MAX_ROWS
    ):
        raise InitialCMSOfficeBindingError("initial_retained_mapping_input_invalid")
    members = json.loads(input_bytes)
    if type(members) is not list or len(members) != membership_rows:
        raise InitialCMSOfficeBindingError("initial_retained_mapping_accounting_invalid")
    distinct_npis = {member["provider_id"] for member in members if member["provider_system"] == "npi"}
    validity_by_npi = {npi: is_valid_npi(npi) for npi in distinct_npis}
    selected_members = [
        member for member in members if member["provider_system"] != "npi" or validity_by_npi[member["provider_id"]]
    ]
    return selected_members, json.dumps(
        selected_members, sort_keys=True, separators=(",", ":"), allow_nan=False
    ).encode()


def _safe_source_address(office):
    """Exclude unsupported raw field shapes before the address codec boundary."""
    address = office["source_address"]
    return (
        type(address) is list
        and len(address) == 6
        and type(address[0]) is str
        and bool(address[0].strip())
        and all(value is None or type(value) is str for value in address)
    )


async def _stage_verified_retained_page(connection, batch, cursor, input_name, source_table, proofs_by_page):
    members, selected_bytes = _verified_membership_input(batch)
    await _stage_verified_member_page(
        connection,
        batch.source,
        members,
        selected_bytes,
        _page_key(batch, cursor),
        input_name,
        source_table,
        proofs_by_page,
    )


async def _stage_verified_member_page(
    connection, source_pin, members, selected_bytes, key, input_name, source_table, proofs_by_page
):
    """Stage native mapped members; the caller must first verify their Source read."""
    _, count = await drain_operation(asyncio.to_thread(_encode, selected_bytes), preserve_cancellation=True)
    if count != len(members):
        raise InitialCMSOfficeBindingError("initial_retained_mapping_accounting_invalid")
    if key in proofs_by_page or len(proofs_by_page) >= MAX_INPUT_BYTES // 512:
        raise InitialCMSOfficeBindingError("initial_cms_office_page_accounting_invalid")
    if not proofs_by_page:
        await connection.execute(f"CREATE INDEX ON pg_temp.{_identifier(source_table)}(page_key)")
    copied = await drain_operation(
        connection.copy_records_to_table(
            source_table,
            schema_name="pg_temp",
            columns=("network_id", "provider_system", "provider_id", "location_id", "evidence_id", "page_key"),
            records=[
                (
                    member["network_id"],
                    member["provider_system"],
                    member["provider_id"],
                    UUID(member["location_id"]),
                    member["evidence_id"],
                    key,
                )
                for member in members
            ],
        ),
        preserve_cancellation=True,
    )
    if copied != f"COPY {len(members)}":
        raise InitialCMSOfficeBindingError("initial_retained_mapping_accounting_invalid")
    summary = await connection.fetchrow(
        _VERIFIED_SOURCE_SQL.format(source=_identifier(source_pin.read_schema_name)),
        selected_bytes.decode(),
        source_pin.source_id,
        fhir_physical_source(source_pin).dataset_id,
    )
    if summary["offices_json"] is None:
        raise InitialCMSOfficeBindingError("initial_cms_office_canonical_bound_exceeded")
    offices = [office for office in json.loads(summary["offices_json"]) if _safe_source_address(office)]
    canonical = json.loads(await _canonicalize([tuple(office["source_address"]) for office in offices]))
    if len(canonical) != len(offices):
        raise InitialCMSOfficeBindingError("initial_retained_mapping_accounting_invalid")
    office_inputs = [
        (key, ordinal, json.dumps(office), json.dumps(canonical[ordinal - 1]))
        for ordinal, office in enumerate(offices, 1)
    ]
    copied = await drain_operation(
        connection.copy_records_to_table(
            input_name,
            schema_name="pg_temp",
            columns=("page_key", "ordinal", "office", "canonical"),
            records=office_inputs,
        ),
        preserve_cancellation=True,
    )
    if copied != f"COPY {len(office_inputs)}":
        raise InitialCMSOfficeBindingError("initial_retained_mapping_accounting_invalid")
    proofs_by_page[key] = (0, None)


async def _verified_office_match_keys(connection, input_name, address_relation):
    """Count narrow complete matches before fetching any full base-row JSON."""
    keys_name, winners_name = "initial_cms_keys_" + uuid4().hex, "initial_cms_winners_" + uuid4().hex
    await connection.execute(f"""CREATE TEMP TABLE {_identifier(keys_name)} ON COMMIT DROP AS
      SELECT page_key,ordinal,coalesce(office->>'provider_system','npi') AS provider_system,
        office->>'provider_id' AS provider_id,
        CASE WHEN coalesce(office->>'provider_system','npi')='npi'
          THEN (office->>'provider_id')::bigint END AS npi,
        (canonical->>'address_key')::uuid AS address_key
      FROM pg_temp.{_identifier(input_name)}""")
    await connection.execute(f"ANALYZE pg_temp.{_identifier(keys_name)}")
    await connection.execute(f"""CREATE TEMP TABLE {_identifier(winners_name)} ON COMMIT DROP AS
      WITH selected_base AS MATERIALIZED (
        SELECT entity_type,entity_id,npi,address_key,location_key FROM {address_relation}
        WHERE address_precision='street' AND type='practice' AND inferred_npi IS NULL
          AND ((entity_type::text,entity_id::text,address_key,npi IS NULL,coalesce(npi,0)) IN (
            SELECT provider_system,provider_id,address_key,npi IS NULL,coalesce(npi,0)
            FROM pg_temp.{_identifier(keys_name)}
          )) IS TRUE
      ), matched AS (
        SELECT input.page_key,input.ordinal,address.location_key,
          count(*) OVER(PARTITION BY input.page_key,input.ordinal) AS matches
        FROM pg_temp.{_identifier(keys_name)} input JOIN selected_base address
          ON address.entity_type=input.provider_system AND address.entity_id=input.provider_id
          AND CASE WHEN input.provider_system='npi' THEN address.npi=input.npi ELSE address.npi IS NULL END
          AND address.address_key=input.address_key
      ) SELECT page_key,ordinal,location_key FROM matched WHERE matches=1""")
    await connection.execute(f"ANALYZE pg_temp.{_identifier(winners_name)}")
    await connection.execute(f"DROP TABLE pg_temp.{_identifier(keys_name)}")
    return winners_name


async def _finish_verified_office_matches(
    connection, input_name, result_name, address_source, proofs_by_page, *, source_aliases=False
):
    """Join the complete staged input once, then retain exact unambiguous addresses."""
    if type(source_aliases) is not bool:
        raise InitialCMSOfficeBindingError("initial_retained_mapping_source_invalid")
    address_relation = f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
    winners_name = await _verified_office_match_keys(connection, input_name, address_relation)
    # Keep late hydration on the primary key instead of hashing or spooling full base rows.
    complete = f"""WITH matched AS NOT MATERIALIZED (
      SELECT input.page_key,input.office,input.canonical,winner.location_key,
        to_jsonb(address) AS address,1::bigint AS matches
      FROM pg_temp.{_identifier(winners_name)} winner
      JOIN LATERAL (
        SELECT staged.page_key,staged.office,staged.canonical FROM pg_temp.{_identifier(input_name)} staged
        WHERE staged.page_key=winner.page_key AND staged.ordinal=winner.ordinal OFFSET 0
      ) input ON true
      JOIN LATERAL (
        SELECT * FROM {address_relation} source_address
        WHERE source_address.location_key=winner.location_key OFFSET 0
      ) address ON true
    ), encoded AS (""" + _MATCH_BODY.split("), encoded AS (", 1)[1].replace("{page_column}", "page_key,")
    await connection.execute(
        f"CREATE TEMP TABLE {_identifier(result_name)} ON COMMIT DROP AS "
        + complete.format()
        + " SELECT page_key,office FROM encoded WHERE invalid IS false"
    )
    await connection.execute(f"DROP TABLE pg_temp.{_identifier(winners_name)}")
    await connection.execute(f"CREATE INDEX ON {_identifier(result_name)}(page_key)")
    for key in proofs_by_page:
        summary = await connection.fetchrow(
            f"""SELECT CASE WHEN count(*)<=5000
          AND coalesce(sum(octet_length(office::text)),0)+2*count(*)+2<=8388608
          THEN coalesce(jsonb_agg(office ORDER BY office::text),'[]'::jsonb)::text END AS offices_json
          FROM pg_temp.{_identifier(result_name)} WHERE page_key=$1""",
            key,
        )
        if summary["offices_json"] is None:
            raise InitialCMSOfficeBindingError("initial_cms_office_canonical_bound_exceeded")
        offices = json.loads(summary["offices_json"])
        retained = json.loads(await _canonicalize([tuple(office["address"]) for office in offices]))
        if len(retained) != len(offices):
            raise InitialCMSOfficeBindingError("initial_retained_mapping_accounting_invalid")
        verified_offices = [office for office, canonical in zip(offices, retained) if office["canonical"] == canonical]
        await connection.execute(
            f"DELETE FROM pg_temp.{_identifier(result_name)} matched WHERE page_key=$1 AND NOT EXISTS(SELECT FROM jsonb_array_elements($2::jsonb) allowed(office) WHERE allowed.office=matched.office)",
            key,
            json.dumps(verified_offices),
        )
    await _prune_verified_offices(connection, result_name, source_aliases, proofs_by_page)


async def _verified_office_identities(connection, result_name, source_aliases):
    """Index narrow global identities before population; keep result row IDs until pruning finishes."""
    name = "office_prune_" + uuid4().hex
    relation = "pg_temp." + _identifier(name)
    await drain_operation(
        connection.execute(f"""CREATE TEMP TABLE {relation}(
          row_id tid NOT NULL,page_key text,provider_system text,provider_id text,location_id text,
          location_key text,provider_identity text COLLATE "C" NOT NULL
        ) ON COMMIT DROP"""),
        preserve_cancellation=True,
    )
    for columns in (
        "page_key",
        "location_key,provider_identity",
        "provider_system,provider_id,location_id,location_key",
    ):
        await drain_operation(connection.execute(f"CREATE INDEX ON {relation}({columns})"), preserve_cancellation=True)
    identity_fields = "provider_system,provider_id" + ("" if source_aliases else ",location_id")
    await drain_operation(
        connection.execute(f"""INSERT INTO {relation}(row_id,page_key,provider_system,provider_id,location_id,
          location_key,provider_identity) SELECT row_id,page_key,provider_system,provider_id,location_id,
          location_key,jsonb_build_array({identity_fields})::text FROM (
          SELECT ctid AS row_id,page_key,office->>'provider_system' AS provider_system,
            office->>'provider_id' AS provider_id,office->>'location_id' AS location_id,
            office->>'location_key' AS location_key FROM pg_temp.{_identifier(result_name)}
        ) identities"""),
        preserve_cancellation=True,
    )
    return relation


async def _prune_verified_offices(connection, result_name, source_aliases, proofs_by_page):
    """Prune each page against an immutable copy of the complete global identities."""
    async with connection.transaction():
        relation = await _verified_office_identities(connection, result_name, source_aliases)
        extrema_queries = []
        for field, equality in (
            ("provider_identity", "other.location_key=identity.location_key"),
            (
                "location_key",
                "(other.provider_system,other.provider_id,other.location_id)="
                "(identity.provider_system,identity.provider_id,identity.location_id)",
            ),
        ):
            query = f"SELECT {field} FROM {relation} other WHERE {equality} AND {field} IS NOT NULL ORDER BY {field}"
            extrema_queries.append(f"({query} LIMIT 1)<>({query} DESC LIMIT 1)")
        for key in proofs_by_page:
            await drain_operation(
                connection.execute(
                    f"DELETE FROM pg_temp.{_identifier(result_name)} WHERE ctid=ANY(ARRAY("
                    f"SELECT row_id FROM {relation} identity WHERE page_key=$1 "
                    f"AND coalesce(({extrema_queries[0]}) OR ({extrema_queries[1]}),false)))",
                    key,
                ),
                preserve_cancellation=True,
            )
        await drain_operation(connection.execute(f"DROP TABLE {relation}"), preserve_cancellation=True)


async def _prune_unmatched_members(connection, source_table, result_name, proofs_by_page):
    """Prune authenticated pages with bounded workfiles after global office ambiguity checks."""
    await drain_operation(
        connection.execute(
            f"DELETE FROM pg_temp.{_identifier(source_table)} WHERE page_key IS NULL OR NOT(page_key=ANY($1::text[]))",
            list(proofs_by_page),
        ),
        preserve_cancellation=True,
    )
    for key in proofs_by_page:
        await drain_operation(
            connection.execute(
                f"""WITH matched_keys AS MATERIALIZED (
          SELECT office->>'provider_system' AS provider_system,office->>'provider_id' AS provider_id,
            office->>'location_id' AS location_id FROM pg_temp.{_identifier(result_name)} WHERE page_key=$1
        ) DELETE FROM pg_temp.{_identifier(source_table)} member WHERE member.page_key=$1 AND NOT EXISTS(
          SELECT FROM matched_keys matched
          WHERE (matched.provider_system,matched.provider_id,matched.location_id::uuid)
            =(member.provider_system,member.provider_id,member.location_id))""",
                key,
            ),
            preserve_cancellation=True,
        )


async def prepare_verified_retained_office_mapping(
    connection, address_source, runtime_roles, pages, source_table, *, control_schema, mapping_scope
):
    """Keep one complete mapped set for membership and binding construction."""
    await _require_transaction(connection)
    address_source.__post_init__()
    input_name, result_name = "initial_cms_inputs_" + uuid4().hex, "initial_cms_matches_" + uuid4().hex
    await connection.execute(
        f"CREATE TEMP TABLE {_identifier(input_name)}(page_key text NOT NULL,ordinal integer NOT NULL,office jsonb NOT NULL,canonical jsonb NOT NULL,PRIMARY KEY(page_key,ordinal)) ON COMMIT DROP"
    )
    proofs_by_page = {}
    async for batch, cursor, retained_read in pages:
        await _verify_retained_office_read(connection, batch.source, retained_read)
        await _stage_verified_retained_page(connection, batch, cursor, input_name, source_table, proofs_by_page)
    await _finish_verified_office_matches(connection, input_name, result_name, address_source, proofs_by_page)
    await _prune_unmatched_members(connection, source_table, result_name, proofs_by_page)
    matches = _OfficeMatches(
        result_name,
        address_source,
        proofs_by_page,
        *mapping_scope,
    )
    for key in proofs_by_page:
        count = await connection.fetchval(
            f"SELECT count(*) FROM pg_temp.{_identifier(result_name)} WHERE page_key=$1", key
        )
        proofs_by_page[key] = (count, None)
        proofs_by_page[key] = (
            count,
            hashlib.sha256((await _matched_page(connection, matches, key)).encode()).hexdigest(),
        )
    await connection.execute(f"DROP TABLE pg_temp.{_identifier(input_name)}")
    return matches


async def verified_retained_offices(connection, matches, address_source, runtime_roles, control_schema):
    """Compare the selected named address edition and complete bounded match content."""
    await _require_transaction(connection)
    if type(matches) is not _OfficeMatches or matches.address_source != address_source:
        raise InitialCMSOfficeBindingError("initial_cms_office_match_scope_differs")
    _identifier(matches.table_name)
    address_source.__post_init__()
    digest = hashlib.sha256()
    for key, (_, expected) in matches.page_proofs_by_key.items():
        raw = await _matched_page(connection, matches, key)
        if hashlib.sha256(raw.encode()).hexdigest() != expected:
            raise InitialCMSOfficeBindingError("initial_cms_office_match_scope_differs")
        digest.update((key + expected + "\n").encode())
    return _digest(
        (
            _VERIFIED_POLICY,
            asdict(address_source),
            matches.recipe_sha256,
            matches.approved_generation_id,
            digest.hexdigest(),
        )
    )


async def copy_verified_retained_offices(connection, matches, copy_target):
    """COPY the complete verified set; never preload stale retained bindings."""
    await _copy_bindings(connection, copy_target, ())
    for key in matches.page_proofs_by_key:
        offices = json.loads(await _matched_page(connection, matches, key))
        bindings = [
            (
                office["provider_system"],
                office["provider_id"],
                UUID(office["location_id"]),
                office["location_key"],
                office["provider_system"],
                office["provider_id"],
            )
            for office in offices
        ]
        await _copy_bindings(connection, copy_target, bindings)
    if await connection.fetchval(f"""SELECT EXISTS(SELECT 1 FROM {_identifier(copy_target.schema_name)}.provider_location_binding
      GROUP BY location_key HAVING count(*)>1)"""):
        raise InitialCMSOfficeBindingError("initial_cms_office_binding_ambiguous")


async def _match_source_office_records(
    connection, source_summary, address_source, *, address_relation=None, address_arguments=()
):
    """Share canonical matching after each distinct caller establishes source authority."""
    if (
        source_summary["invalid"]
        or source_summary["pair_count"] != source_summary["source_count"]
        or source_summary["offices_json"] is None
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_source_unresolved")
    source_offices = json.loads(source_summary["offices_json"])
    canonical = await _canonicalize([tuple(office["source_address"]) for office in source_offices])
    matched = await connection.fetchrow(
        _MATCH_SQL.format(
            address=address_relation
            or f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
        ),
        source_summary["offices_json"],
        canonical,
        *address_arguments,
    )
    if matched["invalid"] or matched["offices_json"] is None:
        raise InitialCMSOfficeBindingError("initial_cms_office_correspondence_unresolved")
    offices = json.loads(matched["offices_json"])
    retained = await _canonicalize([tuple(office["address"]) for office in offices])
    checked = await connection.fetchrow(_CHECK_SQL, matched["offices_json"], retained)
    if checked["valid"] is not True:
        raise InitialCMSOfficeBindingError("initial_cms_office_full_identity_differs")
    if matched["missing_count"]:
        raise SourceOfficeNotReady("registry_source_office_not_ready")
    bindings = [
        (
            binding["provider_system"],
            binding["provider_id"],
            UUID(binding["location_id"]),
            binding["location_key"],
            binding["entity_type"],
            binding["entity_id"],
        )
        for binding in json.loads(checked["bindings_json"])
    ]
    return offices, bindings


async def _copy_source_office_records(connection, copy_target, bindings):
    """Keep every authenticated alias while checking physical ownership across pages."""
    await _copy_bindings(connection, copy_target, bindings)
    if await connection.fetchval(
        f"""SELECT EXISTS(SELECT 1 FROM {_identifier(copy_target.schema_name)}.provider_location_binding
          WHERE location_key=ANY($1::varchar[]) GROUP BY location_key
          HAVING count(DISTINCT (provider_system,provider_id,entity_type,entity_id))>1)""",
        [binding[3] for binding in bindings],
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_binding_ambiguous")
