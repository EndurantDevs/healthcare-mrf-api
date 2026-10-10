# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prove exact CMS Location correspondence with closed unified offices."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import json
from dataclasses import asdict, dataclass
from uuid import UUID

from process.network_address_projection import PinnedAddressSource, _identifier
from process.network_custom_address_source import _require_transaction, _source_identity
from process.network_fhir_membership_source import (
    FHIRMembershipBatch,
    PinnedFHIRMembershipSource,
    read_fhir_membership_batch,
)
from process.network_initial_source_office_bindings import _candidate, _copy_bindings, _validate_roles
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, MembershipCopyTarget, _encode
from process.registry_source_recipe_composition import _require_recipe_custody
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe


class InitialCMSOfficeBindingError(ValueError):
    """Complete source custody or exact office correspondence is unavailable."""


@dataclass(frozen=True)
class PinnedCMSOfficeAddressSource:
    address_source: PinnedAddressSource
    address_identity: tuple[int, int, int, str]
    source_generation_id: str
    membership_generation_id: str
    membership_input_sha256: str
    selected_offices_sha256: str
    after_resource: tuple[str, str] | None
    office_count: int


def _digest(document):
    return hashlib.sha256(
        json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()
    ).hexdigest()


async def _verify_batch(connection, batch, after_resource, control_schema):
    if (
        type(batch) is not FHIRMembershipBatch
        or type(batch.source) is not PinnedFHIRMembershipSource
        or batch.source.source_id != "cms-npd"
        or not batch.approved_only
        or batch.unresolved_rows
        or type(batch.input_bytes) is not bytes
        or len(batch.input_bytes) > MAX_INPUT_BYTES
        or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_input_invalid")
    recipe = RegistrySourceMembershipRecipe(batch.source, batch.binding_coordinates)
    await _require_recipe_custody(connection, recipe)
    replay = await read_fhir_membership_batch(
        connection,
        batch.source,
        after_resource=after_resource,
        limit=max(1, batch.source_rows),
        registry_schema=control_schema,
        approved_source=batch.approved_source,
        binding_coordinates=batch.binding_coordinates,
        approved_only=True,
    )
    if replay != batch:
        raise InitialCMSOfficeBindingError("initial_cms_office_batch_differs")
    _, count = await asyncio.to_thread(_encode, batch.input_bytes)
    if count != batch.membership_rows:
        raise InitialCMSOfficeBindingError("initial_cms_office_accounting_invalid")


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

_MATCH_SQL = """WITH offices AS MATERIALIZED (
 SELECT office,ordinal,($2::jsonb)->(ordinal::integer-1) AS canonical
 FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY item(office,ordinal)
), matched AS MATERIALIZED (
 SELECT office,canonical,address.location_key,to_jsonb(address) AS address,
 count(address.location_key) OVER(PARTITION BY ordinal) AS matches
 FROM offices LEFT JOIN {address} address ON address.entity_type=coalesce(office->>'provider_system','npi')
 AND address.entity_id=office->>'provider_id'
 AND CASE WHEN coalesce(office->>'provider_system','npi')='npi'
 THEN address.npi=(office->>'provider_id')::bigint ELSE address.npi IS NULL END
 AND address.address_key=(canonical->>'address_key')::uuid
 AND address.address_precision='street' AND address.type='practice' AND address.inferred_npi IS NULL
), encoded AS (
 SELECT office||jsonb_build_object('canonical',canonical,'location_key',location_key,
 'address',jsonb_build_array(address->'first_line',address->'second_line',address->'city_name',
 address->'state_name',address->'postal_code',address->'country_code'),
 'address_row_sha256',encode(sha256(convert_to(address::text,'UTF8')),'hex')) AS office,
 matches<>1 OR canonical->>'identity_key' NOT LIKE '%|street'
 OR location_key !~ '^[0-9a-f]{{64}}$' OR canonical IS NULL AS invalid FROM matched
)
SELECT coalesce(bool_or(invalid),false) AS invalid,
 CASE WHEN count(*)<=5000 AND coalesce(sum(octet_length(office::text)),0)+2*count(*)+2<=8388608
 THEN coalesce(jsonb_agg(office ORDER BY office::text),'[]'::jsonb)::text END AS offices_json FROM encoded"""

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
 AND (SELECT count(*) FROM bindings)=(SELECT count(DISTINCT location_key) FROM bindings) AS valid,
 coalesce(jsonb_agg(to_jsonb(bindings) ORDER BY provider_id,location_id),'[]'::jsonb)::text AS bindings_json
 FROM bindings"""


async def _canonicalize(addresses):
    native = importlib.import_module("ptg2_address_canon")
    canonical = await asyncio.to_thread(native.canonicalize_batch, addresses)
    encoded = json.dumps(canonical, separators=(",", ":"), allow_nan=False)
    if len(encoded.encode()) > 2 * MAX_INPUT_BYTES:
        raise InitialCMSOfficeBindingError("initial_cms_office_canonical_bound_exceeded")
    return encoded


async def _office_records(connection, batch, address_source):
    source_summary = await connection.fetchrow(
        _SOURCE_SQL.format(source=_identifier(batch.source.read_schema_name)),
        batch.input_bytes.decode(),
        batch.source.source_id,
        batch.source.dataset_id,
    )
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
            address=f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
        ),
        source_summary["offices_json"],
        canonical,
    )
    if matched["invalid"] or matched["offices_json"] is None:
        raise InitialCMSOfficeBindingError("initial_cms_office_correspondence_unresolved")
    offices = json.loads(matched["offices_json"])
    retained = await _canonicalize([tuple(office["address"]) for office in offices])
    checked = await connection.fetchrow(_CHECK_SQL, matched["offices_json"], retained)
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


async def _capture_offices(connection, batch, address_source, runtime_roles, after_resource, control_schema):
    if type(address_source) is not PinnedAddressSource or address_source.table_name != "entity_address_unified":
        raise InitialCMSOfficeBindingError("initial_cms_office_address_pin_invalid")
    _validate_roles(runtime_roles)
    await _require_transaction(connection)
    await _verify_batch(connection, batch, after_resource, control_schema)
    identity = await _source_identity(connection, address_source, runtime_roles, address=True)
    offices, bindings = await _office_records(connection, batch, address_source)
    pin = PinnedCMSOfficeAddressSource(
        address_source,
        identity,
        batch.source.generation_id,
        batch.generation_id,
        batch.input_sha256,
        _digest(offices),
        after_resource,
        len(bindings),
    )
    return pin, bindings


async def pin_cms_office_address_source(
    connection, batch, address_source, *, runtime_roles, after_resource=None, control_schema=None
):
    """Pin full immutable source and selected office correspondence before candidate creation.

    This is a correspondence receipt; it does not assert that the unified office
    was originally ingested from this CMS edition. Source site and canonical
    address UUIDs remain distinct identities.
    """
    pin, _ = await _capture_offices(connection, batch, address_source, runtime_roles, after_resource, control_schema)
    return pin


async def copy_initial_cms_office_bindings(
    connection, batch, copy_target, address_pin, *, runtime_roles, after_resource=None, control_schema=None
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
        await _candidate(connection, copy_target, batch, address_pin.address_source, _control_namespace(control_schema))
        actual, bindings = await _capture_offices(
            connection, batch, address_pin.address_source, runtime_roles, after_resource, control_schema
        )
        if actual != address_pin:
            raise InitialCMSOfficeBindingError("initial_cms_office_pin_differs")
        await _copy_bindings(connection, copy_target, bindings)
        if await connection.fetchval(
            f"""SELECT EXISTS(SELECT 1 FROM {_identifier(copy_target.schema_name)}.provider_location_binding
              WHERE location_key=ANY($1::varchar[]) GROUP BY location_key HAVING count(*)>1)""",
            [binding[3] for binding in bindings],
        ):
            raise InitialCMSOfficeBindingError("initial_cms_office_binding_ambiguous")
        return {
            "component": "initial_cms_office_correspondence",
            "revision": 1,
            "office_count": len(bindings),
            "correspondence_sha256": _digest(asdict(actual)),
        }
