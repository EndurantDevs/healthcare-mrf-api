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
from process.network_custom_address_source import _artifact_dependency_identity, _require_transaction
from process.network_fhir_membership_source import (
    FHIRMembershipBatch,
    PinnedFHIRMembershipSource,
    read_fhir_membership_batch,
    read_retained_fhir_membership_batch,
)
from process.network_initial_source_office_bindings import _candidate, _copy_bindings, _validate_roles
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_membership_copy import MAX_INPUT_BYTES, MAX_ROWS, MembershipCopyTarget, _encode
from process.registry_source_recipe_composition import _require_recipe_custody
from process.registry_source_recipe_store import RegistrySourceMembershipRecipe
from process.uhc_flex_practitioner_async_safety import drain_operation


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
        await retained_read.verify(connection, batch.source.retained_capture)
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
 'address_row_sha256',encode(sha256(convert_to(address::text,'UTF8')),'hex')) AS office,
 matches<>1 OR canonical->>'identity_key' NOT LIKE '%|street'
 OR location_key !~ '^[0-9a-f]{{64}}$' OR canonical IS NULL AS invalid FROM matched
)"""

_MATCH_SQL = (
    """WITH offices AS MATERIALIZED (
 SELECT office,ordinal,($2::jsonb)->(ordinal::integer-1) AS canonical
 FROM jsonb_array_elements($1::jsonb) WITH ORDINALITY item(office,ordinal)
),
"""
    + _MATCH_BODY.replace("{page_column}", "")
    + """
SELECT coalesce(bool_or(invalid),false) AS invalid,
 CASE WHEN count(*)<=5000 AND coalesce(sum(octet_length(office::text)),0)+2*count(*)+2<=8388608
 THEN coalesce(jsonb_agg(office ORDER BY office::text),'[]'::jsonb)::text END AS offices_json FROM encoded"""
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
 AND (SELECT count(*) FROM bindings)=(SELECT count(DISTINCT location_key) FROM bindings) AS valid,
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
    return source_summary["offices_json"], canonical


def _page_key(batch, after_resource):
    return _digest((batch.source.generation_id, batch.generation_id, batch.input_sha256, after_resource))


@dataclass(frozen=True)
class _OfficeMatches:
    table_name: str
    table_oid: int
    address_source: PinnedAddressSource
    address_identity: tuple
    transaction: tuple
    task: object
    page_proofs_by_key: dict
    recipe_sha256: str | None = None
    approved_generation_id: str | None = None


_ACTIVE_OFFICE_MATCHES = ContextVar("initial_cms_office_matches", default=None)


async def _transaction_identity(connection):
    return tuple(await connection.fetchrow("SELECT pg_backend_pid(),pg_current_xact_id()::text,current_user::text"))


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
        + " SELECT page_key,office,invalid FROM encoded"
    )
    await connection.execute(
        f"CREATE TEMP TABLE {_identifier(result_name)} ON COMMIT DROP AS "
        + complete.format(address=f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}")
    )
    if await connection.fetchval(
        f"SELECT EXISTS(SELECT 1 FROM pg_temp.{_identifier(result_name)} WHERE invalid IS DISTINCT FROM false)"
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_correspondence_unresolved")
    await connection.execute(f"CREATE INDEX ON {_identifier(result_name)}(page_key)")


@asynccontextmanager
async def prepare_cms_office_matches(connection, address_source, runtime_roles, pages, *, control_schema):
    """Match bounded complete inputs once, retaining exact native transaction custody."""
    await _require_transaction(connection)
    identity = await _artifact_dependency_identity(
        connection, address_source, runtime_roles, address=True, control_schema=control_schema
    )
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
            await connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + result_name),
            address_source,
            identity,
            await _transaction_identity(connection),
            asyncio.current_task(),
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


async def _office_records(connection, batch, address_source, office_matches=None, after_resource=None):
    if office_matches is not None:
        if (
            type(office_matches) is not _OfficeMatches
            or office_matches.task is not asyncio.current_task()
            or office_matches.address_source != address_source
            or office_matches.transaction != await _transaction_identity(connection)
            or office_matches.table_oid
            != await connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + office_matches.table_name)
        ):
            raise InitialCMSOfficeBindingError("initial_cms_office_match_scope_differs")
        key = _page_key(batch, after_resource)
        matched_json = await _matched_page(connection, office_matches, key)
        if hashlib.sha256(matched_json.encode()).hexdigest() != office_matches.page_proofs_by_key[key][1]:
            raise InitialCMSOfficeBindingError("initial_cms_office_match_scope_differs")
        matched_by_field = {"invalid": False, "offices_json": matched_json}
    else:
        source_json, canonical = await _source_offices(connection, batch)
        matched_by_field = await connection.fetchrow(
            _MATCH_SQL.format(
                address=f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
            ),
            source_json,
            canonical,
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
    retained_read=None,
):
    office_matches = _ACTIVE_OFFICE_MATCHES.get()
    if type(address_source) is not PinnedAddressSource or address_source.table_name != "entity_address_unified":
        raise InitialCMSOfficeBindingError("initial_cms_office_address_pin_invalid")
    _validate_roles(runtime_roles)
    await _require_transaction(connection)
    await _verify_batch(connection, batch, after_resource, control_schema, retained_read)
    identity = await _artifact_dependency_identity(
        connection, address_source, runtime_roles, address=True, control_schema=_control_namespace(control_schema)[1:-1]
    )
    if office_matches is not None and (
        type(office_matches) is not _OfficeMatches
        or office_matches.task is not asyncio.current_task()
        or identity != office_matches.address_identity
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_match_scope_differs")
    offices, bindings = await _office_records(connection, batch, address_source, office_matches, after_resource)
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
    connection,
    batch,
    address_source,
    *,
    runtime_roles,
    after_resource=None,
    control_schema=None,
    retained_read=None,
):
    """Pin full immutable source and selected office correspondence before candidate creation.

    This is a correspondence receipt; it does not assert that the unified office
    was originally ingested from this CMS edition. Source site and canonical
    address UUIDs remain distinct identities.
    """
    pin, _ = await _capture_offices(
        connection, batch, address_source, runtime_roles, after_resource, control_schema, retained_read
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
        await _candidate(connection, copy_target, batch, address_pin.address_source, _control_namespace(control_schema))
        actual, bindings = await _capture_offices(
            connection,
            batch,
            address_pin.address_source,
            runtime_roles,
            after_resource,
            control_schema,
            retained_read,
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
            "component": "initial_retained_fhir_office_correspondence"
            if batch.source.retained_capture is not None
            else "initial_cms_office_correspondence",
            "revision": 1,
            "office_count": len(bindings),
            "correspondence_sha256": _digest(asdict(actual)),
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
    from process.provider_directory_projection_fhir_values import is_valid_npi

    if (
        type(batch) is not FHIRMembershipBatch
        or batch.source.retained_capture is None
        or not batch.approved_only
        or len(batch.input_bytes) > MAX_INPUT_BYTES
        or hashlib.sha256(batch.input_bytes).hexdigest() != batch.input_sha256
    ):
        raise InitialCMSOfficeBindingError("initial_retained_mapping_input_invalid")
    members = json.loads(batch.input_bytes)
    if type(members) is not list or len(members) != batch.membership_rows:
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
    _, count = await drain_operation(asyncio.to_thread(_encode, selected_bytes), preserve_cancellation=True)
    if count != len(members):
        raise InitialCMSOfficeBindingError("initial_retained_mapping_accounting_invalid")
    key = _page_key(batch, cursor)
    if key in proofs_by_page or len(proofs_by_page) >= MAX_INPUT_BYTES // 512:
        raise InitialCMSOfficeBindingError("initial_cms_office_page_accounting_invalid")
    copied = await connection.copy_records_to_table(
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
    )
    if copied != f"COPY {len(members)}":
        raise InitialCMSOfficeBindingError("initial_retained_mapping_accounting_invalid")
    summary = await connection.fetchrow(
        _VERIFIED_SOURCE_SQL.format(source=_identifier(batch.source.read_schema_name)),
        selected_bytes.decode(),
        batch.source.source_id,
        batch.source.dataset_id,
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
    if (
        await connection.copy_records_to_table(
            input_name,
            schema_name="pg_temp",
            columns=("page_key", "ordinal", "office", "canonical"),
            records=office_inputs,
        )
        != f"COPY {len(office_inputs)}"
    ):
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


async def _finish_verified_office_matches(connection, input_name, result_name, address_source, proofs_by_page):
    """Join the complete staged input once, then retain exact unambiguous addresses."""
    address_relation = f"{_identifier(address_source.schema_name)}.{_identifier(address_source.table_name)}"
    winners_name = await _verified_office_match_keys(connection, input_name, address_relation)
    # Keep late hydration on the primary key instead of hashing or spooling full base rows.
    complete = f"""WITH matched AS NOT MATERIALIZED (
      SELECT input.page_key,input.office,input.canonical,winner.location_key,
        to_jsonb(address) AS address,1::bigint AS matches
      FROM pg_temp.{_identifier(winners_name)} winner
      JOIN pg_temp.{_identifier(input_name)} input USING(page_key,ordinal)
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
    await connection.execute(f"""WITH ambiguous_locations AS MATERIALIZED (
      SELECT office->>'location_key' AS location_key FROM pg_temp.{_identifier(result_name)}
      GROUP BY 1 HAVING count(DISTINCT jsonb_build_array(office->>'provider_system',office->>'provider_id',office->>'location_id'))>1
    ), ambiguous_sources AS MATERIALIZED (
      SELECT office->>'provider_system' AS provider_system,office->>'provider_id' AS provider_id,
        office->>'location_id' AS location_id FROM pg_temp.{_identifier(result_name)}
      GROUP BY 1,2,3 HAVING count(DISTINCT office->>'location_key')>1
    ) DELETE FROM pg_temp.{_identifier(result_name)} matched WHERE EXISTS(
      SELECT FROM ambiguous_locations WHERE location_key=matched.office->>'location_key'
    ) OR EXISTS(SELECT FROM ambiguous_sources WHERE
      (provider_system,provider_id,location_id)=
      (matched.office->>'provider_system',matched.office->>'provider_id',matched.office->>'location_id'))""")


async def prepare_verified_retained_office_mapping(
    connection, address_source, runtime_roles, pages, source_table, *, control_schema, mapping_scope
):
    """Keep one private transaction-bound verified set for membership and binding construction."""
    await _require_transaction(connection)
    identity = await _artifact_dependency_identity(
        connection, address_source, runtime_roles, address=True, control_schema=control_schema
    )
    input_name, result_name = "initial_cms_inputs_" + uuid4().hex, "initial_cms_matches_" + uuid4().hex
    await connection.execute(
        f"CREATE TEMP TABLE {_identifier(input_name)}(page_key text NOT NULL,ordinal integer NOT NULL,office jsonb NOT NULL,canonical jsonb NOT NULL,PRIMARY KEY(page_key,ordinal)) ON COMMIT DROP"
    )
    proofs_by_page = {}
    async for batch, cursor, retained_read in pages:
        await retained_read.verify(connection, batch.source.retained_capture)
        await _stage_verified_retained_page(connection, batch, cursor, input_name, source_table, proofs_by_page)
    await _finish_verified_office_matches(connection, input_name, result_name, address_source, proofs_by_page)
    await connection.execute(f"""DELETE FROM pg_temp.{_identifier(source_table)} member WHERE NOT EXISTS(
      SELECT FROM pg_temp.{_identifier(result_name)} matched WHERE matched.page_key=member.page_key
      AND (matched.office->>'provider_system',matched.office->>'provider_id',(matched.office->>'location_id')::uuid)
        =(member.provider_system,member.provider_id,member.location_id))""")
    matches = _OfficeMatches(
        result_name,
        await connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + result_name),
        address_source,
        identity,
        await _transaction_identity(connection),
        asyncio.current_task(),
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
    """Recheck the exact original task, native transaction, table and selected match hashes."""
    await _require_transaction(connection)
    if (
        type(matches) is not _OfficeMatches
        or matches.task is not asyncio.current_task()
        or matches.address_source != address_source
        or matches.transaction != await _transaction_identity(connection)
        or matches.table_oid
        != await connection.fetchval("SELECT to_regclass($1)::oid::bigint", "pg_temp." + matches.table_name)
        or matches.address_identity
        != await _artifact_dependency_identity(
            connection, address_source, runtime_roles, address=True, control_schema=control_schema
        )
    ):
        raise InitialCMSOfficeBindingError("initial_cms_office_match_scope_differs")
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
            matches.address_identity,
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
