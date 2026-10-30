# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Derive current office pairs only from independently sealed original SOURCE facts."""

import hashlib
import json
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, replace
from uuid import uuid4

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import APPROVED_NETWORK_BINDINGS_SQL
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, fhir_physical_source
from process.network_initial_cms_office_bindings import _match_source_office_records
from process.network_membership_candidate_lifecycle import _control_namespace
from process.registry_source_contribution_artifacts import (
    ResolvedSourceFactArtifact,
    authenticate_anchored_source_fact_metadata,
    retained_source_fact_descriptors,
    verify_anchored_source_fact_contents,
)
from process.registry_source_contribution_store import recipe_contribution_identity, source_mapping_sha256
from process.registry_source_office_origin import resolve_source_office_origin
from process.uhc_flex_practitioner_async_safety import drain_operation

MAX_PAIR_ROWS = 1000
MAX_PAIR_BYTES = 8 * 1024**2


@dataclass(frozen=True)
class FactOfficeContext:
    approved: object
    address_source: object
    runtime_roles: tuple
    control_schema: str
    origin_manifest_sha256: str | None = None
    retained_read: object = None
    page_sink: object = None


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _sha(document):
    return hashlib.sha256(_canonical(document).encode()).hexdigest()


async def fact_office_artifacts(connection, recipes, source_manifest, control_schema):
    """Select exact original owners natively; each use still verifies full fact contents."""
    if source_manifest is None:
        return {}
    from process.network_serving_read import resolve_network_serving_manifest

    actual = await resolve_network_serving_manifest(
        connection, generation_id=source_manifest.generation_id, control_schema=control_schema
    )
    if actual != source_manifest:
        raise ValueError("registry_fact_office_manifest_changed")
    wanted_identities = {
        recipe_contribution_identity(recipe)
        for recipe in recipes
        if type(recipe.source_pin) is PinnedFHIRMembershipSource
        and (recipe.source_pin.retained_epoch is not None or recipe.source_pin.retained_capture is not None)
    }
    if not wanted_identities or "registry_source_contributions" not in actual.source_generations:
        return {}
    descriptors = await retained_source_fact_descriptors(connection, actual.candidate_id, control_schema=control_schema)
    selected_descriptors = tuple(
        descriptor
        for descriptor in descriptors
        if (descriptor["recipe_sha256"], descriptor["capture_sha256"]) in wanted_identities
    )
    artifacts_by_recipe = {}
    for offset in range(0, len(selected_descriptors), 100):
        page = await authenticate_anchored_source_fact_metadata(
            connection, selected_descriptors[offset : offset + 100], control_schema=control_schema
        )
        if artifacts_by_recipe.keys() & page.keys():
            raise ValueError("registry_fact_office_source_duplicate")
        artifacts_by_recipe.update(page)
    return artifacts_by_recipe


_PAIRS_SQL = """WITH {bindings}
SELECT DISTINCT fact_json->>'provider_system' COLLATE "C" AS provider_system,
 fact_json->>'provider_id' COLLATE "C" AS provider_id,(fact_json->>'location_id')::uuid AS location_id
FROM {facts} facts JOIN approved_network_bindings approved
 ON fact_json->>'omission_reason' IS NULL
 AND approved.record_json->>'source_key'=fact_json->>'source_key'
 AND approved.record_json->'source_scope_json'=fact_json->'source_scope_json'
WHERE facts.recipe_sha256=$8 AND fact_json->'location_id'<>'null'::jsonb
 AND fact_json->>'provider_system' IS NOT NULL AND fact_json->>'provider_id' IS NOT NULL
ORDER BY provider_system,provider_id,location_id"""


@dataclass
class _PairCursorOwner:
    connection: object
    transaction: object
    name: str
    started: bool = False

    async def start(self):
        """Record successful BEGIN before cancellation can return to the caller."""
        await self.transaction.start()
        self.started = True

    async def finish(self):
        """Close the healthy portal before releasing its owned savepoint."""
        await self.connection.execute(f"CLOSE {_identifier(self.name)}")
        await self.transaction.commit()
        self.started = False


@asynccontextmanager
async def _pair_cursor(connection, recipe, artifact, context):
    """A savepoint owns the portal, including rollback when cancellation aborts FETCH."""
    name = "registry_fact_offices_" + uuid4().hex
    owner = _PairCursorOwner(connection, connection.transaction(), name)
    try:
        await drain_operation(owner.start(), preserve_cancellation=True)
        query = _PAIRS_SQL.format(
            bindings=APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_control_namespace(context.control_schema)),
            facts=artifact.relation,
        )
        await connection.execute(
            f"DECLARE {_identifier(name)} NO SCROLL CURSOR FOR {query}",
            context.approved.approved_revision,
            *recipe.binding_coordinates.sql_parameters,
            artifact.descriptor["recipe_sha256"],
        )
        yield name
        await drain_operation(owner.finish(), preserve_cancellation=True)
    except BaseException:
        if owner.started:
            try:
                await drain_operation(owner.transaction.rollback(), preserve_cancellation=False)
            except BaseException:
                connection.terminate()
        raise


def _pair_documents(rows):
    records = [dict(row, location_id=str(row["location_id"])) for row in rows]
    encoded = _canonical(records)
    if len(rows) > MAX_PAIR_ROWS or len(encoded.encode()) > MAX_PAIR_BYTES:
        raise ValueError("registry_fact_office_pair_bound_exceeded")
    return encoded


_LOCATION_SQL = """WITH pairs AS MATERIALIZED (
 SELECT * FROM jsonb_to_recordset($1::jsonb)
 AS pair(provider_system text,provider_id text,location_id uuid)
), offices AS MATERIALIZED (
 SELECT pair.*,location.resource_id,location.payload_hash,location.payload_json::jsonb AS document,
   release.resource_id IS NOT NULL AS has_release
 FROM pairs pair LEFT JOIN {source}.provider_directory_entity_source_binding binding
 ON binding.source_id=$2 AND binding.resource_type='Location' AND binding.site_id=pair.location_id
 LEFT JOIN {source}.provider_directory_dataset_resource location
 ON location.dataset_id=$3 AND location.resource_type='Location' AND location.resource_id=binding.resource_id
 {witness}
 LEFT JOIN {source}.provider_directory_entity_release_evidence release
 ON release.source_id=$2 AND release.release_id=$4 AND release.resource_type='Location'
 AND release.resource_id=location.resource_id AND release.payload_sha256={payload_sha256}
), encoded AS (
 SELECT jsonb_build_object('provider_id',provider_id,'location_id',location_id::text,
 'resource_id',resource_id,'payload_sha256',payload_hash,
 'source_address',jsonb_build_array(document->'first_line',document->'second_line',document->'city_name',
 document->'state_name',document->'postal_code',document->'country_code'))
 ||CASE WHEN provider_system='npi' THEN '{{}}'::jsonb
 ELSE jsonb_build_object('provider_system',provider_system) END AS office,
 resource_id IS NULL OR NOT has_release OR provider_system NOT IN ('npi','provider_directory') AS invalid FROM offices
)
SELECT (SELECT count(*) FROM pairs) AS pair_count,count(*) AS source_count,
 coalesce(bool_or(invalid),false) AS invalid,
 CASE WHEN count(*)<=5000 AND coalesce(sum(octet_length(office::text)),0)+2*count(*)+2<=8388608
 THEN coalesce(jsonb_agg(office ORDER BY office::text),'[]'::jsonb)::text END AS offices_json FROM encoded"""


def _location_query(source):
    namespace = _identifier(source.read_schema_name)
    witness = ""
    payload_sha256 = "location.payload_hash"
    if source.source_id == "cms-npd":
        witness = f"""LEFT JOIN {namespace}.provider_directory_cms_npd_resource_witness witness
        ON witness.dataset_id=location.dataset_id AND witness.resource_type=location.resource_type
        AND witness.resource_id=location.resource_id AND witness.source_id=$2 AND witness.release_id=$4
        AND witness.normalized_payload_hash=location.payload_hash
        AND (location.acquired_resource_sha256 IS NULL OR location.acquired_resource_sha256=witness.raw_payload_sha256)"""
        payload_sha256 = "witness.raw_payload_sha256"
    return _LOCATION_SQL.format(source=namespace, witness=witness, payload_sha256=payload_sha256)


async def _verify_source_page(connection, recipe, context):
    """Full source admission precedes this call; every page retains fresh native custody."""
    source = recipe.source_pin
    if source.retained_capture is not None:
        from process.network_fhir_membership_source import RetainedFHIRSuccessorReads
        from process.network_fhir_retained_capture import RetainedFHIRRead

        reads = context.retained_read
        if source.retained_successor is None:
            if type(reads) is not RetainedFHIRRead:
                raise ValueError("registry_fact_office_read_unavailable")
            await reads.verify(connection, source.retained_capture)
        else:
            if type(reads) is not RetainedFHIRSuccessorReads:
                raise ValueError("registry_fact_office_read_unavailable")
            await reads.original.verify(connection, source.retained_capture)
            await reads.physical.verify(connection, source.retained_successor.retained_capture)
    else:
        from process.network_fhir_source_epoch import require_retained_cms_fhir_source_epoch

        for pin in (replace(source, retained_successor=None), source.retained_successor):
            if pin is not None:
                await require_retained_cms_fhir_source_epoch(connection, pin, pin.retained_epoch, verify_content=False)


async def _match_pair_page(connection, recipe, encoded, context):
    await _verify_source_page(connection, recipe, context)
    physical = fhir_physical_source(recipe.source_pin)
    source_summary = await connection.fetchrow(
        _location_query(recipe.source_pin),
        encoded,
        recipe.source_pin.source_id,
        physical.dataset_id,
        physical.release_id,
    )
    origin = await resolve_source_office_origin(
        connection,
        context.address_source,
        runtime_roles=context.runtime_roles,
        control_schema=context.control_schema,
        origin_manifest_sha256=context.origin_manifest_sha256,
        first_parameter=3,
    )
    offices, bindings = await _match_source_office_records(
        connection,
        source_summary,
        context.address_source,
        address_relation=origin.relation_sql,
        address_arguments=origin.arguments,
    )
    return offices, bindings, origin.address_identity


async def capture_fact_office_pages(connection, recipe, artifact, context):
    """Reauthenticate original facts, remap every pair and prove selected native Location rows."""
    if type(context) is not FactOfficeContext or type(artifact) is not ResolvedSourceFactArtifact:
        raise ValueError("registry_fact_office_scope_invalid")
    recipe_sha256, capture_sha256 = recipe_contribution_identity(recipe)
    if (artifact.descriptor["recipe_sha256"], artifact.descriptor["capture_sha256"]) != (recipe_sha256, capture_sha256):
        raise ValueError("registry_fact_office_capture_changed")
    owner_map = {**artifact.descriptor, **asdict(artifact.reference)}
    actual = await verify_anchored_source_fact_contents(connection, (owner_map,), control_schema=context.control_schema)
    if recipe_sha256 not in actual:
        raise ValueError("registry_fact_office_capture_changed")
    artifact = actual[recipe_sha256]
    mapping = await source_mapping_sha256(connection, recipe, context.approved, context.control_schema)
    from process.registry_source_recipe_composition import _require_recipe_custody

    if recipe.source_pin.retained_capture is not None and context.retained_read is not None:
        await _verify_source_page(connection, recipe, context)
    else:
        read = await _require_recipe_custody(connection, recipe, context.approved, context.control_schema)
        context = replace(context, retained_read=read)
    origin = await resolve_source_office_origin(
        connection,
        context.address_source,
        runtime_roles=context.runtime_roles,
        control_schema=context.control_schema,
        origin_manifest_sha256=context.origin_manifest_sha256,
        first_parameter=3,
    )
    digest = hashlib.sha256((_sha({"address_identity": origin.address_identity}) + "\n").encode())
    office_rows, page_count = 0, 0
    async with _pair_cursor(connection, recipe, artifact, context) as cursor:
        while True:
            pairs = await connection.fetch(f"FETCH FORWARD {MAX_PAIR_ROWS} FROM {_identifier(cursor)}")
            if not pairs:
                break
            encoded = _pair_documents(pairs)
            offices, bindings, address_identity = await _match_pair_page(connection, recipe, encoded, context)
            if context.page_sink is not None:
                await context.page_sink(bindings)
            page_hash = _sha({"pairs": json.loads(encoded), "offices": offices, "address_identity": address_identity})
            digest.update((page_hash + "\n").encode())
            office_rows += len(bindings)
            page_count += 1
    return {
        "recipe_sha256": recipe_sha256,
        "mode": "facts",
        "mapping_sha256": mapping,
        "physical_source_sha256": fhir_physical_source(recipe.source_pin).generation_id,
        "fact_artifact": {
            **asdict(artifact.reference),
            **{
                key: artifact.descriptor[key]
                for key in ("capture_sha256", "transform_version", "fact_sha256", "fact_rows")
            },
        },
        "office_rows": office_rows,
        "page_count": page_count,
        "correspondence_sha256": digest.hexdigest(),
    }
