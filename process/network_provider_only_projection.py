# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prepare provider-only evidence; only independent SOURCE parity can seal it.

This private candidate adds no serving capability. Its caller must authenticate
the complete source inventory, close both heaps under independent custody, bind
the returned receipt to that closure, and pin the approved revision before reads.
"""

import hashlib
import re
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from process.network_address_projection import _identifier
from process.network_approved_membership_source import ApprovedMembershipSource
from process.network_approved_source_bindings import (
    APPROVED_NETWORK_BINDINGS_SQL,
    require_retained_approved_network_source_bindings,
)
from process.network_fhir_membership_source import (
    FHIRMembershipBatchBoundsError,
    PinnedFHIRMembershipSource,
    fhir_physical_source,
    read_fhir_source_witness_page_v2,
)
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_membership_copy import MembershipCopyTarget
from process.registry_source_contribution_artifacts import (
    _validated_descriptor,
    authenticate_anchored_source_fact_metadata,
    source_fact_artifact_reference,
    verify_anchored_source_fact_contents,
)
from process.registry_source_contribution_store import (
    CONTRIBUTION_TABLE,
    FACT_ANCHOR_FIELDS,
    copy_fhir_fact_page,
    copy_fhir_source_witness_page_v2,
    create_contribution_stage,
    decode_fhir_source_witness_page_v2,
    map_fhir_facts,
    map_fhir_source_witnesses_v2,
    recipe_contribution_identity,
    validate_source_witness_descriptor_v2,
    verified_office_fact_identity,
)
from process.registry_source_recipe_composition import (
    RegistrySourceRecipeStage,
    _read_recipe_page,
    _require_recipe_custody,
)
from process.registry_source_recipe_store import (
    RegistrySourceMembershipRecipe,
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)
from process.uhc_flex_practitioner_async_safety import drain_operation

TABLE_NAME = "network_provider_only_evidence"
INVENTORY_TABLE = "network_source_inventory"
COLUMNS = "network_id,provider_system,provider_id,evidence_id"

# Keep the legacy selected-set expression exact, including JSON null versus SQL NULL.
_SELECT_SQL = """SELECT DISTINCT fact_json->>'provider_system' AS provider_system,fact_json->>'provider_id' AS provider_id,
    approved.network_id,encode(sha256(convert_to(jsonb_set(fact_json->'evidence_basis','{{6}}',
      to_jsonb(approved.network_id))::text,'UTF8')),'hex') AS evidence_id
  FROM {facts} fact JOIN selected_bindings approved ON fact.recipe_sha256=approved.recipe_sha256
    AND fact.fact_json->>'source_key'=approved.record_json->>'source_key'
    AND fact.fact_json->'source_scope_json'=approved.record_json->'source_scope_json'
  WHERE fact_json->'location_id'='null'::jsonb AND fact_json->'omission_reason'='null'::jsonb"""

_SELECT_SQL_V2 = """SELECT DISTINCT edge.fact_json->>'provider_system' AS provider_system,
    edge.fact_json->>'provider_id' AS provider_id,approved.network_id,
    encode(sha256(convert_to(jsonb_set(edge.fact_json->'evidence_basis','{{6}}',
      to_jsonb(approved.network_id))::text,'UTF8')),'hex') AS evidence_id
  FROM {facts} edge JOIN {facts} header ON header.recipe_sha256=edge.recipe_sha256
    AND header.fact_json->>'kind'='resource'
    AND header.fact_json->>'resource_type'=edge.fact_json->>'resource_type'
    AND header.fact_json->>'resource_id'=edge.fact_json->>'resource_id'
    AND header.fact_json->'payload_sha256'=edge.fact_json->'payload_sha256'
  JOIN selected_bindings approved ON edge.recipe_sha256=approved.recipe_sha256
    AND edge.fact_json->>'source_key'=approved.record_json->>'source_key'
    AND edge.fact_json->'source_scope_json'=approved.record_json->'source_scope_json'
  WHERE edge.fact_json->>'kind'='edge' AND header.fact_json->'malformed'='false'::jsonb
    AND header.fact_json->'omission_reason'='null'::jsonb
    AND edge.fact_json->'provider_only'='true'::jsonb AND edge.fact_json->'location_id'='null'::jsonb
    AND edge.fact_json->'provider_unresolved'='false'::jsonb
    AND edge.fact_json->'location_unresolved'='false'::jsonb
    AND edge.fact_json->'network_witness_complete'='true'::jsonb"""


class ProviderOnlyProjectionError(ValueError):
    """Candidate scope, SOURCE evidence or exact projection parity differs."""


def _require(condition):
    if not condition:
        raise ProviderOnlyProjectionError("network_provider_only_projection_differs")


def _relation(schema_name):
    return _identifier(schema_name) + "." + TABLE_NAME


def _native_descriptor(descriptor):
    if (
        type(descriptor) is dict
        and type(descriptor.get("descriptor_version")) is int
        and descriptor["descriptor_version"] == 2
    ):
        native = validate_source_witness_descriptor_v2(descriptor)
        return {**native, **dict.fromkeys(FACT_ANCHOR_FIELDS)}
    return _validated_descriptor(descriptor)


def _source(recipe, descriptor):
    recipes = decode_registry_source_recipes(canonical_registry_source_recipes((recipe,)))
    recipe = recipes[0]
    _require(type(recipe.source_pin) is PinnedFHIRMembershipSource)
    native = _native_descriptor(descriptor)
    identity = verified_office_fact_identity if native.get("descriptor_version") == 2 else recipe_contribution_identity
    _require((native["recipe_sha256"], native["capture_sha256"]) == identity(recipe))
    source_fact_artifact_reference(descriptor)
    return recipe, native


async def _candidate(connection, target, approved, control_schema, *, preparing):
    _require(type(target) is MembershipCopyTarget and connection.is_in_transaction())
    if approved is not None:
        _require(type(approved) is ApprovedMembershipSource)
    namespace = _control_namespace(control_schema)
    row = await connection.fetchrow(
        f"""SELECT dataset_id,schema_id,producer_id,candidate_id,schema_name,state,approved_custom_revision,
        source_generations->>'custom_membership' AS approved_generation
        FROM {namespace}.network_membership_candidate WHERE candidate_id=$1"""
        + (" FOR UPDATE" if preparing else ""),
        UUID(target.candidate_id),
    )
    _require(
        row is not None
        and all(
            str(row[key]) == getattr(target, key)
            for key in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
        )
        and row["state"] == ("open" if preparing else "ready")
    )
    if approved is not None:
        _require(
            row["approved_custom_revision"] == approved.approved_revision
            and row["approved_generation"] == approved.generation_id
        )


async def _create_evidence_heap(connection, relation, *, temporary=False):
    # Independent expected rows must never inherit producer constraints or indexes.
    await connection.execute(f"""CREATE {"TEMP " if temporary else ""}TABLE {relation}(
      network_id integer NOT NULL CHECK(network_id>0),
      provider_system text COLLATE "C" NOT NULL CHECK(provider_system IN ('npi','provider_directory')),
      provider_id text COLLATE "C" NOT NULL CHECK(btrim(provider_id)<>'' AND octet_length(provider_id)<=1024),
      evidence_id text COLLATE "C" NOT NULL CHECK(evidence_id ~ '^[0-9a-f]{{64}}$'),
      PRIMARY KEY(network_id,provider_system,provider_id,evidence_id))""")


async def create_provider_only_projection(connection, copy_target, *, control_schema=None):
    """Create an empty preparation heap for the exact open candidate, never a sentinel."""
    await _candidate(connection, copy_target, None, control_schema, preparing=True)
    relation = _relation(copy_target.schema_name)
    await _create_evidence_heap(connection, relation)
    await connection.execute(f"""CREATE INDEX provider_only_identity_idx ON {relation}
      (provider_system,provider_id,network_id,evidence_id)""")


async def prepare_provider_only_projection(connection, copy_target, recipes, stage, approved, *, control_schema=None):
    """Project producer facts without reopening SOURCE; independent custody still checks every row."""
    recipes_by_identity, descriptors_by_identity = _staged_sources(recipes, stage)
    await _candidate(connection, copy_target, approved, control_schema, preparing=True)
    await create_provider_only_projection(connection, copy_target, control_schema=control_schema)
    await _append_staged_sources(
        connection,
        _relation(copy_target.schema_name),
        recipes_by_identity,
        descriptors_by_identity,
        stage,
        approved,
        control_schema,
    )


def _source_query(recipe, fact_relation, control_schema, *, descriptor_version=None):
    source = recipe.source_pin
    capture = source.retained_capture
    scope_key = "alias_scope" if capture is None else "capture_sha256"
    organization = (
        " AND record_json->'source_scope_json'->>'organization_id'=record_json->>'source_key'"
        if capture is not None
        else ""
    )
    bindings = APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_control_namespace(control_schema))
    return (
        f"WITH {bindings}, selected_bindings AS MATERIALIZED ("
        f"SELECT $8::text AS recipe_sha256,network_id,record_json FROM approved_network_bindings "
        f"WHERE record_json->'source_scope_json'->>'{scope_key}'=$9::text{organization}) "
        + (_SELECT_SQL_V2 if descriptor_version == 2 else _SELECT_SQL).format(facts=fact_relation)
    )


def _source_arguments(recipe, approved, *, descriptor_version=None):
    capture = recipe.source_pin.retained_capture
    scope = recipe.source_pin.alias_scope if capture is None else capture.capture_sha256
    return (
        approved.approved_revision,
        *recipe.binding_coordinates.sql_parameters,
        (verified_office_fact_identity if descriptor_version == 2 else recipe_contribution_identity)(recipe)[0],
        scope,
    )


async def _append(connection, destination, fact_relation, recipe, approved, control_schema, *, descriptor_version=None):
    selected = _source_query(recipe, fact_relation, control_schema, descriptor_version=descriptor_version)
    await connection.execute(
        f"INSERT INTO {destination}({COLUMNS}) SELECT {COLUMNS} FROM ({selected}) selected ON CONFLICT DO NOTHING",
        *_source_arguments(recipe, approved, descriptor_version=descriptor_version),
    )


async def append_provider_only_source(connection, copy_target, recipe, descriptor, approved, *, control_schema=None):
    """Prepare one source's distinct rows; replay has no authority until full parity."""
    recipe, native = _source(recipe, descriptor)
    await _candidate(connection, copy_target, approved, control_schema, preparing=True)
    await require_retained_approved_network_source_bindings(
        connection, approved, recipe.binding_coordinates, control_schema=control_schema
    )
    if source_fact_artifact_reference(descriptor) is None:
        fact_relation = _identifier(copy_target.schema_name) + "." + CONTRIBUTION_TABLE
    else:
        artifacts = await verify_anchored_source_fact_contents(
            connection, [descriptor], control_schema=_control_namespace(control_schema)[1:-1]
        )
        fact_relation = artifacts[native["recipe_sha256"]].relation
    mapper = map_fhir_source_witnesses_v2 if native.get("descriptor_version") == 2 else map_fhir_facts
    actual = await mapper(
        connection, recipe, approved, fact_relation, "unused_provider_only", control_schema, insert=False
    )
    _require(actual == native)
    await _append(
        connection,
        _relation(copy_target.schema_name),
        fact_relation,
        recipe,
        approved,
        control_schema,
        descriptor_version=native.get("descriptor_version"),
    )


async def _rebuild_source(
    connection, recipe, fact_table, approved, control_schema, office_authority, *, descriptor_version=None
):
    if descriptor_version == 2:
        return await _rebuild_source_witnesses_v2(
            connection, recipe, fact_table, approved, control_schema, office_authority
        )
    source_read = await _require_recipe_custody(connection, recipe, approved, control_schema, office_authority)
    cursor, generation, ordinal, limit = None, None, 0, 1000
    while True:
        batch, next_cursor, limit = await _read_recipe_page(
            connection, recipe, approved, cursor, control_schema, limit, source_read, capture_facts=True
        )
        _require(
            batch.source == recipe.source_pin
            and batch.approved_source == approved
            and batch.binding_coordinates == recipe.binding_coordinates
            and batch.complete_raw_facts is True
            and not batch.unresolved_rows
            and not batch.unresolved_source_rows
            and hashlib.sha256(batch.input_bytes).hexdigest() == batch.input_sha256
            and (generation is None or generation == batch.generation_id)
        )
        generation = batch.generation_id
        ordinal = await copy_fhir_fact_page(
            connection, batch, fact_table, recipe_contribution_identity(recipe)[0], ordinal
        )
        if not batch.source_rows:
            _require(not batch.membership_rows and not batch.omitted_rows)
            return
        _require(next_cursor is not None and next_cursor != cursor)
        cursor = next_cursor


async def _source_witness_census_v2(connection, source_pin):
    physical = fhir_physical_source(source_pin)
    membership_kinds = ("PractitionerRole", "OrganizationAffiliation")
    if physical.retained_capture is not None:
        count = sum(
            amount for kind, amount in physical.retained_capture.origin.resource_counts if kind in membership_kinds
        )
    else:
        count = await connection.fetchval(
            f"SELECT count(*) FROM {_identifier(physical.read_schema_name)}.provider_directory_dataset_resource "
            "WHERE dataset_id=$1 AND resource_type=ANY($2::text[])",
            physical.dataset_id,
            list(membership_kinds),
        )
    _require(type(count) is int and 0 <= count < 2**63)
    return count


async def _rebuild_source_witnesses_v2(connection, recipe, fact_table, approved, control_schema, office_authority):
    source_read = await _require_recipe_custody(connection, recipe, approved, control_schema, office_authority)
    expected_resources = await _source_witness_census_v2(connection, recipe.source_pin)
    resource_count = 0
    cursor, ordinal, limit = None, 0, 1000
    while True:
        try:
            page = await read_fhir_source_witness_page_v2(
                connection,
                recipe.source_pin,
                registry_schema=control_schema,
                after_resource=cursor,
                limit=limit,
                approved_source=approved,
                binding_coordinates=recipe.binding_coordinates,
                retained_read=source_read,
            )
        except FHIRMembershipBatchBoundsError:
            if limit == 1:
                raise
            limit //= 2
            continue
        _require(page.source == recipe.source_pin)
        documents = decode_fhir_source_witness_page_v2(page)
        resources = [
            (document["resource_type"], document["resource_id"])
            for document in documents
            if document["kind"] == "resource"
        ]
        _require(cursor is None or all(identity > cursor for identity in resources))
        resource_count += page.source_rows
        _require(resource_count <= expected_resources)
        if not page.source_rows:
            _require(page.next_resource is None and resource_count == expected_resources)
            return
        ordinal = await copy_fhir_source_witness_page_v2(
            connection, page, fact_table, verified_office_fact_identity(recipe)[0], ordinal
        )
        _require(page.next_resource is not None and (cursor is None or page.next_resource > cursor))
        cursor = page.next_resource


async def _receipt(connection, relation):
    digest, count, after_keys = hashlib.sha256(), 0, None
    while True:
        page = await connection.fetch(
            f"""SELECT {COLUMNS},encode(sha256(convert_to(jsonb_build_array({COLUMNS})::text,'UTF8')),'hex')
            AS fingerprint FROM {relation} WHERE $1::integer IS NULL OR
              (network_id,provider_system,provider_id,evidence_id)>($1,$2::text,$3::text,$4::text)
            ORDER BY network_id,provider_system COLLATE "C",provider_id COLLATE "C",evidence_id COLLATE "C"
            LIMIT 1000""",
            *(after_keys or (None, None, None, None)),
        )
        if not page:
            return {"row_count": count, "rows_sha256": digest.hexdigest()}
        for row in page:
            after_keys = tuple(row[key] for key in COLUMNS.split(","))
            digest.update(row["fingerprint"].encode("ascii"))
            count += 1


@asynccontextmanager
async def _expected_evidence(connection):
    """Own a fixed trusted expected heap; producer constraints never enter this definition."""
    name = "provider_only_expected_" + uuid4().hex
    expected = "pg_temp." + _identifier(name)
    try:
        await _create_evidence_heap(connection, expected, temporary=True)
        yield name, expected
    finally:
        await drain_operation(connection.execute(f"DROP TABLE IF EXISTS {expected}"), preserve_cancellation=True)


async def _expected_receipt(connection, copy_target, expected, source_count, source_identities_sha256):
    """Compare both complete native sets before returning a non-authoritative receipt."""
    actual = _relation(copy_target.schema_name)
    parity = await connection.fetchrow(f"""SELECT
      (SELECT count(*) FROM {actual}) AS actual_count,(SELECT count(*) FROM {expected}) AS expected_count,
      EXISTS((SELECT {COLUMNS} FROM {actual} EXCEPT ALL SELECT {COLUMNS} FROM {expected})
      UNION ALL (SELECT {COLUMNS} FROM {expected} EXCEPT ALL SELECT {COLUMNS} FROM {actual})) AS differs""")
    _require(parity is not None and parity["differs"] is False and parity["actual_count"] == parity["expected_count"])
    receipt = await _receipt(connection, actual)
    _require(receipt["row_count"] == parity["actual_count"])
    return {**receipt, "source_count": source_count, "source_identities_sha256": source_identities_sha256}


async def verify_provider_only_projection(
    connection, copy_target, source_entries, approved, *, control_schema=None, office_authority=None
):
    """Rebuild SOURCE independently from a complete authenticated inventory stream.

    Entries are ordered (recipe, descriptor) pairs, not producer projection rows.
    The caller must match this source identity commitment to the independently
    checked inventory and retain native writer closure through publication.
    """
    await _candidate(connection, copy_target, approved, control_schema, preparing=False)
    identities, source_count, previous = hashlib.sha256(), 0, ""
    async with _expected_evidence(connection) as (name, expected):
        facts = name + "_facts"
        fact_relation = "pg_temp." + _identifier(facts)
        try:
            await create_contribution_stage(connection, facts)
            async for recipe, descriptor in source_entries:
                recipe, native = _source(recipe, descriptor)
                identity = native["recipe_sha256"]
                _require(identity > previous)
                previous = identity
                identities.update(identity.encode("ascii"))
                source_count += 1
                await connection.execute(f"TRUNCATE {fact_relation}")
                version = native.get("descriptor_version")
                await _rebuild_source(
                    connection, recipe, facts, approved, control_schema, office_authority, descriptor_version=version
                )
                mapper = map_fhir_source_witnesses_v2 if version == 2 else map_fhir_facts
                rebuilt = await mapper(
                    connection, recipe, approved, fact_relation, "unused_provider_only", control_schema, insert=False
                )
                _require(rebuilt == native)
                await _append(
                    connection, expected, fact_relation, recipe, approved, control_schema, descriptor_version=version
                )
            return await _expected_receipt(connection, copy_target, expected, source_count, identities.hexdigest())
        finally:
            await drain_operation(
                connection.execute(f"DROP TABLE IF EXISTS {fact_relation}"), preserve_cancellation=True
            )


def _staged_sources(recipes, stage):
    _require(
        type(stage) is RegistrySourceRecipeStage
        and type(stage.contribution_revision) is int
        and stage.contribution_revision == 2
        and type(stage.contribution_receipts) is tuple
        and len(stage.contribution_receipts) <= 1000
    )
    descriptors_by_identity, recipes_by_identity, recipe_ids = {}, {}, set()
    for descriptor in stage.contribution_receipts:
        native = _native_descriptor(descriptor)
        _require(FACT_ANCHOR_FIELDS <= descriptor.keys() and native["recipe_sha256"] not in descriptors_by_identity)
        source_fact_artifact_reference(descriptor)
        descriptors_by_identity[native["recipe_sha256"]] = dict(descriptor)
    for count, recipe in enumerate(recipes, 1):
        _require(count <= 1000 and type(recipe) is RegistrySourceMembershipRecipe)
        identity = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
        _require(identity not in recipe_ids)
        recipe_ids.add(identity)
        if type(recipe.source_pin) is not PinnedFHIRMembershipSource:
            continue
        _require(identity not in recipes_by_identity and identity in descriptors_by_identity)
        _source(recipe, descriptors_by_identity[identity])
        recipes_by_identity[identity] = recipe
    _require(recipes_by_identity.keys() == descriptors_by_identity.keys())
    return recipes_by_identity, descriptors_by_identity


async def _append_staged_sources(
    connection, expected, recipes_by_identity, descriptors_by_identity, stage, approved, control_schema
):
    identities = hashlib.sha256()
    ordered = sorted(recipes_by_identity)
    for offset in range(0, len(ordered), 100):
        selected_ids = ordered[offset : offset + 100]
        anchored_descriptors = [
            descriptors_by_identity[identity]
            for identity in selected_ids
            if source_fact_artifact_reference(descriptors_by_identity[identity]) is not None
        ]
        artifacts = (
            (
                await authenticate_anchored_source_fact_metadata(
                    connection, anchored_descriptors, control_schema=_control_namespace(control_schema)[1:-1]
                )
            )
            if anchored_descriptors
            else {}
        )
        _require(set(artifacts) == {descriptor["recipe_sha256"] for descriptor in anchored_descriptors})
        for identity in selected_ids:
            descriptor = descriptors_by_identity[identity]
            if source_fact_artifact_reference(descriptor) is None:
                _require(
                    type(stage.contribution_table_name) is str
                    and re.fullmatch(r"registry_source_fact_[0-9a-f]{32}", stage.contribution_table_name) is not None
                )
                relation = "pg_temp." + _identifier(stage.contribution_table_name)
            else:
                relation = artifacts[identity].relation
            await _append(
                connection,
                expected,
                relation,
                recipes_by_identity[identity],
                approved,
                control_schema,
                descriptor_version=descriptor.get("descriptor_version"),
            )
            identities.update(identity.encode("ascii"))
    return identities.hexdigest()


async def _verify_staged_provider_only_projection(
    connection, copy_target, recipes, stage, approved, *, control_schema=None
):
    """Consume only the custodian's own independently verified stage before its DROP.

    The custody entrypoint must have already checked independent stage receipts,
    native fact parity, retention and composition, with SOURCE/actor locks held.
    A producer stage DTO cannot establish that provenance. This private helper
    returns parity metadata, never a seal or capability, and reuses no raw SOURCE
    reads, mapping, or content hashes. The issuer must bind every FHIR identity to its
    independently verified complete inventory before issuing custody.
    """
    recipes_by_identity, descriptors_by_identity = _staged_sources(recipes, stage)
    await _candidate(connection, copy_target, approved, control_schema, preparing=False)
    async with _expected_evidence(connection) as (_name, expected):
        identity_sha256 = await _append_staged_sources(
            connection, expected, recipes_by_identity, descriptors_by_identity, stage, approved, control_schema
        )
        return await _expected_receipt(connection, copy_target, expected, len(recipes_by_identity), identity_sha256)


def provider_only_page_sql(schema_name, *, control_schema=None):
    """Constant-shape SQL after independent closure; arguments are revision, networks,
    limit, offset, maximum evidence, maximum bytes and pinned generation.

    The approved-record lookup index and its native plan remain integration work;
    this eliminates full recipe decoding and generated per-source SQL, not all IO.
    """
    from api.network_provider_only_read import _PAGE_SQL

    namespace, inventory = _control_namespace(control_schema), _identifier(schema_name) + "." + INVENTORY_TABLE
    # Share the existing response, ordering and byte/evidence bounds exactly.
    suffix = "), identities AS MATERIALIZED (" + _PAGE_SQL.split("), identities AS MATERIALIZED (", 1)[1]
    prefix = f"""WITH selected_bindings AS MATERIALIZED (
      SELECT DISTINCT parsed.network_id FROM ONLY {namespace}.registry_approved_record binding
      CROSS JOIN LATERAL (SELECT CASE
        WHEN jsonb_typeof(binding.record_json->'network_id')='number'
          AND binding.record_json->>'network_id' ~ '^[1-9][0-9]{{0,9}}$'
        THEN CASE WHEN (binding.record_json->>'network_id')::numeric<=2147483647
          THEN (binding.record_json->>'network_id')::integer END END AS network_id) parsed
      JOIN ONLY {inventory} source ON source.source_system='fhir'
        AND binding.record_json->>'source_system'=source.source_system
        AND binding.record_json->>'source_id'=source.source_id
        AND binding.record_json->>'dataset_schema'=source.dataset_schema
        AND binding.record_json->>'dataset_id'=source.dataset_id
        AND binding.record_json->>'producer_id'=source.producer_id
        AND binding.record_json->>'edition_id'=source.edition_id
        AND binding.record_json->'source_scope_json'->>source.fhir_scope_key=source.fhir_scope_value
        AND (source.fhir_scope_key='alias_scope' OR source.fhir_scope_key='capture_sha256'
          AND binding.record_json->'source_scope_json'->>'organization_id'=binding.record_json->>'source_key')
      JOIN ONLY {namespace}.registry_approved_record network ON network.approved_revision=$1
        AND network.record_kind='network' AND network.record_key=parsed.network_id::text
        AND network.record_json->'archived'='false'::jsonb
        AND jsonb_typeof(network.record_json->'network_id')='number'
        AND network.record_json->>'network_id'=parsed.network_id::text
      WHERE binding.approved_revision=$1 AND binding.record_kind='network_binding'
        AND binding.record_json->'archived'='false'::jsonb AND parsed.network_id=ANY($2::integer[])
    ), selected AS MATERIALIZED (
      SELECT evidence.* FROM ONLY {_relation(schema_name)} evidence
      WHERE network_id=ANY($2::integer[]) AND EXISTS(
        SELECT 1 FROM selected_bindings approved WHERE approved.network_id=evidence.network_id)
    """
    return prefix + suffix.format(first=2, limit=3, offset=4, maximum_evidence=5, maximum_bytes=6, generation=7)
