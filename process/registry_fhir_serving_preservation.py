# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Prove whole-network FHIR absence against an authenticated predecessor vector.

Only temporary native stages contain selected identities. Counts and ordered
digests bind them to the independently sealed candidate; receipts select no rows.
"""

import hashlib
import json
import re
from dataclasses import asdict, dataclass, replace
from typing import NamedTuple
from uuid import UUID, uuid4, uuid5

from db.plan_release_storage import storage_identifier
from process.network_address_projection import copy_network_address_projection_rows
from process.network_approved_membership_source import pin_retained_approved_membership_source
from process.network_approved_source_bindings import require_retained_approved_network_source_bindings
from process.network_fhir_membership_source import PinnedFHIRMembershipSource, fhir_physical_source
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_source_observation_store import source_observation_namespace
from process.registry_source_recipe_composition import (
    OFFICE_MAPPING_KEY,
    SourceRecipeReadContext,
    require_registry_source_recipe_custody,
)
from process.registry_source_recipe_store import (
    RECIPE_DIGEST_KEY,
    canonical_registry_source_recipes,
    resolve_retained_registry_source_recipes,
    verify_registry_source_recipes,
)

SUMMARY_KEY = "registry_fhir_serving_preservation"
_DIGESTS = {
    "predecessor_manifest_sha256",
    "previous_recipes_sha256",
    "current_recipes_sha256",
    "coverage_sha256",
    "missing_networks_sha256",
    "membership_sha256",
    "bindings_sha256",
    "addresses_sha256",
    "operation_sha256",
}
_COUNTS = {"approved_revision", "expected_head", "missing_networks", "carried_rows", "binding_rows", "address_rows"}
_FIELDS = (
    _DIGESTS
    | _COUNTS
    | {"component", "revision", "predecessor_generation", "predecessor_candidate_id", "request_id", "operation"}
)
_ROOT_FIELDS = {"previous_recipe_root", "current_recipe_root"}


class FHIRServingPreservationError(ValueError):
    """The complete retained identity or whole-network carry cannot be proved."""


def _fail(reason):
    raise FHIRServingPreservationError("registry_fhir_serving_preservation_" + reason)


def _canonical(document):
    return json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)


def validate_fhir_serving_preservation(receipt):
    """Validate the compact closed receipt without treating it as row authority."""
    if type(receipt) is not dict:
        _fail("receipt_invalid")
    revision = receipt.get("revision")
    if (
        type(revision) is not int
        or revision not in (1, 2)
        or set(receipt) != _FIELDS | (_ROOT_FIELDS if revision == 2 else set())
    ):
        _fail("receipt_invalid")
    if receipt["component"] != SUMMARY_KEY:
        _fail("receipt_invalid")
    if revision == 2:
        _validate_recipe_roots(receipt)
    if any(type(receipt[key]) is not str or re.fullmatch("[0-9a-f]{64}", receipt[key]) is None for key in _DIGESTS):
        _fail("receipt_invalid")
    if any(type(receipt[key]) is not int or not 0 <= receipt[key] < 2**63 for key in _COUNTS):
        _fail("receipt_invalid")
    if type(receipt["predecessor_generation"]) is not int or not 0 < receipt["predecessor_generation"] < 2**63:
        _fail("receipt_invalid")
    try:
        for field in ("predecessor_candidate_id", "request_id"):
            identity = receipt[field]
            if type(identity) is not str or str(UUID(identity)) != identity or not UUID(identity).int:
                _fail("receipt_invalid")
    except ValueError, TypeError, AttributeError:
        _fail("receipt_invalid")
    if (receipt["missing_networks"] == 0) != (receipt["carried_rows"] == 0):
        _fail("receipt_invalid")
    if type(receipt["operation"]) is not str or receipt["operation"] not in {"forward", "source_rollback"}:
        _fail("receipt_invalid")
    if (
        not receipt["expected_head"]
        or not 0 <= receipt["address_rows"] <= receipt["binding_rows"] <= receipt["carried_rows"]
    ):
        _fail("receipt_invalid")
    if receipt["operation"] == "forward" and (
        receipt["predecessor_generation"] != receipt["expected_head"]
        or receipt["operation_sha256"] != hashlib.sha256(b"forward").hexdigest()
    ):
        _fail("receipt_invalid")
    if len(_canonical(receipt).encode()) > 4096:
        _fail("receipt_invalid")
    return dict(receipt)


def _validate_recipe_roots(receipt):
    """Revision two explicitly commits complete bounded recipe-only inventories."""
    from process.registry_source_inventory_contract import SourceInventoryRoot

    try:
        for side in ("previous", "current"):
            root = SourceInventoryRoot(**receipt[side + "_recipe_root"])
            if (
                root.contribution_count
                or root.derivation_count
                or root.recipes_sha256 != receipt[side + "_recipes_sha256"]
            ):
                raise ValueError
    except ValueError, TypeError, KeyError:
        _fail("receipt_invalid")


def fhir_serving_preservation_sha256(receipt):
    """Hash the canonical closed receipt, never a caller-supplied ID collection."""
    return hashlib.sha256(_canonical(validate_fhir_serving_preservation(receipt)).encode()).hexdigest()


def verify_fhir_serving_preservation(candidate):
    """Require paired immutable scope and report fields, including zero carry."""
    report, generations = candidate.get("validation_json") or {}, candidate["source_generations"]
    report = json.loads(report) if type(report) is str else report
    generations = json.loads(generations) if type(generations) is str else generations
    if type(report) is not dict or type(generations) is not dict:
        _fail("receipt_invalid")
    if SUMMARY_KEY not in report and SUMMARY_KEY not in generations:
        return None
    receipt = validate_fhir_serving_preservation(report.get(SUMMARY_KEY))
    if (
        generations.get(SUMMARY_KEY) != fhir_serving_preservation_sha256(receipt)
        or receipt["approved_revision"] != candidate["approved_custom_revision"]
    ):
        _fail("receipt_invalid")
    if receipt["revision"] == 2:
        from process.registry_source_inventory_reference import verify_source_recipe_metadata

        metadata = verify_source_recipe_metadata(candidate)
        root = receipt["current_recipe_root"]
        if metadata.inventory is None or (root["entry_count"], root["recipes_sha256"]) != (
            metadata.recipe_count,
            metadata.recipes_sha256,
        ):
            _fail("receipt_invalid")
    return receipt


def _family(recipe):
    coordinates = recipe.binding_coordinates
    return coordinates.source_system, coordinates.source_id, coordinates.dataset_schema, coordinates.producer_id


def _slot(recipe):
    source = recipe.source_pin
    capture = source.retained_capture
    # Reviewed products/captures keep their exact pinned-capture semantics.
    return (*_family(recipe), source.endpoint_id, None if capture is None else str(capture.capture_id))


def _recipe_commitment(recipes, inventory):
    if type(inventory) is not bool:
        _fail("receipt_invalid")
    if not inventory:
        return hashlib.sha256(canonical_registry_source_recipes(recipes).encode()).hexdigest(), None
    from process.registry_source_inventory_preparation import SourceInventoryPreparation

    root = SourceInventoryPreparation(recipes).recipe_root
    return root.recipes_sha256, asdict(root)


def _fhir_recipes(recipes, *, inventory=False):
    selected_recipes = tuple(recipe for recipe in recipes if type(recipe.source_pin) is PinnedFHIRMembershipSource)
    return (
        _recipe_commitment(selected_recipes, True)[0]
        if inventory
        else canonical_registry_source_recipes(selected_recipes)
    )


def fhir_vector_succession(previous, current):
    """Conserve every protected physical endpoint; other families retain their own authority."""
    previous_by_slot = {
        _slot(recipe): recipe for recipe in previous if type(recipe.source_pin) is PinnedFHIRMembershipSource
    }
    current_by_slot = {
        _slot(recipe): recipe for recipe in current if type(recipe.source_pin) is PinnedFHIRMembershipSource
    }
    if (
        len(previous_by_slot) != sum(type(recipe.source_pin) is PinnedFHIRMembershipSource for recipe in previous)
        or len(current_by_slot) != sum(type(recipe.source_pin) is PinnedFHIRMembershipSource for recipe in current)
        or not previous_by_slot.keys() <= current_by_slot.keys()
    ):
        _fail("vector_incomplete")
    return tuple((recipe, current_by_slot[slot]) for slot, recipe in previous_by_slot.items())


def require_fhir_source_event_delta(previous, current, assertion, *, inventory=False):
    """Derive the actual vector delta; a checkpoint's shape or digest is insufficient."""
    pairs = fhir_vector_succession(previous, current)
    changed_recipes = [
        (old, new)
        for old, new in pairs
        if canonical_registry_source_recipes((old,)) != canonical_registry_source_recipes((new,))
    ]
    if assertion is None:
        return
    if assertion["kind"] != "cms":
        if changed_recipes:
            _fail("event_delta")
        return
    checkpoint = assertion["checkpoint"]
    if len(changed_recipes) != 1:
        _fail("event_delta")
    old, new = changed_recipes[0]
    if (checkpoint["replaced_coordinates"], checkpoint["coordinates"], checkpoint["recipe_sha256"]) != (
        asdict(old.binding_coordinates),
        asdict(new.binding_coordinates),
        hashlib.sha256(canonical_registry_source_recipes((new,)).encode()).hexdigest(),
    ):
        _fail("event_delta")
    previous_remaining_recipes = tuple(recipe for recipe in previous if recipe != old)
    current_remaining_recipes = tuple(recipe for recipe in current if recipe != new)
    if _recipe_commitment(previous_remaining_recipes, inventory) != _recipe_commitment(
        current_remaining_recipes, inventory
    ):
        _fail("event_delta")


async def _ordered_digest(connection, query, arguments=()):
    """Stream fixed-size native row hashes; neither complete rows nor ID sets are collected."""
    digest, count = hashlib.sha256(), 0
    native = (
        "SELECT octet_length(document)::bigint,sha256(convert_to(document,'UTF8')) "
        f"FROM ({query}) ordered_rows(document)"
    )
    async for row in connection.cursor(native, *arguments, prefetch=1000):
        digest.update(row[0].to_bytes(8, "big"))
        digest.update(row[1])
        count += 1
    return count, digest.hexdigest()


@dataclass(frozen=True)
class FHIRServingPreservation:
    predecessor: object
    network_table: str
    membership_query: str
    binding_query: str
    address_query: str
    receipt: dict
    address_arguments: tuple = ()
    temporary_tables: tuple = ()


class _IdentityTables(NamedTuple):
    inventory: str
    coverage: str
    subjects: str
    current_subjects: str
    reviewed_subjects: str
    missing: str


class _PreviousSelection(NamedTuple):
    namespace: str
    raw: str
    effective: str
    ambiguous: str
    prior_missing: str


async def append_fhir_preservation_memberships(connection, stage, preservation):
    """Append only selected prior raw facts; keep current extraction accounting separate."""
    if preservation is None:
        return stage
    table = "pg_temp." + storage_identifier(stage.table_name)
    await connection.execute(f"""INSERT INTO {table}(ordinal,input_json)
      SELECT {stage.membership_rows}+row_number() OVER(ORDER BY to_jsonb(row)::text COLLATE "C"),
        to_jsonb(row)::text FROM ({preservation.membership_query}) row""")
    count = await connection.fetchval(f"SELECT count(*) FROM {table}")
    if count != stage.membership_rows + preservation.receipt["carried_rows"]:
        _fail("membership_accounting")
    generation = hashlib.sha256(
        _canonical([stage.generation_sha256, fhir_serving_preservation_sha256(preservation.receipt)]).encode()
    ).hexdigest()
    return replace(
        stage, membership_rows=count, generation_sha256=generation, selected_stage=stage, preservation=preservation
    )


async def _coverage(
    connection, recipes, approved, control_schema, side, inventory, coverage, *, source_witness_runtime=None
):
    """Authenticate complete native families before any membership eligibility filter."""
    for recipe in recipes:
        if type(recipe.source_pin) is not PinnedFHIRMembershipSource:
            continue
        await require_registry_source_recipe_custody(
            connection,
            recipe,
            approved,
            control_schema,
            **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
        )
        await require_retained_approved_network_source_bindings(
            connection, approved, recipe.binding_coordinates, control_schema=control_schema
        )
        source_pin, coordinates = recipe.source_pin, recipe.binding_coordinates
        slot = _canonical(_slot(recipe))
        await connection.execute(
            f"INSERT INTO {inventory} VALUES($1,$2,$3::jsonb,$4::jsonb,$5)",
            side,
            slot,
            _canonical(_family(recipe)),
            _canonical(coordinates.sql_parameters),
            source_pin.endpoint_id,
        )
        relation = storage_identifier(source_pin.read_schema_name) + ".provider_directory_dataset_resource"
        await connection.execute(f"LOCK TABLE {relation} IN ACCESS SHARE MODE NOWAIT")
        await connection.execute(
            f"INSERT INTO {coverage} SELECT $1,$2,resource_id,payload_hash FROM {relation} "
            "WHERE dataset_id=$3 AND resource_type='Organization'",
            side,
            slot,
            fhir_physical_source(source_pin).dataset_id,
        )
    if await connection.fetchval(
        f"SELECT EXISTS(SELECT FROM {coverage} WHERE organization_id !~ '^[A-Za-z0-9.-]{{1,64}}$' "
        "OR payload_hash !~ '^[0-9a-f]{64}$')"
    ):
        _fail("coverage_invalid")


async def _subjects(connection, inventory, control, revision, destination, side, *, historical=False):
    """Resolve immutable approved subjects; distinguish all families and product scopes."""
    await connection.execute(
        f"""INSERT INTO {destination}
      SELECT DISTINCT (b.record_json->>'network_id')::integer,i.slot,b.record_json->>'source_key',
        b.record_json->>'source_system'='fhir'
          AND b.record_json->'source_scope_json' ? 'organization_id'
          AND b.record_json->>'source_key'=b.record_json->'source_scope_json'->>'organization_id' AS supported
      FROM {control}.registry_approved_record b LEFT JOIN LATERAL (
        SELECT exact.slot FROM {inventory} exact WHERE exact.side=$2
          AND exact.coordinates=jsonb_build_array(b.record_json->>'source_system',b.record_json->>'source_id',
            b.record_json->>'dataset_schema',b.record_json->>'dataset_id',
            b.record_json->>'producer_id',b.record_json->>'edition_id')
        UNION ALL
        SELECT family.slot FROM {inventory} family WHERE $3::boolean AND family.side=$2
          AND family.family=jsonb_build_array(b.record_json->>'source_system',b.record_json->>'source_id',
            b.record_json->>'dataset_schema',b.record_json->>'producer_id')
          AND (SELECT count(*) FROM {inventory} same WHERE same.side=$2 AND same.family=family.family)=1
          AND NOT EXISTS(SELECT FROM {inventory} exact WHERE exact.side=$2
            AND exact.coordinates=jsonb_build_array(b.record_json->>'source_system',b.record_json->>'source_id',
              b.record_json->>'dataset_schema',b.record_json->>'dataset_id',
              b.record_json->>'producer_id',b.record_json->>'edition_id'))
      ) i ON true
      WHERE b.approved_revision=$1 AND b.record_kind='network_binding'
        AND b.record_json->'archived'='false'::jsonb""",
        revision,
        side,
        historical,
    )


async def _preservation_baseline(
    connection, predecessor, recipes, request_id, expected_head, approved_revision, control_schema, *, inventory=False
):
    """Authenticate the exact predecessor, operation and retained approved pins."""
    if predecessor is None or RECIPE_DIGEST_KEY not in predecessor.source_generations:
        return None
    actual = await resolve_network_serving_manifest(
        connection, generation_id=predecessor.generation_id, control_schema=control_schema
    )
    if actual != predecessor:
        _fail("predecessor_changed")
    _require_predecessor_seal(actual, predecessor)
    from process.registry_publication_queue import require_registry_source_rollback_composition

    operation_sha256 = await require_registry_source_rollback_composition(
        connection,
        request_id,
        predecessor,
        approved_revision,
        expected_head,
        control_schema=control_schema,
    )
    previous = await resolve_retained_registry_source_recipes(connection, predecessor, control_schema=control_schema)
    if not any(type(recipe.source_pin) is PinnedFHIRMembershipSource for recipe in previous):
        return None
    fhir_vector_succession(previous, recipes)
    candidate = await connection.fetchrow(
        f"SELECT * FROM {source_observation_namespace(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
        UUID(predecessor.candidate_id),
    )
    old_receipt = verify_fhir_serving_preservation(dict(candidate))
    if _fhir_recipes(previous, inventory=inventory) == _fhir_recipes(recipes, inventory=inventory) and (
        old_receipt is None or not old_receipt["carried_rows"]
    ):
        return None
    old_approved = await pin_retained_approved_membership_source(
        connection, approved_revision=predecessor.approved_custom_revision, control_schema=control_schema
    )
    current_approved = await pin_retained_approved_membership_source(
        connection, approved_revision=approved_revision, control_schema=control_schema
    )
    return previous, old_approved, current_approved, old_receipt, operation_sha256


def _require_predecessor_seal(actual, supplied=None):
    """Match the resolver's authenticated legacy pins or independent custody seal."""
    if not hasattr(actual, "_custody_seal_sha256"):
        relation_pins = _legacy_predecessor_pins(actual)
        if supplied is not None and (actual != supplied or _legacy_predecessor_pins(supplied) != relation_pins):
            _fail("predecessor_changed")
        return
    seal = getattr(actual, "_custody_seal_sha256", None)
    if type(seal) is not str or re.fullmatch("[0-9a-f]{64}", seal) is None:
        _fail("predecessor_changed")
    if supplied is not None and getattr(supplied, "_custody_seal_sha256", None) != seal:
        _fail("predecessor_changed")


def _legacy_predecessor_pins(manifest):
    from process.network_compact_layout import CompactAddressRelation
    from process.network_serving_read import PinnedNetworkServingManifest

    if (
        type(manifest) is not PinnedNetworkServingManifest
        or manifest.compact_layout is not None
        or manifest.custody_control is not None
        or hasattr(manifest, "_custody_seal_sha256")
    ):
        _fail("predecessor_changed")
    relation_pins = manifest.candidate_relation_pins
    if (
        type(relation_pins) is not tuple
        or not relation_pins
        or any(
            type(pin) is not CompactAddressRelation or pin.schema_name != manifest.schema_name for pin in relation_pins
        )
        or not any(
            pin.table_name == "entity_address_unified" and pin.table_oid == manifest.address_table_oid
            for pin in relation_pins
        )
    ):
        _fail("predecessor_changed")
    return relation_pins


async def _identity_stages(
    connection, previous, recipes, old_approved, current_approved, approved_revision, control_schema, **source_options
):
    """Stage complete authenticated Organization families and both reviewed subject maps."""
    if set(source_options) - {"source_witness_runtime"}:
        _fail("source_runtime_invalid")
    identity = uuid4().hex
    names = [
        "fhir_preserve_" + kind + "_" + identity
        for kind in ("recipes", "coverage", "subjects", "current", "reviewed", "missing")
    ]
    relations = tuple("pg_temp." + storage_identifier(name) for name in names)
    inventory, coverage, subjects, current_subjects, reviewed_subjects, missing = relations
    await connection.execute(
        f"CREATE TEMP TABLE {inventory}(side text,slot text,family jsonb,coordinates jsonb,endpoint text) ON COMMIT DROP"
    )
    await connection.execute(
        f"CREATE TEMP TABLE {coverage}(side text,slot text,organization_id text,payload_hash text,PRIMARY KEY(side,slot,organization_id)) ON COMMIT DROP"
    )
    await connection.execute(
        f"CREATE TEMP TABLE {subjects}(network_id integer,slot text,organization_id text,supported boolean) ON COMMIT DROP"
    )
    for relation in (current_subjects, reviewed_subjects):
        await connection.execute(f"CREATE TEMP TABLE {relation}(LIKE {subjects}) ON COMMIT DROP")
    await _coverage(
        connection, previous, old_approved, control_schema, "previous", inventory, coverage, **source_options
    )
    await _coverage(
        connection, recipes, current_approved, control_schema, "current", inventory, coverage, **source_options
    )
    await _subjects(
        connection,
        inventory,
        source_observation_namespace(control_schema),
        old_approved.approved_revision,
        subjects,
        "previous",
        historical=True,
    )
    await _subjects(
        connection,
        inventory,
        source_observation_namespace(control_schema),
        approved_revision,
        current_subjects,
        "current",
    )
    await _subjects(
        connection,
        inventory,
        source_observation_namespace(control_schema),
        approved_revision,
        reviewed_subjects,
        "current",
        historical=True,
    )
    return _IdentityTables(inventory, coverage, subjects, current_subjects, reviewed_subjects, missing)


async def _previous_selection(connection, predecessor, relations, old_receipt):
    """Recover only the predecessor's independently sealed carry set."""
    inventory, coverage, subjects, current_subjects, reviewed_subjects, missing = relations
    prior = storage_identifier(predecessor.schema_name)
    raw = prior + ".network_membership_source"
    effective = prior + ".network_membership"
    if not await connection.fetchval("SELECT to_regclass($1) IS NOT NULL", raw):
        _fail("baseline_unavailable")
    await connection.execute(
        f"LOCK TABLE {raw},{effective},{prior}.provider_location_binding IN ACCESS SHARE MODE NOWAIT"
    )
    if await connection.fetchval(f"""SELECT EXISTS(SELECT FROM {subjects} s JOIN {raw} m USING(network_id)
      WHERE s.supported AND s.slot IS NULL)"""):
        _fail("identity_ambiguous")
    # A target's complete historical subject must be unambiguous. Do not infer
    # whole-network disappearance from one FHIR binding in a mixed target.
    ambiguous = f"""SELECT network_id FROM {subjects} GROUP BY network_id
      HAVING bool_or(NOT supported OR slot IS NULL) OR count(DISTINCT(slot,organization_id))<>1"""
    prior_missing = f"""SELECT DISTINCT m.network_id FROM {raw} m
      WHERE EXISTS(SELECT FROM {subjects} s WHERE s.network_id=m.network_id AND s.supported AND s.slot IS NOT NULL)
      AND m.network_id NOT IN ({ambiguous})
      AND NOT EXISTS(SELECT FROM {subjects} s JOIN {coverage} c
        ON c.side='previous' AND (c.slot,c.organization_id)=(s.slot,s.organization_id)
        WHERE s.network_id=m.network_id)"""
    if old_receipt is not None:
        count, digest = await _ordered_digest(
            connection, f"SELECT network_id::text FROM ({prior_missing}) ids ORDER BY network_id"
        )
        if (count, digest) != (old_receipt["missing_networks"], old_receipt["missing_networks_sha256"]):
            _fail("prior_selection_changed")
    elif await connection.fetchval(f"SELECT EXISTS({prior_missing})"):
        _fail("prior_selection_unproven")
    return _PreviousSelection(prior, raw, effective, ambiguous, prior_missing)


async def _select_missing_networks(connection, relations, previous_selection, approved_revision, control_schema):
    """End carry on exact reappearance or current approved identity, before eligibility filtering."""
    inventory, coverage, subjects, current_subjects, reviewed_subjects, missing = relations
    prior, raw, effective, ambiguous, prior_missing = previous_selection
    await connection.execute(f"""CREATE TEMP TABLE {missing} ON COMMIT DROP AS
      SELECT DISTINCT m.network_id FROM {raw} m
      WHERE EXISTS(SELECT FROM {subjects} s WHERE s.network_id=m.network_id AND s.supported AND s.slot IS NOT NULL)
      AND EXISTS(SELECT FROM {source_observation_namespace(control_schema)}.registry_approved_record network
        WHERE network.approved_revision={approved_revision} AND network.record_kind='network'
          AND network.record_key=m.network_id::text AND network.record_json->'archived'='false'::jsonb)
      AND (EXISTS(SELECT FROM {effective} e WHERE e.network_id=m.network_id)
        OR m.network_id IN ({prior_missing}))
      AND EXISTS(SELECT FROM {subjects} original JOIN {reviewed_subjects} reviewed
        ON (original.network_id,original.organization_id)=(reviewed.network_id,reviewed.organization_id)
          AND (original.slot=reviewed.slot OR reviewed.slot IS NULL)
        WHERE original.network_id=m.network_id AND original.supported AND reviewed.supported)
      AND NOT EXISTS(SELECT FROM {current_subjects} s JOIN {coverage} c
        ON c.side='current' AND (c.slot,c.organization_id)=(s.slot,s.organization_id)
        WHERE s.network_id=m.network_id AND s.supported)
      AND NOT EXISTS(SELECT FROM {subjects} s JOIN {coverage} c
        ON c.side='current' AND (c.slot,c.organization_id)=(s.slot,s.organization_id)
        WHERE s.network_id=m.network_id)""")
    if await connection.fetchval(f"""SELECT EXISTS(SELECT FROM {missing} WHERE network_id IN ({ambiguous})
      OR network_id IN (SELECT network_id FROM {reviewed_subjects} GROUP BY network_id
        HAVING bool_or(NOT supported OR slot IS NULL) OR count(DISTINCT(slot,organization_id))<>1))"""):
        _fail("identity_ambiguous")
    await connection.execute(f"ALTER TABLE {missing} ADD PRIMARY KEY(network_id)")


def _carry_queries(predecessor, missing, raw, address_relation=None):
    """Select the complete raw backing and only its exact provider/site/address tuples."""
    prior = storage_identifier(predecessor.schema_name)
    membership_query = f"SELECT m.* FROM {raw} m JOIN {missing} ids USING(network_id)"
    binding_query = f"""SELECT DISTINCT b.* FROM {prior}.provider_location_binding b
      WHERE EXISTS(SELECT FROM ({membership_query}) m WHERE
        (m.provider_system,m.provider_id,m.location_id)=(b.provider_system,b.provider_id,b.location_id))"""
    addresses = address_relation or prior + ".entity_address_unified"
    address_query = f"""SELECT a.* FROM {addresses} a
      WHERE EXISTS(SELECT FROM ({binding_query}) b WHERE
        (b.location_key,b.entity_type,b.entity_id)=(a.location_key,a.entity_type,a.entity_id))"""
    return membership_query, binding_query, address_query


async def _predecessor_addresses(connection, predecessor):
    """Use every sealed logical address, including carriers hidden by ordinary API visibility."""
    from api.network_compact_address import compact_address_native_sql, compact_address_read_relations

    layout = predecessor.compact_layout
    if layout is None:
        relation = storage_identifier(predecessor.schema_name) + ".entity_address_unified"
        relations = ((relation, predecessor.address_table_oid),)
        query, arguments = relation, ()
    else:
        query, arguments = compact_address_native_sql(layout, first_parameter=1, visible_membership_sql="TRUE")
        relations = tuple(
            (
                storage_identifier(relation.schema_name) + "." + storage_identifier(relation.table_name),
                relation.table_oid,
            )
            for relation in compact_address_read_relations(layout)
        )
    for relation, oid in relations:
        await connection.execute(f"LOCK TABLE ONLY {relation} IN ACCESS SHARE MODE NOWAIT")
        if await connection.fetchval("SELECT to_regclass($1)::oid::bigint", relation) != oid:
            _fail("predecessor_changed")
    return query, arguments


async def _carry_counters(connection, missing, coverage, queries, address_arguments=()):
    """Hash fixed-size native row digests while retaining exact multiset counts."""
    membership_query, binding_query, address_query = queries
    counters_by_name = {}
    counters_by_name["missing_networks"], counters_by_name["missing_networks_sha256"] = await _ordered_digest(
        connection, f"SELECT network_id::text FROM {missing} ORDER BY network_id"
    )
    for count_name, digest_name, query in (
        ("carried_rows", "membership_sha256", membership_query),
        ("binding_rows", "bindings_sha256", binding_query),
        ("address_rows", "addresses_sha256", address_query),
    ):
        counters_by_name[count_name], counters_by_name[digest_name] = await _ordered_digest(
            connection,
            f'SELECT to_jsonb(row)::text FROM ({query}) row ORDER BY to_jsonb(row)::text COLLATE "C"',
            address_arguments if count_name == "address_rows" else (),
        )
    _, coverage_digest = await _ordered_digest(
        connection,
        f'SELECT to_jsonb(row)::text FROM {coverage} row ORDER BY side,slot COLLATE "C",organization_id COLLATE "C"',
    )
    return counters_by_name, coverage_digest


def _carry_receipt(predecessor, previous, recipes, provenance, counters_by_name, coverage_digest, *, inventory=False):
    """Bind the selected native rows to their exact predecessor and operation."""
    request_id, expected_head, approved_revision, operation_sha256 = provenance
    previous_digest, previous_root = _recipe_commitment(previous, inventory)
    current_digest, current_root = _recipe_commitment(recipes, inventory)
    receipt = validate_fhir_serving_preservation(
        {
            "component": SUMMARY_KEY,
            "revision": 2 if inventory else 1,
            "predecessor_generation": predecessor.generation_id,
            "predecessor_candidate_id": predecessor.candidate_id,
            "predecessor_manifest_sha256": predecessor.manifest_sha256,
            "request_id": str(request_id),
            "expected_head": expected_head,
            "operation": "source_rollback" if operation_sha256 is not None else "forward",
            "operation_sha256": operation_sha256 or hashlib.sha256(b"forward").hexdigest(),
            "approved_revision": approved_revision,
            "previous_recipes_sha256": previous_digest,
            "current_recipes_sha256": current_digest,
            "coverage_sha256": coverage_digest,
            **counters_by_name,
            **({"previous_recipe_root": previous_root, "current_recipe_root": current_root} if inventory else {}),
        }
    )
    if counters_by_name["carried_rows"] and (
        not counters_by_name["binding_rows"] or not counters_by_name["address_rows"]
    ):
        _fail("address_unresolved")
    return receipt


async def prepare_fhir_serving_preservation(
    connection, predecessor, recipes, *, request_id, expected_head, approved_revision, control_schema, **source_options
):
    """Derive exact whole-network carry under the caller's repeatable-read snapshot."""
    if set(source_options) - {"source_witness_runtime", "inventory"}:
        _fail("source_runtime_invalid")
    inventory = source_options.pop("inventory", False)
    if not connection.is_in_transaction() or await connection.fetchval("SHOW transaction_isolation") not in {
        "repeatable read",
        "serializable",
    }:
        _fail("requires_repeatable_read")
    _recipe_commitment(recipes, inventory)
    baseline = await _preservation_baseline(
        connection,
        predecessor,
        recipes,
        request_id,
        expected_head,
        approved_revision,
        control_schema,
        inventory=inventory,
    )
    if baseline is None:
        return None
    previous, old_approved, current_approved, old_receipt, operation_sha256 = baseline
    async with connection.transaction():
        relations = await _identity_stages(
            connection,
            previous,
            recipes,
            old_approved,
            current_approved,
            approved_revision,
            control_schema,
            **source_options,
        )
        previous_selection = await _previous_selection(connection, predecessor, relations, old_receipt)
        address_relation, address_arguments = await _predecessor_addresses(connection, predecessor)
        await _select_missing_networks(connection, relations, previous_selection, approved_revision, control_schema)
        queries = _carry_queries(predecessor, relations.missing, previous_selection.raw, address_relation)
        counters_by_name, coverage_digest = await _carry_counters(
            connection, relations.missing, relations.coverage, queries, address_arguments
        )
        provenance = request_id, expected_head, approved_revision, operation_sha256
        receipt = _carry_receipt(
            predecessor, previous, recipes, provenance, counters_by_name, coverage_digest, inventory=inventory
        )
        return FHIRServingPreservation(
            predecessor, relations.missing, *queries, receipt, address_arguments, tuple(relations)
        )


async def _copy_carry_address_rows(connection, schema_name, query, arguments):
    """Retain the existing adaptive native COPY bounds with reserved logical-layout parameters."""
    if not arguments:
        return await copy_network_address_projection_rows(connection, schema_name, query)
    from process.network_bootstrap_sources import (
        _COPY_BATCH_BYTES,
        _COPY_BATCH_ROWS,
        _COPY_FRAMING_BYTES,
        _copy_native_batch,
        _CopyBatchTooLarge,
    )

    copied, last_key, limit = 0, None, _COPY_BATCH_ROWS
    while True:
        parameters = arguments if last_key is None else (*arguments, last_key)
        predicate = "TRUE" if last_key is None else f"a.location_key>${len(arguments) + 1}"
        bounds = await connection.fetchrow(
            "SELECT count(*)::bigint AS row_count,max(location_key) AS last_key,"
            "COALESCE(sum(octet_length(record_send(batch))::bigint),0)::bigint AS native_bytes "
            f"FROM ({query} WHERE {predicate} ORDER BY a.location_key LIMIT {limit}) batch",
            *parameters,
        )
        count = bounds["row_count"]
        if not count:
            return copied
        if bounds["native_bytes"] + _COPY_FRAMING_BYTES > _COPY_BATCH_BYTES:
            if count == 1:
                _fail("address_copy_bound")
            limit = max(1, count // 2)
            continue
        selected = f"{query} WHERE {predicate} AND a.location_key<=${len(parameters) + 1} ORDER BY a.location_key"
        try:
            await _copy_native_batch(
                connection, selected, (*parameters, bounds["last_key"]), schema_name, "entity_address_unified", count
            )
        except _CopyBatchTooLarge:
            if count == 1:
                _fail("address_copy_bound")
            limit = max(1, count // 2)
            continue
        copied += count
        last_key = bounds["last_key"]


def _effective_carry_queries(preservation, approved_context):
    """Apply the same approved office precedence as the complete binding proof."""
    if approved_context is None:
        return preservation.address_query, preservation.binding_query
    from process.network_custom_address_source import _APPROVED_CTE

    if (
        type(approved_context) is not tuple
        or len(approved_context) != 2
        or type(approved_context[0]) is not int
        or not 0 <= approved_context[0] < 2**63
    ):
        _fail("source_changed")
    revision, schema = approved_context
    prefix = _APPROVED_CTE.format(control=storage_identifier(schema))
    if set(re.findall(r"\$(\d+)", prefix)) != {"1"}:
        _fail("source_changed")
    prefix = re.sub(r"\$1\b", str(revision), prefix)
    addresses = (
        prefix
        + f"""SELECT old.* FROM ({preservation.address_query}) old
      WHERE NOT EXISTS(SELECT FROM custom WHERE custom.location_key=old.location_key)"""
    )
    bindings = (
        prefix
        + f"""SELECT expected.provider_system,expected.provider_id,expected.location_id,
      coalesce(custom.location_key,expected.location_key) AS location_key,
      CASE WHEN custom.location_key IS NULL THEN expected.entity_type ELSE custom.provider_system END AS entity_type,
      CASE WHEN custom.location_key IS NULL THEN expected.entity_id ELSE custom.provider_id END AS entity_id
      FROM ({preservation.binding_query}) expected LEFT JOIN custom USING(provider_system,provider_id,location_id)"""
    )
    return addresses, bindings


async def carry_fhir_preservation_addresses(
    connection,
    preservation,
    target_schema,
    *,
    writing=False,
    address_relation=None,
    address_arguments=(),
    approved_context=None,
):
    """Copy bounded complete address rows, refusing any location version conflict."""
    if preservation is None:
        return None
    target_namespace = storage_identifier(target_schema)
    addresses, bindings = _effective_carry_queries(preservation, approved_context)
    actual_addresses = address_relation or target_namespace + ".entity_address_unified"
    arguments = (*preservation.address_arguments, *address_arguments)
    if writing and address_relation is not None:
        _fail("address_parity")
    if await connection.fetchval(
        f"""SELECT EXISTS(SELECT FROM ({addresses}) old
      JOIN {actual_addresses} current USING(location_key)
      WHERE to_jsonb(old)-'canonical_network_ids' IS DISTINCT FROM to_jsonb(current)-'canonical_network_ids')""",
        *arguments,
    ):
        _fail("address_version_conflict")
    if writing:
        await _copy_carry_address_rows(
            connection,
            target_schema,
            f"SELECT a.* FROM (SELECT old.* FROM ({addresses}) old WHERE NOT EXISTS(SELECT FROM {target_namespace}.entity_address_unified current WHERE current.location_key=old.location_key)) a",
            preservation.address_arguments,
        )
        await connection.execute(
            f"INSERT INTO {target_namespace}.provider_location_binding {bindings} ON CONFLICT DO NOTHING"
        )
    if await connection.fetchval(
        f"""SELECT EXISTS(SELECT FROM ({bindings}) expected
      LEFT JOIN {target_namespace}.provider_location_binding actual USING(provider_system,provider_id,location_id)
      WHERE actual.location_key IS NULL OR (expected.location_key,expected.entity_type,expected.entity_id)
        IS DISTINCT FROM (actual.location_key,actual.entity_type,actual.entity_id))
      OR EXISTS(SELECT FROM ({addresses}) expected LEFT JOIN {actual_addresses} actual USING(location_key)
        WHERE actual.location_key IS NULL OR to_jsonb(expected)-'canonical_network_ids'
          IS DISTINCT FROM to_jsonb(actual)-'canonical_network_ids')""",
        *arguments,
    ):
        _fail("address_parity")
    return fhir_serving_preservation_sha256(preservation.receipt)


async def cleanup_fhir_serving_preservation(connection, preservation, *, primary=None):
    """Drain only this proof's temporary identity selections, preserving a primary failure."""
    from process.uhc_flex_practitioner_async_safety import drain_operation

    if preservation is None:
        return
    try:
        for relation in preservation.temporary_tables:
            if re.fullmatch(r'pg_temp\."fhir_preserve_[a-z]+_[0-9a-f]{32}"', relation) is None:
                _fail("selection_changed")
            await drain_operation(
                connection.execute(f"DROP TABLE IF EXISTS {relation}"), preserve_cancellation=primary is None
            )
    except BaseException as cleanup_error:
        if primary is None:
            raise
        primary.add_note("FHIR preservation cleanup failed; caller rollback remains required.")
        primary._registry_preservation_cleanup_error = cleanup_error


async def _obligation_predecessor(connection, candidate, receipt, control_schema, reviewed_source_manifest):
    """Choose historical scope only after rechecking existing audited operation evidence."""
    expected = candidate["expected_head"]
    from process.registry_publication_queue import (
        registry_source_rollback_composition_provenance,
        verify_registry_source_rollback_provenance,
    )

    operation = verify_registry_source_rollback_provenance(candidate)
    generation = (
        reviewed_source_manifest.generation_id
        if reviewed_source_manifest is not None
        else operation["target_generation"]
        if operation is not None
        else receipt["predecessor_generation"]
        if receipt is not None
        else expected
    )
    predecessor = await resolve_network_serving_manifest(
        connection, generation_id=generation, control_schema=control_schema
    )
    _require_predecessor_seal(predecessor, reviewed_source_manifest)
    if reviewed_source_manifest is not None and predecessor != reviewed_source_manifest:
        _fail("predecessor_changed")
    if operation is not None:
        if operation["manifest_sha256"] != predecessor.manifest_sha256:
            _fail("predecessor_changed")
        if reviewed_source_manifest is None:
            actual_operation = await registry_source_rollback_composition_provenance(
                connection,
                UUID(operation["request_id"]),
                predecessor,
                candidate["approved_custom_revision"],
                expected,
                control_schema=control_schema,
            )
            if actual_operation != operation:
                _fail("predecessor_changed")
    return predecessor


async def _candidate_recipes(connection, candidate, inventory_entries, control_schema):
    """Load authenticated native inventory when needed, never reinterpret its reference as a legacy array."""
    from process.network_membership_copy import MembershipCopyTarget
    from process.registry_source_contribution_store import _inventory_source_metadata
    from process.registry_source_inventory_reference import verify_source_recipe_metadata
    from process.registry_source_inventory_store import _registered_inventory_entries

    metadata = verify_source_recipe_metadata(candidate)
    if metadata.inventory is None:
        return verify_registry_source_recipes(candidate), None
    if inventory_entries is None:
        target = MembershipCopyTarget(
            *(
                str(candidate[field])
                for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
            )
        )
        inventory_entries = await _registered_inventory_entries(
            connection, target, candidate, control_schema=control_schema
        )
    _root, recipes, _descriptors = _inventory_source_metadata(candidate, inventory_entries)
    return recipes, inventory_entries


async def fhir_preservation_obligation(
    connection, candidate, *, control_schema, reviewed_source_manifest=None, inventory_entries=None
):
    """Derive the publication obligation from the real predecessor, never output metadata."""
    receipt = verify_fhir_serving_preservation(candidate)
    expected = candidate["expected_head"]
    if not expected:
        if receipt is not None:
            _fail("predecessor_changed")
        return None
    predecessor = await _obligation_predecessor(
        connection, candidate, receipt, control_schema, reviewed_source_manifest
    )
    previous = (
        await resolve_retained_registry_source_recipes(connection, predecessor, control_schema=control_schema)
        if RECIPE_DIGEST_KEY in predecessor.source_generations
        else ()
    )
    required = any(type(recipe.source_pin) is PinnedFHIRMembershipSource for recipe in previous)
    if required and receipt is None:
        prior_candidate = await connection.fetchrow(
            f"SELECT * FROM {source_observation_namespace(control_schema)}.network_membership_candidate WHERE candidate_id=$1",
            UUID(predecessor.candidate_id),
        )
        prior_receipt = verify_fhir_serving_preservation(dict(prior_candidate))
        recipes, entries = await _candidate_recipes(connection, candidate, inventory_entries, control_schema)
        if (prior_receipt is None or not prior_receipt["carried_rows"]) and (
            _fhir_recipes(previous, inventory=entries is not None)
            == _fhir_recipes(recipes, inventory=entries is not None)
        ):
            return None
    if not required and receipt is None:
        return None
    if receipt is None:
        _fail("proof_required")
    if (
        receipt["expected_head"] != expected
        or receipt["predecessor_candidate_id"] != predecessor.candidate_id
        or receipt["predecessor_manifest_sha256"] != predecessor.manifest_sha256
        or str(uuid5(UUID(receipt["request_id"]), "dataset")) != str(candidate["dataset_id"])
        or receipt["operation"] == "forward"
        and predecessor.generation_id != expected
    ):
        _fail("predecessor_changed")
    generations = candidate["source_generations"]
    generations = json.loads(generations) if type(generations) is str else generations
    if generations.get("retained_network_source") != predecessor.manifest_sha256:
        _fail("predecessor_changed")
    return predecessor


def _exclusive_fhir_cohort(control_schema):
    """Include expected-zero canonical targets and exclude every mixed/product target."""
    return f"""SELECT (record_json->>'network_id')::integer AS network_id
      FROM {source_observation_namespace(control_schema)}.registry_approved_record
      WHERE approved_revision=ANY($1::bigint[]) AND record_kind='network_binding'
        AND record_json->'archived'='false'::jsonb
      GROUP BY record_json->>'network_id'
      HAVING bool_and(record_json->>'source_system'='fhir'
        AND record_json->'source_scope_json' ? 'organization_id'
        AND NOT (record_json->'source_scope_json' ? 'insurance_plan_id'))"""


async def _current_raw_parity(connection, candidate, preservation, current_stage, recipe_counts, control_schema):
    """Compare both directions using only the approved-input cohort for mixed vectors."""
    from process.registry_source_retention import registry_source_stage_projection

    fhir_count, all_count = recipe_counts
    generations_by_name = candidate["source_generations"]
    generations_by_name = json.loads(generations_by_name) if type(generations_by_name) is str else generations_by_name
    if fhir_count == all_count and current_stage.generation_sha256 != generations_by_name.get(
        "registry_source_membership"
    ):
        _fail("source_changed")
    namespace = storage_identifier(candidate["schema_name"])
    expected = registry_source_stage_projection("pg_temp." + storage_identifier(current_stage.table_name))
    expected += " UNION ALL " + preservation.membership_query
    if fhir_count == all_count:
        comparison = f"""WITH expected AS MATERIALIZED ({expected})
          SELECT EXISTS((SELECT * FROM expected EXCEPT ALL SELECT * FROM {namespace}.network_membership_source)
            UNION ALL (SELECT * FROM {namespace}.network_membership_source EXCEPT ALL SELECT * FROM expected))"""
        differs = await connection.fetchval(comparison)
    else:
        cohort = _exclusive_fhir_cohort(control_schema)
        comparison = f"""WITH cohort AS MATERIALIZED ({cohort}), replayed AS MATERIALIZED ({expected}),
          expected AS (SELECT m.* FROM replayed m JOIN cohort USING(network_id)),
          actual AS (SELECT m.* FROM {namespace}.network_membership_source m JOIN cohort USING(network_id))
          SELECT EXISTS((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
            UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))"""
        differs = await connection.fetchval(
            comparison, [preservation.predecessor.approved_custom_revision, candidate["approved_custom_revision"]]
        )
    if differs:
        _fail("membership_parity")


async def _verify_current_fhir_rows(
    connection, candidate, preservation, recipes, approved, control_schema, *, source_witness_runtime=None
):
    """Replay all current FHIR inputs without acquiring unrelated graph authority."""
    from process.registry_source_recipe_composition import stage_registry_source_recipes

    fhir_recipes = tuple(recipe for recipe in recipes if type(recipe.source_pin) is PinnedFHIRMembershipSource)
    if not fhir_recipes:
        return
    office_inputs = await _current_office_inputs(connection, candidate, recipes, approved, control_schema)
    current_stage = await stage_registry_source_recipes(
        connection,
        fhir_recipes,
        approved,
        request_id=uuid4(),
        control_schema=control_schema,
        read_context=SourceRecipeReadContext(
            verified_office_inputs=office_inputs, source_witness_runtime=source_witness_runtime
        ),
    )
    primary = None
    try:
        if office_inputs is not None:
            await _verify_current_offices(
                connection, candidate, fhir_recipes, approved, current_stage, office_inputs, control_schema
            )
        await _current_raw_parity(
            connection, candidate, preservation, current_stage, (len(fhir_recipes), len(recipes)), control_schema
        )
    except BaseException as error:
        primary = error
        raise
    finally:
        from process.network_source_contribution_custody import _cleanup_independent_source_stage

        await _cleanup_independent_source_stage(connection, current_stage, None, primary)


async def _current_office_inputs(connection, candidate, recipes, approved, control_schema):
    """Select mapping only from a paired receipt and independently authenticated final office input."""
    from process.network_address_projection import PinnedAddressSource
    from process.network_custom_address_source import verify_artifact_dependency_identity
    from process.network_membership_publication import publication_candidate_readiness
    from process.registry_initial_source_composition import verify_initial_source_office_receipt

    generations_by_name = candidate["source_generations"]
    generations_by_name = json.loads(generations_by_name) if type(generations_by_name) is str else generations_by_name
    if OFFICE_MAPPING_KEY not in generations_by_name:
        return None
    mapping_sha256 = generations_by_name[OFFICE_MAPPING_KEY]
    summary = verify_initial_source_office_receipt(candidate)
    if (
        approved.total_rows
        or not recipes
        or summary is None
        or summary["correspondence_sha256"] != mapping_sha256
        or not all(
            type(recipe.source_pin) is PinnedFHIRMembershipSource and recipe.source_pin.retained_capture is not None
            for recipe in recipes
        )
    ):
        _fail("source_changed")
    report_by_name = candidate["validation_json"]
    report_by_name = json.loads(report_by_name) if type(report_by_name) is str else report_by_name
    address_source = PinnedAddressSource(**report_by_name["address_source"])
    writer_roles = publication_candidate_readiness(candidate)[3]
    runtime_roles = tuple(sorted(set((*writer_roles["loader_roles"], *writer_roles["reader_roles"]))))
    await verify_artifact_dependency_identity(
        connection, address_source, runtime_roles, address=True, control_schema=control_schema
    )
    return address_source, runtime_roles


async def _verify_current_offices(connection, candidate, recipes, approved, stage, office_inputs, control_schema):
    """Match independently derived office correspondence and raw source generations."""
    from process.registry_initial_source_composition import (
        verified_retained_source_office_summary,
        verify_initial_source_office_receipt,
    )

    address_source, runtime_roles = office_inputs
    summary = await verified_retained_source_office_summary(
        connection,
        recipes,
        approved,
        address_source,
        runtime_roles,
        control_schema,
        stage.office_matches,
    )
    if summary != verify_initial_source_office_receipt(candidate):
        _fail("source_changed")
    generations_by_name = candidate["source_generations"]
    generations_by_name = json.loads(generations_by_name) if type(generations_by_name) is str else generations_by_name
    raw_generations_by_name = {
        f"initial_source_recipe_{index}": generation for index, generation in enumerate(stage.raw_source_generations)
    }
    if generations_by_name.get(OFFICE_MAPPING_KEY) != summary["correspondence_sha256"]:
        _fail("source_changed")
    if raw_generations_by_name != {
        name: generation
        for name, generation in generations_by_name.items()
        if name.startswith("initial_source_recipe_")
    }:
        _fail("source_changed")


async def _verify_current_overlay(connection, copy_target, candidate, approved, control_schema):
    """Apply today's full approved add/remove commands to the independently checked raw heap."""
    from process.registry_membership_composition_stage import (
        drop_registry_command_stage,
        stage_approved_membership_commands,
        verify_registry_composition_rows,
    )

    command_stage = await stage_approved_membership_commands(
        connection, approved, uuid4(), control_schema=control_schema
    )
    from process.uhc_flex_practitioner_async_safety import drain_operation

    primary = None
    try:
        await verify_registry_composition_rows(connection, copy_target, candidate, command_stage)
    except BaseException as error:
        primary = error
        raise
    finally:
        try:
            await drain_operation(
                drop_registry_command_stage(connection, command_stage), preserve_cancellation=primary is None
            )
        except BaseException as cleanup_error:
            if primary is None:
                raise
            primary.add_note("FHIR preservation command cleanup failed; caller rollback remains required.")
            primary._registry_preservation_cleanup_error = cleanup_error


async def rederive_fhir_serving_preservation(
    connection,
    copy_target,
    candidate,
    *,
    control_schema,
    inventory_entries=None,
    reviewed_source_manifest=None,
    source_witness_runtime=None,
):
    """Reconstruct phase-local carry selection; target rows are checked separately after materialization."""
    if any(
        str(candidate[field]) != getattr(copy_target, field)
        for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name")
    ):
        _fail("source_changed")
    predecessor = await fhir_preservation_obligation(
        connection,
        candidate,
        control_schema=control_schema,
        inventory_entries=inventory_entries,
        reviewed_source_manifest=reviewed_source_manifest,
    )
    if predecessor is None:
        return None
    stored = verify_fhir_serving_preservation(candidate)
    recipes, entries = await _candidate_recipes(connection, candidate, inventory_entries, control_schema)
    previous = await resolve_retained_registry_source_recipes(connection, predecessor, control_schema=control_schema)
    report_by_name = candidate["validation_json"]
    report_by_name = json.loads(report_by_name) if type(report_by_name) is str else report_by_name
    require_fhir_source_event_delta(
        previous, recipes, report_by_name.get("source_event_assertion"), inventory=entries is not None
    )
    preservation = await prepare_fhir_serving_preservation(
        connection,
        predecessor,
        recipes,
        request_id=UUID(stored["request_id"]),
        expected_head=candidate["expected_head"],
        approved_revision=candidate["approved_custom_revision"],
        control_schema=control_schema,
        inventory=stored["revision"] == 2,
        **({"source_witness_runtime": source_witness_runtime} if source_witness_runtime is not None else {}),
    )
    if preservation is None or preservation.receipt != stored:
        error = FHIRServingPreservationError("registry_fhir_serving_preservation_selection_changed")
        await cleanup_fhir_serving_preservation(connection, preservation, primary=error)
        raise error
    return preservation


async def verify_fhir_preservation_target_rows(connection, copy_target, candidate, preservation, *, control_schema):
    """Verify carried raw facts, bindings and every historical address field against real target storage."""
    if preservation is None:
        return
    from process.network_approved_membership_source import pin_retained_approved_membership_source

    approved = await pin_retained_approved_membership_source(
        connection, approved_revision=candidate["approved_custom_revision"], control_schema=control_schema
    )
    generations_by_source = candidate["source_generations"]
    if type(generations_by_source) is str:
        generations_by_source = json.loads(generations_by_source)
    if generations_by_source.get("custom_membership") != approved.generation_id:
        _fail("source_changed")
    namespace = storage_identifier(copy_target.schema_name)
    if await connection.fetchval(f"""WITH expected AS MATERIALIZED ({preservation.membership_query}),
      actual AS MATERIALIZED (SELECT m.* FROM {namespace}.network_membership_source m
        JOIN {preservation.network_table} ids USING(network_id))
      SELECT EXISTS((SELECT * FROM expected EXCEPT ALL SELECT * FROM actual)
        UNION ALL (SELECT * FROM actual EXCEPT ALL SELECT * FROM expected))"""):
        _fail("membership_parity")
    from process.network_compact_readiness import is_compact_candidate

    relation, arguments = None, ()
    if is_compact_candidate(candidate):
        from api.network_compact_address import compact_address_native_sql
        from process.network_compact_dependencies import capture_compact_dependencies

        layout = await capture_compact_dependencies(connection, candidate, control_schema=control_schema)
        relation, arguments = compact_address_native_sql(
            layout, first_parameter=len(preservation.address_arguments) + 1, visible_membership_sql="TRUE"
        )
    await carry_fhir_preservation_addresses(
        connection,
        preservation,
        copy_target.schema_name,
        address_relation=relation,
        address_arguments=arguments,
        approved_context=(candidate["approved_custom_revision"], control_schema),
    )


async def verify_fhir_preservation_rows(
    connection,
    copy_target,
    candidate,
    *,
    control_schema,
    office_authority=None,
    inventory_entries=None,
    reviewed_source_manifest=None,
    **source_options,
):
    """Rebuild current inventory once, keeping current proofs and carried target rows separate."""
    if set(source_options) - {"source_witness_runtime"}:
        _fail("source_runtime_invalid")
    from process.network_approved_membership_source import pin_approved_membership_source
    from process.network_source_contribution_custody import verify_independent_source_contributions

    preservation = await rederive_fhir_serving_preservation(
        connection,
        copy_target,
        candidate,
        control_schema=control_schema,
        inventory_entries=inventory_entries,
        reviewed_source_manifest=reviewed_source_manifest,
        **source_options,
    )
    if preservation is None:
        return None
    primary = None
    try:
        await verify_fhir_preservation_target_rows(
            connection, copy_target, candidate, preservation, control_schema=control_schema
        )
        recipes, entries = await _candidate_recipes(connection, candidate, inventory_entries, control_schema)
        if entries is not None:
            await verify_independent_source_contributions(
                connection,
                copy_target,
                candidate,
                control_schema=control_schema,
                office_authority=office_authority,
                inventory_entries=entries,
                preservation=preservation,
                **source_options,
            )
        else:
            approved = await pin_approved_membership_source(
                connection, approved_revision=candidate["approved_custom_revision"], control_schema=control_schema
            )
            await _verify_current_fhir_rows(
                connection, candidate, preservation, recipes, approved, control_schema, **source_options
            )
            await _verify_current_overlay(connection, copy_target, candidate, approved, control_schema)
        return preservation.receipt
    except BaseException as error:
        primary = error
        raise
    finally:
        await cleanup_fhir_serving_preservation(connection, preservation, primary=primary)
