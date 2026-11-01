# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Immutable FHIR contributions and direct owners; receipts never grant custody."""

import hashlib
import json
import re
from collections.abc import Mapping
from uuid import UUID

from db.registry_schema import registry_schema
from process.network_address_projection import _identifier
from process.network_approved_source_bindings import (
    APPROVED_NETWORK_BINDINGS_SQL,
    require_approved_network_source_bindings,
)
from process.network_fhir_membership_source import (
    PinnedFHIRMembershipSource,
    fhir_normalized_capture,
    fhir_physical_source,
)
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_serving_read import resolve_network_serving_manifest
from process.registry_source_recipe_store import canonical_registry_source_recipes, verify_registry_source_recipes

SUMMARY_KEY = "registry_source_contributions"
CONTRIBUTION_TABLE = "network_source_contribution"
TRANSFORM_VERSION = "fhir-source-facts-v1"
GENERIC_TRANSFORM_VERSION = "generic-fhir-source-facts-v1"
from process.network_fhir_retained_capture_contract import GENERIC_UNVERIFIED_NETWORK

MAX_RECEIPT_BYTES = 524288
_COUNTERS = (
    "fact_rows",
    "membership_rows",
    "omitted_rows",
    "raw_source_rows",
    "accepted_raw_source_rows",
    "omitted_raw_source_rows",
    "provider_only_rows",
)
_HASH_FIELDS = ("recipe_sha256", "capture_sha256", "mapping_sha256", "fact_sha256", "source_generation")
_DESCRIPTOR_FIELDS = {
    *_COUNTERS,
    *_HASH_FIELDS,
    "transform_version",
}
FACT_ANCHOR_FIELDS = frozenset(("fact_candidate_id", "fact_table_oid", "fact_seal_sha256"))
_FACT_FIELDS = {
    "resource_type",
    "resource_id",
    "payload_sha256",
    "omission_reason",
    "source_key",
    "source_scope_json",
    "provider_system",
    "provider_id",
    "location_id",
    "evidence_basis",
}


class RegistrySourceContributionError(ValueError):
    """Closed contribution metadata or its retained native facts differ."""


def _canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _digest(value):
    return hashlib.sha256(_canonical(value).encode()).hexdigest()


def _unique_object(pairs):
    field_by_key = {}
    for key, value in pairs:
        if key in field_by_key:
            raise ValueError
        field_by_key[key] = value
    return field_by_key


def recipe_contribution_identity(recipe):
    """The accepted capture and its effective date are part of the immutable pin."""
    recipe_sha256 = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    return recipe_sha256, _digest([recipe_sha256, fhir_fact_transform(recipe.source_pin)])


def fhir_fact_transform(source_pin):
    """Keep old CMS identities fixed; generic permanent omissions have their own transform."""
    return (
        GENERIC_TRANSFORM_VERSION
        if fhir_normalized_capture(fhir_physical_source(source_pin)) is not None
        else TRANSFORM_VERSION
    )


async def source_mapping_sha256(connection, recipe, approved, control_schema):
    """Fingerprint only effective mappings in this complete source identity."""
    await require_approved_network_source_bindings(
        connection, approved, recipe.binding_coordinates, control_schema=control_schema
    )
    sql = """WITH {bindings}, fingerprints AS (
      SELECT encode(sha256(convert_to(jsonb_build_array(record_json->'source_key',
        record_json->'source_scope_json',network_id)::text,'UTF8')),'hex') AS fingerprint
      FROM approved_network_bindings
    ) SELECT encode(sha256(convert_to(coalesce(string_agg(fingerprint,'' ORDER BY fingerprint),''),'UTF8')),'hex')
      FROM fingerprints"""
    return await connection.fetchval(
        sql.format(bindings=APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_control_namespace(control_schema))),
        approved.approved_revision,
        *recipe.binding_coordinates.sql_parameters,
    )


def _validate_fact_anchor(descriptor):
    anchor_values = [descriptor[field] for field in FACT_ANCHOR_FIELDS]
    if all(value is None for value in anchor_values):
        return
    if any(value is None for value in anchor_values):
        raise ValueError
    identity = UUID(descriptor["fact_candidate_id"]) if type(descriptor["fact_candidate_id"]) is str else None
    if (
        identity is None
        or not identity.int
        or str(identity) != descriptor["fact_candidate_id"]
        or type(descriptor["fact_table_oid"]) is not int
        or not 0 < descriptor["fact_table_oid"] < 2**32
        or type(descriptor["fact_seal_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", descriptor["fact_seal_sha256"]) is None
    ):
        raise ValueError


def locally_owned_source_descriptor(descriptor):
    """Represent newly extracted source facts without a circular self-seal reference."""
    return {**descriptor, **dict.fromkeys(FACT_ANCHOR_FIELDS)}


def _validate_inventory_contribution(receipt, inventory_root):
    """Dispatch closed inventory summaries without hydrating source entries."""
    from process.registry_source_inventory_summaries import (
        validate_contribution_summary_v3,
        validate_contribution_summary_v4,
        validate_contribution_summary_v5,
    )

    document = json.loads(receipt, object_pairs_hook=_unique_object) if type(receipt) is str else receipt
    if type(document) is dict and type(document.get("revision")) is int and document["revision"] == 5:
        return validate_contribution_summary_v5(document, inventory_root)
    if type(document) is dict and type(document.get("revision")) is int and document["revision"] == 4:
        return validate_contribution_summary_v4(document, inventory_root)
    return validate_contribution_summary_v3(receipt, inventory_root)


def validate_registry_source_contribution_receipt(receipt, *, inventory_root=None):
    """Validate legacy descriptors, or only the new summary with its explicit inventory root."""
    try:
        if type(receipt) is str and len(receipt) > MAX_RECEIPT_BYTES:
            raise ValueError
        if inventory_root is not None:
            return _validate_inventory_contribution(receipt, inventory_root)
        if type(receipt) is str:
            if len(receipt.encode()) > MAX_RECEIPT_BYTES:
                raise ValueError
            receipt = json.loads(receipt, object_pairs_hook=_unique_object)
        if (
            type(receipt) is not dict
            or set(receipt) != {"component", "revision", "table_name", "sources"}
            or receipt["component"] != SUMMARY_KEY
            or type(receipt["revision"]) is not int
            or receipt["revision"] not in (1, 2)
            or receipt["table_name"] != CONTRIBUTION_TABLE
            or type(receipt["sources"]) is not list
            or len(receipt["sources"]) > 100
            or len(_canonical(receipt).encode()) > MAX_RECEIPT_BYTES
        ):
            raise ValueError
        identities = []
        for descriptor in receipt["sources"]:
            if (
                type(descriptor) is not dict
                or set(descriptor) != _DESCRIPTOR_FIELDS | (FACT_ANCHOR_FIELDS if receipt["revision"] == 2 else set())
                or descriptor["transform_version"] not in {TRANSFORM_VERSION, GENERIC_TRANSFORM_VERSION}
                or any(type(descriptor[field]) is not int or not 0 <= descriptor[field] < 2**63 for field in _COUNTERS)
                or any(
                    type(descriptor[field]) is not str or re.fullmatch(r"[0-9a-f]{64}", descriptor[field]) is None
                    for field in _HASH_FIELDS
                )
                or descriptor["raw_source_rows"]
                != descriptor["accepted_raw_source_rows"] + descriptor["omitted_raw_source_rows"]
                or descriptor["fact_rows"] < descriptor["raw_source_rows"]
                or any(descriptor[field] > descriptor["fact_rows"] for field in ("membership_rows", "omitted_rows"))
                or descriptor["provider_only_rows"] > descriptor["accepted_raw_source_rows"]
                or descriptor["capture_sha256"]
                != _digest([descriptor["recipe_sha256"], descriptor["transform_version"]])
                or descriptor["source_generation"]
                != _digest([descriptor["capture_sha256"], descriptor["mapping_sha256"]])
            ):
                raise ValueError
            if receipt["revision"] == 2:
                _validate_fact_anchor(descriptor)
            identities.append(descriptor["recipe_sha256"])
        if identities != sorted(set(identities)):
            raise ValueError
        return json.loads(_canonical(receipt))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_contribution_invalid") from None


def registry_source_contribution_receipt(stage, *, inventory_root=None):
    """Describe the exact staged FHIR facts for candidate registration."""
    descriptors = list(stage.contribution_receipts)
    revision = getattr(stage, "contribution_revision", 2)
    if revision == 2:
        descriptors = [
            validate_source_witness_descriptor_v2(descriptor)
            if "descriptor_version" in descriptor
            else locally_owned_source_descriptor(descriptor)
            if not FACT_ANCHOR_FIELDS <= descriptor.keys()
            else descriptor
            for descriptor in descriptors
        ]
    if inventory_root is not None:
        from process.registry_source_inventory_summaries import contribution_summary_v3, contribution_summary_v4

        if type(revision) is not int or revision != 2:
            raise RegistrySourceContributionError("registry_source_contribution_stage_invalid")
        summary = (
            contribution_summary_v4
            if any("descriptor_version" in descriptor for descriptor in descriptors)
            else contribution_summary_v3
        )
        return summary(inventory_root, sorted(descriptors, key=lambda descriptor: descriptor["recipe_sha256"]))
    if not descriptors:
        return None
    return validate_registry_source_contribution_receipt(
        {
            "component": SUMMARY_KEY,
            "revision": revision,
            "table_name": CONTRIBUTION_TABLE,
            "sources": sorted(descriptors, key=lambda descriptor: descriptor["recipe_sha256"]),
        }
    )


def _inventory_root(candidate):
    recipes = candidate.get("source_recipes_json")
    if isinstance(recipes, Mapping) or type(recipes) is str and recipes.lstrip().startswith("{"):
        from process.registry_source_inventory_reference import verify_source_recipe_metadata

        return verify_source_recipe_metadata(candidate).inventory.root
    return None


def _inventory_source_metadata(candidate, inventory_entries):
    """Fold complete supplied rows, then decode canonical singletons; this grants no native custody."""
    from process.registry_source_inventory_contract import SourceInventoryAccumulator
    from process.registry_source_inventory_preparation import SourceInventoryPreparation
    from process.registry_source_recipe_store import decode_registry_source_recipes

    try:
        root = _inventory_root(candidate)
        if root is None:
            return None
        if type(inventory_entries) is not tuple:
            raise ValueError
        accumulator = SourceInventoryAccumulator()
        for entry in inventory_entries:
            accumulator.accept(entry)
        if accumulator.finish() != root:
            raise ValueError
        recipes = tuple(decode_registry_source_recipes("[" + entry.recipe_json + "]")[0] for entry in inventory_entries)
        preparation = SourceInventoryPreparation(recipes)
        descriptors = tuple(
            json.loads(entry.contribution_json) for entry in inventory_entries if entry.contribution_json is not None
        )
        for descriptor in descriptors:
            preparation.add_contribution(descriptor)
        for entry in inventory_entries:
            if entry.office_derivation_json is None:
                raise ValueError
            preparation.add_derivation(json.loads(entry.office_derivation_json))
        if preparation.finish()[0].root != root:
            raise ValueError
        return root, preparation.recipes, tuple(sorted(descriptors, key=lambda source: source["recipe_sha256"]))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_contribution_inventory_invalid") from None


def _require_stage_receipt(stage, receipt, root):
    actual = registry_source_contribution_receipt(stage, inventory_root=root)
    if actual != receipt and not (
        root is not None and receipt is None and root.contribution_count == 0 and not stage.contribution_receipts
    ):
        raise RegistrySourceContributionError("registry_source_contribution_stage_invalid")


def registry_source_contribution_sha256(receipt, *, inventory_root=None):
    """Hash only a validated closed contribution receipt."""
    return _digest(validate_registry_source_contribution_receipt(receipt, inventory_root=inventory_root))


def verify_registry_source_contributions(candidate):
    """Require paired registration; independent candidate custody remains mandatory."""
    candidate_by_name = candidate
    try:
        if not isinstance(candidate_by_name, Mapping):
            candidate_by_name = dict(candidate_by_name)
        generations, validation = (
            candidate_by_name["source_generations"],
            candidate_by_name.get("validation_json") or {},
        )
        from process.registry_source_recipe_store import (
            MAX_SOURCE_RECIPE_INPUT_BYTES,
            _decode_document,
        )

        recipes = candidate_by_name.get("source_recipes_json")
        for raw_document in (generations, validation, recipes):
            if type(raw_document) is str and (
                len(raw_document) > MAX_SOURCE_RECIPE_INPUT_BYTES
                or len(raw_document.encode()) > MAX_SOURCE_RECIPE_INPUT_BYTES
            ):
                raise ValueError
        if isinstance(recipes, Mapping) or type(recipes) is str and recipes.lstrip().startswith("{"):
            from process.registry_source_inventory_reference import verify_source_recipe_metadata

            root = verify_source_recipe_metadata(candidate_by_name).inventory.root
            generations, validation = _decode_document(generations), _decode_document(validation)
            if type(validation) is not dict:
                raise ValueError
            if SUMMARY_KEY not in generations and SUMMARY_KEY not in validation and root.contribution_count == 0:
                return None
            receipt = validate_registry_source_contribution_receipt(validation[SUMMARY_KEY], inventory_root=root)
            if generations[SUMMARY_KEY] != registry_source_contribution_sha256(receipt, inventory_root=root):
                raise ValueError
            return receipt
        generations = json.loads(generations) if type(generations) is str else generations
        validation = json.loads(validation) if type(validation) is str else validation
        if SUMMARY_KEY not in generations and SUMMARY_KEY not in validation:
            return None
        receipt = validate_registry_source_contribution_receipt(validation[SUMMARY_KEY])
        if generations[SUMMARY_KEY] != registry_source_contribution_sha256(receipt):
            raise ValueError
        recipe_identities = {
            recipe_contribution_identity(recipe)
            for recipe in verify_registry_source_recipes(candidate_by_name)
            if type(recipe.source_pin) is PinnedFHIRMembershipSource
        }
        if {
            (source_descriptor["recipe_sha256"], source_descriptor["capture_sha256"])
            for source_descriptor in receipt["sources"]
        } != recipe_identities:
            raise ValueError
        return receipt
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_contribution_invalid") from None


async def retained_source_contributions(
    connection, source_manifest, control_schema, *, selected_recipes=None, witnesses=False
):
    """Authenticate predecessor metadata and original native fact owners without copying."""
    from process.registry_source_contribution_artifacts import (
        retained_source_fact_descriptors,
        verify_anchored_source_fact_contents,
    )

    if type(witnesses) is not bool:
        raise RegistrySourceContributionError("registry_source_contribution_invalid")
    contribution_identity = verified_office_fact_identity if witnesses else recipe_contribution_identity
    wanted = (
        None
        if selected_recipes is None
        else {
            contribution_identity(recipe)[1]
            for recipe in selected_recipes
            if type(recipe.source_pin) is PinnedFHIRMembershipSource
        }
    )
    if source_manifest is None or SUMMARY_KEY not in source_manifest.source_generations or wanted == set():
        return {}
    current = await resolve_network_serving_manifest(
        connection, generation_id=source_manifest.generation_id, control_schema=control_schema
    )
    if current != source_manifest:
        raise RegistrySourceContributionError("registry_source_contribution_unavailable")
    descriptors = await retained_source_fact_descriptors(
        connection, current.candidate_id, control_schema=control_schema
    )
    selected_descriptors = tuple(
        validate_source_witness_descriptor_v2(descriptor) if witnesses else descriptor
        for descriptor in descriptors
        if ("descriptor_version" in descriptor) == witnesses
        and (wanted is None or descriptor["capture_sha256"] in wanted)
    )
    artifacts = await verify_anchored_source_fact_contents(
        connection, selected_descriptors, control_schema=control_schema
    )
    return {artifact.descriptor["capture_sha256"]: artifact for artifact in artifacts.values()}


async def create_contribution_stage(connection, table_name):
    """Create the isolated native fact stage with per-source ordinal identity."""
    await connection.execute(f"""CREATE TEMP TABLE {_identifier(table_name)}(
      recipe_sha256 text NOT NULL CHECK(recipe_sha256 ~ '^[0-9a-f]{{64}}$'),
      ordinal bigint NOT NULL CHECK(ordinal>0),fact_json jsonb NOT NULL,
      PRIMARY KEY(recipe_sha256,ordinal))""")


def _validate_fhir_fact(fact, source_pin):
    if type(fact) is not dict or set(fact) != _FACT_FIELDS:
        raise ValueError
    if (
        fact["resource_type"] not in {"PractitionerRole", "OrganizationAffiliation"}
        or type(fact["resource_id"]) is not str
        or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", fact["resource_id"]) is None
        or type(fact["payload_sha256"]) is not str
        or re.fullmatch(r"[0-9a-f]{64}", fact["payload_sha256"]) is None
    ):
        raise ValueError
    edge_fields = _FACT_FIELDS - {"resource_type", "resource_id", "payload_sha256", "omission_reason"}
    if fact["omission_reason"] is not None:
        allowed_reasons = {"inactive", "expired", "future"}
        if fhir_normalized_capture(fhir_physical_source(source_pin)) is not None:
            allowed_reasons.add(GENERIC_UNVERIFIED_NETWORK)
        if fact["omission_reason"] not in allowed_reasons or any(fact[field] is not None for field in edge_fields):
            raise ValueError
        return
    _validate_fhir_scope(fact, source_pin)
    provider_id, system = fact["provider_id"], fact["provider_system"]
    if (
        type(provider_id) is not str
        or (system == "npi" and re.fullmatch(r"[12][0-9]{9}", provider_id) is None)
        or (
            system == "provider_directory"
            and (source_pin.source_id != "cms-npd" or re.fullmatch(r"cms_[0-9a-f]{64}", provider_id) is None)
        )
        or system not in {"npi", "provider_directory"}
    ):
        raise ValueError
    location_id = fact["location_id"]
    if location_id is not None and (
        type(location_id) is not str or str(UUID(location_id)) != location_id or not UUID(location_id).int
    ):
        raise ValueError
    _validate_fhir_evidence(fact, source_pin)


def _validate_fhir_scope(fact, source_pin):
    """Retain exact native-capture scope without manufacturing legacy identity."""
    capture = fhir_normalized_capture(source_pin)
    if capture is not None:
        scope = fact["source_scope_json"]
        if (
            type(fact["source_key"]) is not str
            or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", fact["source_key"]) is None
            or type(scope) is not dict
            or scope != {"organization_id": fact["source_key"], "capture_sha256": capture.capture_sha256}
        ):
            raise ValueError
        return
    scope = fact["source_scope_json"]
    if (
        type(fact["source_key"]) is not str
        or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", fact["source_key"]) is None
        or type(scope) is not dict
        or set(scope) != {"organization_id", "legacy_uuid", "alias_scope"}
        or scope["organization_id"] != fact["source_key"]
        or scope["alias_scope"] != source_pin.alias_scope
        or type(scope["legacy_uuid"]) is not str
        or str(UUID(scope["legacy_uuid"])) != scope["legacy_uuid"]
        or not UUID(scope["legacy_uuid"]).int
    ):
        raise ValueError


def _validate_fhir_evidence(fact, source_pin):
    source_pin = fhir_physical_source(source_pin)
    """Require the native evidence basis to describe this exact accepted source edge."""
    provider_system, provider_identity, office_id = fact["provider_system"], fact["provider_id"], fact["location_id"]
    basis = fact["evidence_basis"]
    if (
        type(basis) is not list
        or len(basis) != (9 if provider_system == "npi" else 13)
        or basis[:7]
        != [
            source_pin.dataset_id,
            source_pin.source_id,
            source_pin.release_id,
            fact["resource_type"],
            fact["resource_id"],
            fact["payload_sha256"],
            None,
        ]
        or basis[-1] != office_id
        or (provider_system == "npi" and basis[7] != provider_identity)
        or (
            provider_system == "provider_directory"
            and (
                basis[7:9] != [provider_system, provider_identity]
                or basis[9] not in {"Practitioner", "Organization"}
                or type(basis[10]) is not str
                or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", basis[10]) is None
                or type(basis[11]) is not str
                or re.fullmatch(r"[0-9a-f]{64}", basis[11]) is None
            )
        )
    ):
        raise ValueError


async def copy_fhir_fact_page(connection, batch, table_name, recipe_sha256, after_ordinal):
    """COPY one bounded fact page; never accept the JSON as independent admission."""
    try:
        if type(batch.source_fact_bytes) is not bytes or len(batch.source_fact_bytes) > 8388608:
            raise ValueError
        facts = json.loads(batch.source_fact_bytes, object_pairs_hook=_unique_object)
        if type(facts) is not list or len(facts) > 5000:
            raise ValueError
        for fact in facts:
            _validate_fhir_fact(fact, batch.source)
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_contribution_facts_invalid") from None
    records = [(recipe_sha256, after_ordinal + ordinal, _canonical(fact)) for ordinal, fact in enumerate(facts, 1)]
    if records:
        status = await connection.copy_records_to_table(
            table_name, schema_name="pg_temp", columns=("recipe_sha256", "ordinal", "fact_json"), records=records
        )
        if status != f"COPY {len(records)}":
            raise RegistrySourceContributionError("registry_source_contribution_accounting_invalid")
    return after_ordinal + len(records)


async def contribution_fact_sha256(connection, relation, recipe_sha256):
    """Hash ordered native facts with bounded transfers and strict ordinal continuity."""
    digest, cursor = hashlib.sha256(), 0
    while True:
        page = await connection.fetch(
            f"SELECT ordinal,encode(sha256(convert_to(jsonb_build_array(ordinal,fact_json)::text,'UTF8')),'hex') "
            f"AS fingerprint FROM {relation} WHERE recipe_sha256=$1 AND ordinal>$2 ORDER BY ordinal LIMIT 1000",
            recipe_sha256,
            cursor,
        )
        if not page:
            return digest.hexdigest(), cursor
        for row in page:
            if row["ordinal"] != cursor + 1:
                raise RegistrySourceContributionError("registry_source_contribution_ordinal_invalid")
            digest.update(row["fingerprint"].encode("ascii"))
            cursor = row["ordinal"]


async def reuse_fhir_facts(connection, retained_relation, source, table_name, recipe_sha256):
    """Materialize only an explicitly retained legacy-v1 contract after content verification."""
    if await contribution_fact_sha256(connection, retained_relation, recipe_sha256) != (
        source["fact_sha256"],
        source["fact_rows"],
    ):
        raise RegistrySourceContributionError("registry_source_contribution_content_invalid")
    status = await connection.execute(
        f"""INSERT INTO pg_temp.{_identifier(table_name)}
      SELECT recipe_sha256,ordinal,fact_json FROM {retained_relation} WHERE recipe_sha256=$1""",
        recipe_sha256,
    )
    if status != f"INSERT 0 {source['fact_rows']}":
        raise RegistrySourceContributionError("registry_source_contribution_accounting_invalid")


_FACT_MAP_SQL = """WITH {bindings}, facts AS MATERIALIZED (
 SELECT fact_json FROM {fact_relation} WHERE recipe_sha256=$8
), mapped AS MATERIALIZED (
 SELECT fact_json,approved.network_id FROM facts LEFT JOIN approved_network_bindings approved
 ON fact_json->>'omission_reason' IS NULL AND approved.record_json->>'source_key'=fact_json->>'source_key'
 AND approved.record_json->'source_scope_json'=fact_json->'source_scope_json'
), resources AS (
 SELECT fact_json->>'resource_type',fact_json->>'resource_id',bool_or(network_id IS NOT NULL) AS accepted
 FROM mapped GROUP BY 1,2
), inserted AS (
 {insert}
)
SELECT (SELECT count(*) FROM facts) AS fact_rows,
 (SELECT count(*) FROM mapped WHERE network_id IS NOT NULL AND fact_json->>'location_id' IS NOT NULL) AS membership_rows,
 (SELECT count(*) FROM mapped WHERE network_id IS NULL AND (fact_json->>'omission_reason' IS NULL OR fact_json->>'omission_reason'='generic_unverified_unapproved_network')) AS omitted_rows,
 (SELECT count(*) FROM resources) AS raw_source_rows,
 (SELECT count(*) FROM resources WHERE accepted) AS accepted_raw_source_rows,
 (SELECT count(*) FROM resources WHERE NOT accepted) AS omitted_raw_source_rows,
 (SELECT count(DISTINCT (fact_json->>'resource_type',fact_json->>'resource_id')) FROM mapped
   WHERE network_id IS NOT NULL AND fact_json->>'location_id' IS NULL) AS provider_only_rows,
 (SELECT count(*) FROM inserted) AS inserted_rows"""


async def map_fhir_facts(connection, recipe, approved, fact_relation, raw_table, control_schema, *, insert=True):
    """Apply exact approved bindings to retained facts and return complete accounting."""
    identity, capture = recipe_contribution_identity(recipe)
    mapping = await source_mapping_sha256(connection, recipe, approved, control_schema)
    insert_sql = f"""INSERT INTO pg_temp.{_identifier(raw_table)}
      SELECT network_id,fact_json->>'provider_system',fact_json->>'provider_id',
        (fact_json->>'location_id')::uuid,
        encode(sha256(convert_to(jsonb_set(fact_json->'evidence_basis','{{6}}',to_jsonb(network_id))::text,'UTF8')),'hex')
      FROM mapped WHERE network_id IS NOT NULL AND fact_json->>'location_id' IS NOT NULL RETURNING 1"""
    accounting = await connection.fetchrow(
        _FACT_MAP_SQL.format(
            bindings=APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_control_namespace(control_schema)),
            fact_relation=fact_relation,
            insert=insert_sql if insert else "SELECT 1 WHERE false",
        ),
        approved.approved_revision,
        *recipe.binding_coordinates.sql_parameters,
        identity,
    )
    descriptor_by_field = {field: accounting[field] for field in _COUNTERS}
    fact_sha256, fact_rows = await contribution_fact_sha256(connection, fact_relation, identity)
    if fact_rows != descriptor_by_field["fact_rows"]:
        raise RegistrySourceContributionError("registry_source_contribution_accounting_invalid")
    descriptor_by_field.update(
        fact_sha256=fact_sha256,
        recipe_sha256=identity,
        capture_sha256=capture,
        mapping_sha256=mapping,
        transform_version=fhir_fact_transform(recipe.source_pin),
        source_generation=_digest([capture, mapping]),
    )
    return descriptor_by_field


async def retain_registry_source_contributions(
    connection, copy_target, stage, *, control_schema=None, inventory_entries=None
):
    """Retain the verified stage only for its already registered open candidate."""
    from process.network_membership_candidate_lifecycle import _locked_candidate, _require_open, _require_transaction

    _require_transaction(connection, copy_target)
    async with connection.transaction():
        candidate = await _locked_candidate(connection, copy_target, _control_namespace(control_schema))
        _require_open(candidate)
        receipt = verify_registry_source_contributions(candidate)
        inventory = _inventory_source_metadata(candidate, inventory_entries)
        root = None if inventory is None else inventory[0]
        _require_stage_receipt(stage, receipt, root)
        if receipt is None and root is None:
            return None
        relation = f"{_identifier(copy_target.schema_name)}.{CONTRIBUTION_TABLE}"
        await connection.execute(f"LOCK TABLE {relation} IN ACCESS EXCLUSIVE MODE")
        own_rows = sum(
            descriptor["fact_rows"]
            for descriptor in stage.contribution_receipts
            if descriptor.get("fact_candidate_id") is None
        )
        if own_rows and await connection.fetchval(f"SELECT count(*) FROM {relation}") == 0:
            stage_relation = f"pg_temp.{_identifier(stage.contribution_table_name)}"
            await connection.execute(f"INSERT INTO {relation} SELECT * FROM {stage_relation}")
        await verify_registry_source_contributions_rows(
            connection,
            copy_target,
            candidate,
            source_stage=stage,
            control_schema=control_schema,
            **({"inventory_entries": inventory_entries} if root is not None else {}),
        )
        return receipt


async def verify_registry_source_contributions_rows(
    connection, copy_target, candidate, *, source_stage=None, control_schema=None, inventory_entries=None
):
    """Verify complete per-descriptor native accounting and optional staged multiset parity."""
    receipt = verify_registry_source_contributions(candidate)
    inventory = _inventory_source_metadata(candidate, inventory_entries)
    if receipt is None and inventory is None:
        return None
    root = None if inventory is None else inventory[0]
    descriptors = receipt["sources"] if inventory is None else inventory[2]
    if not connection.is_in_transaction() or any(
        str(candidate[field]) != getattr(copy_target, field)
        for field in ("candidate_id", "dataset_id", "schema_id", "producer_id", "schema_name")
    ):
        raise RegistrySourceContributionError("registry_source_contribution_scope_invalid")
    relation = f"{_identifier(copy_target.schema_name)}.{CONTRIBUTION_TABLE}"
    native_counts = await connection.fetch(f"SELECT recipe_sha256,count(*) AS fact_rows FROM {relation} GROUP BY 1")
    actual_by_recipe = {native_count["recipe_sha256"]: native_count["fact_rows"] for native_count in native_counts}
    expected_by_recipe = {
        descriptor["recipe_sha256"]: descriptor["fact_rows"]
        for descriptor in descriptors
        if descriptor["fact_rows"] and descriptor.get("fact_candidate_id") is None
    }
    if actual_by_recipe != expected_by_recipe:
        raise RegistrySourceContributionError("registry_source_contribution_accounting_invalid")
    await _verify_fact_owners(connection, descriptors, control_schema, inventory=root is not None)
    for descriptor in descriptors:
        if descriptor.get("fact_candidate_id") is not None:
            continue
        if await contribution_fact_sha256(connection, relation, descriptor["recipe_sha256"]) != (
            descriptor["fact_sha256"],
            descriptor["fact_rows"],
        ):
            raise RegistrySourceContributionError("registry_source_contribution_content_invalid")
    if source_stage is not None:
        _require_stage_receipt(source_stage, receipt, root)
        if source_stage.contribution_table_name is None and not descriptors:
            return receipt
        stage_relation = f"pg_temp.{_identifier(source_stage.contribution_table_name)}"
        differs = await connection.fetchval(f"""WITH differences AS (
          (SELECT * FROM {relation} EXCEPT ALL SELECT * FROM {stage_relation})
          UNION ALL (SELECT * FROM {stage_relation} EXCEPT ALL SELECT * FROM {relation}))
          SELECT EXISTS(SELECT 1 FROM differences)""")
        if differs:
            raise RegistrySourceContributionError("registry_source_contribution_parity_invalid")
    return receipt


async def _verify_fact_owners(connection, descriptors, control_schema, *, inventory):
    """Keep the legacy owner API bound while holding every selected original-owner lock."""
    from process.registry_source_contribution_artifacts import verify_anchored_source_fact_contents

    pages = (
        (descriptors,)
        if not inventory
        else (descriptors[offset : offset + 100] for offset in range(0, len(descriptors), 100))
    )
    for page in pages:
        await verify_anchored_source_fact_contents(
            connection, page, control_schema=registry_schema() if control_schema is None else control_schema
        )


# This explicit contract shares the fact heap, never a v1 decoder or default identity.
SOURCE_WITNESS_DESCRIPTOR_VERSION_V2 = 2
_WITNESS_COUNTERS_V2 = (*_COUNTERS, "unresolved_rows", "unresolved_raw_source_rows")
_WITNESS_COMMON_V2 = {
    "fact_version",
    "kind",
    "source_binding",
    "resource_type",
    "resource_id",
    "payload_sha256",
}
_WITNESS_RESOURCE_V2 = _WITNESS_COMMON_V2 | {"malformed", "omission_reason"}
_WITNESS_EDGE_V2 = _WITNESS_COMMON_V2 | {
    "source_key",
    "source_scope_json",
    "provider_system",
    "provider_id",
    "location_id",
    "provider_only",
    "provider_unresolved",
    "location_unresolved",
    "network_witness_complete",
    "evidence_basis",
}


def verified_office_fact_identity(recipe):
    """Select v2 explicitly; default recipe/fact identities remain permanently v1."""
    from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2

    if type(recipe.source_pin) is not PinnedFHIRMembershipSource:
        raise RegistrySourceContributionError("registry_source_witness_descriptor_invalid")
    identity = hashlib.sha256(canonical_registry_source_recipes((recipe,)).encode()).hexdigest()
    return identity, _digest([identity, FHIR_SOURCE_WITNESS_TRANSFORM_V2])


def validate_source_witness_descriptor_v2(descriptor):
    """Decode only the new closed accounting shape; this metadata grants no custody."""
    from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2

    try:
        if type(descriptor) is str:
            if len(descriptor.encode()) > MAX_RECEIPT_BYTES:
                raise ValueError
            descriptor = json.loads(descriptor, object_pairs_hook=_unique_object)
        fields = {
            *_WITNESS_COUNTERS_V2,
            *_HASH_FIELDS,
            *FACT_ANCHOR_FIELDS,
            "descriptor_version",
            "transform_version",
        }
        if (
            type(descriptor) is not dict
            or set(descriptor) != fields
            or type(descriptor["descriptor_version"]) is not int
            or descriptor["descriptor_version"] != SOURCE_WITNESS_DESCRIPTOR_VERSION_V2
            or descriptor["transform_version"] != FHIR_SOURCE_WITNESS_TRANSFORM_V2
            or any(
                type(descriptor[field]) is not int or not 0 <= descriptor[field] < 2**63
                for field in _WITNESS_COUNTERS_V2
            )
            or any(
                type(descriptor[field]) is not str or re.fullmatch(r"[0-9a-f]{64}", descriptor[field]) is None
                for field in _HASH_FIELDS
            )
            or descriptor["raw_source_rows"]
            != sum(
                descriptor[field]
                for field in (
                    "accepted_raw_source_rows",
                    "unresolved_raw_source_rows",
                    "omitted_raw_source_rows",
                )
            )
            or descriptor["fact_rows"] < descriptor["raw_source_rows"]
            or (descriptor["raw_source_rows"] == 0 and descriptor["fact_rows"] != 0)
            or sum(descriptor[field] for field in ("membership_rows", "omitted_rows", "unresolved_rows"))
            > descriptor["fact_rows"] - descriptor["raw_source_rows"]
            or any(
                descriptor[field] > descriptor["fact_rows"] - descriptor["raw_source_rows"]
                for field in ("membership_rows", "omitted_rows", "unresolved_rows")
            )
            or descriptor["provider_only_rows"] > descriptor["raw_source_rows"]
            or descriptor["capture_sha256"] != _digest([descriptor["recipe_sha256"], descriptor["transform_version"]])
            or descriptor["source_generation"] != _digest([descriptor["capture_sha256"], descriptor["mapping_sha256"]])
        ):
            raise ValueError
        _validate_fact_anchor(descriptor)
        return json.loads(_canonical(descriptor))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_witness_descriptor_invalid") from None


SOURCE_MAPPING_DESCRIPTOR_VERSION_V3 = 3
_MAPPING_COUNTERS_V3 = (
    *_WITNESS_COUNTERS_V2,
    "accepted_input_rows",
    "ignored_input_rows",
    "provider_only_evidence_rows",
)


def _mapping_coverage_v3(network_ids):
    if (
        type(network_ids) is not list
        or any(type(network_id) is not int or not 0 < network_id < 2**31 for network_id in network_ids)
        or network_ids != sorted(set(network_ids))
        or len(_canonical(network_ids).encode()) > MAX_RECEIPT_BYTES
    ):
        raise ValueError


def source_mapping_generation_v3(capture_sha256, mapping_sha256, covered_network_ids):
    """Commit the explicit mapping revision and coverage; metadata grants no custody."""
    try:
        if any(
            type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None
            for digest in (capture_sha256, mapping_sha256)
        ):
            raise ValueError
        _mapping_coverage_v3(covered_network_ids)
        return _digest([capture_sha256, mapping_sha256, SOURCE_MAPPING_DESCRIPTOR_VERSION_V3, covered_network_ids])
    except ValueError, TypeError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_mapping_descriptor_invalid") from None


def _mapping_input_rows_v3(descriptor):
    from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2

    return descriptor["fact_rows"] - (
        descriptor["raw_source_rows"] if descriptor["transform_version"] == FHIR_SOURCE_WITNESS_TRANSFORM_V2 else 0
    )


def _mapping_counts_v3(descriptor):
    from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2

    count = descriptor
    accepted, outputs = count["accepted_input_rows"], count["membership_rows"] + count["provider_only_evidence_rows"]
    if (
        count["raw_source_rows"]
        != sum(
            count[name]
            for name in ("accepted_raw_source_rows", "unresolved_raw_source_rows", "omitted_raw_source_rows")
        )
        or count["fact_rows"] < count["raw_source_rows"]
        or (count["raw_source_rows"] == 0 and count["fact_rows"] != 0)
        or sum(count[name] for name in ("accepted_input_rows", "omitted_rows", "unresolved_rows", "ignored_input_rows"))
        != _mapping_input_rows_v3(count)
        or count["provider_only_rows"] > min(count["raw_source_rows"], accepted)
        or count["provider_only_rows"] > count["provider_only_evidence_rows"]
        or count["accepted_raw_source_rows"] > min(accepted, outputs)
        or outputs > accepted * len(count["covered_network_ids"])
        or outputs >= 2**63
        or (accepted == 0) != (outputs == 0)
        or (count["provider_only_rows"] == 0) != (count["provider_only_evidence_rows"] == 0)
    ):
        raise ValueError
    if count["transform_version"] != FHIR_SOURCE_WITNESS_TRANSFORM_V2 and (
        count["unresolved_rows"] != 0
        or count["unresolved_raw_source_rows"] != 0
        or count["provider_only_rows"] > count["accepted_raw_source_rows"]
    ):
        raise ValueError


def validate_source_mapping_descriptor_v3(descriptor):
    """Decode mapped output separately from unchanged raw facts; no Source authority is inferred."""
    from process.network_fhir_membership_source import FHIR_SOURCE_WITNESS_TRANSFORM_V2

    try:
        if type(descriptor) is str:
            if len(descriptor.encode()) > MAX_RECEIPT_BYTES:
                raise ValueError
            descriptor = json.loads(descriptor, object_pairs_hook=_unique_object)
        fields = {
            *_MAPPING_COUNTERS_V3,
            *_HASH_FIELDS,
            *FACT_ANCHOR_FIELDS,
            "descriptor_version",
            "transform_version",
            "covered_network_ids",
        }
        if (
            type(descriptor) is not dict
            or set(descriptor) != fields
            or len(_canonical(descriptor).encode()) > MAX_RECEIPT_BYTES
            or type(descriptor["descriptor_version"]) is not int
            or descriptor["descriptor_version"] != SOURCE_MAPPING_DESCRIPTOR_VERSION_V3
            or descriptor["transform_version"]
            not in (TRANSFORM_VERSION, GENERIC_TRANSFORM_VERSION, FHIR_SOURCE_WITNESS_TRANSFORM_V2)
            or any(
                type(descriptor[name]) is not int or not 0 <= descriptor[name] < 2**63 for name in _MAPPING_COUNTERS_V3
            )
            or any(
                type(descriptor[name]) is not str or re.fullmatch(r"[0-9a-f]{64}", descriptor[name]) is None
                for name in _HASH_FIELDS
            )
            or descriptor["capture_sha256"] != _digest([descriptor["recipe_sha256"], descriptor["transform_version"]])
            or descriptor["source_generation"]
            != source_mapping_generation_v3(
                descriptor["capture_sha256"], descriptor["mapping_sha256"], descriptor["covered_network_ids"]
            )
        ):
            raise ValueError
        _mapping_coverage_v3(descriptor["covered_network_ids"])
        _mapping_counts_v3(descriptor)
        _validate_fact_anchor(descriptor)
        return json.loads(_canonical(descriptor))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_mapping_descriptor_invalid") from None


def validate_source_contribution_descriptor(descriptor):
    """Dispatch closed legacy, witness-v2 and mapping-v3 shapes without changing raw format."""
    try:
        if type(descriptor) is str:
            if len(descriptor.encode()) > MAX_RECEIPT_BYTES:
                raise ValueError
            descriptor = json.loads(descriptor, object_pairs_hook=_unique_object)
        if type(descriptor) is not dict:
            raise ValueError
        if "descriptor_version" in descriptor:
            version = descriptor["descriptor_version"]
            if type(version) is not int:
                raise ValueError
            if version == 2:
                return validate_source_witness_descriptor_v2(descriptor)
            if version == 3:
                return validate_source_mapping_descriptor_v3(descriptor)
            raise ValueError
        return validate_registry_source_contribution_receipt(
            {
                "component": SUMMARY_KEY,
                "revision": 2 if FACT_ANCHOR_FIELDS.intersection(descriptor) else 1,
                "table_name": CONTRIBUTION_TABLE,
                "sources": [descriptor],
            }
        )["sources"][0]
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_contribution_invalid") from None


def _validate_witness_common_v2(fact, source_pin):
    from process.network_fhir_membership_source import fhir_source_witness_binding_v2

    if (
        type(fact) is not dict
        or type(fact.get("fact_version")) is not int
        or fact["fact_version"] != 2
        or fact.get("source_binding") != fhir_source_witness_binding_v2(source_pin)
        or fact.get("resource_type") not in {"PractitionerRole", "OrganizationAffiliation"}
        or type(fact.get("resource_id")) is not str
        or not 1 <= len(fact["resource_id"]) <= 256
        or "\x00" in fact["resource_id"]
        or type(fact.get("payload_sha256")) is not str
        or not 1 <= len(fact["payload_sha256"]) <= 256
    ):
        raise ValueError


def _validate_witness_scope_v2(fact, source_pin):
    """Validate the closed edge and its source-scoped network witness."""
    if set(fact) != _WITNESS_EDGE_V2 or any(
        type(fact[field]) is not bool
        for field in (
            "provider_only",
            "provider_unresolved",
            "location_unresolved",
            "network_witness_complete",
        )
    ):
        raise ValueError
    key, scope = fact["source_key"], fact["source_scope_json"]
    if key is not None and (type(key) is not str or re.fullmatch(r"[A-Za-z0-9.-]{1,64}", key) is None):
        raise ValueError
    if type(scope) is not dict or scope.get("organization_id") != key:
        raise ValueError
    capture = fhir_normalized_capture(source_pin)
    if fhir_normalized_capture(fhir_physical_source(source_pin)) is not None:
        if scope != {
            "organization_id": key,
            "capture_sha256": capture.capture_sha256,
        }:
            raise ValueError
    elif (
        set(scope) != {"organization_id", "legacy_uuid", "alias_scope"}
        or scope["alias_scope"] != fhir_physical_source(source_pin).alias_scope
        or (
            scope["legacy_uuid"] is not None
            and (
                type(scope["legacy_uuid"]) is not str
                or str(UUID(scope["legacy_uuid"])) != scope["legacy_uuid"]
                or not UUID(scope["legacy_uuid"]).int
            )
        )
        or (fact["network_witness_complete"] and scope["legacy_uuid"] is None)
    ):
        raise ValueError
    if fact["network_witness_complete"] and key is None:
        raise ValueError


def _validate_witness_edge_v2(fact, source_pin):
    """Validate provider/location resolution and exact physical evidence after source scope."""
    _validate_witness_scope_v2(fact, source_pin)
    provider, system = fact["provider_id"], fact["provider_system"]
    if system not in {"npi", "provider_directory"} or (
        system == "provider_directory" and source_pin.source_id != "cms-npd"
    ):
        raise ValueError
    is_valid_provider = (
        type(provider) is str
        and re.fullmatch(r"[12][0-9]{9}" if system == "npi" else r"cms_[0-9a-f]{64}", provider) is not None
    )
    if fact["provider_unresolved"] == is_valid_provider:
        raise ValueError
    if fact["provider_unresolved"] and provider is not None:
        raise ValueError
    location = fact["location_id"]
    if location is not None and (
        type(location) is not str or str(UUID(location)) != location or not UUID(location).int
    ):
        raise ValueError
    if fact["provider_only"] and (location is not None or fact["location_unresolved"]):
        raise ValueError
    if location is None and not fact["provider_only"] and not fact["location_unresolved"]:
        raise ValueError
    physical = fhir_physical_source(source_pin)
    basis = fact["evidence_basis"]
    if (
        type(basis) is not list
        or len(basis) != (9 if system == "npi" else 13)
        or basis[:7]
        != [
            physical.dataset_id,
            physical.source_id,
            physical.release_id,
            fact["resource_type"],
            fact["resource_id"],
            fact["payload_sha256"],
            None,
        ]
        or basis[-1] != location
        or (system == "npi" and basis[7] != provider)
        or (system == "provider_directory" and basis[7:9] != [system, provider])
    ):
        raise ValueError
    if is_valid_provider:
        _validate_fhir_evidence(fact, source_pin)


def decode_source_witness_page_v2(page):
    """Validate a complete bounded raw census before COPY; never authenticate a source."""
    from process.network_fhir_membership_source import FHIRSourceWitnessPageV2

    try:
        if (
            type(page) is not FHIRSourceWitnessPageV2
            or type(page.source) is not PinnedFHIRMembershipSource
            or type(page.source_rows) is not int
            or not 0 <= page.source_rows <= 5000
            or type(page.source_fact_bytes) is not bytes
            or len(page.source_fact_bytes) > 8388608
        ):
            raise ValueError
        facts = json.loads(page.source_fact_bytes, object_pairs_hook=_unique_object)
        if type(facts) is not list or len(facts) > 10000:
            raise ValueError
        resources, edges = {}, []
        for fact in facts:
            _validate_witness_common_v2(fact, page.source)
            identity = fact["resource_type"], fact["resource_id"]
            if fact.get("kind") == "resource":
                if (
                    set(fact) != _WITNESS_RESOURCE_V2
                    or type(fact["malformed"]) is not bool
                    or fact["omission_reason"] not in {None, "inactive", "expired", "future"}
                    or identity in resources
                    or (
                        not fact["malformed"]
                        and (
                            re.fullmatch(r"[A-Za-z0-9.-]{1,64}", fact["resource_id"]) is None
                            or re.fullmatch(r"[0-9a-f]{64}", fact["payload_sha256"]) is None
                        )
                    )
                ):
                    raise ValueError
                resources[identity] = fact
            elif fact.get("kind") == "edge":
                _validate_witness_edge_v2(fact, page.source)
                edges.append(fact)
            else:
                raise ValueError
        if (
            len(resources) != page.source_rows
            or len(edges) > 5000
            or page.next_resource != (max(resources) if resources else None)
            or any(
                (header := resources.get((edge["resource_type"], edge["resource_id"]))) is None
                or header["payload_sha256"] != edge["payload_sha256"]
                or (header["omission_reason"] is not None and not header["malformed"])
                for edge in edges
            )
        ):
            raise ValueError
        return facts
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceContributionError("registry_source_witness_facts_invalid") from None


decode_fhir_source_witness_page_v2 = decode_source_witness_page_v2


async def copy_source_witness_page_v2(connection, page, table_name, recipe_sha256, after_ordinal):
    """COPY explicitly decoded v2 facts in existing bounded native chunks."""
    from process.uhc_flex_practitioner_async_safety import drain_operation

    if not connection.is_in_transaction():
        raise RegistrySourceContributionError("registry_source_witness_transaction_required")
    facts = decode_fhir_source_witness_page_v2(page)
    if (
        type(recipe_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", recipe_sha256) is None
        or type(after_ordinal) is not int
        or not 0 <= after_ordinal < 2**63 - len(facts)
    ):
        raise RegistrySourceContributionError("registry_source_witness_facts_invalid")
    for offset in range(0, len(facts), 5000):
        fact_records = [
            (recipe_sha256, after_ordinal + offset + ordinal, _canonical(fact))
            for ordinal, fact in enumerate(facts[offset : offset + 5000], 1)
        ]
        status = await drain_operation(
            connection.copy_records_to_table(
                table_name,
                schema_name="pg_temp",
                columns=("recipe_sha256", "ordinal", "fact_json"),
                records=fact_records,
            ),
            preserve_cancellation=True,
        )
        if status != f"COPY {len(fact_records)}":
            raise RegistrySourceContributionError("registry_source_contribution_accounting_invalid")
    return after_ordinal + len(facts)


copy_fhir_source_witness_page_v2 = copy_source_witness_page_v2

_WITNESS_MAP_SQL_V2 = """WITH {bindings}, facts AS MATERIALIZED (
 SELECT fact_json FROM {fact_relation} WHERE recipe_sha256=$8
), headers AS MATERIALIZED (
 SELECT fact_json, fact_json->>'resource_type' AS resource_type,fact_json->>'resource_id' AS resource_id
 FROM facts WHERE fact_json->>'kind'='resource'
), edges AS MATERIALIZED (
 SELECT fact_json FROM facts WHERE fact_json->>'kind'='edge'
), invalid AS (
 SELECT 1 FROM facts WHERE fact_json->'fact_version' IS DISTINCT FROM '2'::jsonb
 OR fact_json->'source_binding' IS DISTINCT FROM $10::jsonb
 OR fact_json->>'kind' IS NULL OR fact_json->>'kind' NOT IN ('resource','edge')
 UNION ALL SELECT 1 FROM headers GROUP BY resource_type,resource_id HAVING count(*)<>1
 UNION ALL SELECT 1 FROM edges LEFT JOIN headers ON
 headers.resource_type=edges.fact_json->>'resource_type' AND headers.resource_id=edges.fact_json->>'resource_id'
 WHERE headers.fact_json IS NULL OR headers.fact_json->'payload_sha256' IS DISTINCT FROM edges.fact_json->'payload_sha256'
), mapped AS MATERIALIZED (
 SELECT edges.fact_json,headers.fact_json AS header,approved.network_id,
   (headers.fact_json->>'malformed')::boolean OR (edges.fact_json->>'provider_unresolved')::boolean
   OR (edges.fact_json->>'location_unresolved')::boolean OR edges.fact_json->>'source_key' IS NULL
   OR (NOT (edges.fact_json->>'network_witness_complete')::boolean AND (NOT $9::boolean OR approved.network_id IS NOT NULL))
   AS unresolved
 FROM edges JOIN headers ON headers.resource_type=edges.fact_json->>'resource_type'
 AND headers.resource_id=edges.fact_json->>'resource_id'
 LEFT JOIN approved_network_bindings approved ON approved.record_json->>'source_key'=edges.fact_json->>'source_key'
 AND approved.record_json->'source_scope_json'=edges.fact_json->'source_scope_json'
), edge_status AS MATERIALIZED (
 SELECT *,NOT unresolved AND header->>'omission_reason' IS NULL AND network_id IS NULL AS omitted,
 NOT unresolved AND header->>'omission_reason' IS NULL AND network_id IS NOT NULL AS accepted FROM mapped
), resource_status AS (
 SELECT headers.resource_type,headers.resource_id,
 CASE WHEN (headers.fact_json->>'malformed')::boolean OR coalesce(bool_or(edge_status.unresolved),false)
   OR (headers.fact_json->>'omission_reason' IS NULL AND count(edge_status.fact_json)=0) THEN 'unresolved'
 WHEN headers.fact_json->>'omission_reason' IS NOT NULL THEN 'omitted'
 WHEN coalesce(bool_or(edge_status.accepted),false) THEN 'accepted' ELSE 'omitted' END AS outcome
 FROM headers LEFT JOIN edge_status ON edge_status.fact_json->>'resource_type'=headers.resource_type
 AND edge_status.fact_json->>'resource_id'=headers.resource_id GROUP BY headers.resource_type,headers.resource_id,headers.fact_json
), inserted AS (
 {insert}
)
SELECT EXISTS(SELECT 1 FROM invalid) AS invalid,(SELECT count(*) FROM facts) AS fact_rows,
 (SELECT count(*) FROM edge_status WHERE accepted AND fact_json->>'location_id' IS NOT NULL) AS membership_rows,
 (SELECT count(*) FROM edge_status WHERE omitted) AS omitted_rows,
 (SELECT count(*) FROM edge_status WHERE unresolved) AS unresolved_rows,
 (SELECT count(*) FROM headers) AS raw_source_rows,
 (SELECT count(*) FROM resource_status WHERE outcome='accepted') AS accepted_raw_source_rows,
 (SELECT count(*) FROM resource_status WHERE outcome='unresolved') AS unresolved_raw_source_rows,
 (SELECT count(*) FROM resource_status WHERE outcome='omitted') AS omitted_raw_source_rows,
 (SELECT count(DISTINCT(fact_json->>'resource_type',fact_json->>'resource_id')) FROM edge_status
 WHERE accepted AND (fact_json->>'provider_only')::boolean) AS provider_only_rows,
 (SELECT count(*) FROM inserted) AS inserted_rows"""


def _mapped_witness_descriptor_v2(accounting, identity, capture, mapping, fact_sha256, transform_version):
    """Bind native mapping counters and fact content to one explicitly versioned descriptor."""
    return validate_source_witness_descriptor_v2(
        {
            **{field: accounting[field] for field in _WITNESS_COUNTERS_V2},
            **dict.fromkeys(FACT_ANCHOR_FIELDS),
            "descriptor_version": SOURCE_WITNESS_DESCRIPTOR_VERSION_V2,
            "recipe_sha256": identity,
            "capture_sha256": capture,
            "mapping_sha256": mapping,
            "fact_sha256": fact_sha256,
            "source_generation": _digest([capture, mapping]),
            "transform_version": transform_version,
        }
    )


async def map_fhir_source_witnesses_v2(
    connection,
    recipe,
    approved,
    fact_relation,
    raw_table,
    control_schema,
    *,
    insert=True,
):
    """Reclassify exact native witnesses under selected bindings, without office authority.

    The caller must independently authenticate/re-extract fresh facts, or verify
    their exact immutable original anchor. Office policy/address/correspondence is
    separately bound and is never inferred from this descriptor.
    """
    from process.network_fhir_membership_source import (
        FHIR_SOURCE_WITNESS_TRANSFORM_V2,
        fhir_source_witness_binding_v2,
    )

    if not connection.is_in_transaction():
        raise RegistrySourceContributionError("registry_source_witness_transaction_required")
    if type(insert) is not bool:
        raise RegistrySourceContributionError("registry_source_witness_descriptor_invalid")
    identity, capture = verified_office_fact_identity(recipe)
    mapping = await source_mapping_sha256(connection, recipe, approved, control_schema)
    insert_sql = f"""INSERT INTO pg_temp.{_identifier(raw_table)}
      SELECT network_id,fact_json->>'provider_system',fact_json->>'provider_id',
        (fact_json->>'location_id')::uuid,
        encode(sha256(convert_to(jsonb_set(fact_json->'evidence_basis','{{6}}',to_jsonb(network_id))::text,'UTF8')),'hex')
      FROM edge_status WHERE accepted AND fact_json->>'location_id' IS NOT NULL
        AND NOT EXISTS(SELECT 1 FROM invalid) RETURNING 1"""
    accounting = await connection.fetchrow(
        _WITNESS_MAP_SQL_V2.format(
            bindings=APPROVED_NETWORK_BINDINGS_SQL.format(namespace=_control_namespace(control_schema)),
            fact_relation=fact_relation,
            insert=insert_sql if insert else "SELECT 1 WHERE false",
        ),
        approved.approved_revision,
        *recipe.binding_coordinates.sql_parameters,
        identity,
        fhir_normalized_capture(fhir_physical_source(recipe.source_pin)) is not None,
        _canonical(fhir_source_witness_binding_v2(recipe.source_pin)),
    )
    if accounting["invalid"]:
        raise RegistrySourceContributionError("registry_source_witness_facts_invalid")
    fact_sha256, fact_rows = await contribution_fact_sha256(connection, fact_relation, identity)
    if fact_rows != accounting["fact_rows"] or accounting["inserted_rows"] != (
        accounting["membership_rows"] if insert else 0
    ):
        raise RegistrySourceContributionError("registry_source_contribution_accounting_invalid")
    return _mapped_witness_descriptor_v2(
        accounting, identity, capture, mapping, fact_sha256, FHIR_SOURCE_WITNESS_TRANSFORM_V2
    )
