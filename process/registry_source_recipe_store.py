# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed raw membership recipes retained privately for approved-map replay.

Receipts and their hashes do not confer source admission. Reviewed readers must
revalidate sealed source metadata and resolve the latest approved map when they
recompose these recipes. No approved revision or legacy alias selection is saved.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, fields
from uuid import UUID

import asyncpg

from process.network_address_projection import _identifier
from process.network_approved_source_bindings import RegistryNetworkSourceCoordinates
from process.network_fhir_membership_source import SOURCE_WITNESS_PRODUCER, PinnedFHIRMembershipSource
from process.network_fhir_source_epoch import validate_retained_cms_fhir_source_epoch
from process.network_fhir_source_witness_contract import SourceWitnessCaptureAnchor
from process.network_fhir_source_witness_contract import validate_capture as validate_source_witness_capture
from process.network_fhir_typed_recipe import PinnedTypedFHIRMembershipSource
from process.network_legacy_membership_source import PinnedACAMembershipSource, aca_source_descriptor
from process.network_membership_candidate_lifecycle import _control_namespace
from process.network_serving_read import PinnedNetworkServingManifest, resolve_network_serving_manifest
from process.reference_family_archive import (
    ReferenceFamilyArchiveError,
    ReferenceFamilyPreparedSource,
    ReferenceFamilyStageOwnership,
    validate_reference_family_manifest,
    validate_reference_family_validation_receipt,
)
from process.registry_ptg_office_membership_contract import PinnedPTGOfficeMembershipSource

MAX_SOURCE_RECIPES = 100
MAX_SOURCE_RECIPE_BYTES = 524288
MAX_SOURCE_RECIPE_INPUT_BYTES = 1048576
RECIPE_DIGEST_KEY = "registry_source_recipes"
SELECTION_POLICY = "approved-bindings-only"
_ACA_BATCH_KIND = "aca_initial_batch"


class RegistrySourceRecipeError(ValueError):
    """A closed recipe or retained source proof differs; messages contain no input."""


@dataclass(frozen=True)
class RegistrySourceMembershipRecipe:
    source_pin: (
        PinnedFHIRMembershipSource
        | PinnedACAMembershipSource
        | PinnedPTGOfficeMembershipSource
        | PinnedTypedFHIRMembershipSource
    )
    binding_coordinates: RegistryNetworkSourceCoordinates
    selection_policy: str = SELECTION_POLICY

    def __post_init__(self):
        _validate_recipe_scope(self)


def _validate_recipe_scope(recipe):
    source_pin, coordinates = recipe.source_pin, recipe.binding_coordinates
    if (
        type(coordinates) is not RegistryNetworkSourceCoordinates
        or type(recipe.selection_policy) is not str
        or recipe.selection_policy != SELECTION_POLICY
    ):
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    if type(source_pin) is PinnedTypedFHIRMembershipSource:
        source_pin.__post_init__()
        if asdict(coordinates) != source_pin.catalog_descriptor["coordinates"]:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        return
    if type(source_pin) is PinnedFHIRMembershipSource:
        expected_scope = ("fhir", source_pin.source_id, source_pin.schema_name, source_pin.dataset_id)
        if source_pin.source_witness_capture is not None and (
            coordinates.producer_id != SOURCE_WITNESS_PRODUCER
            or coordinates.edition_id != source_pin.source_witness_capture.capture_id
        ):
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    elif type(source_pin) is PinnedACAMembershipSource:
        ownership = source_pin.prepared.ownership
        expected_scope = ("aca", source_pin.source_id, ownership.schema_name, str(ownership.dataset_id))
        if source_pin.prepared.manifest.importer_id != "mrf" or aca_source_descriptor(source_pin).get(
            "network_bindings"
        ) != {**asdict(coordinates), "source_key_kind": "hios_plan_id"}:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    elif type(source_pin) is PinnedPTGOfficeMembershipSource:
        expected_scope = source_pin.coordinates.sql_parameters[:4]
        if coordinates != source_pin.coordinates:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    else:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    if coordinates.sql_parameters[:4] != expected_scope:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")


def _closed_document(document, model):
    if type(document) is not dict or set(document) != {field.name for field in fields(model)}:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    return document


def _oid(value):
    if type(value) is not int or not 0 < value <= 4294967295:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    return value


def _ownership_inventory(inventory, width):
    if type(inventory) is not list or len(inventory) > 256:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    retained_entries = []
    for entry in inventory:
        if type(entry) is not list or len(entry) != width:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        _identifier(entry[0])
        _oid(entry[1])
        for name in entry[2:]:
            _identifier(name)
        retained_entries.append(tuple(entry))
    if (
        retained_entries != sorted(retained_entries)
        or len({entry[0] for entry in retained_entries}) != len(retained_entries)
        or len({entry[1] for entry in retained_entries}) != len(retained_entries)
    ):
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    return tuple(retained_entries)


def _decode_ownership(document):
    document = _closed_document(document, ReferenceFamilyStageOwnership)
    dataset_id = UUID(document["dataset_id"]) if type(document["dataset_id"]) is str else None
    if dataset_id is None or not dataset_id.int or str(dataset_id) != document["dataset_id"]:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    _identifier(document["schema_name"])
    _oid(document["schema_oid"])
    relation_oids = _ownership_inventory(document["relation_oids"], 2)
    sequence_oids = _ownership_inventory(document["sequence_oids"], 4)
    auxiliary_oid = document["auxiliary_oid"]
    if auxiliary_oid is not None:
        _oid(auxiliary_oid)
    all_oids = [oid for _, oid in relation_oids] + [entry[1] for entry in sequence_oids]
    if auxiliary_oid is not None:
        all_oids.append(auxiliary_oid)
    if len(all_oids) != len(set(all_oids)):
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    return ReferenceFamilyStageOwnership(
        document["importer_id"],
        dataset_id,
        document["schema_name"],
        document["schema_oid"],
        relation_oids,
        sequence_oids,
        auxiliary_oid,
    )


def _decode_aca_pin(pin_by_field):
    fields_before = {field.name for field in fields(PinnedACAMembershipSource)} - {
        "retained_successor",
        "retained_epoch",
    }
    if type(pin_by_field) is not dict or set(pin_by_field) not in (
        fields_before,
        fields_before | {"retained_successor"},
        fields_before | {"retained_epoch"},
    ):
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    pin_by_field = dict(pin_by_field)
    if "retained_successor" in pin_by_field:
        successor = pin_by_field["retained_successor"]
        if type(successor) is not dict or set(successor) != fields_before:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        pin_by_field["retained_successor"] = _decode_aca_pin(successor)
    if "retained_epoch" in pin_by_field:
        from process.registry_aca_global_epoch import decode_pinned_aca_global_epoch

        pin_by_field["retained_epoch"] = decode_pinned_aca_global_epoch(pin_by_field["retained_epoch"])
    prepared = pin_by_field["prepared"]
    if type(prepared) is not dict or set(prepared) != {"manifest", "ownership"}:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    if type(pin_by_field["runtime_roles"]) is not list:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    if type(pin_by_field["validation"]) is not dict or type(pin_by_field["validation"].get("package_id")) is not str:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    _oid(pin_by_field["validation"].get("sealed_owner_oid"))
    return PinnedACAMembershipSource(
        ReferenceFamilyPreparedSource(
            validate_reference_family_manifest(prepared["manifest"]),
            _decode_ownership(prepared["ownership"]),
        ),
        validate_reference_family_validation_receipt(pin_by_field["validation"]),
        **{
            key: field_value
            for key, field_value in pin_by_field.items()
            if key not in {"prepared", "validation", "runtime_roles"}
        },
        runtime_roles=tuple(pin_by_field["runtime_roles"]),
    )


def _decode_recipe(document):
    if type(document) is not dict or set(document) != {
        "source_kind",
        "source_pin",
        "binding_coordinates",
        "selection_policy",
    }:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    if document["source_kind"] == "fhir_typed":
        from process.network_fhir_typed_capture import TypedGraphAnchor

        pin_fields_by_name = dict(_closed_document(document["source_pin"], PinnedTypedFHIRMembershipSource))
        pin_fields_by_name["anchor"] = TypedGraphAnchor(
            **_closed_document(pin_fields_by_name["anchor"], TypedGraphAnchor)
        )
        source_pin = PinnedTypedFHIRMembershipSource(**pin_fields_by_name)
    elif document["source_kind"] == "fhir":
        source_pin = _decode_fhir_pin(document["source_pin"])
    elif document["source_kind"] == "aca":
        source_pin = _decode_aca_pin(document["source_pin"])
    elif document["source_kind"] == "ptg":
        pin_by_field = dict(_closed_document(document["source_pin"], PinnedPTGOfficeMembershipSource))
        pin_by_field["coordinates"] = RegistryNetworkSourceCoordinates(
            **_closed_document(pin_by_field["coordinates"], RegistryNetworkSourceCoordinates)
        )
        source_pin = PinnedPTGOfficeMembershipSource(**pin_by_field)
    else:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    coordinates = RegistryNetworkSourceCoordinates(
        **_closed_document(document["binding_coordinates"], RegistryNetworkSourceCoordinates)
    )
    return RegistrySourceMembershipRecipe(source_pin, coordinates, document["selection_policy"])


def _decode_fhir_pin(document):
    """Decode closed flat source pins and at most one independently retained successor."""
    if type(document) is not dict:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    values_by_name = dict(document)
    successor = values_by_name.pop("retained_successor", None)
    if "retained_successor" in document:
        if type(successor) is not dict or "retained_successor" in successor:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        successor = _decode_flat_fhir_pin(successor)
    return PinnedFHIRMembershipSource(**_decode_flat_fhir_pin_values(values_by_name), retained_successor=successor)


def _decode_flat_fhir_pin(document):
    return PinnedFHIRMembershipSource(**_decode_flat_fhir_pin_values(document))


def _decode_flat_fhir_pin_values(document):
    all_names = {field.name for field in fields(PinnedFHIRMembershipSource)} - {"retained_successor"}
    optional_fields = {
        "retained_epoch",
        "retained_capture",
        "source_witness_capture",
        "source_witness_anchor",
        "edition_id",
    }
    named = all_names - optional_fields
    legacy = named - {"resource_relation"} | {"resource_table_oid"}
    retired_fields = {"custody_owner_role", "custody_runtime_roles", "custody_proof_sha256", "custody_catalog_sha256"}
    groups = (set(), {"retained_epoch"}, {"retained_capture"}, {"source_witness_capture", "source_witness_anchor"})
    shapes = [
        base | group | edition for base in (named, legacy) for group in groups for edition in (set(), {"edition_id"})
    ] + [legacy | retired_fields]
    if type(document) is not dict or set(document) not in shapes:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    values_by_name = {
        key: field_value for key, field_value in document.items() if key not in retired_fields | {"resource_table_oid"}
    }
    if (
        "resource_table_oid" in document
        or type(document.get("retained_epoch")) is dict
        and "schema_oid" in document["retained_epoch"]
    ):
        # The already published edition remains opaque; only logical names/data are decoded.
        values_by_name["edition_id"] = hashlib.sha256(
            json.dumps(document, sort_keys=True, separators=(",", ":")).encode()
        ).hexdigest()
    if "resource_table_oid" in document:
        if type(document["resource_table_oid"]) is not int or not 0 < document["resource_table_oid"] < 2**32:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        values_by_name["resource_relation"] = {
            "schema": document["schema_name"],
            "name": "provider_directory_dataset_resource",
        }
    elif type(values_by_name["resource_relation"]) is not dict:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    if "retained_epoch" in document:
        if type(document["retained_epoch"]) is not dict:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        values_by_name["retained_epoch"] = validate_retained_cms_fhir_source_epoch(document["retained_epoch"])
    if "retained_capture" in document:
        from process.network_fhir_retained_capture_contract import validate_capture

        if type(document["retained_capture"]) is not dict:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        values_by_name["retained_capture"] = validate_capture(document["retained_capture"])
    if "source_witness_capture" in document:
        if type(document["source_witness_capture"]) is not dict:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        values_by_name["source_witness_capture"] = validate_source_witness_capture(document["source_witness_capture"])
        values_by_name["source_witness_anchor"] = SourceWitnessCaptureAnchor(
            **_closed_document(document["source_witness_anchor"], SourceWitnessCaptureAnchor)
        )
    return values_by_name


def _recipe_document(recipe):
    if type(recipe) is not RegistrySourceMembershipRecipe:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    _validate_recipe_scope(recipe)
    if type(recipe.source_pin) is PinnedTypedFHIRMembershipSource:
        source_kind, source_by_field = "fhir_typed", asdict(recipe.source_pin)
    elif type(recipe.source_pin) is PinnedFHIRMembershipSource:
        source_kind, source_by_field = "fhir", recipe.source_pin.coordinates
    elif type(recipe.source_pin) is PinnedPTGOfficeMembershipSource:
        source_kind, source_by_field = "ptg", asdict(recipe.source_pin)
    else:
        ownership_document = asdict(recipe.source_pin.prepared.ownership)
        ownership_document["dataset_id"] = str(recipe.source_pin.prepared.ownership.dataset_id)
        source_kind = "aca"
        source_by_field = {
            **recipe.source_pin.coordinates,
            "prepared": {"manifest": recipe.source_pin.prepared.manifest.as_dict(), "ownership": ownership_document},
            "validation": recipe.source_pin.validation.as_dict(),
        }
    if type(recipe.source_pin) is PinnedACAMembershipSource and recipe.source_pin.retained_successor is not None:
        successor = recipe.source_pin.retained_successor
        source_by_field["retained_successor"] = _recipe_document(
            RegistrySourceMembershipRecipe(
                successor,
                RegistryNetworkSourceCoordinates(
                    **{
                        key: coordinate_value
                        for key, coordinate_value in aca_source_descriptor(successor)["network_bindings"].items()
                        if key != "source_key_kind"
                    }
                ),
            )
        )["source_pin"]
    if type(recipe.source_pin) is PinnedACAMembershipSource and recipe.source_pin.retained_epoch is not None:
        source_by_field["retained_epoch"] = recipe.source_pin.retained_epoch.as_dict()
    return {
        "source_kind": source_kind,
        "source_pin": source_by_field,
        "binding_coordinates": asdict(recipe.binding_coordinates),
        "selection_policy": recipe.selection_policy,
    }


def _canonical_json(document):
    encoded = json.dumps(document, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False)
    if len(encoded.encode()) > MAX_SOURCE_RECIPE_BYTES:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    return encoded


def _unique_document(pairs):
    document_by_key = {}
    for key, value in pairs:
        if key in document_by_key:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        document_by_key[key] = value
    return document_by_key


def _decode_document(value):
    if type(value) is str:
        if len(value.encode()) > MAX_SOURCE_RECIPE_INPUT_BYTES:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        return json.loads(value, object_pairs_hook=_unique_document, parse_constant=_reject_json_constant)
    return value


def _reject_json_constant(value):
    raise RegistrySourceRecipeError("registry_source_recipes_invalid")


def _is_initial_batch_prepared(prepared):
    """Recognize only the closed initial ACA capture, never an arbitrary shared object."""
    if type(prepared) is not dict or set(prepared) != {"manifest", "ownership"}:
        return False
    manifest = prepared["manifest"]
    return (
        type(manifest) is dict
        and type(manifest.get("source_metadata")) is dict
        and "registry_aca_initial_batch" in manifest["source_metadata"]
    )


def _pack_recipe_documents(documents):
    """Share only identical initial-batch capture and validation within this array."""
    ordinary, batches_by_capture = [], {}
    for document in sorted(documents, key=_canonical_json):
        pin = document["source_pin"]
        if document["source_kind"] != "aca" or not _is_initial_batch_prepared(pin["prepared"]):
            ordinary.append(document)
            continue
        capture_map = {name: pin[name] for name in ("prepared", "validation")}
        key = _canonical_json(capture_map)
        if key not in batches_by_capture:
            batches_by_capture[key] = {"source_kind": _ACA_BATCH_KIND, **capture_map, "recipes": []}
        batches_by_capture[key]["recipes"].append(
            {**document, "source_pin": {name: value for name, value in pin.items() if name not in capture_map}}
        )
    return sorted([*ordinary, *batches_by_capture.values()], key=_canonical_json)


def _batch_recipe_documents(document):
    """Restore shallow views of one bounded capture; reject overrides and nesting."""
    if (
        set(document) != {"source_kind", "prepared", "validation", "recipes"}
        or not _is_initial_batch_prepared(document["prepared"])
        or type(document["validation"]) is not dict
        or type(document["recipes"]) is not list
        or not 1 <= len(document["recipes"]) <= MAX_SOURCE_RECIPES
    ):
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    for child in document["recipes"]:
        if (
            type(child) is not dict
            or child.get("source_kind") != "aca"
            or type(child.get("source_pin")) is not dict
            or {"prepared", "validation"} & child["source_pin"].keys()
        ):
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        yield {
            **child,
            "source_pin": {
                **child["source_pin"],
                "prepared": document["prepared"],
                "validation": document["validation"],
            },
        }


def _decode_wire_recipes(documents):
    """Bound logical recipes before expanding any shared capture or returning output."""
    count = 0
    for document in documents:
        if type(document) is not dict:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        children = document.get("recipes") if document.get("source_kind") == _ACA_BATCH_KIND else None
        if document.get("source_kind") == _ACA_BATCH_KIND and type(children) is not list:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        count += len(children) if children is not None else 1
        if count > MAX_SOURCE_RECIPES:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    recipes = []
    for document in documents:
        children = _batch_recipe_documents(document) if document.get("source_kind") == _ACA_BATCH_KIND else (document,)
        recipes.extend(_decode_recipe(child) for child in children)
    return tuple(recipes)


def canonical_registry_source_recipes(recipes) -> str:
    """Validate and sort a complete recipe batch into bounded canonical UTF-8 JSON."""
    try:
        if type(recipes) not in {tuple, list} or len(recipes) > MAX_SOURCE_RECIPES:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        documents = _pack_recipe_documents([_recipe_document(recipe) for recipe in recipes])
        canonical = _canonical_json(documents)
        decoded_recipes = _decode_wire_recipes(json.loads(canonical))
        identities = [(type(recipe.source_pin).__name__, recipe.source_pin.generation_id) for recipe in decoded_recipes]
        if len(set(identities)) != len(identities):
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        return canonical
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, ReferenceFamilyArchiveError:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid") from None


def decode_registry_source_recipes(value) -> tuple[RegistrySourceMembershipRecipe, ...]:
    """Reject a malformed whole batch; no partial recipe output is returned."""
    try:
        documents = _decode_document(value)
        if type(documents) is not list or len(documents) > MAX_SOURCE_RECIPES:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        _canonical_json(documents)
        recipes = _decode_wire_recipes(documents)
        canonical = canonical_registry_source_recipes(recipes)
        return _decode_wire_recipes(json.loads(canonical))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, ReferenceFamilyArchiveError:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid") from None


def canonical_decoded_registry_source_recipes(document):
    """Check complete old/new recipe syntax while retaining original canonical transport bytes."""
    raw = _decode_document(document)
    decoded = decode_registry_source_recipes(raw)
    encoded = _canonical_json(raw)
    if canonical_registry_source_recipes(decoded) != encoded:

        def is_projected_legacy(recipe_document):
            """Recognize a validated historical pin whose physical fields were projected out."""
            pin = recipe_document.get("source_pin") if type(recipe_document) is dict else None
            return type(pin) is dict and (
                "resource_table_oid" in pin
                or type(pin.get("retained_epoch")) is dict
                and "schema_oid" in pin["retained_epoch"]
            )

        if not any(is_projected_legacy(recipe_document) for recipe_document in raw):
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        if raw != sorted(raw, key=_canonical_json):
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        # Every item has already passed its exact closed decoder above.
    return encoded


def registry_source_recipes_sha256(canonical: str) -> str:
    """Hash only the exact canonical representation, never a permissive JSON input."""
    if (
        type(canonical) is not str
        or canonical_registry_source_recipes(decode_registry_source_recipes(canonical)) != canonical
    ):
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    return hashlib.sha256(canonical.encode()).hexdigest()


def verify_registry_source_recipes(candidate: Mapping) -> tuple[RegistrySourceMembershipRecipe, ...]:
    """Bind private recipes to their digest in the public source generation map."""
    try:
        if not isinstance(candidate, Mapping):
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        recipes = decode_registry_source_recipes(candidate["source_recipes_json"])
        generations = _decode_document(candidate["source_generations"])
        if type(generations) is not dict:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        raw = _decode_document(candidate["source_recipes_json"])
        retained_id = generations.get(RECIPE_DIGEST_KEY)
        canonical = canonical_decoded_registry_source_recipes(raw)
        expected = hashlib.sha256(canonical.encode()).hexdigest() if recipes else None
        if (recipes and retained_id != expected) or (not recipes and RECIPE_DIGEST_KEY in generations):
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        return recipes
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid") from None


def _decode_source_inventory_recipes(entries):
    """Decode already folded singleton entries without applying the legacy whole-array cap."""
    return tuple(decode_registry_source_recipes("[" + entry.recipe_json + "]")[0] for entry in entries)


def _source_inventory_recipe_digest(recipes):
    """Preserve the legacy SHA state over exact array bytes without materializing a large array."""
    from process.registry_source_inventory_preparation import SourceInventoryPreparation

    preparation = SourceInventoryPreparation(recipes)
    digest = hashlib.sha256(b"[")
    for index, recipe in enumerate(preparation.recipes):
        if index:
            digest.update(b",")
        digest.update(canonical_registry_source_recipes((recipe,))[1:-1].encode())
    digest.update(b"]")
    if digest.hexdigest() != preparation.recipe_root.recipes_sha256:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    return digest


async def _retained_inventory_recipes(connection, candidate, control_schema, *, api_read=False):
    from process.network_membership_copy import MembershipCopyTarget
    from process.registry_source_inventory_store import load_source_inventory

    target = MembershipCopyTarget(
        *(str(candidate[field]) for field in ("dataset_id", "schema_id", "producer_id", "candidate_id", "schema_name"))
    )
    if type(api_read) is not bool:
        raise RegistrySourceRecipeError("registry_source_recipes_unavailable")
    _, entries = await load_source_inventory(connection, target, candidate, control_schema=control_schema)
    return _decode_source_inventory_recipes(entries)


async def resolve_retained_registry_source_recipes(connection, pinned_manifest, *, control_schema=None, api_read=False):
    """Read exact retained manifest and complete inventory data in the pinned transaction."""
    try:
        if (
            type(api_read) is not bool
            or type(pinned_manifest) is not PinnedNetworkServingManifest
            or not connection.is_in_transaction()
            or await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}
        ):
            raise RegistrySourceRecipeError("registry_source_recipes_unavailable")
        if api_read and await connection.fetchval("SHOW transaction_read_only") != "on":
            raise RegistrySourceRecipeError("registry_source_recipes_unavailable")
        current = await resolve_network_serving_manifest(
            connection,
            generation_id=pinned_manifest.generation_id,
            control_schema=control_schema,
            **({"api_read": True} if api_read else {}),
        )
        if current != pinned_manifest:
            raise RegistrySourceRecipeError("registry_source_recipes_unavailable")
        namespace = _control_namespace(control_schema)
        candidate = await connection.fetchrow(
            f"""SELECT dataset_id,schema_id,producer_id,candidate_id,schema_name,state,
              CASE WHEN octet_length(source_recipes_json::text)<=$2
              THEN source_recipes_json::text END AS source_recipes_json,source_generations::text
              FROM {namespace}.network_membership_candidate WHERE candidate_id=$1 AND state='published'""",
            UUID(pinned_manifest.candidate_id),
            MAX_SOURCE_RECIPE_INPUT_BYTES,
        )
        if candidate is None:
            raise RegistrySourceRecipeError("registry_source_recipes_unavailable")
        candidate_by_field = dict(candidate)
        from process.registry_source_inventory_reference import verify_source_recipe_metadata

        if verify_source_recipe_metadata(candidate_by_field).inventory is None:
            return verify_registry_source_recipes(candidate_by_field)
        if (
            str(candidate_by_field["candidate_id"]) != current.candidate_id
            or candidate_by_field["schema_name"] != current.schema_name
            or candidate_by_field["state"] != "published"
            or _decode_document(candidate_by_field["source_generations"]) != current.source_generations
        ):
            raise RegistrySourceRecipeError("registry_source_recipes_unavailable")
        return await _retained_inventory_recipes(
            connection, candidate_by_field, namespace[1:-1], **({"api_read": True} if api_read else {})
        )
    except (ValueError, TypeError, KeyError, UnicodeError, RecursionError, asyncpg.PostgresError) as error:
        raise RegistrySourceRecipeError("registry_source_recipes_unavailable") from error
