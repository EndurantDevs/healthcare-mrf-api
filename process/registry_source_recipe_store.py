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
from process.network_fhir_membership_source import PinnedFHIRMembershipSource
from process.network_fhir_source_epoch import validate_retained_cms_fhir_source_epoch
from process.network_legacy_membership_source import PinnedACAMembershipSource
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


class RegistrySourceRecipeError(ValueError):
    """A closed recipe or retained source proof differs; messages contain no input."""


@dataclass(frozen=True)
class RegistrySourceMembershipRecipe:
    source_pin: PinnedFHIRMembershipSource | PinnedACAMembershipSource | PinnedPTGOfficeMembershipSource
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
    if type(source_pin) is PinnedFHIRMembershipSource:
        expected_scope = ("fhir", source_pin.source_id, source_pin.schema_name, source_pin.dataset_id)
    elif type(source_pin) is PinnedACAMembershipSource:
        ownership = source_pin.prepared.ownership
        expected_scope = ("aca", source_pin.source_id, ownership.schema_name, str(ownership.dataset_id))
        if source_pin.prepared.manifest.importer_id != "mrf" or source_pin.prepared.manifest.source_metadata.get(
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


def _decode_aca_pin(document):
    document = _closed_document(document, PinnedACAMembershipSource)
    prepared = document["prepared"]
    if type(prepared) is not dict or set(prepared) != {"manifest", "ownership"}:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    if type(document["runtime_roles"]) is not list:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    if type(document["validation"]) is not dict or type(document["validation"].get("package_id")) is not str:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    _oid(document["validation"].get("sealed_owner_oid"))
    return PinnedACAMembershipSource(
        ReferenceFamilyPreparedSource(
            validate_reference_family_manifest(prepared["manifest"]),
            _decode_ownership(prepared["ownership"]),
        ),
        validate_reference_family_validation_receipt(document["validation"]),
        **{key: value for key, value in document.items() if key not in {"prepared", "validation", "runtime_roles"}},
        runtime_roles=tuple(document["runtime_roles"]),
    )


def _decode_recipe(document):
    if type(document) is not dict or set(document) != {
        "source_kind",
        "source_pin",
        "binding_coordinates",
        "selection_policy",
    }:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    if document["source_kind"] == "fhir":
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
    """Decode the unchanged legacy pin or the complete retained CMS custody pin."""
    all_names = {field.name for field in fields(PinnedFHIRMembershipSource)}
    custody_names = all_names - {"retained_epoch"}
    legacy_names = {name for name in custody_names if not name.startswith("custody_")}
    epoch_names = legacy_names | {"retained_epoch"}
    if type(document) is not dict or set(document) not in (legacy_names, custody_names, epoch_names):
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    values_by_name = dict(document)
    if set(document) == custody_names:
        if type(document["custody_runtime_roles"]) is not list or document["custody_owner_role"] is None:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        values_by_name["custody_runtime_roles"] = tuple(document["custody_runtime_roles"])
    if set(document) == epoch_names:
        if type(document["retained_epoch"]) is not dict:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        values_by_name["retained_epoch"] = validate_retained_cms_fhir_source_epoch(document["retained_epoch"])
    return PinnedFHIRMembershipSource(**values_by_name)


def _recipe_document(recipe):
    if type(recipe) is not RegistrySourceMembershipRecipe:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid")
    _validate_recipe_scope(recipe)
    if type(recipe.source_pin) is PinnedFHIRMembershipSource:
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


def canonical_registry_source_recipes(recipes) -> str:
    """Validate and sort a complete recipe batch into bounded canonical UTF-8 JSON."""
    try:
        if type(recipes) not in {tuple, list} or len(recipes) > MAX_SOURCE_RECIPES:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        documents = json.loads(_canonical_json([_recipe_document(recipe) for recipe in recipes]))
        decoded_recipes = [_decode_recipe(document) for document in documents]
        identities = [(type(recipe.source_pin).__name__, recipe.source_pin.generation_id) for recipe in decoded_recipes]
        if len(set(identities)) != len(identities):
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        return _canonical_json(sorted(documents, key=_canonical_json))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, ReferenceFamilyArchiveError:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid") from None


def decode_registry_source_recipes(value) -> tuple[RegistrySourceMembershipRecipe, ...]:
    """Reject a malformed whole batch; no partial recipe output is returned."""
    try:
        documents = _decode_document(value)
        if type(documents) is not list or len(documents) > MAX_SOURCE_RECIPES:
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        _canonical_json(documents)
        recipes = tuple(_decode_recipe(document) for document in documents)
        canonical = canonical_registry_source_recipes(recipes)
        return tuple(_decode_recipe(document) for document in json.loads(canonical))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, ReferenceFamilyArchiveError:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid") from None


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
        expected = registry_source_recipes_sha256(canonical_registry_source_recipes(recipes)) if recipes else None
        if (recipes and generations.get(RECIPE_DIGEST_KEY) != expected) or (
            not recipes and RECIPE_DIGEST_KEY in generations
        ):
            raise RegistrySourceRecipeError("registry_source_recipes_invalid")
        return recipes
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceRecipeError("registry_source_recipes_invalid") from None


async def resolve_retained_registry_source_recipes(connection, pinned_manifest, *, control_schema=None):
    """Recheck exact retained manifest and native writer closure without any writes."""
    try:
        if (
            type(pinned_manifest) is not PinnedNetworkServingManifest
            or not connection.is_in_transaction()
            or await connection.fetchval("SHOW transaction_isolation") not in {"repeatable read", "serializable"}
        ):
            raise RegistrySourceRecipeError("registry_source_recipes_unavailable")
        current = await resolve_network_serving_manifest(
            connection,
            generation_id=pinned_manifest.generation_id,
            control_schema=control_schema,
        )
        if current != pinned_manifest:
            raise RegistrySourceRecipeError("registry_source_recipes_unavailable")
        namespace = _control_namespace(control_schema)
        candidate = await connection.fetchrow(
            f"""SELECT CASE WHEN octet_length(source_recipes_json::text)<=$2
              THEN source_recipes_json::text END AS source_recipes_json,source_generations::text
              FROM {namespace}.network_membership_candidate WHERE candidate_id=$1 AND state='published'""",
            UUID(pinned_manifest.candidate_id),
            MAX_SOURCE_RECIPE_INPUT_BYTES,
        )
        if candidate is None:
            raise RegistrySourceRecipeError("registry_source_recipes_unavailable")
        return verify_registry_source_recipes(dict(candidate))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError, asyncpg.PostgresError:
        raise RegistrySourceRecipeError("registry_source_recipes_unavailable") from None
