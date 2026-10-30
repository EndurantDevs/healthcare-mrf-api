# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pair complete source metadata without granting SOURCE, mapping or custody authority."""

import hashlib
import json
from copy import deepcopy
from dataclasses import replace

from process.network_fhir_membership_source import (
    FHIR_SOURCE_WITNESS_TRANSFORM_V2,
    PinnedFHIRMembershipSource,
    fhir_physical_source,
    fhir_source_witness_binding_v2,
)
from process.registry_source_contribution_store import (
    FACT_ANCHOR_FIELDS,
    _canonical,
    fhir_fact_transform,
    recipe_contribution_identity,
    validate_source_contribution_descriptor,
    verified_office_fact_identity,
)
from process.registry_source_inventory_contract import (
    MAX_INVENTORY_BYTES,
    RegistrySourceInventoryError,
    SourceInventoryAccumulator,
    encode_source_inventory_entry,
    make_source_inventory_entry,
)
from process.registry_source_inventory_reference import SourceInventoryReference
from process.registry_source_office_summary_v3 import validate_derivation
from process.registry_source_recipe_store import decode_registry_source_recipes
from process.registry_verified_office_derivation import validate_verified_office_derivation_v2

_FACT_FIELDS = ("capture_sha256", "transform_version", "fact_sha256", "fact_rows")


def _bound(byte_count):
    if byte_count > MAX_INVENTORY_BYTES:
        raise RegistrySourceInventoryError("registry_source_inventory_unsupported_bound")


def _recipe_base(recipes):
    """Charge complete recipe-only rows before sorting, then normalize the same canonical bytes."""
    staged, encoded_bytes = [], 2
    for ordinal, recipe in enumerate(recipes, 1):
        entry = make_source_inventory_entry(ordinal, recipe)
        encoded_bytes += len(encode_source_inventory_entry(entry)) + bool(staged)
        _bound(encoded_bytes)
        staged.append(entry)
    accumulator = SourceInventoryAccumulator()
    recipes_by_sha, entries_by_sha, identities = {}, {}, []
    for ordinal, entry in enumerate(sorted(staged, key=lambda row: row.recipe_json), 1):
        entry = replace(entry, ordinal=ordinal)
        singleton = "[" + entry.recipe_json + "]"
        (recipe,) = decode_registry_source_recipes(singleton)
        identity = hashlib.sha256(singleton.encode()).hexdigest()
        if identity in entries_by_sha:
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
        accumulator.accept(entry)
        recipes_by_sha[identity], entries_by_sha[identity] = recipe, entry
        identities.append(identity)
    root = accumulator.finish()
    if root.encoded_bytes != encoded_bytes:
        raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
    return tuple(identities), recipes_by_sha, entries_by_sha, root


def _descriptor(document):
    descriptor = validate_source_contribution_descriptor(document)
    if not FACT_ANCHOR_FIELDS <= descriptor.keys():
        raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
    return descriptor


def _check_fhir_descriptor(recipe, descriptor):
    if descriptor["transform_version"] == FHIR_SOURCE_WITNESS_TRANSFORM_V2:
        identity, capture = verified_office_fact_identity(recipe)
        if descriptor["recipe_sha256"] != identity or descriptor["capture_sha256"] != capture:
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
        return
    identity, capture = recipe_contribution_identity(recipe)
    if (
        descriptor["recipe_sha256"] != identity
        or descriptor["capture_sha256"] != capture
        or descriptor["transform_version"] != fhir_fact_transform(recipe.source_pin)
    ):
        raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")


def _check_derivation(recipe, descriptor, derivation):
    is_fhir = type(recipe.source_pin) is PinnedFHIRMembershipSource
    is_witness = descriptor is not None and descriptor["transform_version"] == FHIR_SOURCE_WITNESS_TRANSFORM_V2
    if "derivation_version" in derivation:
        if (
            not is_fhir
            or not is_witness
            or derivation["source_binding"] != fhir_source_witness_binding_v2(recipe.source_pin)
            or derivation["contribution_sha256"] != hashlib.sha256(_canonical(descriptor).encode()).hexdigest()
            or derivation["mapping_sha256"] != descriptor["mapping_sha256"]
            or any(
                derivation["fact_artifact"][field] != descriptor[field]
                for field in (*_FACT_FIELDS, *FACT_ANCHOR_FIELDS)
            )
            or derivation["input_membership_rows"] != descriptor["membership_rows"]
        ):
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
        return
    if is_witness:
        raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
    physical = fhir_physical_source(recipe.source_pin) if is_fhir else recipe.source_pin
    if derivation["physical_source_sha256"] != physical.generation_id:
        raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
    if not is_fhir:
        if derivation["mode"] != "raw":
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
        return
    if descriptor is None or derivation["mapping_sha256"] != descriptor["mapping_sha256"]:
        raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
    if derivation["mode"] == "facts":
        artifact = derivation["fact_artifact"]
        if any(
            descriptor[field] is None or artifact[field] != descriptor[field] for field in FACT_ANCHOR_FIELDS
        ) or any(artifact[field] != descriptor[field] for field in _FACT_FIELDS):
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")


class SourceInventoryPreparation:
    """Retain at most 1,000 entry payloads/64 MiB while joining complete metadata across producer awaits.

    Contributions precede their FHIR office derivations. Neither the recipe-only
    properties nor a completed reference proves admission, mappings or custody.
    """

    def __init__(self, recipes):
        try:
            self._identities, self._recipes_by_sha, self._entries_by_sha, self._recipe_root = _recipe_base(recipes)
        except RegistrySourceInventoryError:
            raise
        except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid") from None
        self._recipes = tuple(self._recipes_by_sha[identity] for identity in self._identities)
        self._fhir_identities = {
            identity
            for identity, recipe in self._recipes_by_sha.items()
            if type(recipe.source_pin) is PinnedFHIRMembershipSource
        }
        self._contributions, self._derivations = set(), set()
        self._encoded_bytes = self._recipe_root.encoded_bytes
        self._has_failed, self._finished = False, None

    @property
    def recipes(self):
        """Return a defensive normalized canonical tuple for existing typed producers, without finishing."""
        return deepcopy(self._recipes)

    @property
    def recipe_root(self):
        """Expose the immutable recipe-only commitment, not a complete inventory or custody proof."""
        return self._recipe_root

    def _require_open(self):
        if self._has_failed or self._finished is not None:
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")

    def _identity(self, document):
        if type(document) is not dict or type(document.get("recipe_sha256")) is not str:
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
        identity = document["recipe_sha256"]
        if identity not in self._recipes_by_sha:
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid")
        return identity

    def _commit(self, identity, entry):
        prospective = self._encoded_bytes - len(encode_source_inventory_entry(self._entries_by_sha[identity]))
        prospective += len(encode_source_inventory_entry(entry))
        _bound(prospective)
        self._entries_by_sha[identity], self._encoded_bytes = entry, prospective

    def add_contribution(self, document):
        """Attach one explicitly decoded descriptor to its FHIR recipe before retaining more bytes."""
        self._require_open()
        try:
            identity = self._identity(document)
            if identity not in self._fhir_identities or identity in self._contributions:
                raise ValueError
            descriptor = _descriptor(document)
            recipe = self._recipes_by_sha[identity]
            _check_fhir_descriptor(recipe, descriptor)
            current = self._entries_by_sha[identity]
            entry = make_source_inventory_entry(current.ordinal, recipe, contribution=descriptor)
            self._commit(identity, entry)
            self._contributions.add(identity)
        except RegistrySourceInventoryError:
            self._has_failed = True
            raise
        except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
            self._has_failed = True
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid") from None

    def add_derivation(self, document):
        """Attach one coherent raw/fact derivation for a recipe; rejected rows permanently poison the build."""
        self._require_open()
        try:
            identity = self._identity(document)
            if identity in self._derivations:
                raise ValueError
            recipe, current = self._recipes_by_sha[identity], self._entries_by_sha[identity]
            descriptor = json.loads(current.contribution_json) if current.contribution_json is not None else None
            derivation = (
                validate_verified_office_derivation_v2(document)
                if "derivation_version" in document
                else validate_derivation(document)
            )
            _check_derivation(recipe, descriptor, derivation)
            entry = make_source_inventory_entry(
                current.ordinal, recipe, contribution=descriptor, office_derivation=derivation
            )
            self._commit(identity, entry)
            self._derivations.add(identity)
        except RegistrySourceInventoryError:
            self._has_failed = True
            raise
        except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
            self._has_failed = True
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid") from None

    def finish(self):
        """Close a complete build and return its idempotent immutable reference/payload tuple."""
        if self._finished is not None:
            return self._finished
        self._require_open()
        try:
            if self._contributions != self._fhir_identities or self._derivations != set(self._identities):
                raise ValueError
            accumulator = SourceInventoryAccumulator()
            entries = tuple(self._entries_by_sha[identity] for identity in self._identities)
            for entry in entries:
                accumulator.accept(entry)
            root = accumulator.finish()
            if root.encoded_bytes != self._encoded_bytes:
                raise ValueError
            self._finished = SourceInventoryReference(root), entries
            return self._finished
        except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
            self._has_failed = True
            raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid") from None


def prepare_source_inventory(recipes, contributions, office_derivations):
    """Pair all singleton metadata without changing legacy codecs or asserting SOURCE authority."""
    try:
        preparation = SourceInventoryPreparation(recipes)
        for descriptor in contributions:
            preparation.add_contribution(descriptor)
        for derivation in office_derivations:
            preparation.add_derivation(derivation)
        return preparation.finish()
    except RegistrySourceInventoryError:
        raise
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_preparation_invalid") from None
