# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Closed candidate-local inventory references; metadata alone grants no read authority."""

from collections.abc import Mapping
from dataclasses import asdict, dataclass

from db.registry_source_inventory_schema import INVENTORY_COMPONENT, INVENTORY_TABLE, MAX_REFERENCE_BYTES
from process.registry_source_inventory_contract import (
    RegistrySourceInventoryError,
    SourceInventoryRoot,
    _canonical,
    _document,
)

INVENTORY_DIGEST_KEY = INVENTORY_COMPONENT


@dataclass(frozen=True)
class SourceInventoryReference:
    root: SourceInventoryRoot

    def __post_init__(self):
        if type(self.root) is not SourceInventoryRoot:
            raise RegistrySourceInventoryError("registry_source_inventory_reference_invalid")

    def as_dict(self):
        """Keep physical table selection fixed while retaining bounded commitments."""
        return {
            "component": INVENTORY_COMPONENT,
            "revision": 1,
            "table_name": INVENTORY_TABLE,
            "root": asdict(self.root),
        }


def encode_source_inventory_reference(reference):
    """Serialize a reference without transferring recipes or claiming native custody."""
    if type(reference) is not SourceInventoryReference:
        raise RegistrySourceInventoryError("registry_source_inventory_reference_invalid")
    encoded = _canonical(reference.as_dict())
    if len(encoded.encode()) > MAX_REFERENCE_BYTES:
        raise RegistrySourceInventoryError("registry_source_inventory_reference_invalid")
    return encoded


def decode_source_inventory_reference(value):
    """Reject alternate tables, unknown fields and malformed roots before native lookup."""
    try:
        document = _document(value, MAX_REFERENCE_BYTES)
        if (
            set(document) != {"component", "revision", "table_name", "root"}
            or document["component"] != INVENTORY_COMPONENT
            or type(document["revision"]) is not int
            or document["revision"] != 1
            or document["table_name"] != INVENTORY_TABLE
            or type(document["root"]) is not dict
        ):
            raise ValueError
        return SourceInventoryReference(SourceInventoryRoot(**document["root"]))
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_reference_invalid") from None


@dataclass(frozen=True)
class SourceRecipeMetadata:
    """Registration commitments only; neither payload access nor source custody."""

    recipe_count: int
    recipes_sha256: str | None
    inventory: SourceInventoryReference | None


def verify_source_recipe_metadata(candidate):
    """Check either retained format without hydrating a referenced inventory."""
    from process.registry_source_inventory_contract import source_inventory_root
    from process.registry_source_recipe_store import (
        RECIPE_DIGEST_KEY,
        _decode_document,
        verify_registry_source_recipes,
    )

    try:
        if not isinstance(candidate, Mapping):
            raise ValueError
        document = _decode_document(candidate["source_recipes_json"])
        if type(document) is not dict:
            recipes = verify_registry_source_recipes(candidate)
            generations = _decode_document(candidate["source_generations"])
            if INVENTORY_DIGEST_KEY in generations:
                raise ValueError
            return SourceRecipeMetadata(len(recipes), generations.get(RECIPE_DIGEST_KEY), None)
        reference = decode_source_inventory_reference(document)
        generations = _decode_document(candidate["source_generations"])
        if type(generations) is not dict:
            raise ValueError
        count = reference.root.entry_count
        digest = reference.root.recipes_sha256 if count else None
        if (
            generations.get(INVENTORY_DIGEST_KEY) != reference.root.entries_sha256
            or count
            and generations.get(RECIPE_DIGEST_KEY) != digest
            or not count
            and (RECIPE_DIGEST_KEY in generations or reference.root != source_inventory_root(()))
        ):
            raise ValueError
        return SourceRecipeMetadata(count, digest, reference)
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_metadata_invalid") from None


def source_inventory_heaps(candidate):
    """These two heaps are mandatory even for empty referenced inventories."""
    metadata = verify_source_recipe_metadata(
        {**candidate, "source_recipes_json": candidate.get("source_recipes_json", [])}
    )
    if metadata.inventory is None:
        return frozenset()
    from process.network_provider_only_projection import TABLE_NAME

    return frozenset((INVENTORY_TABLE, TABLE_NAME))
