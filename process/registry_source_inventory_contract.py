# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Bounded immutable source metadata commitments, without source or physical custody authority.

This preparatory contract does not install storage, admit sources, or change serving
readers. Existing source codecs remain authoritative for individual metadata shapes.
"""

import hashlib
import json
import re
from dataclasses import asdict, dataclass

from process.registry_source_contribution_store import (
    MAX_RECEIPT_BYTES,
    validate_source_contribution_descriptor,
)
from process.registry_source_office_summary_v3 import (
    MAX_V3_RECEIPT_BYTES,
    validate_derivation,
)
from process.registry_source_recipe_store import (
    canonical_registry_source_recipes,
    decode_registry_source_recipes,
)
from process.registry_verified_office_derivation import validate_verified_office_derivation_v2

MAX_INVENTORY_ENTRIES = 1000
MAX_INVENTORY_BYTES = 64 * 1024 * 1024
MAX_INVENTORY_PAGE_BYTES = 1024 * 1024
_DOMAIN = b"registry-source-inventory:v1\0"


class RegistrySourceInventoryError(ValueError):
    """The inventory is malformed, exceeds its explicit budget, or differs from its commitment."""


def _canonical(document):
    return json.dumps(
        document,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    )


def _unique_object(pairs):
    field_by_key = {}
    for key, value in pairs:
        if key in field_by_key:
            raise ValueError
        field_by_key[key] = value
    return field_by_key


def _reject_constant(value):
    raise ValueError


def _document(value, maximum):
    if type(value) is str:
        if len(value.encode()) > maximum:
            raise ValueError
        value = json.loads(value, object_pairs_hook=_unique_object, parse_constant=_reject_constant)
    if type(value) is not dict or len(_canonical(value).encode()) > maximum:
        raise ValueError
    return value


def _contribution(value, recipe_sha256):
    if value is None:
        return None
    descriptor = _document(value, MAX_RECEIPT_BYTES)
    descriptor = validate_source_contribution_descriptor(descriptor)
    if descriptor["recipe_sha256"] != recipe_sha256:
        raise ValueError
    return _canonical(descriptor)


def _derivation(value, recipe_sha256):
    if value is None:
        return None
    document = _document(value, MAX_V3_RECEIPT_BYTES)
    derivation = (
        validate_verified_office_derivation_v2(document)
        if "derivation_version" in document
        else validate_derivation(document)
    )
    if derivation["recipe_sha256"] != recipe_sha256:
        raise ValueError
    return _canonical(derivation)


@dataclass(frozen=True)
class SourceInventoryEntry:
    ordinal: int
    recipe_json: str
    contribution_json: str | None = None
    office_derivation_json: str | None = None

    def __post_init__(self):
        encode_source_inventory_entry(self)


def make_source_inventory_entry(ordinal, recipe, *, contribution=None, office_derivation=None):
    """Retain exact existing singleton recipe bytes and independently validated optional metadata."""
    try:
        singleton = canonical_registry_source_recipes((recipe,))
        recipe_sha256 = hashlib.sha256(singleton.encode()).hexdigest()
        return SourceInventoryEntry(
            ordinal,
            singleton[1:-1],
            _contribution(contribution, recipe_sha256),
            _derivation(office_derivation, recipe_sha256),
        )
    except RegistrySourceInventoryError:
        raise
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_invalid") from None


def _entry_details(entry):
    if type(entry) is not SourceInventoryEntry or type(entry.ordinal) is not int or entry.ordinal < 1:
        raise ValueError
    if entry.ordinal > MAX_INVENTORY_ENTRIES:
        raise RegistrySourceInventoryError("registry_source_inventory_unsupported_bound")
    if type(entry.recipe_json) is not str:
        raise ValueError
    singleton = "[" + entry.recipe_json + "]"
    (recipe,) = decode_registry_source_recipes(singleton)
    if canonical_registry_source_recipes((recipe,)) != singleton:
        raise ValueError
    recipe_sha256 = hashlib.sha256(singleton.encode()).hexdigest()
    if (
        _contribution(entry.contribution_json, recipe_sha256) != entry.contribution_json
        or _derivation(entry.office_derivation_json, recipe_sha256) != entry.office_derivation_json
    ):
        raise ValueError
    encoded = _canonical(asdict(entry)).encode()
    if len(encoded) + 2 > MAX_INVENTORY_PAGE_BYTES:
        raise RegistrySourceInventoryError("registry_source_inventory_unsupported_bound")
    return encoded, (type(recipe.source_pin).__name__, recipe.source_pin.generation_id)


def _checked_entry(entry):
    try:
        return _entry_details(entry)
    except RegistrySourceInventoryError:
        raise
    except ValueError, TypeError, KeyError, UnicodeError, RecursionError:
        raise RegistrySourceInventoryError("registry_source_inventory_invalid") from None


def encode_source_inventory_entry(entry):
    """Encode one closed immutable row; the bytes confer neither admission nor custody."""
    return _checked_entry(entry)[0]


@dataclass(frozen=True)
class SourceInventoryRoot:
    revision: int
    entry_count: int
    contribution_count: int
    derivation_count: int
    encoded_bytes: int
    recipe_bytes: int
    recipes_sha256: str
    entries_sha256: str

    def __post_init__(self):
        if (
            type(self.revision) is not int
            or self.revision != 1
            or type(self.entry_count) is not int
            or not 0 <= self.entry_count <= MAX_INVENTORY_ENTRIES
            or any(
                type(count) is not int or not 0 <= count <= self.entry_count
                for count in (self.contribution_count, self.derivation_count)
            )
            or type(self.encoded_bytes) is not int
            or not 2 <= self.encoded_bytes <= MAX_INVENTORY_BYTES
            or type(self.recipe_bytes) is not int
            or not 2 <= self.recipe_bytes <= self.encoded_bytes
            or any(
                type(digest) is not str or re.fullmatch(r"[0-9a-f]{64}", digest) is None
                for digest in (self.recipes_sha256, self.entries_sha256)
            )
        ):
            raise RegistrySourceInventoryError("registry_source_inventory_invalid")


def _ordered_row(entry, expected, previous, identities):
    encoded, identity = _checked_entry(entry)
    if entry.ordinal != expected or (previous is not None and entry.recipe_json <= previous) or identity in identities:
        raise RegistrySourceInventoryError("registry_source_inventory_invalid")
    identities.add(identity)
    return encoded


class SourceInventoryAccumulator:
    """Fold synchronous or asynchronous pages using one row and at most 1,000 source identities.

    An invalid entry poisons the fold. Finishing is idempotent and closes it to
    further entries; neither a partial fold nor its digest establishes custody.
    """

    def __init__(self):
        self._recipe_digest = hashlib.sha256(b"[")
        self._entry_digest = hashlib.sha256(_DOMAIN + b"[")
        self._count = self._contribution_count = self._derivation_count = 0
        self._encoded_bytes = self._recipe_bytes = 2
        self._previous_recipe, self._identities = None, set()
        self._has_failed = False
        self._finished_root = None

    def accept(self, entry):
        """Validate and commit exactly one next entry, retaining no entry or page collection."""
        if self._has_failed or self._finished_root is not None:
            raise RegistrySourceInventoryError("registry_source_inventory_invalid")
        try:
            encoded = _ordered_row(entry, self._count + 1, self._previous_recipe, self._identities)
            separator = b"," if self._count else b""
            recipe = entry.recipe_json.encode()
            encoded_bytes = self._encoded_bytes + len(separator) + len(encoded)
            if encoded_bytes > MAX_INVENTORY_BYTES:
                raise RegistrySourceInventoryError("registry_source_inventory_unsupported_bound")
        except RegistrySourceInventoryError:
            self._has_failed = True
            raise
        self._entry_digest.update(separator + encoded)
        self._recipe_digest.update(separator + recipe)
        self._count += 1
        self._contribution_count += entry.contribution_json is not None
        self._derivation_count += entry.office_derivation_json is not None
        self._encoded_bytes = encoded_bytes
        self._recipe_bytes += len(separator) + len(recipe)
        self._previous_recipe = entry.recipe_json

    def finish(self):
        """Close a successful fold and return its constant-size commitment."""
        if self._has_failed:
            raise RegistrySourceInventoryError("registry_source_inventory_invalid")
        if self._finished_root is None:
            recipe_digest, entry_digest = self._recipe_digest.copy(), self._entry_digest.copy()
            recipe_digest.update(b"]")
            entry_digest.update(b"]")
            self._finished_root = SourceInventoryRoot(
                1,
                self._count,
                self._contribution_count,
                self._derivation_count,
                self._encoded_bytes,
                self._recipe_bytes,
                recipe_digest.hexdigest(),
                entry_digest.hexdigest(),
            )
        return self._finished_root


def source_inventory_root(entries):
    """Fold a synchronous stream; async readers can call the same accumulator between page awaits.

    The recipe digest matches the existing canonical array wherever the legacy
    complete-batch codec accepts it. Page boundaries never enter either hash.
    """
    accumulator = SourceInventoryAccumulator()
    for entry in entries:
        accumulator.accept(entry)
    return accumulator.finish()


def encode_source_inventory_root(root):
    """Encode a constant-size metadata commitment, with no physical or authorization claims."""
    if type(root) is not SourceInventoryRoot:
        raise RegistrySourceInventoryError("registry_source_inventory_invalid")
    return _canonical(asdict(root)).encode()


def verify_source_inventory_root(entries, expected_root):
    """Reject incomplete or substituted streams against an independently retained root."""
    if type(expected_root) is not SourceInventoryRoot or source_inventory_root(entries) != expected_root:
        raise RegistrySourceInventoryError("registry_source_inventory_invalid")
    return expected_root


@dataclass(frozen=True)
class SourceInventoryPage:
    entries: tuple[SourceInventoryEntry, ...]
    payload: bytes
    next_after_ordinal: int | None


def source_inventory_page(entries, *, after_ordinal, row_limit, byte_limit):
    """Assemble a keyset result plus one lookahead; the exact JSON-list delimiters count toward bytes.

    Callers supply rows after the cursor in canonical ordinal order. Lookahead
    is consumed: requery from the returned cursor, never continue the input
    iterator. This is not a storage reader or a completeness proof; verify the
    final stream/root too.
    """
    if (
        type(after_ordinal) is not int
        or not 0 <= after_ordinal <= MAX_INVENTORY_ENTRIES
        or type(row_limit) is not int
        or not 1 <= row_limit <= MAX_INVENTORY_ENTRIES
        or type(byte_limit) is not int
        or not 2 <= byte_limit <= MAX_INVENTORY_PAGE_BYTES
    ):
        raise RegistrySourceInventoryError("registry_source_inventory_invalid")
    selected, encoded_rows, identities = [], [], set()
    encoded_bytes, previous, has_more = 2, None, False
    for entry in entries:
        encoded = _ordered_row(entry, after_ordinal + len(selected) + 1, previous, identities)
        required_bytes = len(encoded) + bool(selected)
        if len(selected) == row_limit or encoded_bytes + required_bytes > byte_limit:
            if not selected:
                raise RegistrySourceInventoryError("registry_source_inventory_page_too_small")
            has_more = True
            break
        selected.append(entry)
        encoded_rows.append(encoded)
        encoded_bytes += required_bytes
        previous = entry.recipe_json
    return SourceInventoryPage(
        tuple(selected),
        b"[" + b",".join(encoded_rows) + b"]",
        selected[-1].ordinal if has_more else None,
    )
