# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Resolve family-block data layouts through schema-qualified relation names."""

import hmac
from collections import OrderedDict
from dataclasses import dataclass

from process.custom_import.definition import CustomImportDefinition
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.query_projection_binding import QueryProjectionUnavailable, _digest
from process.custom_import.runner_types import CandidateRunnerError
from process.custom_import.storage_layout import snapshot_schema

_BINDING_CACHE_LIMIT = 64
_BINDING_CACHE: OrderedDict[tuple, "FamilyBlockBinding"] = OrderedDict()


class FamilyBlockUnavailable(CandidateRunnerError):
    """A required data layout or snapshot scope differs."""


@dataclass(frozen=True)
class FamilyBlockBinding:
    family_id: int
    layout: FamilyBlockLayout
    tables_by_role: object

    def manifest(self, row):
        """Validate a stored content manifest with this canonical data layout."""
        return self.layout.manifest(row)


def binding_cache_key(binding):
    """Identify one immutable table binding for bounded metadata reuse."""
    return (
        binding.family_id,
        binding.tables_by_role["family"].schema,
        binding.layout.definition.digest,
        tuple(sorted(binding.layout.collection_slots.items())),
    )


def _lookup_key(family_id, definition, collection_slots, schema_name):
    default_schema = snapshot_schema(family_id)
    if type(definition) is not CustomImportDefinition:
        raise ValueError("family layout requires a parsed definition")
    resolved_schema = default_schema if schema_name is None else schema_name
    if type(resolved_schema) is not str or not resolved_schema or "\x00" in resolved_schema:
        raise ValueError("snapshot schema name is invalid")
    slots = tuple(sorted(collection_slots.items()))
    if any(type(name) is not str or type(slot) is not int or not 0 < slot < 2**31 for name, slot in slots):
        raise ValueError("family layout collection slots differ")
    return (family_id, resolved_schema, definition.digest, slots)


def checked_binding(family_id, definition, collection_slots, *, schema_name=None):
    """Construct or reuse immutable typed relations without database-specific identities."""
    try:
        key = _lookup_key(family_id, definition, collection_slots, schema_name)
        cached = _BINDING_CACHE.get(key)
        if cached is not None:
            _BINDING_CACHE.move_to_end(key)
            return cached
        layout = FamilyBlockLayout(definition, collection_slots)
        tables = layout.tables(family_id, schema_name=key[1])
        binding = FamilyBlockBinding(family_id, layout, tables)
        _BINDING_CACHE[key] = binding
        if len(_BINDING_CACHE) > _BINDING_CACHE_LIMIT:
            _BINDING_CACHE.popitem(last=False)
        return binding
    except (TypeError, ValueError, KeyError, AttributeError) as error:
        raise FamilyBlockUnavailable("family block data layout differs") from error


def _positive_identity(value):
    if type(value) is not int or not 0 < value < 2**63:
        raise QueryProjectionUnavailable("family block projection identity is invalid")
    return value


def check_projection_manifest(binding, manifest):
    """Bind projection data to its logical snapshot and canonical definition."""
    if type(binding) is not FamilyBlockBinding:
        raise QueryProjectionUnavailable("family block projection data binding is invalid")
    check_projection_scope(manifest, binding.family_id, binding.layout.definition)


def check_projection_scope(manifest, family_id, definition):
    """Validate projection identity after the caller pins compact storage."""
    try:
        if _positive_identity(manifest.family_id) != _positive_identity(family_id) or not hmac.compare_digest(
            _digest(manifest.definition_sha256, "projection definition"),
            bytes.fromhex(definition.digest),
        ):
            raise QueryProjectionUnavailable("family block projection snapshot or definition differs")
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise QueryProjectionUnavailable("family block projection data binding is invalid") from error


async def resolve_pinned_family_block_binding(session, pin, definition, collection_slots):
    """Use the authorized snapshot name without catalog or connection checks."""
    from process.custom_import.read_identity import GenerationSnapshotBinding

    if (
        type(pin) is not GenerationSnapshotBinding
        or pin.legacy_snapshot is not False
        or type(pin.schema_name) is not str
        or not pin.schema_name
    ):
        raise FamilyBlockUnavailable("family block generation binding is missing")
    return checked_binding(pin.family_id, definition, collection_slots, schema_name=pin.schema_name)
