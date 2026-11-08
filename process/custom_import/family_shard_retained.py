# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Lossless legacy record inputs; the owner must separately prove pinned origin."""

from dataclasses import replace
from types import MappingProxyType

from process.custom_import import runner_codec
from process.custom_import.family_block_records import FamilyBlockError, FamilyRecord, SparseRecord, record_parts
from process.custom_import.runner_types import CandidateRunnerError


def _original_record(definition, collection, revision, key, scalars):
    kind = "root" if collection is None else "child"
    if type(key) is not tuple or len(key) != 2:
        raise FamilyBlockError("retained key evidence differs")
    if collection is not None and (
        revision["canonical_child_key"] != key[0] or bytes(revision["child_key_sha256"]) != key[1]
    ):
        raise FamilyBlockError("retained native child key differs")
    sparse = SparseRecord(
        revision["canonical_payload"],
        key[0],
        bytes(revision["payload_sha256"]),
        key[1],
        None if collection is None else revision["canonical_parent_key"],
        None if collection is None else bytes(revision["parent_key_sha256"]),
    )
    original = FamilyRecord(
        revision[f"{kind}_revision_id"],
        revision["pack_id"],
        revision["source_ordinal"],
        None,
        MappingProxyType(dict(scalars)),
        sparse,
    )
    # Check stored hashes before considering any parsing or compact representation.
    record_parts(definition, collection, original, retained=True)
    return original


def restore_legacy_record(definition, collection, revision, *, key, scalars):
    """Keep canonical bytes compact and every other spelling in the sparse lane.

    ``revision`` and ``key`` are complete original rows from the pinned base;
    ``scalars`` are independently stored native values, never payload projections.
    This verifies byte hashes, not base membership, producer or copy provenance.
    Destination ID allocation and all those owner joins remain mandatory.
    """
    original = _original_record(definition, collection, revision, key, scalars)
    fields = definition.root_fields if collection is None else runner_codec.fields_by_collection(definition)[collection]
    try:
        decoded = runner_codec.payload_values(fields, original.sparse.payload_text, label="retained record")
        compact = replace(original, values_by_field=MappingProxyType(dict(decoded)), sparse=None)
        wire, parts = record_parts(definition, collection, compact, retained=True)
    except CandidateRunnerError, FamilyBlockError, RecursionError, TypeError, ValueError:
        return original
    if wire[3] != original.sparse.payload_text or parts[1] != original.sparse.key_text:
        return original
    if collection is not None:
        parent_by_field = {
            part.root_field: decoded.get(part.child_field)
            for part in definition.collections_by_name[collection].parent_key
        }
        try:
            parent_key = runner_codec.root_key_document(definition, parent_by_field)
        except CandidateRunnerError:
            return original
        if parent_key != original.sparse.parent_key_text:
            return original
    return compact
