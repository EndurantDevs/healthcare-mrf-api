# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Merge complete shard candidate streams into the existing profile reducer."""

import heapq
from collections.abc import Sequence

from process.custom_import import materialization as selection
from process.custom_import.family_block_layout import FamilyBlockLayout
from process.custom_import.family_block_records import record_parts
from process.custom_import.family_shard_output import AllocatedFamily
from process.custom_import.materialization import (
    GenerationIdentity,
    ValidatedWinnerCandidateStream,
    WinnerCandidate,
    WinnerMaterializationError,
)
from process.custom_import.runner_codec import fields_by_collection, payload_values


def _record_values(definition, collection, record):
    if record.sparse is None:
        return record.values_by_field
    fields = definition.root_fields if collection is None else fields_by_collection(definition)[collection]
    return payload_values(fields, record.sparse.payload_text, label="winner retained payload")


def _query_values(definition, collection, record, fields):
    values = _record_values(definition, collection, record)
    return {field: values[field] for field in fields if field in values}


def family_candidates(layout, family, *, check=None):
    """Original root/child contexts; callers still prove native generation membership.

    Read sparse payloads through the existing strict value reader. Independent
    projected scalars cannot stand in for payload values used by old OUTPUT.
    """
    if type(layout) is not FamilyBlockLayout or type(family) is not AllocatedFamily:
        raise WinnerMaterializationError("winner family requires original allocated inputs")
    if check is not None:
        check()
    definition = layout.definition
    root_values = _query_values(definition, None, family.root, definition.query.root_fields)
    yield WinnerCandidate(
        family.entity_binding_id,
        family.family_revision_id,
        family.expected_family_sha256,
        0,
        None,
        None,
        root_values,
    )
    collection = definition.query.child_collection
    scopes = selection._profile_scopes(definition, layout.collection_slots)
    if collection is None or not any(scope.collection_slot for scope in scopes):
        return
    children = family.children_by_collection[collection]()
    try:
        for child in children:
            if check is not None:
                check()
            _wire, parts = record_parts(definition, collection, child, retained=family.retained)
            child_values = _query_values(definition, collection, child, definition.query.child_fields)
            yield WinnerCandidate(
                family.entity_binding_id,
                family.family_revision_id,
                family.expected_family_sha256,
                layout.collection_slots[collection],
                child.revision_id,
                parts[0],
                root_values | child_values,
            )
    finally:
        close = getattr(children, "close", None)
        if close is not None:
            close()


def _profile_inputs(definition, generation, profile_slot, shards, child_collection_slots):
    selection._validated_definition(definition)
    if not isinstance(generation, GenerationIdentity):
        raise WinnerMaterializationError("generation identity is malformed")
    if type(profile_slot) is not int or not 1 <= profile_slot <= len(definition.selection_profiles):
        raise WinnerMaterializationError("winner profile slot is outside the materialization profile range")
    if not isinstance(shards, Sequence) or isinstance(shards, (str, bytes)):
        raise WinnerMaterializationError("winner shards require a bounded sequence of validated streams")
    for shard in shards:
        if not isinstance(shard, ValidatedWinnerCandidateStream):
            raise WinnerMaterializationError("winner shards require validated generation streams")
        if shard.generation != generation:
            raise WinnerMaterializationError("winner shard generation does not match materialization")
    scopes = selection._profile_scopes(definition, child_collection_slots)
    return scopes, selection._winner_candidate_contract(definition, scopes)


def _ordered_shard(shard, profile, scope, contract):
    previous = None
    for raw_candidate in shard.consume():
        candidate = selection._normalize_winner_candidate(raw_candidate, contract)
        if candidate.candidate.context_collection_slot != scope.collection_slot:
            continue
        selection._validate_selection_values(profile, candidate, contract.fields_by_id)
        canonical, digest = selection._context_key(profile, candidate, contract.fields_by_id)
        key = (raw_candidate.entity_binding_id, digest, canonical)
        if previous is not None and key < previous:
            raise WinnerMaterializationError("winner shard candidates are not in entity/context order")
        previous = key
        yield key, raw_candidate


def iter_shard_profile_winners(
    definition,
    *,
    generation,
    profile_slot,
    shards,
    child_collection_slots=None,
):
    """Reduce all raw candidates, retaining one head per shard and one winner.

    Each shard must be caller-validated generation membership, sorted for this
    profile by entity, context digest and exact context text. Supply a fresh
    single-use stream for each profile. No local winner reduction is permitted:
    losing candidates can still carry context-collision evidence.

    Results remain provisional until actual exhaustion, just like the existing
    ordered reducer. The owner must not seal on a prefix or on an interrupted
    iterator. The caller's shard plan bounds the number of simultaneously held
    heads; source rows and all winners are never accumulated here.
    """
    scopes, contract = _profile_inputs(definition, generation, profile_slot, shards, child_collection_slots)
    profile, scope = definition.selection_profiles[profile_slot - 1], scopes[profile_slot - 1]
    ordered_streams = [_ordered_shard(shard, profile, scope, contract) for shard in shards]
    merged = heapq.merge(*ordered_streams, key=lambda item: item[0])
    candidates = ValidatedWinnerCandidateStream(generation, (candidate for _key, candidate in merged))
    yield from selection.iter_ordered_profile_winners(
        definition,
        generation=generation,
        profile_slot=profile_slot,
        candidates=candidates,
        child_collection_slots=child_collection_slots,
    )
