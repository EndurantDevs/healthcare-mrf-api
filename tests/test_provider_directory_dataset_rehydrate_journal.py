# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure checkpoint protocol tests; stored structure is never native authority."""

from copy import deepcopy
from dataclasses import replace

import pytest

from process import provider_directory_dataset_rehydrate_journal as journal
from process.provider_directory_dataset_rehydrate_journal_store import _preserve_history
from process.provider_directory_dataset_rehydrate_types import DatasetRehydrationError
from process.provider_directory_output_snapshot_custody import PreparedOutputReceipt, _operation_digest
from process.provider_directory_output_snapshot_retention import RetainedOutputReceipt, retained_name
from process.provider_directory_output_snapshot_stage import OutputStage
from tests.test_provider_directory_output_snapshot_retention import relation


def _case():
    target_name = "provider_directory_location"
    operation_by_field = dict(
        kind="rehydrate",
        run_id="repair",
        producer="fixture",
        source_pins=[
            dict(
                source_id="source",
                endpoint_id="endpoint",
                dataset_id="dataset",
                acquisition_root_run_id="root",
                dataset_hash="a" * 64,
                resource_hash_contract="legacy_mapped_v1",
                semantic_projection_as_of=None,
                publication_metadata_sha256="b" * 64,
            )
        ],
        resource_types=["Location"],
        source_fence_digest="c" * 64,
    )
    digest = _operation_digest(operation_by_field, (target_name,))
    old, new = relation(target_name, target_name, 101), relation(target_name, "stage", 201)
    stage = OutputStage(old.schema, target_name, "stage", new.oid, digest, old)
    prepared = PreparedOutputReceipt(1, digest, old, new)
    retained = RetainedOutputReceipt(
        1,
        digest,
        old.schema,
        target_name,
        retained_name(old.oid),
        replace(old, relation=retained_name(old.oid)),
        replace(new, relation=target_name),
    )
    journal_value = journal.begin_preparation(
        journal.output_journal({}), operation_by_field, (target_name,), ["Location"]
    )
    return journal_value, stage, prepared, retained


def _sealed():
    value, stage, prepared, retained = _case()
    value = journal.record_stage(value, stage, cursor=["source", "last"], rows=2, receipt=prepared)
    return journal.seal_preparation(value), retained


def test_same_intent_resumes_exact_stages_and_changed_intent_refuses():
    value, stage, _, _ = _case()
    copied = journal.record_stage(value, stage, cursor=["source", "last"], rows=2)
    active = copied["active"]
    assert journal.begin_preparation(copied, active["operation"], active["targets"], active["members"]) == copied
    changed_by_field = {**active["operation"], "run_id": "another"}
    with pytest.raises(DatasetRehydrationError, match="active_conflict"):
        journal.begin_preparation(copied, changed_by_field, active["targets"], active["members"])
    assert value["active"]["stages"] == {}


@pytest.mark.parametrize("mutation", ["oid", "incumbent", "rows", "cursor"])
def test_active_stage_substitution_or_cursor_rewind_refuses(mutation):
    value, stage, _, _ = _case()
    copied = journal.record_stage(value, stage, cursor=["source", "last"], rows=2)
    candidate, cursor_values, rows = stage, ["source", "last"], 2
    if mutation == "oid":
        candidate = replace(stage, oid=202)
    elif mutation == "incumbent":
        candidate = replace(stage, incumbent=replace(stage.incumbent, oid=102))
    elif mutation == "rows":
        rows = 1
    else:
        cursor_values = ["source", "earlier"]
    with pytest.raises(DatasetRehydrationError, match="stage_changed|cursor_changed"):
        journal.record_stage(copied, candidate, cursor=cursor_values, rows=rows)


def test_seal_requires_complete_target_set_and_keeps_immutable_prepared_receipts():
    value, stage, prepared, _ = _case()
    with pytest.raises(DatasetRehydrationError, match="preparation_incomplete"):
        journal.seal_preparation(value)
    copied = journal.record_stage(value, stage, cursor=["source", "last"], rows=2)
    with pytest.raises(DatasetRehydrationError, match="preparation_incomplete"):
        journal.seal_preparation(copied)
    sealed = journal.seal_preparation(
        journal.record_stage(copied, stage, cursor=["source", "last"], rows=2, receipt=prepared)
    )
    assert journal.seal_preparation(sealed) == sealed
    with pytest.raises(DatasetRehydrationError, match="sealed_stage_changed"):
        journal.record_stage(sealed, stage, cursor=["source", "later"], rows=3, receipt=prepared)


def test_publish_and_inverse_cut_preserve_all_previous_generations():
    sealed, retained = _sealed()
    digest = retained.operation_digest
    published = journal.publish_generation(sealed, digest, (retained,), {"Location": {"input": 2}})
    assert published["active"] is None
    prior = deepcopy(published["generations"])
    operation_by_field = {**sealed["active"]["operation"], "kind": "rollback", "rollback_of": digest}
    inverse_digest = _operation_digest(operation_by_field, tuple(sealed["active"]["targets"]))
    inverse = RetainedOutputReceipt(
        1,
        inverse_digest,
        retained.schema,
        retained.target,
        retained_name(retained.new_oid),
        replace(retained.successor, relation=retained_name(retained.new_oid)),
        replace(retained.incumbent, relation=retained.target),
    )
    restored = journal.record_rollback(
        published, operation_by_field, sealed["active"]["targets"], ["Location"], (inverse,), {"Location": {"input": 2}}
    )
    assert restored["generations"][digest] == prior[digest]
    assert len(restored["generations"]) == 2
    _preserve_history(published, restored)


@pytest.mark.parametrize("mutation", ["remove", "prepared", "published", "active"])
def test_store_cannot_erase_active_custody_or_retained_history(mutation):
    sealed, retained = _sealed()
    old = journal.publish_generation(sealed, retained.operation_digest, (retained,), {"Location": {"input": 2}})
    journal_by_field = deepcopy(old)
    if mutation == "remove":
        journal_by_field["generations"] = {}
    elif mutation == "prepared":
        journal_by_field["generations"][retained.operation_digest]["prepared"] = []
    elif mutation == "published":
        journal_by_field["generations"][retained.operation_digest]["published"] = None
    else:
        old = sealed
        journal_by_field = {**sealed, "active": None, "generations": {}}
    with pytest.raises(DatasetRehydrationError, match="generation_removed|generation_changed|active_removed"):
        _preserve_history(old, journal_by_field)


@pytest.mark.parametrize("protocol", [None, 0, True, 2])
def test_unknown_protocol_refuses_instead_of_discarding_history(protocol):
    with pytest.raises(DatasetRehydrationError, match="shape_invalid"):
        journal.output_journal({"output_snapshots": {"protocol": protocol, "active": None, "generations": {}}})


def test_completed_subset_history_merges_with_new_member_without_rewriting_generations():
    sealed, retained = _sealed()
    published = journal.publish_generation(sealed, retained.operation_digest, (retained,), {"Location": {"input": 2}})
    empty = journal.output_journal({})
    saved = deepcopy((published, empty))
    merged = journal.merge_completed_histories([published, empty])
    assert merged == published and (published, empty) == saved
    _preserve_history(published, merged)
    _preserve_history(empty, merged)


@pytest.mark.parametrize("damage", ["active", "unpublished", "conflicting_published"])
def test_completed_history_merge_refuses_active_or_conflicting_member_evidence(damage):
    sealed, retained = _sealed()
    published = journal.publish_generation(sealed, retained.operation_digest, (retained,), {"Location": {"input": 2}})
    other = journal.output_journal({})
    if damage == "active":
        other = sealed
    elif damage == "unpublished":
        other = deepcopy(sealed)
        other["active"] = None
    else:
        other = deepcopy(published)
        other["generations"][retained.operation_digest]["proofs"]["Location"]["input"] = 3
    with pytest.raises(DatasetRehydrationError, match="member_receipts_disagree"):
        journal.merge_completed_histories([published, other])
