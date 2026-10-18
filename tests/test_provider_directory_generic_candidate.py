# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Pure complete-key and bounded-copy contracts; no database fixture is needed."""

import hashlib
import json

import pytest

from process import provider_directory_generic_candidate as candidate


def _resource(resource_id="example", *, values=("one",), dataset="dataset"):
    payload = {"values": list(values)}
    return {
        "dataset_id": dataset,
        "resource_type": "Organization",
        "resource_id": resource_id,
        "payload_hash": hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest(),
        "payload_json": payload,
        "acquired_resource_sha256": None,
    }


def _merge(left, right):
    values = sorted(set(left["payload_json"]["values"]) | set(right["payload_json"]["values"]))
    return _resource(right["resource_id"], values=values)


def _reduce(rows, *, prepare=dict, merge=_merge):
    return list(candidate.reduce_sorted_resources(rows, "dataset", prepare_row=prepare, merge_rows=merge))


def test_complete_key_reduction_preserves_semantic_union_replay_and_order():
    rows = [_resource("first"), _resource("first", values=("two",)), _resource("first"), _resource("second")]
    result = _reduce(iter(rows))
    assert result == [_resource("first", values=("one", "two")), _resource("second")]
    assert _reduce(rows + [_resource("second")]) == result
    assert rows[0]["payload_json"] == {"values": ["one"]}


def test_one_identity_is_complete_before_it_is_emitted():
    visited_names = []

    def rows():
        for name in ("first", "first", "second"):
            visited_names.append(name)
            yield _resource(name)

    reduced = candidate.reduce_sorted_resources(rows(), "dataset", prepare_row=dict, merge_rows=_merge)
    assert next(reduced) == _resource("first")
    assert visited_names == ["first", "first", "second"]
    assert list(reduced) == [_resource("second")]


@pytest.mark.parametrize(
    "rows",
    ([_resource(dataset="other")], [_resource("second"), _resource("first")], [{**_resource(), "payload_hash": "bad"}]),
)
def test_identity_order_and_hash_refuse_before_publication(rows):
    with pytest.raises(candidate.GenericCandidateError):
        _reduce(rows)


def test_semantic_callbacks_cannot_change_the_complete_key():
    with pytest.raises(candidate.GenericCandidateError, match="dataset_mismatch"):
        _reduce([_resource()], prepare=lambda row: row | {"dataset_id": "other"})
    with pytest.raises(candidate.GenericCandidateError, match="merged_row_invalid"):
        _reduce([_resource(), _resource()], merge=lambda left, right: right | {"resource_id": "other"})


def _witness(resource_id="example"):
    return {
        "dataset_id": "dataset",
        "source_id": "cms-npd",
        "release_id": "a" * 64,
        "resource_type": "Organization",
        "resource_id": resource_id,
        "raw_payload_sha256": "b" * 64,
        "normalized_payload_hash": "c" * 64,
        "raw_payload_json": {"resourceType": "Organization", "id": resource_id},
    }


def test_witness_replay_is_exact_across_batch_boundaries():
    rows = [_witness("first"), _witness("first"), _witness("second")]
    assert list(candidate.reduce_sorted_witnesses(iter(rows), "dataset", "a" * 64)) == [rows[0], rows[2]]


@pytest.mark.parametrize(
    "field,value",
    (("raw_payload_sha256", "d" * 64), ("normalized_payload_hash", "d" * 64), ("raw_payload_json", {"changed": True})),
)
def test_conflicting_witness_replay_refuses(field, value):
    rows = [_witness(), _witness() | {field: value}]
    with pytest.raises(candidate.GenericCandidateError, match="witness_replay_conflict"):
        list(candidate.reduce_sorted_witnesses(rows, "dataset", "a" * 64))


def test_witness_source_and_release_cannot_cross_snapshots():
    for changed in (_witness() | {"release_id": "e" * 64}, _witness() | {"source_id": "other"}):
        with pytest.raises(candidate.GenericCandidateError, match="witness_identity_invalid"):
            list(candidate.reduce_sorted_witnesses([changed], "dataset", "a" * 64))


def test_copy_preparation_bounds_rows_bytes_and_unicode(monkeypatch):
    monkeypatch.setattr(candidate, "COPY_MAX_ROWS", 2)
    records = [("first",), ("second",), ("third",)]
    assert list(candidate.copy_record_batches(records)) == [records[:2], records[2:]]
    monkeypatch.setattr(candidate, "COPY_MAX_BYTES", 32)
    assert list(candidate.copy_record_batches([("éé",)])) == [[("éé",)]]
    with pytest.raises(candidate.GenericCandidateError, match="copy_record_too_large"):
        list(candidate.copy_record_batches([("ééx",)]))
    with pytest.raises(candidate.GenericCandidateError, match="copy_value_invalid"):
        list(candidate.copy_record_batches([(object(),)]))
