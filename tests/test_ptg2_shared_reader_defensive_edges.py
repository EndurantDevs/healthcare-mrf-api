# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Small failure-atomicity checks shared by strict PTG readers."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_db_serving_v3
from api import ptg2_db_sidecars as sidecars
from api import ptg2_tables
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
)
from api.ptg2_types import PTG2ServingIndex
from process.ptg_parts import ptg2_serving_binary_v3_primitives as primitives
from tests.test_ptg2_manifest_tables import (
    FakeSession,
    strict_serving_index,
    strict_snapshot_row,
)
from tests.test_ptg2_shared_serving_api import (
    _RecordingOneRowSession,
    _candidate_audit_access,
    _candidate_descriptor_row,
)


def test_serving_index_defaults_every_nested_collection() -> None:
    index = PTG2ServingIndex.from_payload({})

    assert index.snapshot_id == ""
    assert index.version == 1
    assert index.plans == {}
    assert index.procedures == {}
    assert index.providers == {}
    assert index.rates == {}


def test_forward_retention_bound_defaults_and_validates_filter_size() -> None:
    assert sidecars.forward_price_index_retention_upper_bound(2, 1) > 0

    with pytest.raises(ValueError, match="filter coordinate count"):
        sidecars.forward_price_index_retention_upper_bound(
            2,
            1,
            filter_coordinate_count=False,
        )


def test_forward_fragment_rejects_a_parser_that_emits_no_occurrence(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        sidecars, "_decode_serving_binary_payload", Mock(return_value=b"x")
    )
    monkeypatch.setattr(
        sidecars,
        "decode_dense_source_header",
        Mock(return_value=(1, 1, 0)),
    )
    monkeypatch.setattr(sidecars, "read_strict_uvarint", Mock(return_value=(0, 1)))
    monkeypatch.setattr(
        sidecars,
        "_visit_forward_occurrences",
        Mock(return_value=(1, None)),
    )

    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="no occurrences"):
        sidecars._visit_forward_fragment_unchecked(
            {"entry_count": 1},
            provider_filter=None,
            fragment_cursor=sidecars._ForwardFragmentCursor(),
            validation=sidecars._ForwardFragmentValidation(
                expected_source_count=1,
            ),
            occurrence_consumer=lambda *_coordinate: None,
        )


def test_exact_occurrence_filter_rejects_empty_normalized_scope(
    monkeypatch,
) -> None:
    monkeypatch.setattr(
        sidecars,
        "_normalized_provider_set_filter",
        Mock(return_value=()),
    )
    options = sidecars._ForwardBatchOptions(
        shared_snapshot_key=1,
        source_count=1,
        price_dictionary_item_count=1,
        price_dictionary_block_bytes=16,
        occurrence_keys=((7, 5, 0),),
    )

    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="must not be empty"):
        sidecars._normalized_batch_occurrence_filters(options, (7,))


class _RejectingList(list):
    def append(self, _value) -> None:
        raise RuntimeError("synthetic append failure")


def test_forward_result_append_failure_releases_its_row_claim() -> None:
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    batch = sidecars._MutableForwardOccurrenceBatch(
        {7: _RejectingList()},
        budget,
        None,
    )

    with pytest.raises(RuntimeError, match="append failure"):
        batch.retain(7, 5, 9, 0)

    assert budget.retained_bytes == 0
    assert batch.occurrence_count == 0


class _RejectingCodeKeys:
    def __len__(self) -> int:
        return 1

    def __iter__(self):
        raise RuntimeError("synthetic allocation failure")


def test_forward_result_allocation_failure_releases_container_claim() -> None:
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    with pytest.raises(RuntimeError, match="allocation failure"):
        sidecars._new_mutable_forward_occurrence_batch(
            _RejectingCodeKeys(),
            budget,
            None,
        )

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_limit", (-1, False))
async def test_forward_result_rejects_invalid_limit_before_visit(
    invalid_limit,
) -> None:
    options = sidecars._ForwardBatchOptions(1, 1, 1, 16)

    with pytest.raises(ValueError, match="limit must not be negative"):
        await sidecars._decoded_forward_batch_keys(
            object(),
            (7,),
            options,
            max_occurrences=invalid_limit,
        )


class _RejectingItemsDict(dict):
    def items(self):
        raise RuntimeError("synthetic freeze failure")


def test_forward_result_freeze_failure_releases_peak_claim() -> None:
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    with pytest.raises(RuntimeError, match="freeze failure"):
        sidecars._freeze_forward_occurrence_batch(
            _RejectingItemsDict({7: []}),
            budget,
        )

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_forward_price_freeze_failure_releases_mutable_claim(
    monkeypatch,
) -> None:
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=16 * 1024)
    mutable_index = {(7, 5, 0): [9]}

    async def claimed_mutable_index(*_args, **_kwargs):
        budget.claim(
            sidecars._mutable_forward_index_retained_bytes(mutable_index),
            category="a synthetic mutable forward index",
        )
        return mutable_index

    monkeypatch.setattr(
        sidecars,
        "_mutable_forward_price_index",
        claimed_mutable_index,
    )
    monkeypatch.setattr(
        sidecars,
        "_build_frozen_forward_price_index",
        Mock(side_effect=RuntimeError("synthetic price freeze failure")),
    )

    with pytest.raises(RuntimeError, match="price freeze failure"):
        await sidecars.lookup_forward_price_index_from_db(
            object(),
            (7,),
            retention_budget=budget,
            shared_snapshot_key=1,
            source_count=1,
            price_dictionary_item_count=1,
            price_dictionary_block_bytes=16,
        )

    assert budget.retained_bytes == 0


@pytest.mark.asyncio
@pytest.mark.parametrize("invalid_limit", (-1, False))
async def test_price_membership_lookup_rejects_invalid_atom_limit(
    invalid_limit,
) -> None:
    with pytest.raises(sidecars.PTG2ManifestArtifactError, match="invalid atom limit"):
        await sidecars.lookup_price_atom_memberships_from_db(
            object(),
            1,
            (0,),
            maximum_selected_atom_count=invalid_limit,
        )


@pytest.mark.asyncio
async def test_price_membership_lookup_rechecks_decoder_atom_limit(
    monkeypatch,
) -> None:
    logical_block = SimpleNamespace(payload=b"x", entry_count=1)
    monkeypatch.setattr(
        sidecars,
        "_shared_logical_blocks_by_key",
        AsyncMock(return_value={0: logical_block}),
    )
    monkeypatch.setattr(
        sidecars,
        "_logical_blocks_by_physical_identity",
        Mock(return_value={b"physical": ((0, logical_block),)}),
    )
    monkeypatch.setattr(sidecars, "_claim_logical_block_processing", Mock())
    monkeypatch.setattr(
        ptg2_db_serving_v3,
        "_decode_price_membership_block",
        Mock(return_value={0: (1, 2)}),
    )

    with pytest.raises(
        sidecars.PTG2ManifestArtifactError, match="exceeds its atom limit"
    ):
        await sidecars.lookup_price_atom_memberships_from_db(
            object(),
            1,
            (0,),
            maximum_selected_atom_count=1,
        )


@pytest.mark.parametrize("invalid_limit", (-1, False))
def test_primitive_selected_atom_limit_requires_nonnegative_integer(
    invalid_limit,
) -> None:
    with pytest.raises(ValueError, match="non-negative integer"):
        primitives._validate_selected_atom_limit(invalid_limit)


def _legacy_header(payload: bytes):
    return primitives._price_membership_header(payload)


@pytest.mark.parametrize(
    ("payload", "message"),
    (
        (b"\x01\x03\x02\x01\x01\x01\x00\x00\x00", "strictly ordered"),
        (b"\x01\x03\x01\x01\x00", "cannot be empty"),
        (b"\x01\x03\x01\x01\x01\x00\x00", "atom keys are truncated"),
        (b"\x01\x03\x00\x00", "trailing bytes"),
    ),
    ids=("duplicate-price", "empty-atoms", "truncated-atoms", "trailing-byte"),
)
def test_legacy_membership_selection_rejects_record_corruption(
    payload,
    message,
) -> None:
    with pytest.raises(ValueError, match=message):
        primitives._legacy_selected_price_memberships(
            payload,
            _legacy_header(payload),
            {1},
            10,
            primitives.decode_dense_keys,
        )


def test_legacy_membership_selection_rejects_unordered_selected_atoms() -> None:
    payload = (
        b"\x01\x03\x01\x01\x02" + (2).to_bytes(3, "little") + (1).to_bytes(3, "little")
    )

    with pytest.raises(ValueError, match="atom keys are not ordered"):
        primitives._legacy_selected_price_memberships(
            payload,
            _legacy_header(payload),
            {1},
            10,
            primitives.decode_dense_keys,
        )


def test_legacy_membership_selection_returns_bounded_result() -> None:
    payload = b"\x01\x03\x01\x01\x01" + (2).to_bytes(3, "little")

    assert primitives._legacy_selected_price_memberships(
        payload,
        _legacy_header(payload),
        {1},
        1,
        primitives.decode_dense_keys,
    ) == {1: (2,)}


@pytest.mark.asyncio
async def test_candidate_source_witness_rejects_malformed_json() -> None:
    access = _candidate_audit_access()
    row = _candidate_descriptor_row(access.source_key)
    row["layout_source_witness"] = "{"

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="source witness"):
        await ptg2_tables.snapshot_serving_tables(
            _RecordingOneRowSession(row),
            access.snapshot_id,
            candidate_audit_access=access,
        )


@pytest.mark.asyncio
async def test_published_source_set_rejects_malformed_json() -> None:
    row = strict_snapshot_row()
    row["snapshot_source_set"] = "{"

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match="source set"):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([row]),
            "malformed-published-source-set",
        )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("serving_index_updates", "row_updates", "message"),
    (
        ({}, {"attested_source_key": ""}, "source key is missing"),
        ({"code_count": -1}, {}, "code count is invalid"),
        ({"code_count": 0, "serving_rates": 1}, {}, "missing code metadata"),
    ),
    ids=("missing-source-key", "negative-code-count", "rates-without-code"),
)
async def test_published_descriptor_rejects_late_binding_drift(
    serving_index_updates,
    row_updates,
    message,
) -> None:
    serving_index = strict_serving_index()
    serving_index.update(serving_index_updates)
    row = strict_snapshot_row(serving_index, **row_updates)

    with pytest.raises(ptg2_tables.PTG2ManifestArtifactError, match=message):
        await ptg2_tables.snapshot_serving_tables(
            FakeSession([row]),
            "late-binding-drift",
        )
