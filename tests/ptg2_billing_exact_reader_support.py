# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Shared synthetic graph fixtures for exact billing-reader tests."""

from __future__ import annotations

from unittest.mock import AsyncMock

from api import ptg2_billing_exact_reader as reader
from api.ptg2_billing_entity_source_resolution import (
    BillingEntitySourceWitness,
    ResolvedBillingEntitySourceScope,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_tax_identity_source_projection import (
    tax_identity_source_publication_from_metadata,
)

GROUP_A = "aa" * 16
GROUP_B = "bb" * 16
SET_X = "11" * 16
SET_Y = "22" * 16


def _publication(**overrides):
    metadata_by_field = {
        "contract": "ptg2_provider_group_tax_identity_source_v1",
        "content_contract": "ptg2_provider_group_tax_identity_source_content_v1",
        "binding_contract": "ptg2_tax_identity_rate_source_binding_v1",
        "binding_vector_contract": "ptg2_tax_identity_source_binding_vector_v1",
        "token_policy_id": "ptg-tin-hmac-sha256-v1:test",
        "token_policy_descriptor_sha256": "1" * 64,
        "source_ordinal_map_digest": "2" * 64,
        "source_count": 2,
        "provider_group_occurrence_count": 7,
        "matched_ein_count": 5,
        "missing_count": 1,
        "malformed_count": 1,
        "unsupported_type_count": 0,
        "content_digest": "3" * 64,
        "artifact_byte_count": 455,
        "binding_vector_digest": "4" * 64,
    }
    metadata_by_field.update(overrides)
    return tax_identity_source_publication_from_metadata(metadata_by_field)


def _tables(
    *,
    v4: bool = True,
    source_publication=...,
) -> PTG2ServingTables:
    if source_publication is ...:
        source_publication = _publication()
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        shared_snapshot_key=17,
        storage_generation="shared_blocks_v4" if v4 else "shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout=(
            "packed_snapshot_maps_v4" if v4 else "dense_shared_blocks_v3"
        ),
        source_count=2,
        price_dictionary_item_count=128,
        price_dictionary_block_bytes=32,
        provider_shard_span=1024,
        provider_tax_identity_source_publication=source_publication,
    )


def _scope(
    *,
    snapshot_key: int = 17,
    source_publication=...,
) -> ResolvedBillingEntitySourceScope:
    if source_publication is ...:
        source_publication = _publication()
    return ResolvedBillingEntitySourceScope(
        snapshot_key=snapshot_key,
        publication=source_publication,
        witnesses=(
            BillingEntitySourceWitness(0, 0, GROUP_A),
            BillingEntitySourceWitness(1, 0, GROUP_B),
        ),
    )


def _claimed_forward_lookup(forward_result):
    async def read_claimed_result(*_args, retention_budget, **_kwargs):
        retained_bytes = (
            reader.ptg2_db_sidecars.forward_occurrence_batch_retained_bytes(
                forward_result
            )
        )
        retention_budget.claim(
            retained_bytes,
            category="the synthetic exact-reader forward result",
        )
        return forward_result

    return AsyncMock(return_value=forward_result, side_effect=read_claimed_result)


def _patch_graph(
    monkeypatch,
    *,
    sets_by_group=None,
    set_keys_by_id=None,
    group_keys_by_id=None,
    groups_by_set=None,
    occurrences_by_code=None,
):
    """Install one complete synthetic graph/read-sidecar projection."""

    graph_responses_by_name = {
        "_manifest_sets_by_group": (
            sets_by_group
            if sets_by_group is not None
            else {GROUP_A: (SET_X,), GROUP_B: (SET_X, SET_Y)}
        ),
        "_provider_set_keys_for_ids": (
            set_keys_by_id if set_keys_by_id is not None else {SET_X: 3, SET_Y: 4}
        ),
        "_shared_provider_group_keys_for_ids": (
            group_keys_by_id
            if group_keys_by_id is not None
            else {GROUP_A: 7, GROUP_B: 8}
        ),
        "_v4_exact_groups_by_set": (
            groups_by_set if groups_by_set is not None else {3: (7, 8), 4: (8,)}
        ),
    }
    for function_name, response_value in graph_responses_by_name.items():
        monkeypatch.setattr(
            reader.ptg2_serving,
            function_name,
            AsyncMock(return_value=response_value),
        )
    forward_result = (
        occurrences_by_code
        if occurrences_by_code is not None
        else {
            10: (
                (3, 100, 0),
                (3, 101, 1),
                (4, 103, 1),
            )
        }
    )

    forward = _claimed_forward_lookup(forward_result)
    monkeypatch.setattr(
        reader.ptg2_db_sidecars,
        "lookup_forward_occurrences_batch_from_db",
        forward,
    )
    return forward
