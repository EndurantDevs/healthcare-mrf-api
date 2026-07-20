# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import pytest

import api.ptg2_db_serving_v3 as db_serving_v3
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from process.ptg_parts.ptg2_serving_binary_v3_code_intersection import (
    provider_code_selection,
)
from process.ptg_parts.ptg2_serving_binary_v3 import (
    append_uvarint,
    encode_provider_code_set,
)


def _provider_block(provider_code_rows):
    provider_rows = tuple(provider_code_rows)
    block_span = db_serving_v3.PTG2_SERVING_BINARY_V3_PROVIDER_SET_KEY_BLOCK_SPAN
    block_key = provider_rows[0][0] // block_span
    payload = bytearray()
    append_uvarint(payload, len(provider_rows))
    for provider_set_key, code_keys in provider_rows:
        assert provider_set_key // block_span == block_key
        code_payload, _stats = encode_provider_code_set(code_keys)
        append_uvarint(payload, provider_set_key % block_span)
        append_uvarint(payload, len(code_payload))
        payload.extend(code_payload)
    return block_key, bytes(payload)


def test_provider_code_block_decodes_only_requested_containers(monkeypatch):
    block_key, provider_payload = _provider_block(
        ((3, (1, 2)), (4, (7, 8)), (5, (9, 10)))
    )
    decoded_payloads = []
    original_decoder = db_serving_v3.decode_provider_code_set

    def tracked_decoder(payload):
        decoded_payloads.append(bytes(payload))
        return original_decoder(payload)

    monkeypatch.setattr(db_serving_v3, "decode_provider_code_set", tracked_decoder)

    assert db_serving_v3._decode_provider_code_block(
        provider_payload,
        block_key=block_key,
        entry_count=3,
        requested_provider_set_keys={4},
    ) == {4: (7, 8)}
    assert len(decoded_payloads) == 1


def test_provider_code_block_retains_only_requested_code_memberships(monkeypatch):
    block_key, provider_payload = _provider_block(
        ((3, tuple(range(100_000))), (4, (7, 8)), (5, (9, 10)))
    )
    intersections = []
    payload_views = []
    original_intersection = db_serving_v3.intersect_provider_code_set

    def tracked_intersection(payload, requested_code_keys):
        payload_views.append(payload)
        observed = original_intersection(payload, requested_code_keys)
        intersections.append(observed)
        return observed

    monkeypatch.setattr(
        db_serving_v3,
        "intersect_provider_code_set",
        tracked_intersection,
    )

    assert db_serving_v3._decode_provider_code_block(
        provider_payload,
        block_key=block_key,
        entry_count=3,
        requested_provider_set_keys={3, 4},
        requested_code_selection=provider_code_selection((7, 99_999, 200_000)),
    ) == {3: (7, 99_999), 4: (7,)}
    assert intersections == [(7, 99_999), (7,)]
    assert all(
        isinstance(payload_view, memoryview)
        and payload_view.obj is provider_payload
        for payload_view in payload_views
    )


def test_provider_code_block_applies_exact_selection_per_provider():
    block_key, provider_payload = _provider_block(
        ((3, (7, 8)), (4, (7, 8)))
    )

    assert db_serving_v3._decode_provider_code_block(
        provider_payload,
        block_key=block_key,
        entry_count=2,
        requested_provider_set_keys={3, 4},
        requested_code_selections_by_provider_set={
            3: provider_code_selection((7,)),
            4: provider_code_selection((8,)),
        },
    ) == {3: (7,), 4: (8,)}


def test_provider_code_block_rejects_ambiguous_or_missing_exact_selection():
    block_key, provider_payload = _provider_block(((3, (7, 8)),))

    with pytest.raises(PTG2ManifestArtifactError, match="block 0 is corrupt"):
        db_serving_v3._decode_provider_code_block(
            provider_payload,
            block_key=block_key,
            entry_count=1,
            requested_provider_set_keys={3},
            requested_code_selection=provider_code_selection((7,)),
            requested_code_selections_by_provider_set={
                3: provider_code_selection((8,))
            },
        )

    with pytest.raises(PTG2ManifestArtifactError, match="block 0 is corrupt"):
        db_serving_v3._decode_provider_code_block(
            provider_payload,
            block_key=block_key,
            entry_count=1,
            requested_provider_set_keys={3},
            requested_code_selections_by_provider_set={},
        )


def test_provider_code_block_claims_budget_before_retaining_next_provider():
    block_key, provider_payload = _provider_block(
        ((3, tuple(range(100_000))), (4, (7, 8)), (5, (9, 10)))
    )
    claimed_memberships = []

    def reject_first_claim(provider_set_key, code_keys):
        claimed_memberships.append((provider_set_key, code_keys))
        raise PTG2ManifestArtifactError("retention limit")

    with pytest.raises(PTG2ManifestArtifactError, match="retention limit"):
        db_serving_v3._decode_provider_code_block(
            provider_payload,
            block_key=block_key,
            entry_count=3,
            requested_provider_set_keys={3, 4},
            requested_code_selection=provider_code_selection((7, 99_999)),
            claim_retained_code_keys=reject_first_claim,
        )

    assert claimed_memberships == [(3, (7, 99_999))]
