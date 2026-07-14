# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json

import pytest

from db.models import ProviderDirectoryLocation
from process.provider_directory_dataset_rehydrate import (
    DatasetRehydrationError,
    _proof_complete,
    _validate_payload,
    rehydrate_current_dataset,
)


def _payload() -> dict[str, object]:
    return {
        "resource_id": "location-1",
        "status": "active",
        "name": "Example Clinic",
        "city_name": "Louisville",
    }


def _hash(payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(payload, sort_keys=True).encode()
    ).hexdigest()


def test_retained_payload_accepts_exact_mapped_typed_shape():
    payload = _payload()
    assert _validate_payload(ProviderDirectoryLocation, "location-1", _hash(payload), payload) is None


@pytest.mark.parametrize(
    "payload, expected",
    [
        ({"resource_id": "location-1", "unknown": "value"}, "payload_unknown_field"),
        ({"resource_id": "location-1", "source_id": "wrong"}, "payload_provenance_invalid"),
        ({"resource_id": "other"}, "payload_provenance_invalid"),
    ],
)
def test_retained_payload_rejects_wrong_shape_or_provenance(payload, expected):
    assert _validate_payload(ProviderDirectoryLocation, "location-1", _hash(payload), payload) == expected


def test_exact_proof_rejects_rows_outside_dataset_membership():
    checkpoint = {"input": 2, "mapped": 2, "rejected": 0}
    proof = {"input": 2, "typed": 2, "typed_extra": 1, "canonical_hash_matched": 2, "source_edges": 2, "source_edge_extra": 0}
    assert not _proof_complete(checkpoint, proof, 2)


@pytest.mark.asyncio
async def test_rehydrate_rejects_unbounded_batch_before_database_access():
    with pytest.raises(DatasetRehydrationError, match="batch_size_invalid"):
        await rehydrate_current_dataset(
            object(), "mrf", {}, None, source_id="source", dataset_id="dataset",
            acquisition_root_run_id="root", owner_run_id="owner", batch_size=25_001,
        )
