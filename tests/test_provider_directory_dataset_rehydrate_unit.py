# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json

import pytest

from db.models import ProviderDirectoryLocation
from process.provider_directory_dataset_rehydrate import (
    DatasetRehydrationError,
    RehydrationCheckpoint,
    RehydrationRequest,
    RehydrationRuntime,
    _is_proof_complete,
    _validate_payload,
    rehydrate_current_dataset,
)
from process.provider_directory_dataset_rehydrate_types import ResourceProof


def _payload() -> dict[str, object]:
    return {
        "resource_id": "location-1",
        "status": "active",
        "name": "Example Clinic",
        "city_name": "Louisville",
    }


def _hash(mapped_payload: dict[str, object]) -> str:
    return hashlib.sha256(
        json.dumps(mapped_payload, sort_keys=True).encode()
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
    checkpoint_record = RehydrationCheckpoint(
        state="complete", last_resource_id="location-2", expected_count=2,
        input_count=2, mapped_count=2, rejected_count=0,
    )
    resource_proof = ResourceProof(
        input_count=2, typed_count=2, typed_extra_count=1,
        canonical_hash_count=2, canonical_extra_count=0, source_edge_count=2,
        source_edge_extra_count=0,
    )
    assert not _is_proof_complete(checkpoint_record, resource_proof)


@pytest.mark.asyncio
async def test_rehydrate_rejects_unbounded_batch_before_database_access():
    with pytest.raises(DatasetRehydrationError, match="batch_size_invalid"):
        runtime = RehydrationRuntime(object(), "mrf", {}, None)
        request = RehydrationRequest(
            source_id="source", dataset_id="dataset",
            acquisition_root_run_id="root", owner_run_id="owner",
            batch_size=25_001,
        )
        await rehydrate_current_dataset(
            runtime,
            request,
        )
