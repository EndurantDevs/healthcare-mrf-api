# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import datetime
import importlib
import json
from types import SimpleNamespace

import pytest

from db.models import ProviderDirectoryPractitioner
from process.provider_directory_resource_hash import (
    RESOURCE_HASH_CONTRACT_METADATA_KEY,
    TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT,
    _json_default,
    resource_payload_sha256_for_contract,
)


importer = importlib.import_module("process.provider_directory_fhir")


def test_hash_encoding_and_unknown_contract_are_explicit():
    """Cover both JSON scalar encodings and reject an unknown write policy."""

    assert _json_default(datetime.date(2026, 8, 9)) == "2026-08-09"
    assert _json_default(SimpleNamespace(name="fallback")) == (
        "namespace(name='fallback')"
    )
    with pytest.raises(ValueError, match="resource_hash_contract_invalid"):
        resource_payload_sha256_for_contract({}, "unknown-contract")


@pytest.mark.parametrize(
    "raw_metadata",
    ("{", "[]", 7),
)
def test_dataset_contract_rejects_malformed_serialized_metadata(raw_metadata):
    """Reject invalid JSON, non-object JSON, and non-JSON metadata values."""

    with pytest.raises(RuntimeError, match="resource_hash_contract_invalid"):
        importer._dataset_resource_hash_contract(
            {"publication_metadata_json": raw_metadata}
        )


def test_dataset_contract_accepts_serialized_metadata_object():
    """Read the same explicit contract from stored JSON text and mappings."""

    raw_metadata = json.dumps(
        {
            RESOURCE_HASH_CONTRACT_METADATA_KEY: (
                TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT
            )
        }
    )
    assert importer._dataset_resource_hash_contract(
        {"publication_metadata_json": raw_metadata}
    ) == TRANSPORT_NEUTRAL_RESOURCE_HASH_CONTRACT


@pytest.mark.asyncio
async def test_deferred_dataset_write_requires_hash_contract():
    """Fail before persistence when a deferred dataset write lacks its policy."""

    with pytest.raises(ValueError, match="resource_hash_contract_required"):
        await importer._upsert_deferred_resource_rows(
            ProviderDirectoryPractitioner,
            [{"resource_id": "practitioner-1"}],
            dataset_id="dataset-1",
            track_seen=False,
        )
