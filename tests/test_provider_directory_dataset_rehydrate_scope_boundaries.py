# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from unittest.mock import AsyncMock

import pytest

from process import provider_directory_dataset_rehydrate_scope as rehydrate_scope
from process import provider_directory_dataset_rehydrate_store as rehydrate_store
from process import provider_directory_dataset_rehydrate_types as rehydrate_types
from tests.test_provider_directory_dataset_rehydrate_boundaries import (
    _RehydrateDatabase,
    _checkpoint_record,
    _context,
    _digest_row,
    _request,
    _runtime,
    _scope,
    _scope_record,
)


@pytest.mark.asyncio
async def test_scope_digest_and_checkpoint_boundaries():
    database = _RehydrateDatabase()
    runtime = _runtime(database)
    database.first = AsyncMock(return_value=None)
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="scope_not_found"):
        await rehydrate_scope._load_dataset_scope(runtime, _request())

    invalid_scope_record = _scope_record()
    invalid_scope_record["publication_metadata_json"] = "not-json"
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="scope_not_current"):
        rehydrate_scope._decode_dataset_scope(_request(), invalid_scope_record)
    encoded_scope_record = _scope_record()
    encoded_scope_record["publication_metadata_json"] = json.dumps(
        encoded_scope_record["publication_metadata_json"]
    )
    assert rehydrate_scope._decode_dataset_scope(
        _request(), encoded_scope_record
    ).dataset_id == "dataset-1"

    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="resource_scope_invalid"):
        rehydrate_scope._selected_resource_types(
            runtime,
            _request(resource_types=("Location", "Location")),
            _scope(),
        )

    database.all = AsyncMock(return_value=[])
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="dataset_hash_mismatch"):
        await rehydrate_scope._verify_dataset_digest(runtime, _scope())
    await rehydrate_scope._read_digest_page(runtime, _scope(), ("Location", "location-1"))
    assert database.all.await_args.kwargs["after_type"] == "Location"

    digest = hashlib.sha256()
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="hash_identity_invalid"):
        rehydrate_scope._append_digest_rows(
            digest,
            [{"resource_type": "", "resource_id": "id", "payload_hash": "0" * 64}],
            0,
        )
    count, cursor = rehydrate_scope._append_digest_rows(digest, [_digest_row()], 1)
    assert (count, cursor) == (2, ("Location", "location-1"))

    checkpoint_context = _context(database)
    mismatch_record = _checkpoint_record()
    mismatch_record["endpoint_id"] = "other-endpoint"
    database.first = AsyncMock(return_value=mismatch_record)
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="checkpoint_scope_mismatch"):
        await rehydrate_store._load_checkpoint(checkpoint_context)


@pytest.mark.asyncio
async def test_digest_reads_an_additional_full_page(monkeypatch):
    runtime = _runtime(_RehydrateDatabase())
    scope = _scope()
    full_page = [object()] * 10_000
    monkeypatch.setattr(
        rehydrate_scope,
        "_read_digest_page",
        AsyncMock(side_effect=[full_page, []]),
    )
    monkeypatch.setattr(
        rehydrate_scope,
        "_append_digest_rows",
        lambda _digest, _rows, _count: (1, ("Location", "location-1")),
    )
    with pytest.raises(rehydrate_types.DatasetRehydrationError, match="dataset_hash_mismatch"):
        await rehydrate_scope._verify_dataset_digest(runtime, scope)


@pytest.mark.asyncio
async def test_shared_type_helpers_reject_invalid_identifier_and_skip_cancel():
    with pytest.raises(ValueError, match="invalid SQL identifier"):
        rehydrate_types._quote_identifier("unsafe-name")
    await rehydrate_types._run_cancel_check(
        replace(_runtime(_RehydrateDatabase()), cancel_check=None)
    )
