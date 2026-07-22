# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock, Mock

import pytest

from api import ptg2_candidate_audit_batch as batch
from api import ptg2_candidate_audit_reverse as reverse_scope
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_types import PTG2ServingTables


def _serving_tables():
    return PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=100,
        price_dictionary_block_bytes=2048,
    )


@pytest.mark.asyncio
async def test_candidate_scope_threads_batch_schema_into_graph_fallback(
    monkeypatch,
):
    code_index = CandidateCodeIndex(by_pair={}, by_key={})
    load_codes = AsyncMock(return_value=code_index)
    load_provider_scope = AsyncMock(
        return_value=reverse_scope.CandidateProviderScope({}, None)
    )
    monkeypatch.setattr(batch, "PTG2_SCHEMA", "candidate_schema")
    monkeypatch.setattr(batch, "candidate_code_records_by_pair", load_codes)
    monkeypatch.setattr(
        batch,
        "load_candidate_provider_scope",
        load_provider_scope,
    )
    monkeypatch.setattr(
        batch,
        "validate_persisted_audit_graph_scope",
        Mock(),
    )
    monkeypatch.setattr(
        batch,
        "_load_candidate_provider_indexes",
        AsyncMock(return_value=({}, {})),
    )
    monkeypatch.setattr(
        batch,
        "_provider_network_digests_by_key",
        AsyncMock(return_value={}),
    )

    await batch._candidate_scope_indexes(
        object(),
        _serving_tables(),
        object(),
        (),
        (),
    )

    assert load_provider_scope.await_args.kwargs["schema_name"] == ("candidate_schema")
