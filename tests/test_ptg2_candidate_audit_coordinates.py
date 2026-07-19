from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_batch as batch
from api import ptg2_candidate_audit_codes as codes
from api.ptg2_candidate_audit_codes import CandidateCodeIndex
from api.ptg2_candidate_audit_coordinates import (
    validate_persisted_audit_graph_scope,
    validate_persisted_audit_price_scope,
)
from api.ptg2_candidate_audit_integrity import PersistedAuditOccurrence
from api.ptg2_candidate_audit_projection import CandidatePriceData
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


def _challenge() -> AuditBatchChallenge:
    return AuditBatchChallenge(
        code_system="CPT",
        code="99213",
        npi=1234567890,
        source_artifact_key=0,
        tuple_digest="1" * 64,
        network_name_digests=(),
        multiplicity=1,
    )


def _persisted_occurrence(**overrides) -> PersistedAuditOccurrence:
    occurrence_by_field = {
        "occurrence_id": b"a" * 32,
        "code_key": 7,
        "provider_set_key": 5,
        "price_key": 8,
        "source_artifact_key": 0,
        "npi": 1234567890,
        "atom_ordinal": 0,
        "atom_key": 9,
    }
    occurrence_by_field.update(overrides)
    return PersistedAuditOccurrence(**occurrence_by_field)


def _code_record(
    code_key: int,
    *,
    canonical: bool = True,
    reported_code: str = "99213",
) -> dict:
    return {
        "canonical_system": "CPT" if canonical else None,
        "canonical_code": "99213" if canonical else None,
        "code_key": code_key,
        "reported_code_system": "CPT",
        "reported_code": reported_code,
    }


@pytest.mark.asyncio
async def test_forward_batch_preserves_exact_witness_and_persisted_sources(
    monkeypatch,
):
    lookup = AsyncMock(
        return_value={
            (7, 5, 0): (8,),
            (8, 6, 1): (11,),
            (9, 7, 1): (13,),
        }
    )
    monkeypatch.setattr(batch, "lookup_forward_price_index_from_db", lookup)
    serving_tables = PTG2ServingTables(
        arch_version="postgres_binary_v3",
        storage_generation="shared_blocks_v3",
        cold_lookup_contract="ptg_v3_cold_v2",
        shared_block_layout="dense_shared_blocks_v3",
        shared_snapshot_key=41,
        source_count=2,
        price_dictionary_item_count=100,
        price_dictionary_block_bytes=2048,
    )

    required_occurrences = frozenset(
        {(7, 5, 0), (8, 6, 1), (9, 7, 1)}
    )
    observed = await batch._candidate_forward_price_keys(
        object(),
        serving_tables,
        {7: (5,), 8: (6,), 9: (7,)},
        required_occurrences,
    )

    assert observed == lookup.return_value
    assert lookup.await_args.kwargs["provider_set_keys_by_code"] == {
        7: (5,),
        8: (6,),
        9: (7,),
    }
    assert lookup.await_args.kwargs["occurrence_keys"] is required_occurrences
    assert "source_keys_by_code" not in lookup.await_args.kwargs


def test_persisted_audit_requires_exact_code_and_provider_graph_scope():
    occurrence = _persisted_occurrence()
    code_index = CandidateCodeIndex(
        by_pair={},
        by_key={occurrence.code_key: {"code_key": occurrence.code_key}},
    )

    validate_persisted_audit_graph_scope(
        (occurrence,),
        code_index,
        {occurrence.npi: (occurrence.provider_set_key,)},
    )

    with pytest.raises(PTG2ManifestArtifactError, match="NPI graph"):
        validate_persisted_audit_graph_scope(
            (occurrence,),
            code_index,
            {occurrence.npi: ()},
        )
    with pytest.raises(PTG2ManifestArtifactError, match="code is missing"):
        validate_persisted_audit_graph_scope(
            (occurrence,),
            CandidateCodeIndex(by_pair={}, by_key={}),
            {occurrence.npi: (occurrence.provider_set_key,)},
        )


@pytest.mark.parametrize(
    "price_data",
    [
        CandidatePriceData({}, {8: (9,)}, {}),
        CandidatePriceData({(7, 5, 0): (8,)}, {8: (10,)}, {}),
        CandidatePriceData({(7, 5, 0): (8,)}, {8: ()}, {}),
    ],
)
def test_persisted_audit_requires_exact_forward_and_atom_membership(price_data):
    with pytest.raises(PTG2ManifestArtifactError, match="forward|membership"):
        validate_persisted_audit_price_scope(
            (_persisted_occurrence(),),
            price_data,
        )


def test_persisted_audit_accepts_exact_price_scope_and_rejects_missing_membership():
    occurrence = _persisted_occurrence()
    forward_index = {(7, 5, 0): (8,)}

    validate_persisted_audit_price_scope(
        (occurrence,),
        CandidatePriceData(forward_index, {8: (9,)}, {}),
    )
    with pytest.raises(PTG2ManifestArtifactError, match="membership"):
        validate_persisted_audit_price_scope(
            (occurrence,),
            CandidatePriceData(forward_index, {}, {}),
        )


def test_candidate_code_index_unions_aliases_and_persisted_only_keys(monkeypatch):
    monkeypatch.setattr(codes, "_canonical_code_metadata_row", dict)

    code_index = codes._indexed_candidate_code_records(
        (_challenge(),),
        (8,),
        (_code_record(7), _code_record(8, canonical=False)),
    )

    assert tuple(code_index.by_pair) == (("CPT", "99213"),)
    assert tuple(code_index.by_key) == (7, 8)


def test_candidate_code_index_rejects_inconsistent_aliases(monkeypatch):
    monkeypatch.setattr(codes, "_canonical_code_metadata_row", dict)

    with pytest.raises(PTG2ManifestArtifactError, match="inconsistently"):
        codes._indexed_candidate_code_records(
            (_challenge(),),
            (),
            (_code_record(7), _code_record(7, reported_code="other")),
        )


@pytest.mark.parametrize(
    ("rows", "persisted_code_keys", "message"),
    [
        ((), (), "audit code"),
        ((_code_record(7),), (8,), "persisted audit code"),
    ],
)
def test_candidate_code_index_rejects_incomplete_scope(
    monkeypatch,
    rows,
    persisted_code_keys,
    message,
):
    monkeypatch.setattr(codes, "_canonical_code_metadata_row", dict)

    with pytest.raises(PTG2ManifestArtifactError, match=message):
        codes._indexed_candidate_code_records(
            (_challenge(),),
            persisted_code_keys,
            rows,
        )
