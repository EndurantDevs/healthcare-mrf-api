from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_batch as batch
from api import ptg2_candidate_audit_projection as projection
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_candidate_audit_evidence import (
    canonical_network_name_digests,
    canonical_tuple_digest_without_networks,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError
from scripts.validation import ptg2_v3_source_api_audit as source_audit


def _price_payload() -> dict:
    return {
        "negotiated_type": "negotiated",
        "negotiated_rate": "123.45",
        "expiration_date": "2026-12-31",
        "service_code": ["11"],
        "billing_class": "professional",
        "setting": "office",
        "billing_code_modifier": ["25"],
        "additional_information": "test",
    }


def _challenge() -> AuditBatchChallenge:
    query = source_audit.QueryKey("CPT", "99213", 1234567890)
    canonical_tuple = source_audit.CanonicalTuple.from_parts(
        query,
        "ffs",
        _price_payload(),
        billing_code_type_version="2026",
        name="Office visit",
        description="Established patient",
        network_names=("Alpha Network",),
    )
    return AuditBatchChallenge(
        code_system="CPT",
        code="99213",
        npi=query.npi,
        source_artifact_key=0,
        tuple_digest=canonical_tuple_digest_without_networks(canonical_tuple),
        network_name_digests=canonical_network_name_digests(("Alpha Network",)),
        multiplicity=1,
    )


def _serving_tables() -> PTG2ServingTables:
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


def test_batch_record_fields_supports_mapping_rows_and_pairs():
    mapped_row = type("MappedRow", (), {"_mapping": {"value": 1}})()

    assert batch._record_fields(mapped_row) == {"value": 1}
    assert batch._record_fields((("value", 2),)) == {"value": 2}


def test_provider_filters_union_witness_and_persisted_coordinates():
    challenge = _challenge()
    code_index = batch.CandidateCodeIndex(
        by_pair={("CPT", "99213"): ({"code_key": 7}, {"code_key": 8})},
        by_key={},
    )
    persisted = batch.PersistedAuditOccurrence(
        b"a" * 32,
        9,
        6,
        8,
        1,
        1234567890,
        0,
        10,
    )

    assert batch._provider_filters_by_code_key(
        (challenge,),
        code_index,
        {challenge.npi: (5,)},
        (persisted,),
    ) == {7: (5,), 8: (5,), 9: (6,)}


def test_allowed_sources_skip_code_keys_outside_provider_scope():
    challenge = _challenge()

    assert batch._allowed_source_keys_by_code_key(
        (challenge,),
        {("CPT", "99213"): ({"code_key": 7}, {"code_key": 99})},
        {7: (5,)},
    ) == {7: {0}}


class _NetworkSession:
    def __init__(self, rows):
        self.rows = rows
        self.calls = 0

    async def execute(self, _statement, _params=None):
        self.calls += 1
        return self.rows


@pytest.mark.asyncio
async def test_provider_network_names_handle_empty_complete_and_incomplete_sets():
    empty_session = _NetworkSession(())
    assert await batch._provider_network_names_by_key(
        empty_session,
        _serving_tables(),
        (),
    ) == {}
    assert empty_session.calls == 0

    complete_session = _NetworkSession(
        ({"provider_set_key": 5, "network_names": [" Alpha "]},)
    )
    assert await batch._provider_network_names_by_key(
        complete_session,
        _serving_tables(),
        (5,),
    ) == {5: ("Alpha",)}
    with pytest.raises(PTG2ManifestArtifactError, match="incomplete"):
        await batch._provider_network_names_by_key(
            _NetworkSession(()),
            _serving_tables(),
            (5,),
        )


@pytest.mark.asyncio
async def test_forward_lookup_skips_empty_provider_scope(monkeypatch):
    lookup = AsyncMock()
    monkeypatch.setattr(batch, "lookup_forward_price_index_from_db", lookup)

    assert await batch._candidate_forward_price_keys(
        object(),
        _serving_tables(),
        (),
        {},
        {},
    ) == {}
    lookup.assert_not_awaited()


def test_candidate_projection_reuses_one_tuple_and_deduplicates_availability():
    challenge = _challenge()
    code_fields_by_name = {
        "code_key": 7,
        "reported_code_system": "CPT",
        "reported_code": "99213",
        "negotiation_arrangement": "ffs",
        "billing_code_type_version": "2026",
        "source_name": "Office visit",
        "source_description": "Established patient",
    }
    price_payload = _price_payload()
    network_digests = frozenset(challenge.network_name_digests)

    availability, ledger = batch.candidate_availability_index(
        (challenge,),
        {("CPT", "99213"): (code_fields_by_name,)},
        {challenge.npi: (5, 6)},
        {5: network_digests, 6: network_digests},
        batch.CandidatePriceData(
            {(7, 5, 0): (8,), (7, 6, 0): (8,)},
            {8: (9,)},
            {8: [price_payload]},
        ),
    )

    assert tuple(availability.values()) == ((network_digests,),)
    assert ledger["candidate_occurrence_deliveries"] == 2
    assert ledger["candidate_projection_builds"] == 1
    assert ledger["candidate_projection_reuse_deliveries"] == 1
    assert ledger["duplicate_availability_deliveries"] == 1


def test_candidate_projection_rejects_non_singular_wire_price(monkeypatch):
    monkeypatch.setattr(
        projection,
        "_response_wire_value",
        lambda _exact_fields: {"prices": []},
    )

    with pytest.raises(PTG2ManifestArtifactError, match="not singular"):
        projection._build_canonical_candidate_tuple(
            source_audit.QueryKey("CPT", "99213", 1234567890),
            {},
            (),
            _price_payload(),
        )


def test_candidate_projection_wraps_public_schema_errors(monkeypatch):
    def invalid_public_tuple(*_args):
        raise source_audit.ApiSchemaError("invalid")

    monkeypatch.setattr(
        projection.source_audit,
        "canonical_api_price_tuple",
        invalid_public_tuple,
    )

    with pytest.raises(PTG2ManifestArtifactError, match="public API contract"):
        projection._build_canonical_candidate_tuple(
            source_audit.QueryKey("CPT", "99213", 1234567890),
            {},
            (),
            _price_payload(),
        )
