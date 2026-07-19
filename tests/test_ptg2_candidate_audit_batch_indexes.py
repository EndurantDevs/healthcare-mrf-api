from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace
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


def test_required_occurrence_keys_preserve_provider_source_correlation():
    first = _challenge()
    second = replace(
        first,
        npi=1234567891,
        source_artifact_key=1,
    )
    persisted = batch.PersistedAuditOccurrence(
        b"p" * 32,
        8,
        7,
        14,
        1,
        1234567892,
        0,
        15,
    )

    required = batch._required_candidate_occurrence_keys(
        (first, second),
        {("CPT", "99213"): ({"code_key": 7},)},
        {first.npi: (5,), second.npi: (6,)},
        (persisted,),
    )

    assert required == frozenset({(7, 5, 0), (7, 6, 1), (8, 7, 1)})
    assert (7, 5, 1) not in required
    assert (7, 6, 0) not in required


@pytest.mark.asyncio
async def test_price_load_filters_exact_coordinates_before_hydration(monkeypatch):
    """Pass exact diagonal coordinates into the reader before hydration."""

    first, second, persisted, exact_index = _exact_price_load_case()
    forward_lookup = AsyncMock(return_value=exact_index)

    async def hydrate(_session, _tables, price_keys, *, copy_payloads):
        retained_keys = set(price_keys)
        assert copy_payloads is False
        return SimpleNamespace(
            atom_keys_by_price_key={key: (key + 100,) for key in retained_keys},
            prices_by_key={key: [{"key": key}] for key in retained_keys},
        )

    hydration = AsyncMock(side_effect=hydrate)
    monkeypatch.setattr(batch, "_candidate_forward_price_keys", forward_lookup)
    monkeypatch.setattr(batch, "_version_three_price_hydration", hydration)

    price_load = await batch._load_candidate_price_data(
        object(),
        _serving_tables(),
        (first, second),
        {("CPT", "99213"): ({"code_key": 7},)},
        {first.npi: (5,), second.npi: (6,)},
        {7: (5, 6), 8: (7,)},
        (persisted,),
    )

    assert price_load.data.price_keys_by_occurrence == {
        (7, 5, 0): (10,),
        (7, 6, 1): (13,),
        (8, 7, 1): (14,),
    }
    assert hydration.await_args.args[2] == {10, 13, 14}
    assert forward_lookup.await_args.args[3] == frozenset(
        {(7, 5, 0), (7, 6, 1), (8, 7, 1)}
    )
    assert price_load.selection_io == {
        "exact_candidate_occurrence_coordinates": 3,
        "exact_forward_occurrence_coordinates_returned": 3,
        "exact_forward_price_key_deliveries_returned": 3,
    }


@pytest.mark.asyncio
async def test_price_load_rejects_forward_rows_outside_exact_scope(monkeypatch):
    first, second, persisted, exact_index = _exact_price_load_case()
    escaped_index = {**exact_index, (7, 5, 1): (11,)}
    monkeypatch.setattr(
        batch,
        "_candidate_forward_price_keys",
        AsyncMock(return_value=escaped_index),
    )
    hydration = AsyncMock()
    monkeypatch.setattr(batch, "_version_three_price_hydration", hydration)

    with pytest.raises(PTG2ManifestArtifactError, match="exact occurrence scope"):
        await batch._load_candidate_price_data(
            object(),
            _serving_tables(),
            (first, second),
            {("CPT", "99213"): ({"code_key": 7},)},
            {first.npi: (5,), second.npi: (6,)},
            {7: (5, 6), 8: (7,)},
            (persisted,),
        )

    hydration.assert_not_awaited()


def _exact_price_load_case():
    """Return two diagonal challenges and their exact forward index."""

    first = _challenge()
    second = replace(
        first,
        npi=1234567891,
        source_artifact_key=1,
    )
    persisted = batch.PersistedAuditOccurrence(
        b"p" * 32,
        8,
        7,
        14,
        1,
        1234567892,
        0,
        15,
    )
    exact_index = {
        (7, 5, 0): (10,),
        (7, 6, 1): (13,),
        (8, 7, 1): (14,),
    }
    return first, second, persisted, exact_index


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
        {},
        frozenset(),
    ) == {}
    lookup.assert_not_awaited()


@pytest.mark.asyncio
async def test_forward_lookup_receives_exact_occurrence_filter(monkeypatch):
    lookup = AsyncMock(return_value={(7, 5, 0): (8,)})
    monkeypatch.setattr(batch, "lookup_forward_price_index_from_db", lookup)
    required_occurrences = frozenset({(7, 5, 0)})

    observed = await batch._candidate_forward_price_keys(
        object(),
        _serving_tables(),
        {7: (5,)},
        required_occurrences,
    )

    assert observed == {(7, 5, 0): (8,)}
    assert lookup.await_args.kwargs["occurrence_keys"] is required_occurrences
    assert "source_keys_by_code" not in lookup.await_args.kwargs


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
