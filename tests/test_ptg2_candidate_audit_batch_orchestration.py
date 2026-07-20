from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import ANY, AsyncMock, Mock

import pytest

from api import ptg2_candidate_audit_batch as batch
from api import ptg2_candidate_audit_codes as codes
from api import ptg2_candidate_audit_selection as selection
from api.ptg2_candidate_audit import PTG2CandidateAuditAccess
from api.ptg2_candidate_audit_integrity import (
    CandidateWitnessScope,
    PersistedAuditOccurrence,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
    AuditBatchRequest,
)


def _challenge() -> AuditBatchChallenge:
    return AuditBatchChallenge(
        code_system="CPT",
        code="99213",
        npi=1234567890,
        source_artifact_key=0,
        tuple_digest="a" * 64,
        network_name_digests=("b" * 64,),
        multiplicity=1,
    )


def _persisted_occurrence() -> PersistedAuditOccurrence:
    return PersistedAuditOccurrence(
        occurrence_id=b"p" * 32,
        code_key=8,
        provider_set_key=7,
        price_key=9,
        source_artifact_key=1,
        npi=1111111111,
        atom_ordinal=0,
        atom_key=10,
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


def _access() -> PTG2CandidateAuditAccess:
    return PTG2CandidateAuditAccess(
        snapshot_id="candidate",
        source_key="source",
        plan_id="plan",
        plan_market_type="group",
    )


def _audit_request() -> AuditBatchRequest:
    return AuditBatchRequest(
        snapshot_id="candidate",
        source_key="source",
        plan_id="plan",
        plan_market_type="group",
        audit_sample_digest="c" * 64,
        source_witness_sample_digest="d" * 64,
        source_witness_payload_sha256="e" * 64,
        ordered_source_ordinal_digest="f" * 64,
        challenge_count=1,
        request_digest="1" * 64,
    )


@pytest.mark.asyncio
async def test_provider_graph_lookup_unions_witness_and_persisted_npis(monkeypatch):
    graph_lookup = AsyncMock(
        side_effect=[
            {1234567890: (2, 3), 1111111111: (4,)},
            {2: (5,), 3: (6,), 4: (7,)},
        ]
    )
    monkeypatch.setattr(batch, "lookup_shared_graph_members_from_db", graph_lookup)

    provider_keys_by_npi = await batch._provider_set_keys_by_npi(
        object(),
        _serving_tables(),
        (_challenge(),),
        (_persisted_occurrence(),),
    )

    assert provider_keys_by_npi == {
        1111111111: (7,),
        1234567890: (5, 6),
    }
    assert graph_lookup.await_count == 2


@pytest.mark.asyncio
async def test_code_query_builds_one_plan_scoped_union(monkeypatch):
    expected_index = codes.CandidateCodeIndex(by_pair={}, by_key={})
    indexer = Mock(return_value=expected_index)
    monkeypatch.setattr(codes, "_indexed_candidate_code_records", indexer)
    monkeypatch.setattr(
        codes,
        "_shared_v3_code_scope_sql",
        lambda *_args, **_kwargs: (
            "JOIN scoped_plan ON TRUE",
            ["scoped_plan.allowed"],
            {"scope": "plan"},
            "",
        ),
    )
    session = SimpleNamespace(execute=AsyncMock(return_value=()))

    observed_index = await codes.candidate_code_records_by_pair(
        session,
        _serving_tables(),
        _access(),
        (_challenge(),),
        persisted_code_keys=(8,),
    )

    assert observed_index is expected_index
    query_params = session.execute.await_args.args[1]
    assert query_params["shared_snapshot_key"] == 41
    assert query_params["persisted_code_keys"] == (8,)
    assert query_params["scope"] == "plan"


@pytest.mark.asyncio
async def test_candidate_scope_indexes_resolve_each_coordinate_family_once(
    monkeypatch,
):
    challenge = _challenge()
    persisted = _persisted_occurrence()
    code_index = codes.CandidateCodeIndex(
        by_pair={("CPT", "99213"): ({"code_key": 7},)},
        by_key={7: {"code_key": 7}, 8: {"code_key": 8}},
    )
    code_lookup = AsyncMock(return_value=code_index)
    provider_lookup = AsyncMock(
        return_value={challenge.npi: (5, 6), persisted.npi: (7, 8, 9)}
    )
    graph_validator = Mock()
    provider_code_lookup = AsyncMock(
        return_value={
            5: frozenset((7,)),
            6: frozenset((9,)),
            7: frozenset((8,)),
        }
    )
    network_lookup = AsyncMock(return_value={5: frozenset({"a"}), 7: frozenset({"b"})})
    monkeypatch.setattr(batch, "candidate_code_records_by_pair", code_lookup)
    monkeypatch.setattr(batch, "_provider_set_keys_by_npi", provider_lookup)
    monkeypatch.setattr(batch, "validate_persisted_audit_graph_scope", graph_validator)
    monkeypatch.setattr(
        selection,
        "load_candidate_provider_code_sets",
        provider_code_lookup,
    )
    monkeypatch.setattr(batch, "_provider_network_digests_by_key", network_lookup)

    indexes = await batch._candidate_scope_indexes(
        object(),
        _serving_tables(),
        _access(),
        (challenge,),
        (persisted,),
    )

    assert indexes.code_index is code_index
    assert indexes.provider_sets_by_npi_code == {
        (challenge.npi, 7): (5,),
        (persisted.npi, 8): (7,),
    }
    assert indexes.provider_filters_by_code_key == {7: (5,), 8: (7,)}
    graph_validator.assert_called_once()
    provider_code_lookup.assert_awaited_once()
    assert set(provider_code_lookup.await_args.args[2]) == {5, 6, 7}
    assert set(provider_code_lookup.await_args.args[3]) == {7, 8}
    network_lookup.assert_awaited_once()
    assert network_lookup.await_args.args[2] == {7: (5,), 8: (7,)}


@pytest.mark.asyncio
async def test_provider_network_digest_projection_uses_one_metadata_read(monkeypatch):
    network_lookup = AsyncMock(return_value={5: ("Alpha",), 7: ("Beta",)})
    monkeypatch.setattr(batch, "_provider_network_names_by_key", network_lookup)

    observed = await batch._provider_network_digests_by_key(
        object(),
        _serving_tables(),
        {7: (5,), 8: (7,)},
    )

    assert set(observed) == {5, 7}
    network_lookup.assert_awaited_once()


@pytest.mark.asyncio
async def test_candidate_price_data_hydrates_retained_prices_once(monkeypatch):
    forward_index = {(7, 5, 0): (8,)}
    forward_lookup = AsyncMock(return_value=forward_index)
    hydration = SimpleNamespace(
        atom_keys_by_price_key={8: (9,)},
        prices_by_key={8: [{"negotiated_rate": "1.00"}]},
    )
    hydrate_prices = AsyncMock(return_value=hydration)
    monkeypatch.setattr(batch, "_candidate_forward_price_keys", forward_lookup)
    monkeypatch.setattr(batch, "_version_three_price_hydration", hydrate_prices)

    price_load = await batch._load_candidate_price_data(
        object(),
        _serving_tables(),
        (_challenge(),),
        {("CPT", "99213"): ({"code_key": 7},)},
        {(1234567890, 7): (5,)},
        {7: (5,)},
    )

    assert price_load.data.price_keys_by_occurrence == forward_index
    assert price_load.data.atom_keys_by_price_key == {8: (9,)}
    assert price_load.selection_io == {
        "exact_candidate_occurrence_coordinates": 1,
        "exact_forward_occurrence_coordinates_returned": 1,
        "exact_forward_price_key_deliveries_returned": 1,
    }
    hydrate_prices.assert_awaited_once_with(
        ANY,
        ANY,
        {8},
        copy_payloads=False,
    )


@pytest.mark.asyncio
async def test_candidate_audit_data_runs_each_bounded_index_once(monkeypatch):
    challenge = _challenge()
    persisted = _persisted_occurrence()
    witness_scope = CandidateWitnessScope(
        challenges=(challenge,),
        record_count=1,
        unique_evidence_count=1,
        evidence_reference_count=1,
        persisted_audit_occurrences=(persisted,),
    )
    code_index = codes.CandidateCodeIndex(
        by_pair={("CPT", "99213"): ({"code_key": 7},)},
        by_key={7: {"code_key": 7}},
    )
    scope_indexes = batch._CandidateScopeIndexes(
        code_index=code_index,
        provider_sets_by_npi_code={(challenge.npi, 7): (5,)},
        provider_filters_by_code_key={7: (5,)},
        network_digests_by_key={5: frozenset({"network"})},
    )
    price_data = batch.CandidatePriceData(
        {(7, 5, 0): (8,)},
        {8: (9,)},
        {8: [{"negotiated_rate": "1.00"}]},
    )
    monkeypatch.setattr(batch, "snapshot_serving_tables", AsyncMock(return_value=_serving_tables()))
    monkeypatch.setattr(batch, "validate_candidate_source_scope", AsyncMock(return_value=witness_scope))
    monkeypatch.setattr(batch, "_candidate_scope_indexes", AsyncMock(return_value=scope_indexes))
    price_load = batch._CandidatePriceLoad(
        data=price_data,
        selection_io={"exact_candidate_occurrence_coordinates": 1},
    )
    monkeypatch.setattr(
        batch,
        "_load_candidate_price_data",
        AsyncMock(return_value=price_load),
    )
    price_validator = Mock()
    monkeypatch.setattr(batch, "validate_persisted_audit_price_scope", price_validator)
    monkeypatch.setattr(
        batch,
        "candidate_availability_index",
        Mock(return_value=({("condition",): ()}, {"candidate_projection_builds": 1})),
    )

    audit_data = await batch._candidate_audit_data(
        object(),
        _audit_request(),
        _access(),
    )

    assert audit_data.challenges == (challenge,)
    assert audit_data.persisted_audit_occurrence_count == 1
    assert audit_data.witness_io["payload_reads"] == 1
    assert audit_data.candidate_processing_io == {"candidate_projection_builds": 1}
    price_validator.assert_called_once_with((persisted,), price_data)
