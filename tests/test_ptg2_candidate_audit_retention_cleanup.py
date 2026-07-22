# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Failure-path proofs for candidate-audit decoded-retention ownership."""

from __future__ import annotations

import pytest

from api import ptg2_candidate_audit_forward as candidate_forward
from api import ptg2_candidate_audit_graph as candidate_graph
from api import ptg2_candidate_audit_maps as candidate_maps
from api import ptg2_candidate_audit_networks as candidate_networks
from api import ptg2_candidate_audit_occurrences as candidate_occurrences
from api import ptg2_candidate_audit_projection as candidate_projection
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)
from api.ptg2_types import PTG2ServingTables
from process.ptg_parts.ptg2_candidate_audit_batch_contract import (
    AuditBatchChallenge,
)
from process.ptg_parts.ptg2_manifest_artifacts import PTG2ManifestArtifactError


class _AuditAbort(BaseException):
    pass


class _NetworkSession:
    def __init__(self, rows):
        self.rows = rows

    async def execute(self, _statement, _params=None):
        return self.rows


class _AbortingItems(dict):
    def items(self):
        yield next(iter(super().items()))
        raise _AuditAbort("stop")


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


def test_integer_set_map_skips_empty_bucket_and_replaces_source_claims():
    source_map = {1: {7}, 2: set()}
    source_bytes = (
        candidate_maps.INTEGER_MAP_BYTES
        + 2 * candidate_maps.INTEGER_MAP_BUCKET_BYTES
        + candidate_maps.INTEGER_SET_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    budget.claim(source_bytes, category="source integer map")

    observed = candidate_maps.freeze_integer_set_map(
        source_map,
        budget,
        category="test",
    )

    expected_result_bytes = (
        candidate_maps.INTEGER_MAP_BYTES
        + candidate_maps.INTEGER_MAP_ENTRY_BYTES
        + candidate_maps.INTEGER_TUPLE_BYTES
        + candidate_maps.INTEGER_TUPLE_MEMBERSHIP_BYTES
    )
    assert observed == {1: (7,)}
    assert budget.retained_bytes == expected_result_bytes


def test_integer_set_map_releases_partial_result_on_overflow():
    first_entry_peak = (
        candidate_maps.INTEGER_MAP_ENTRY_BYTES
        + candidate_maps.INTEGER_TUPLE_PEAK_BYTES
        + candidate_maps.INTEGER_TUPLE_PEAK_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=candidate_maps.INTEGER_MAP_BYTES + first_entry_peak
    )

    with pytest.raises(CandidateAuditDecodedRetentionError, match="frozen test tuple"):
        candidate_maps.freeze_integer_set_map(
            {1: {7}, 2: {8}},
            budget,
            category="test",
        )

    assert budget.retained_bytes == 0


def test_graph_helpers_deduplicate_npis_providers_and_reverse_keys():
    npi = 1234567890

    observed = candidate_graph._provider_keys_for_challenges(
        (npi, npi),
        {npi: (4, 4)},
        {4: (7, 7)},
        None,
    )

    assert observed == {npi: {7}}
    assert candidate_graph._reverse_provider_set_keys({npi: (7, 7)}, None) == (7,)
    assert candidate_graph._proven_provider_keys_by_npi(
        {npi: (7,)},
        {npi: (4,)},
        {7: (4,)},
        None,
    ) == {npi: (7,)}


def test_graph_freeze_releases_partial_result_on_overflow():
    source_map = {1: {7}, 2: {8}}
    source_bytes = candidate_graph._NPI_PROVIDER_MAP_BYTES + 2 * (
        candidate_graph._NPI_PROVIDER_MAP_ENTRY_BYTES
        + candidate_graph._NPI_PROVIDER_SET_BYTES
        + candidate_graph._NPI_PROVIDER_SET_MEMBERSHIP_BYTES
    )
    first_entry_peak = (
        candidate_graph._NPI_PROVIDER_MAP_ENTRY_BYTES
        + candidate_graph._PROVIDER_KEY_ORDERING_BYTES
        + candidate_graph._PROVIDER_KEY_ORDERING_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            source_bytes + candidate_graph._NPI_PROVIDER_MAP_BYTES + first_entry_peak
        )
    )
    budget.claim(source_bytes, category="source NPI provider map")

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="frozen NPI provider tuple",
    ):
        candidate_graph._freeze_provider_keys_by_npi(source_map, budget)

    assert budget.retained_bytes == source_bytes


@pytest.mark.asyncio
async def test_network_name_empty_and_incomplete_reads_release_all_claims():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)
    assert (
        await candidate_networks.provider_network_names_by_key(
            _NetworkSession(()),
            _serving_tables(),
            (),
            budget,
        )
        == {}
    )
    assert budget.retained_bytes == 0

    assert (
        await candidate_networks.provider_network_names_by_key(
            _NetworkSession(()),
            _serving_tables(),
            (),
        )
        == {}
    )
    with pytest.raises(PTG2ManifestArtifactError, match="incomplete"):
        await candidate_networks.provider_network_names_by_key(
            _NetworkSession(()),
            _serving_tables(),
            (5,),
        )

    with pytest.raises(PTG2ManifestArtifactError, match="incomplete"):
        await candidate_networks.provider_network_names_by_key(
            _NetworkSession(()),
            _serving_tables(),
            (5,),
            budget,
        )
    assert budget.retained_bytes == 0


@pytest.mark.asyncio
async def test_network_name_duplicate_releases_nested_decode_claims():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=16 * 1024)
    duplicate_rows = (
        {"provider_set_key": 5, "network_names": ["Alpha"]},
        {"provider_set_key": 5, "network_names": ["Beta"]},
    )

    with pytest.raises(PTG2ManifestArtifactError, match="duplicated"):
        await candidate_networks.provider_network_names_by_key(
            _NetworkSession(duplicate_rows),
            _serving_tables(),
            (5,),
            budget,
        )

    assert budget.retained_bytes == 0
    with pytest.raises(PTG2ManifestArtifactError, match="duplicated"):
        await candidate_networks.provider_network_names_by_key(
            _NetworkSession(duplicate_rows),
            _serving_tables(),
            (5,),
        )


@pytest.mark.asyncio
async def test_network_digest_base_exception_releases_names_and_digests(monkeypatch):
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=32 * 1024)
    network_rows = (
        {"provider_set_key": 5, "network_names": ["Alpha"]},
        {"provider_set_key": 6, "network_names": ["Beta"]},
    )
    original_digest_builder = candidate_networks.canonical_network_name_digests

    def abort_second_digest(network_names):
        if tuple(network_names) == ("Beta",):
            raise _AuditAbort("stop")
        return original_digest_builder(network_names)

    monkeypatch.setattr(
        candidate_networks,
        "canonical_network_name_digests",
        abort_second_digest,
    )

    with pytest.raises(_AuditAbort, match="stop"):
        await candidate_networks.provider_network_digests_by_key(
            _NetworkSession(network_rows),
            _serving_tables(),
            {7: (5, 6)},
            budget,
        )

    assert budget.retained_bytes == 0
    with pytest.raises(_AuditAbort, match="stop"):
        await candidate_networks.provider_network_digests_by_key(
            _NetworkSession((network_rows[1],)),
            _serving_tables(),
            {7: (6,)},
        )


@pytest.mark.asyncio
async def test_network_digest_without_budget_replaces_names_with_digests():
    observed = await candidate_networks.provider_network_digests_by_key(
        _NetworkSession(({"provider_set_key": 5, "network_names": ["Alpha"]},)),
        _serving_tables(),
        {7: (5,)},
    )

    assert set(observed) == {5}
    assert len(observed[5]) == 1


def test_candidate_price_scope_rejects_ambiguous_compatibility_shape():
    with pytest.raises(TypeError, match="requires provider indexes"):
        candidate_forward._candidate_price_scope(({},))


@pytest.mark.asyncio
async def test_candidate_hydration_base_exception_releases_price_key_set():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=4096)

    async def abort_hydration(*_args, **_kwargs):
        raise _AuditAbort("stop")

    with pytest.raises(_AuditAbort, match="stop"):
        await candidate_forward._hydrate_candidate_prices(
            object(),
            _serving_tables(),
            {(7, 5, 0): (8, 9)},
            budget,
            abort_hydration,
        )

    assert budget.retained_bytes == 0


def test_preloaded_forward_filter_releases_partial_result_on_overflow():
    first_occurrence_bytes = (
        candidate_forward._PRELOADED_FORWARD_OCCURRENCE_BYTES
        + candidate_forward._PRELOADED_FORWARD_PRICE_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            candidate_forward._PRELOADED_FORWARD_MAP_BYTES + first_occurrence_bytes
        )
    )
    first_key = (7, 5, 0)
    second_key = (7, 6, 0)

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="filtered preloaded forward occurrence",
    ):
        candidate_forward.filter_preloaded_price_index(
            {first_key: (8,), second_key: (9,)},
            frozenset((first_key, second_key)),
            budget,
        )

    assert budget.retained_bytes == 0


def test_preloaded_forward_filter_without_budget_preserves_exact_scope():
    required_key = (7, 5, 0)

    assert candidate_forward.filter_preloaded_price_index(
        {required_key: (8,), (7, 6, 0): (9,)},
        frozenset((required_key,)),
        None,
    ) == {required_key: (8,)}


def test_preloaded_forward_filter_propagates_base_exception_without_budget():
    required_key = (7, 5, 0)

    with pytest.raises(_AuditAbort, match="stop"):
        candidate_forward.filter_preloaded_price_index(
            _AbortingItems({required_key: (8,)}),
            frozenset((required_key,)),
            None,
        )


def test_required_occurrences_deduplicate_coordinates_and_memberships():
    challenge = _challenge()

    observed = candidate_occurrences.required_candidate_occurrence_keys(
        (challenge, challenge),
        {("CPT", "99213"): ({"code_key": 7},)},
        {(challenge.npi, 7): (5, 5)},
    )

    assert observed == frozenset(((7, 5, 0),))


def test_required_occurrence_overflow_releases_all_temporary_claims():
    challenge = _challenge()
    temporary_bytes = (
        2 * candidate_occurrences._OCCURRENCE_SET_BYTES
        + candidate_occurrences._REQUESTED_COORDINATE_BYTES
        + candidate_occurrences._REQUIRED_OCCURRENCE_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=temporary_bytes)

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="frozen required occurrence set",
    ):
        candidate_occurrences.required_candidate_occurrence_keys(
            (challenge,),
            {("CPT", "99213"): ({"code_key": 7},)},
            {(challenge.npi, 7): (5,)},
            retention_budget=budget,
        )

    assert budget.retained_bytes == 0


def test_required_occurrence_propagates_lookup_failure_without_budget():
    with pytest.raises(KeyError):
        candidate_occurrences.required_candidate_occurrence_keys(
            (_challenge(),),
            {},
            {},
        )


def test_projection_coordinate_overflow_releases_partial_set():
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            candidate_projection._COORDINATE_SET_BYTES
            + candidate_projection._COORDINATE_SET_MEMBERSHIP_BYTES
        )
    )

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="ordered candidate coordinates",
    ):
        candidate_projection._requested_candidate_coordinates(
            (_challenge(),),
            budget,
        )

    assert budget.retained_bytes == 0


def test_projection_coordinates_deduplicate_before_freezing():
    challenge = _challenge()

    coordinates, _retained_bytes = (
        candidate_projection._requested_candidate_coordinates(
            (challenge, challenge),
        )
    )

    assert coordinates == (("CPT", "99213", challenge.npi, 0),)


def test_projection_finalize_releases_partial_result_on_overflow():
    source_bytes = (
        candidate_projection._PROJECTION_MAP_BYTES
        + candidate_projection._AVAILABILITY_MAP_BYTES
    )
    first_result_bytes = (
        candidate_projection._AVAILABILITY_RESULT_ENTRY_BYTES
        + candidate_projection._AVAILABILITY_RESULT_MEMBERSHIP_BYTES
    )
    budget = CandidateAuditDecodedRetentionBudget(
        maximum_bytes=(
            source_bytes
            + candidate_projection._AVAILABILITY_RESULT_MAP_BYTES
            + first_result_bytes
        )
    )
    state = candidate_projection._CandidateAvailabilityState(retention_budget=budget)
    state.network_sets_by_condition = {
        ("CPT", "1", 1, 0, "a"): {frozenset(("alpha",))},
        ("CPT", "2", 2, 0, "b"): {frozenset(("beta",))},
    }

    with pytest.raises(
        CandidateAuditDecodedRetentionError,
        match="frozen candidate availability condition",
    ):
        state.finalized()

    assert budget.retained_bytes == source_bytes


def test_projection_cleanup_propagates_base_exceptions_without_budget():
    with pytest.raises(AttributeError):
        candidate_projection._requested_candidate_coordinates((object(),))

    state = candidate_projection._CandidateAvailabilityState()
    state.network_sets_by_condition = _AbortingItems(
        {("CPT", "1", 1, 0, "a"): {frozenset(("alpha",))}}
    )
    with pytest.raises(_AuditAbort, match="stop"):
        state.finalized()
