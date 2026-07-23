from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_candidate_audit_dimensions as dimensions
from api import ptg2_candidate_audit_provider_code_sets as provider_codes
from api import ptg2_candidate_audit_selection as selection
from api import ptg2_db_sidecars
from api.ptg2_candidate_audit_capacity import (
    CandidateAuditDecodedRetentionBudget,
    CandidateAuditDecodedRetentionError,
)
from api.ptg2_candidate_audit_codes import CandidateCodeIndex


def _dimension_case():
    first = SimpleNamespace(code_system="CPT", code="7", npi=1)
    second = SimpleNamespace(code_system="CPT", code="8", npi=2)
    code_index = CandidateCodeIndex(
        by_pair={
            ("CPT", "7"): ({"code_key": 7},),
            ("CPT", "8"): ({"code_key": 8},),
        },
        by_key={},
    )
    provider_keys_by_npi = {first.npi: (5, 6), second.npi: (6, 7)}
    return first, second, code_index, provider_keys_by_npi


def test_provider_request_upper_bound_stops_before_pair_materialization():
    first, _second, code_index, provider_keys_by_npi = _dimension_case()
    expanded_code_index = CandidateCodeIndex(
        by_pair={
            ("CPT", "7"): (
                code_index.by_pair[("CPT", "7")][0],
                {"code_key": 8},
            )
        },
        by_key={},
    )

    upper_bound = dimensions.provider_code_request_upper_bound(
        (first, first),
        expanded_code_index,
        provider_keys_by_npi,
        (),
        maximum_memberships=3,
    )

    assert upper_bound == 4


def test_provider_request_upper_bound_stops_on_persisted_scope():
    occurrence = SimpleNamespace(provider_set_key=5, code_key=7)

    upper_bound = dimensions.provider_code_request_upper_bound(
        (),
        CandidateCodeIndex(by_pair={}, by_key={}),
        {},
        (occurrence, occurrence),
        maximum_memberships=1,
    )

    assert upper_bound == 2


@pytest.mark.asyncio
async def test_dense_provider_request_uses_dimension_first_intersection(
    monkeypatch,
):
    first, second, code_index, provider_keys_by_npi = _dimension_case()
    persisted = SimpleNamespace(
        provider_set_key=9,
        code_key=11,
        npi=3,
    )
    provider_keys_by_npi[persisted.npi] = (persisted.provider_set_key,)
    monkeypatch.setattr(
        selection,
        "PTG2_AUDIT_BATCH_MAX_PROVIDER_CODE_SCOPE",
        3,
    )
    prepared_lookup = AsyncMock()
    dimension_lookup = AsyncMock(
        return_value={
            5: frozenset((7,)),
            6: frozenset((8,)),
            7: frozenset(),
            9: frozenset((11,)),
        }
    )
    monkeypatch.setattr(
        selection,
        "_load_candidate_provider_code_sets_prepared",
        prepared_lookup,
    )
    monkeypatch.setattr(
        selection,
        "load_candidate_provider_code_sets",
        dimension_lookup,
    )

    provider_scope, provider_filters = await selection.load_candidate_provider_indexes(
        object(),
        41,
        (first, second),
        code_index,
        provider_keys_by_npi,
        (persisted,),
        schema_name="mrf",
    )

    assert provider_scope == {
        (first.npi, 7): (5,),
        (second.npi, 8): (6,),
        (persisted.npi, persisted.code_key): (persisted.provider_set_key,),
    }
    assert provider_filters == {7: (5,), 8: (6,), 11: (9,)}
    prepared_lookup.assert_not_awaited()
    assert set(dimension_lookup.await_args.args[2]) == {5, 6, 7, 9}
    assert set(dimension_lookup.await_args.args[3]) == {7, 8, 11}


@pytest.mark.asyncio
async def test_dimension_lookup_transfers_budget_and_prunes_empty_sets(
    monkeypatch,
):
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)
    provider_code_map = {5: (7,), 6: ()}
    retained_lookup_bytes = provider_codes.decoded_provider_code_bytes(
        provider_code_map
    )

    async def lookup(*_args, **kwargs):
        kwargs["decoded_retention_budget"].claim(
            retained_lookup_bytes,
            category="the mocked provider/code lookup map",
        )
        return provider_code_map

    lookup_mock = AsyncMock(side_effect=lookup)
    monkeypatch.setattr(
        selection,
        "lookup_shared_provider_code_intersections_from_db",
        lookup_mock,
    )

    observed_provider_code_sets = (
        await selection.load_candidate_provider_code_sets(
            object(),
            41,
            (5, 6),
            (7,),
            schema_name="mrf",
            retention_budget=budget,
        )
    )

    assert observed_provider_code_sets == {5: frozenset((7,))}
    assert budget.retained_bytes == provider_codes.frozen_provider_code_bytes(
        observed_provider_code_sets
    )
    assert budget.peak_retained_bytes >= (
        retained_lookup_bytes
        + provider_codes.frozen_provider_code_bytes(observed_provider_code_sets)
    )
    assert lookup_mock.await_args.kwargs["decoded_retention_budget"] is budget
    assert lookup_mock.await_args.kwargs["inputs_are_normalized"] is True


@pytest.mark.asyncio
async def test_incomplete_dimension_lookup_releases_decoded_source(
    monkeypatch,
):
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)
    provider_code_map = {5: (7,)}

    async def lookup(*_args, **kwargs):
        kwargs["decoded_retention_budget"].claim(
            provider_codes.decoded_provider_code_bytes(provider_code_map),
            category="the mocked provider/code lookup map",
        )
        return provider_code_map

    monkeypatch.setattr(
        selection,
        "lookup_shared_provider_code_intersections_from_db",
        AsyncMock(side_effect=lookup),
    )

    with pytest.raises(
        Exception,
        match="missing a referenced provider set",
    ):
        await selection.load_candidate_provider_code_sets(
            object(),
            41,
            (5, 6),
            (7,),
            schema_name="mrf",
            retention_budget=budget,
        )

    assert budget.retained_bytes == 0


def test_failed_frozenset_conversion_releases_source_and_result(
    monkeypatch,
):
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)
    provider_code_map = {5: (7,)}
    budget.claim(
        provider_codes.decoded_provider_code_bytes(provider_code_map),
        category="the mocked provider/code lookup map",
    )

    def abort_conversion(_code_keys):
        raise RuntimeError("conversion failed")

    monkeypatch.setattr(
        provider_codes,
        "frozenset",
        abort_conversion,
        raising=False,
    )

    with pytest.raises(RuntimeError, match="conversion failed"):
        provider_codes.freeze_nonempty_provider_code_sets(
            provider_code_map,
            budget,
        )

    assert budget.retained_bytes == 0


def test_frozen_map_base_rejection_releases_decoded_source():
    provider_code_map = {5: (7,)}
    source_bytes = provider_codes.decoded_provider_code_bytes(
        provider_code_map
    )
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=source_bytes)
    budget.claim(source_bytes, category="the mocked provider/code lookup map")

    with pytest.raises(CandidateAuditDecodedRetentionError, match="byte limit"):
        provider_codes.freeze_nonempty_provider_code_sets(
            provider_code_map,
            budget,
        )

    assert budget.retained_bytes == 0


def test_dimension_normalization_without_budget_releases_on_code_limit():
    with pytest.raises(Exception, match="requested code scope"):
        provider_codes._retained_provider_code_dimensions(
            (5,),
            (7, 8),
            None,
            maximum_provider_keys=2,
            maximum_code_keys=1,
        )


def test_dimension_normalization_with_budget_releases_on_code_limit():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)

    with pytest.raises(Exception, match="requested code scope"):
        provider_codes._retained_provider_code_dimensions(
            (5,),
            (7, 8),
            budget,
            maximum_provider_keys=2,
            maximum_code_keys=1,
        )

    assert budget.retained_bytes == 0


def test_budgeted_scope_and_filters_cover_duplicate_memberships():
    first, _second, code_index, _provider_keys_by_npi = _dimension_case()
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)

    provider_scope = selection.candidate_provider_scope_by_npi_code(
        (first, first),
        code_index,
        {first.npi: (5, 6)},
        {5: frozenset((7,)), 6: frozenset()},
        retention_budget=budget,
    )
    provider_filters = selection.provider_filters_by_code_key(
        {
            (first.npi, 7): (5, 6),
            (2, 7): (6, 7),
        },
        budget,
    )

    assert provider_scope == {(first.npi, 7): (5,)}
    assert provider_filters == {7: (5, 6, 7)}


@pytest.mark.parametrize(
    "integer_keys",
    ([1], (-1,), (1, 1), (True,)),
)
def test_prepared_dimensions_reject_noncanonical_keys(integer_keys):
    with pytest.raises(Exception, match="prepared provider keys"):
        ptg2_db_sidecars._validated_prepared_keys(
            integer_keys,
            category="provider",
        )


@pytest.mark.parametrize("with_budget", (False, True))
def test_provider_offset_workspace_releases_on_iteration_failure(with_budget):
    budget = (
        CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)
        if with_budget
        else None
    )

    def provider_keys():
        yield 1
        raise RuntimeError("iteration failed")

    with pytest.raises(RuntimeError, match="iteration failed"):
        ptg2_db_sidecars._requested_offsets_by_block(
            provider_keys(),
            block_span=1024,
            decoded_retention_budget=budget,
        )

    if budget is not None:
        assert budget.retained_bytes == 0


def test_provider_offset_workspace_deduplicates_requested_offsets():
    budget = CandidateAuditDecodedRetentionBudget(maximum_bytes=1024 * 1024)

    offsets_by_block, retained_bytes = (
        ptg2_db_sidecars._requested_offsets_by_block(
            (1, 1, 2, 1025),
            block_span=1024,
            decoded_retention_budget=budget,
        )
    )

    assert offsets_by_block == {0: {1, 2}, 1: {1}}
    assert budget.retained_bytes == retained_bytes
    budget.release(retained_bytes)
