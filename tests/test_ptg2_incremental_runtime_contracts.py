# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Integrity contracts for incremental V4 provider expansion state."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving


_PROVIDER_SET_ID = "01" * 16
_OTHER_PROVIDER_SET_ID = "02" * 16


def _rate_row(provider_set_id=_PROVIDER_SET_ID, provider_set_key=7, **fields):
    return {
        "provider_set_global_id_128": provider_set_id,
        "_ptg_provider_set_key": provider_set_key,
        "serving_content_hash_128": "03" * 16,
        "reported_code_system": "CPT",
        "reported_code": "00001",
        "negotiation_arrangement": "FFS",
        "source_key": 7,
        **fields,
    }


def _state(rows, *selected_provider_set_ids):
    state = serving._IncrementalProviderExpansionState(target_count=2)
    state.row_data.extend(rows)
    state.selected_provider_set_ids.update(
        {provider_set_id: None for provider_set_id in selected_provider_set_ids}
    )
    return state


@pytest.mark.parametrize(
    ("state", "error_match"),
    [
        (
            _state([_rate_row(_OTHER_PROVIDER_SET_ID)], _PROVIDER_SET_ID),
            "missing a selected provider-set key",
        ),
        (
            _state([_rate_row(provider_set_key=True)], _PROVIDER_SET_ID),
            "invalid provider-set key",
        ),
        (
            _state([_rate_row(provider_set_key=None)], _PROVIDER_SET_ID),
            "missing its provider-set key",
        ),
        (
            _state(
                [_rate_row(provider_set_key=7), _rate_row(provider_set_key=8)],
                _PROVIDER_SET_ID,
            ),
            "disagree on provider-set identity",
        ),
        (
            _state(
                [_rate_row(provider_set_key=7)],
                _PROVIDER_SET_ID,
                _OTHER_PROVIDER_SET_ID,
            ),
            "missing a selected provider-set key",
        ),
    ],
)
def test_incremental_selected_set_keys_fail_closed(state, error_match):
    """Reject skipped, malformed, contradictory, or missing selected set keys."""

    with pytest.raises(serving.PTG2ManifestArtifactError, match=error_match):
        serving._incremental_selected_provider_set_keys(state)


@pytest.mark.parametrize(
    ("entries", "error_match"),
    [
        ([SimpleNamespace(provider_set_key=True)], "invalid provider-set key"),
        ([SimpleNamespace(provider_set_key=None)], "missing a provider-set key"),
        ([], "missing its provider sets"),
    ],
)
def test_incremental_code_scope_requires_integer_provider_sets(
    entries,
    error_match,
):
    """Reject compact code scopes without valid integer set coordinates."""

    with pytest.raises(serving.PTG2ManifestArtifactError, match=error_match):
        serving._incremental_code_scope_provider_set_keys(entries)


def test_incremental_prefix_rejects_inconsistent_provider_counts():
    """Require one authenticated provider count for each repeated set row."""

    state = serving._IncrementalProviderExpansionState(target_count=2)
    rate_rows = [
        _rate_row(provider_count=1),
        _rate_row(provider_count=2),
    ]

    with pytest.raises(serving.PTG2ManifestArtifactError, match="disagree on their provider-set count"):
        serving._validate_incremental_provider_set_prefixes(
            (_PROVIDER_SET_ID,),
            rate_rows,
            {_PROVIDER_SET_ID: (1234567890,)},
            state,
        )


def test_incremental_prefix_requires_authenticated_empty_set():
    """Distinguish an authenticated empty set from a missing graph owner."""

    state = serving._IncrementalProviderExpansionState(target_count=2)

    with pytest.raises(serving.PTG2ManifestArtifactError, match="missing an authenticated provider set"):
        serving._validate_incremental_provider_set_prefixes(
            (_PROVIDER_SET_ID,),
            [_rate_row(provider_count=0)],
            {},
            state,
        )


def _incremental_request() -> serving._IncrementalProviderExpansionRequest:
    return serving._IncrementalProviderExpansionRequest(
        code_rows=[{"code_key": 7}],
        args={"plan_id": "synthetic-plan"},
        snapshot_id="synthetic-snapshot",
        source_trace_set_hash=None,
        network_names=[],
        target_count=1,
        descending=False,
        declared_rate_count=1,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("read_result", "expected"),
    [
        ((False, [], False), None),
        ((True, [], False), "materialized"),
        ((True, [_rate_row()], True), "materialized"),
    ],
)
async def test_v4_selector_preserves_unsupported_empty_and_short_pages(
    monkeypatch,
    read_result,
    expected,
):
    """Propagate unsupported reads and mark empty or short pages exhausted."""

    monkeypatch.setattr(
        serving,
        "_v4_provider_expansion_request_caps",
        lambda *_args, **_kwargs: serving._V4ProviderExpansionRequestCaps(
            rate_page_rows=64,
            maximum_rate_rows=100,
            maximum_provider_sets=100,
            maximum_graph_batches=10,
        ),
    )
    monkeypatch.setattr(
        serving,
        "_read_incremental_v4_rate_page",
        AsyncMock(return_value=read_result),
    )
    consume = AsyncMock()
    monkeypatch.setattr(serving, "_consume_incremental_v4_rate_page", consume)

    async def materialize(*_args, **kwargs):
        assert kwargs["exhausted"] is True
        return "materialized"

    monkeypatch.setattr(
        serving,
        "_materialize_incremental_provider_selection",
        materialize,
    )

    selection = await serving._select_v4_provider_expansion(
        object(),
        SimpleNamespace(),
        _incremental_request(),
    )

    assert selection == expected
    assert consume.await_count == (1 if read_result[1] else 0)
