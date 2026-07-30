# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from api import ptg2_serving as serving
from api.ptg2_v4_graph import V4GraphRoot
from tests.ptg2_v4_orchestration_support import _tables


_NPI = 1_093_356_685
_PROVIDER_SET_KEY = 4_792


def _explicit_args() -> dict[str, object]:
    return {
        "npi": _NPI,
        "plan_id": "plan-1",
        "plan_market_type": "group",
        "code_system": "CPT",
        "code": "74329",
    }


@pytest.mark.asyncio
async def test_direct_exact_npi_defers_code_intersection_to_final_read(
    monkeypatch,
) -> None:
    """Return bounded NPI membership without a preliminary forward scan."""

    events: list[tuple[str, object]] = []

    async def graph_lookup(*_args, **kwargs):
        events.append(("graph", dict(kwargs)))
        assert kwargs["allowed_provider_set_keys"] is None
        assert kwargs["max_members"] == 64
        return {_NPI: (_PROVIDER_SET_KEY,)}

    rate_scope = AsyncMock(
        side_effect=AssertionError("exact NPI scope must not scan code rows")
    )

    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"d" * 32)),
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(
        serving,
        "_shared_rate_provider_set_keys",
        rate_scope,
    )

    observed = await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        _explicit_args(),
    )

    assert observed == serving._ExplicitNpiGraphScope(
        _NPI,
        (_PROVIDER_SET_KEY,),
    )
    assert [event_name for event_name, _payload in events] == ["graph"]
    rate_scope.assert_not_awaited()


@pytest.mark.parametrize(
    ("error_type", "message"),
    (
        (
            serving.PTG2SharedBlockError,
            "PTG V4 graph selection exceeds max_members",
        ),
        (
            serving.PTG2ManifestArtifactError,
            "PTG2 V4 graph selection exceeds max_members",
        ),
    ),
)
@pytest.mark.asyncio
async def test_direct_exact_npi_falls_back_after_bounded_graph_refusal(
    monkeypatch,
    error_type,
    message,
) -> None:
    """Retain the exact code-first path when the NPI graph is too broad."""

    events: list[tuple[str, object]] = []

    async def graph_lookup(*_args, **kwargs):
        events.append(("graph", dict(kwargs)))
        if kwargs.get("max_members") is not None:
            raise error_type(message)
        assert kwargs["allowed_provider_set_keys"] == frozenset({7, 8})
        return {_NPI: (8,)}

    async def rate_scope(*_args, **kwargs):
        events.append(("rate", dict(kwargs)))
        assert "provider_set_keys" not in kwargs
        return (7, 8)

    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"d" * 32)),
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(serving, "_shared_rate_provider_set_keys", rate_scope)

    observed = await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        _explicit_args(),
    )

    assert observed == serving._ExplicitNpiGraphScope(_NPI, (8,))
    assert [event_name for event_name, _payload in events] == [
        "graph",
        "rate",
        "graph",
    ]


@pytest.mark.asyncio
async def test_direct_exact_npi_requires_complete_graph_owner_result(
    monkeypatch,
) -> None:
    """Do not treat an omitted graph owner as a proven empty membership."""

    rate_scope = AsyncMock()
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"d" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(return_value={}),
    )
    monkeypatch.setattr(
        serving,
        "_shared_rate_provider_set_keys",
        rate_scope,
    )

    with pytest.raises(
        serving.PTG2ManifestArtifactError,
        match="exact-NPI projection is incomplete",
    ):
        await serving._version_three_explicit_npi_graph_scope(
            object(),
            _tables(),
            _explicit_args(),
        )
    rate_scope.assert_not_awaited()


@pytest.mark.asyncio
async def test_direct_exact_npi_integrity_failure_does_not_fallback(
    monkeypatch,
) -> None:
    """Fail closed instead of hiding a malformed direct graph as a wide scan."""

    rate_scope = AsyncMock()
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "direct_v1", b"d" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(
            side_effect=serving.PTG2SharedBlockError(
                "PTG V4 locator points outside its member page"
            )
        ),
    )
    monkeypatch.setattr(
        serving,
        "_shared_rate_provider_set_keys",
        rate_scope,
    )

    with pytest.raises(
        serving.PTG2SharedBlockError,
        match="locator points outside",
    ):
        await serving._version_three_explicit_npi_graph_scope(
            object(),
            _tables(),
            _explicit_args(),
        )
    rate_scope.assert_not_awaited()


@pytest.mark.parametrize("membership_count", (37, 299, 512))
@pytest.mark.asyncio
async def test_pattern_exact_npi_defers_code_intersection_to_final_read(
    monkeypatch,
    membership_count,
) -> None:
    """Use bounded pattern membership without a preliminary forward scan."""

    events: list[tuple[str, object]] = []
    provider_set_keys = tuple(range(1, membership_count + 1))
    rate_scope = AsyncMock(
        side_effect=AssertionError("exact NPI scope must not scan code rows")
    )

    async def graph_lookup(*_args, **kwargs):
        events.append(("graph", dict(kwargs)))
        assert kwargs["allowed_provider_set_keys"] is None
        assert kwargs["max_members"] == 512
        assert kwargs["max_projection_members"] == 2_048
        return {_NPI: provider_set_keys}

    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "_shared_rate_provider_set_keys",
        rate_scope,
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)

    observed = await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        _explicit_args(),
    )

    assert observed == serving._ExplicitNpiGraphScope(
        _NPI,
        provider_set_keys,
    )
    assert [event_name for event_name, _payload in events] == ["graph"]
    rate_scope.assert_not_awaited()


@pytest.mark.asyncio
async def test_pattern_exact_npi_never_reads_code_scope_during_graph_resolution(
    monkeypatch,
) -> None:
    """Leave code intersection to the provider-filtered serving read."""

    rate_scope = AsyncMock(
        side_effect=AssertionError("exact NPI scope must not scan code rows")
    )
    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )
    monkeypatch.setattr(
        serving,
        "_v4_sets_by_npi",
        AsyncMock(return_value={_NPI: tuple(range(1, 38))}),
    )
    monkeypatch.setattr(
        serving,
        "_shared_rate_provider_set_keys",
        rate_scope,
    )

    observed = await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        _explicit_args(),
    )

    assert observed == serving._ExplicitNpiGraphScope(
        _NPI,
        tuple(range(1, 38)),
    )
    rate_scope.assert_not_awaited()


@pytest.mark.parametrize(
    ("error_type", "message"),
    (
        (
            serving.PTG2SharedBlockError,
            "PTG V4 graph selection exceeds max_members",
        ),
        (
            serving.PTG2ManifestArtifactError,
            "PTG2 V4 graph selection exceeds max_members",
        ),
    ),
)
@pytest.mark.asyncio
async def test_pattern_exact_npi_falls_back_after_bounded_graph_refusal(
    monkeypatch,
    error_type,
    message,
) -> None:
    """Keep the existing code-first path for a pattern-heavy NPI."""

    events: list[tuple[str, object]] = []

    async def graph_lookup(*_args, **kwargs):
        events.append(("graph", dict(kwargs)))
        if kwargs.get("max_members") is not None:
            assert kwargs["max_members"] == 512
            assert kwargs["max_projection_members"] == 2_048
            raise error_type(message)
        assert kwargs["allowed_provider_set_keys"] == frozenset({7, 8})
        return {_NPI: (8,)}

    async def rate_scope(*_args, **kwargs):
        events.append(("rate", dict(kwargs)))
        assert "provider_set_keys" not in kwargs
        return (7, 8)

    monkeypatch.setattr(
        serving,
        "load_v4_graph_root",
        AsyncMock(return_value=V4GraphRoot(17, "pattern_v1", b"p" * 32)),
    )
    monkeypatch.setattr(serving, "_v4_sets_by_npi", graph_lookup)
    monkeypatch.setattr(serving, "_shared_rate_provider_set_keys", rate_scope)

    observed = await serving._version_three_explicit_npi_graph_scope(
        object(),
        _tables(),
        _explicit_args(),
    )

    assert observed == serving._ExplicitNpiGraphScope(_NPI, (8,))
    assert [event_name for event_name, _payload in events] == [
        "graph",
        "rate",
        "graph",
    ]
