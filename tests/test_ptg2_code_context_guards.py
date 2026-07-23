from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from api import ptg2_code_context as code_context


@pytest.mark.asyncio
async def test_code_crosswalk_skips_empty_scope_and_query_failure():
    assert await code_context._query_ptg2_code_crosswalk_edges(
        object(),
        set(),
    ) == []

    session = SimpleNamespace(
        execute=AsyncMock(side_effect=RuntimeError("crosswalk unavailable"))
    )
    assert await code_context._query_ptg2_code_crosswalk_edges(
        session,
        {("CPT", "7")},
    ) == []


@pytest.mark.asyncio
async def test_code_context_rejects_empty_canonical_code(monkeypatch):
    monkeypatch.setattr(
        code_context,
        "canonical_catalog_code",
        lambda _system, _code: "",
    )

    assert await code_context._resolve_ptg2_code_search_context(
        object(),
        code="7",
        code_system="CPT",
    ) is None


@pytest.mark.asyncio
async def test_code_context_rejects_unsupported_system(monkeypatch):
    monkeypatch.setattr(
        code_context,
        "_normalize_code_system",
        lambda _system: "UNSUPPORTED",
    )
    monkeypatch.setattr(code_context, "_normalize_code", lambda _code: "7")
    monkeypatch.setattr(
        code_context,
        "canonical_catalog_code",
        lambda _system, code: code,
    )

    assert await code_context._resolve_ptg2_code_search_context(
        object(),
        code="7",
        code_system="UNSUPPORTED",
    ) is None


@pytest.mark.asyncio
async def test_code_context_rejects_code_without_system():
    assert await code_context._resolve_ptg2_code_search_context(
        object(),
        code="7",
        code_system=None,
    ) is None


@pytest.mark.asyncio
async def test_code_context_preserves_nonnumeric_internal_alias(monkeypatch):
    internal_system = code_context.INTERNAL_PROCEDURE_CODE_SYSTEM
    monkeypatch.setattr(
        code_context,
        "canonical_catalog_code",
        lambda _system, code: code,
    )

    resolved = await code_context._resolve_ptg2_code_search_context(
        object(),
        code="alias",
        code_system=internal_system,
    )

    assert resolved["input_code"] == {
        "code_system": internal_system,
        "code": "ALIAS",
    }
    assert resolved["internal_codes"] == []


@pytest.mark.asyncio
async def test_code_context_accepts_internal_key_without_crosswalk(monkeypatch):
    internal_system = code_context.INTERNAL_PROCEDURE_CODE_SYSTEM
    monkeypatch.setattr(
        code_context,
        "canonical_catalog_code",
        lambda _system, code: code,
    )
    monkeypatch.setattr(code_context, "PTG2_CODE_EXPANSION_HOPS", 0)

    resolved = await code_context._resolve_ptg2_code_search_context(
        object(),
        code="7",
        code_system=internal_system,
    )

    assert resolved["internal_codes"] == [7]


@pytest.mark.asyncio
async def test_code_context_skips_invalid_and_duplicate_edges(monkeypatch):
    invalid_edge_by_field = {
        "from_system": "UNSUPPORTED",
        "from_code": "x",
        "to_system": "CPT",
        "to_code": "7",
    }
    valid_edge_by_field = {
        "from_system": "CPT",
        "from_code": "7",
        "to_system": code_context.INTERNAL_PROCEDURE_CODE_SYSTEM,
        "to_code": "8",
        "match_type": "exact",
        "confidence": 1,
        "source": "test",
    }
    query_edges = AsyncMock(
        side_effect=(
            (
                invalid_edge_by_field,
                valid_edge_by_field,
                valid_edge_by_field,
            ),
            (),
        )
    )
    monkeypatch.setattr(
        code_context,
        "_query_ptg2_code_crosswalk_edges",
        query_edges,
    )

    resolved = await code_context._resolve_ptg2_code_search_context(
        object(),
        code="7",
        code_system="CPT",
    )

    assert resolved["internal_codes"] == [8]
    assert len(resolved["matched_via"]) == 1
    assert query_edges.await_count == 2
