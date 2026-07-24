# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Focused validation tests for targeted shared-layout snapshot removal."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from process.ptg_parts.ptg2_shared_blocks import (
    PTG2_V3_SHARED_GENERATION,
    PTG2_V4_SHARED_GENERATION,
)
from process.ptg_parts.source_snapshot_shared_layout import (
    _row_mapping,
    bound_shared_layout_keys,
)


class _QueryResult:
    def __init__(self, rows: list[dict[str, Any]]):
        self._rows = rows

    def all(self) -> list[dict[str, Any]]:
        return self._rows


class _Session:
    def __init__(self, *query_rows: list[dict[str, Any]]):
        self._query_rows = list(query_rows)
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def execute(
        self,
        statement: Any,
        params: dict[str, Any],
    ) -> _QueryResult:
        self.calls.append((str(statement), params))
        if not self._query_rows:
            raise AssertionError("unexpected shared-layout query")
        return _QueryResult(self._query_rows.pop(0))


def _layout(
    *,
    snapshot_key: int = 11,
    generation: str = PTG2_V4_SHARED_GENERATION,
    state: str = "sealed",
) -> dict[str, Any]:
    return {
        "snapshot_key": snapshot_key,
        "generation": generation,
        "state": state,
    }


async def _bound_keys(
    session: _Session,
    *,
    generation: str = PTG2_V4_SHARED_GENERATION,
    snapshot_key: Any = 11,
) -> tuple[int, ...]:
    return await bound_shared_layout_keys(
        session,
        schema="mrf",
        snapshot_id="snapshot-a",
        expected_generation=generation,
        expected_snapshot_key=snapshot_key,
    )


@pytest.mark.asyncio
async def test_rejects_unsupported_storage_generation_before_querying() -> None:
    session = _Session()

    with pytest.raises(
        ValueError,
        match="unsupported storage generation",
    ):
        await _bound_keys(session, generation="shared_blocks_future")

    assert session.calls == []


@pytest.mark.asyncio
async def test_returns_empty_when_snapshot_has_no_shared_binding() -> None:
    session = _Session([])

    assert (
        await _bound_keys(
            session,
            generation=PTG2_V3_SHARED_GENERATION,
            snapshot_key=None,
        )
        == ()
    )
    assert session.calls[0][1] == {"snapshot_id": "snapshot-a"}


@pytest.mark.asyncio
async def test_rejects_v4_snapshot_without_shared_binding() -> None:
    session = _Session([])

    with pytest.raises(
        ValueError,
        match="missing its shared layout binding",
    ):
        await _bound_keys(session)

    assert session.calls[0][1] == {"snapshot_id": "snapshot-a"}


@pytest.mark.asyncio
async def test_rejects_multiple_shared_layout_bindings() -> None:
    session = _Session([_layout(), _layout(snapshot_key=12)])

    with pytest.raises(
        RuntimeError,
        match="multiple shared layout bindings",
    ):
        await _bound_keys(session)


@pytest.mark.asyncio
async def test_rejects_binding_generation_mismatch() -> None:
    session = _Session(
        [_layout(generation=PTG2_V3_SHARED_GENERATION)]
    )

    with pytest.raises(
        ValueError,
        match="storage generation does not match manifest",
    ):
        await _bound_keys(session)


@pytest.mark.asyncio
async def test_rejects_unsealed_shared_layout() -> None:
    session = _Session([_layout(state="building")])

    with pytest.raises(
        ValueError,
        match="does not reference a sealed shared layout",
    ):
        await _bound_keys(session)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "manifest_snapshot_key",
    [
        None,
        "not-a-number",
        0,
        -1,
        True,
        False,
        11.0,
        11.9,
        "011",
        " 11 ",
        "+11",
    ],
)
async def test_rejects_missing_or_nonpositive_v4_manifest_key(
    manifest_snapshot_key: Any,
) -> None:
    session = _Session([_layout()])

    with pytest.raises(
        ValueError,
        match="manifest is missing its shared snapshot key",
    ):
        await _bound_keys(
            session,
            snapshot_key=manifest_snapshot_key,
        )

    assert session.calls == []


@pytest.mark.asyncio
async def test_rejects_v4_manifest_key_that_differs_from_binding() -> None:
    session = _Session([_layout(snapshot_key=11)])

    with pytest.raises(
        ValueError,
        match="manifest does not match its shared layout binding",
    ):
        await _bound_keys(session, snapshot_key=12)

    assert len(session.calls) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "root_rows",
    [
        [],
        [{"state": "building"}],
        [{"state": "complete"}, {"state": "complete"}],
    ],
    ids=["missing", "incomplete", "multiple"],
)
async def test_rejects_missing_or_incomplete_v4_map_root(
    root_rows: list[dict[str, Any]],
) -> None:
    session = _Session([_layout()], root_rows)

    with pytest.raises(
        ValueError,
        match="missing its complete packed map root",
    ):
        await _bound_keys(session)

    assert session.calls[1][1] == {"snapshot_key": 11}


@pytest.mark.asyncio
async def test_accepts_one_complete_v4_root_for_the_bound_layout() -> None:
    session = _Session([_layout()], [{"state": "complete"}])

    assert await _bound_keys(session) == (11,)
    assert len(session.calls) == 2
    assert "FOR UPDATE OF binding, layout" in session.calls[0][0]
    assert "FOR KEY SHARE" in session.calls[1][0]
    assert session.calls[1][1] == {"snapshot_key": 11}


def test_row_mapping_returns_a_direct_dictionary_without_copying() -> None:
    row_by_field = {"snapshot_key": 11}

    assert _row_mapping(row_by_field) is row_by_field


def test_row_mapping_reads_an_object_mapping() -> None:
    row = SimpleNamespace(_mapping={"snapshot_key": 11})

    assert _row_mapping(row) == {"snapshot_key": 11}


@pytest.mark.asyncio
async def test_accepts_one_sealed_v3_layout_without_querying_v4_root() -> None:
    session = _Session(
        [_layout(generation=PTG2_V3_SHARED_GENERATION)]
    )

    assert (
        await _bound_keys(
            session,
            generation=PTG2_V3_SHARED_GENERATION,
            snapshot_key=None,
        )
        == (11,)
    )
    assert len(session.calls) == 1
