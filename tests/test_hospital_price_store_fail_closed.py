# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Fail-closed boundaries for packed hospital-price storage."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from tests.test_hospital_price_store_unit import (
    _Connection,
    _receipt,
    _store_module,
)


@pytest.mark.asyncio
async def test_packed_copy_rejects_unsafe_driver_and_artifacts(
    tmp_path, monkeypatch
) -> None:
    store, native = _store_module()
    receipt = _receipt(native, tmp_path)
    copy_globals = store.copy_packed_blocks.__globals__

    with pytest.raises(RuntimeError, match="row count is missing"):
        copy_globals["_copy_count"]("invalid")
    with pytest.raises(NotImplementedError, match="binary COPY"):
        await store.copy_packed_blocks(
            _Connection(driver=SimpleNamespace()), receipt, "mrf"
        )

    monkeypatch.setattr(copy_globals["os"], "O_NOFOLLOW", 0)
    with pytest.raises(RuntimeError, match="non-symlink file opens"):
        await store.copy_packed_blocks(_Connection(), receipt, "mrf")
    monkeypatch.undo()

    service_block = next(
        artifact for artifact in receipt.artifacts
        if artifact.kind == "service_block"
    )
    service_block.bytes += 1
    with pytest.raises(RuntimeError, match="COPY file is invalid"):
        await store.copy_packed_blocks(_Connection(), receipt, "mrf")
    service_block.bytes -= 1

    class WrongCountDriver:
        async def copy_to_table(self, _table: str, **kwargs: Any) -> str:
            kwargs["source"].read()
            return "COPY 0"

    with pytest.raises(RuntimeError, match="changed during storage"):
        await store.copy_packed_blocks(
            _Connection(driver=WrongCountDriver()), receipt, "mrf"
        )


@pytest.mark.asyncio
async def test_packed_projection_rejects_missing_and_non_dense_rows(tmp_path) -> None:
    """Reject missing roots and non-dense physical block ordinals."""

    store, native = _store_module()
    receipt = _receipt(native, tmp_path)
    copy_globals = store.validate_packed_storage.__globals__

    with pytest.raises(RuntimeError, match="packed root conflicts"):
        await copy_globals["_validate_packed_root"](
            _Connection(firsts=[None]), receipt, '"mrf"'
        )

    valid_rows = [
        (1, 1, 0, 0), (2, 1, 0, 0), (3, 1, 0, 0), (4, 1, 0, 0)
    ]
    assert await copy_globals["_validate_block_ordinals"](
        _Connection(all_rows=[valid_rows]), receipt, '"mrf"'
    ) == {1: 1, 2: 1, 3: 1, 4: 1}

    receipt.root.payer_plan_selector_block_count = 0
    with pytest.raises(RuntimeError, match="empty block kind"):
        await copy_globals["_validate_block_ordinals"](
            _Connection(all_rows=[valid_rows]),
            receipt,
            '"mrf"',
        )
    with pytest.raises(RuntimeError, match="ordinals are not dense"):
        await copy_globals["_validate_block_ordinals"](
            _Connection(all_rows=[[(1, 2, 0, 1)]]), receipt, '"mrf"'
        )


@pytest.mark.asyncio
async def test_packed_projection_rejects_range_and_selector_drift(tmp_path) -> None:
    """Reject logical-range and selector-page drift before publication."""

    store, native = _store_module()
    receipt = _receipt(native, tmp_path)
    copy_globals = store.validate_packed_storage.__globals__
    zero_fact_receipt = _receipt(native, tmp_path / "zero-facts")
    zero_fact_receipt.root.fact_count = 0
    await copy_globals["_validate_logical_ranges"](
        _Connection(firsts=[(1, 0, 1, True), (1, 1, True)]),
        zero_fact_receipt,
        '"mrf"',
        {1: 1, 2: 0, 3: 1, 4: 0},
    )
    with pytest.raises(RuntimeError, match="logical ranges are not contiguous"):
        await copy_globals["_validate_logical_ranges"](
            _Connection(firsts=[(0, 0, 0, False)]),
            receipt,
            '"mrf"',
            {1: 1, 2: 1, 3: 1, 4: 0},
        )
    with pytest.raises(RuntimeError, match="logical ranges are not contiguous"):
        await copy_globals["_validate_logical_ranges"](
            _Connection(firsts=[(1, 0, 1, True), (1, 1, False)]),
            receipt,
            '"mrf"',
            {1: 1, 2: 1, 3: 1, 4: 0},
        )

    with pytest.raises(RuntimeError, match="selector totals are invalid"):
        await copy_globals["_validate_selector_pages"](
            _Connection(all_rows=[[]]), receipt, '"mrf"'
        )
    with pytest.raises(RuntimeError, match="key ordinals are not dense"):
        await copy_globals["_validate_selector_pages"](
            _Connection(all_rows=[
                [(3, 1, 1), (4, 1, 1)],
                [(3, 1, 1, 1)],
            ]),
            receipt,
            '"mrf"',
        )
    with pytest.raises(RuntimeError, match="selector pages are incomplete"):
        await copy_globals["_validate_selector_pages"](
            _Connection(
                all_rows=[
                    [(3, 1, 1), (4, 1, 1)],
                    [(3, 0, 1, 1), (4, 1, 1, 1)],
                ],
                scalars=[True],
            ),
            receipt,
            '"mrf"',
        )
