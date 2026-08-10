# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import pytest

from tests.ptg2_v3_rust_python_forward_bridge_fixture import (
    _prepare_forward_bridge_scan,
)
from tests.ptg2_v3_rust_python_forward_bridge_support import (
    _assemble_forward_bridge_result,
    _assert_forward_bridge_decoded_rows,
    _assert_forward_bridge_sparse_rows,
    _expected_forward_bridge_rows,
    _install_forward_bridge_mocks,
    _lookup_forward_bridge_rows,
    _run_forward_bridge_finalizer,
)


@pytest.mark.asyncio
async def test_real_rust_v3_forward_writer_bridges_to_strict_python_reader(
    tmp_path,
    monkeypatch,
):
    """Bridge real Rust V3 output into the strict Python shared-block reader."""
    scan = _prepare_forward_bridge_scan(tmp_path, monkeypatch)
    output_directory, summary = _run_forward_bridge_finalizer(tmp_path, scan)
    result = _assemble_forward_bridge_result(scan, output_directory, summary)
    mocks = _install_forward_bridge_mocks(monkeypatch, result)
    expected_rows = _expected_forward_bridge_rows(result)

    decoded_rows = await _lookup_forward_bridge_rows(result)
    _assert_forward_bridge_decoded_rows(decoded_rows, expected_rows)
    mocks.discovery.assert_awaited_once()

    sparse_rows = await _lookup_forward_bridge_rows(
        result,
        provider_set_keys=(1, 2050),
    )
    _assert_forward_bridge_sparse_rows(sparse_rows, expected_rows, mocks)
