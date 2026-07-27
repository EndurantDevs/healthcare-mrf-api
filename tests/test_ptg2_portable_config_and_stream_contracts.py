# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Portable contracts for PTG configuration, streams, and reference identity."""

from __future__ import annotations

from io import BytesIO
from types import SimpleNamespace
import sys

import pytest

from process.ptg_parts import config
from process.ptg_parts import json_streams
from process.ptg_parts import provider_references


def test_configuration_defaults_fail_closed_and_keep_v4_compatibility(
    monkeypatch,
):
    """Invalid tuning must fall back without changing reviewed layout choices."""

    monkeypatch.setenv("PTG_TEST_INVALID_INTEGER", "not-an-integer")
    assert config._env_int("PTG_TEST_INVALID_INTEGER", 7) == 7
    assert config._uses_postgres_binary_provider_membership_graph(
        config.PTG2_SNAPSHOT_ARCH_POSTGRES_BINARY_V3
    )

    default_kind = next(iter(config.PTG2_STAGE_COPY_DEDUPE_DEFAULT_KINDS))
    assert config._uses_ptg2_stage_copy_dedupe(default_kind)
    monkeypatch.setenv(config.PTG2_STAGE_COPY_DEDUPE_ENV, "false")
    assert not config._uses_ptg2_stage_copy_dedupe("synthetic")

    for name in (
        config.PTG2_SERVING_ONLY_IMPORT_ENV,
        config.PTG2_STAGE_SERVING_AS_FINAL_ENV,
        config.PTG2_COMPACT_SERVING_TABLE_ENV,
        config.PTG2_RUST_COMPACT_SERVING_ENV,
        config.PTG2_RUST_SCANNER_ENV,
    ):
        monkeypatch.delenv(name, raising=False)
    assert config._use_serving_only_import()
    assert config._use_stage_serving_as_final()
    assert config._use_compact_serving_table()
    assert config._use_rust_compact_serving()

    monkeypatch.delenv(
        config.PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV,
        raising=False,
    )
    assert config._download_retry_delay_seconds() == 2.0
    monkeypatch.setenv(
        config.PTG2_DOWNLOAD_RETRY_DELAY_SECONDS_ENV,
        "invalid",
    )
    assert config._download_retry_delay_seconds() == 2.0


def test_json_stream_eof_and_zero_position_are_idempotent():
    """Repeated EOF and compaction checks must not consume buffered text."""

    stream = json_streams._JSONTextStream(
        BytesIO(),
        chunk_size=8,
        progress_callback=None,
    )
    stream.buffer = "synthetic"
    stream.is_eof = True

    assert not stream.has_read_next_block()
    stream.compact(force=True)
    assert stream.buffer == "synthetic"
    assert (
        json_streams._find_array_token(
            stream,
            {"rates": '"rates"'},
            len('"rates"'),
        )
        is None
    )


def test_provider_reference_facade_requires_the_loaded_owner(monkeypatch):
    """Reference helpers must not silently bind to a partial facade."""

    monkeypatch.delitem(sys.modules, "process.ptg", raising=False)
    with pytest.raises(RuntimeError, match="facade is not loaded"):
        provider_references._ptg_facade()

    facade = SimpleNamespace()
    monkeypatch.setitem(sys.modules, "process.ptg", facade)
    assert provider_references._ptg_facade() is facade


def test_provider_reference_rows_preserve_tax_identity_across_all_groups():
    """Every source group retains its normalized NPI and billing TIN fields."""

    (
        provider_groups_by_reference,
        provider_rows,
    ) = provider_references._provider_reference_rows(
        {
            "provider_groups": [
                {
                    "provider_group_id": "7",
                    "tin": {
                        "type": "ein",
                        "value": "000000000",
                        "business_name": "Synthetic One",
                    },
                    "npi": [1000000001],
                },
                {
                    "provider_group_id": "8",
                    "tin": {
                        "type": "ein",
                        "value": "111111111",
                        "business_name": "Synthetic Two",
                    },
                    "npi": [1000000002],
                },
            ]
        },
        file_id=19,
        test_mode=False,
    )

    assert sorted(provider_groups_by_reference) == [7, 8]
    assert [provider_row["tin_value"] for provider_row in provider_rows] == [
        "000000000",
        "111111111",
    ]
    assert [
        provider_row["tin_business_name"] for provider_row in provider_rows
    ] == [
        "Synthetic One",
        "Synthetic Two",
    ]
