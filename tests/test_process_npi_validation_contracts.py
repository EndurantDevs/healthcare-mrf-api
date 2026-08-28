# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import asyncio

import importlib

from pathlib import Path

from types import SimpleNamespace

from unittest.mock import AsyncMock

import os

import datetime

import uuid

from contextlib import asynccontextmanager

import pytest

from process.nppes_public_evidence_import import NPPES_RIGHTS_PROOF_SHA256

from tests.test_process_npi_unit import (
    ROOT,
    _AmbiguousPublicationConnection,
    _ShutdownRawConnection,
    _build_minimal_row,
    _fake_make_class_factory,
    _install_shutdown_success_collaborators,
    _shutdown_stage_classes,
    npi_module,
)

@pytest.mark.asyncio
async def test_resolve_npi_address_archive_uses_single_shard_for_small_missing_set(monkeypatch, npi_module):
    stamp_address_keys = AsyncMock()
    resolve_into_archive = AsyncMock(return_value=SimpleNamespace(staged=10, distinct_keys=5))

    monkeypatch.setenv("HLTHPRT_ADDRESS_CANON_NPI_SHARDS", "24")
    monkeypatch.setattr(npi_module.db, "scalar", AsyncMock(return_value=42))
    monkeypatch.setattr(npi_module, "stamp_address_keys", stamp_address_keys)
    monkeypatch.setattr(npi_module, "resolve_into_archive", resolve_into_archive)

    await npi_module.resolve_npi_address_archive(
        staging_table="npi_address_20260613",
        field_map={"first_line": "first_line"},
        schema="mrf",
        cancel_check=AsyncMock(),
    )

    stamp_address_keys.assert_awaited_once()
    assert stamp_address_keys.await_args.kwargs["shards"] == 1
    assert stamp_address_keys.await_args.kwargs["update_existing"] is False
    assert stamp_address_keys.await_args.kwargs["honor_env_override"] is False

@pytest.mark.asyncio
async def test_resolve_npi_address_archive_repairs_only_on_mismatch(monkeypatch, npi_module):
    stamp_address_keys = AsyncMock(return_value=7)
    resolve_into_archive = AsyncMock(
        side_effect=[
            RuntimeError(f"{npi_module.ADDRESS_KEY_MISMATCH_MESSAGE}: stale"),
            SimpleNamespace(staged=10, distinct_keys=5),
        ]
    )

    monkeypatch.setattr(npi_module.db, "scalar", AsyncMock(return_value=0))
    monkeypatch.setattr(npi_module, "stamp_address_keys", stamp_address_keys)
    monkeypatch.setattr(npi_module, "resolve_into_archive", resolve_into_archive)

    stats = await npi_module.resolve_npi_address_archive(
        staging_table="npi_address_20260613",
        field_map={"first_line": "first_line"},
        schema="mrf",
        cancel_check=AsyncMock(),
    )

    assert stats.staged == 10
    assert resolve_into_archive.await_count == 2
    stamp_address_keys.assert_awaited_once()
    assert stamp_address_keys.await_args.kwargs["update_existing"] is True

@pytest.mark.asyncio
async def test_main_creates_one_controlled_import_run(monkeypatch, npi_module):
    control_imports = importlib.import_module("api.control_imports")
    create_run = AsyncMock(
        return_value=({"run_id": "run_npi", "status": "queued"}, True)
    )
    ensure_table = AsyncMock(side_effect=AssertionError("workers must not run schema DDL"))
    monkeypatch.setattr(control_imports, "create_import_run", create_run)
    monkeypatch.setattr(control_imports, "ensure_import_run_table", ensure_table)

    result = await npi_module.main()

    assert result == {"run_id": "run_npi", "status": "queued"}
    ensure_table.assert_not_awaited()
    create_run.assert_awaited_once_with(
        {
            "importer": "npi",
            "params": {},
            "triggered_by": "manual",
        }
    )

@pytest.mark.asyncio
async def test_main_rejects_live_queue_test_mode(monkeypatch, npi_module):
    with pytest.raises(ValueError, match="isolated database"):
        await npi_module.main(test_mode=True)
