# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Fail-closed unit seams for the admission receipt backfill."""

from __future__ import annotations

from contextlib import asynccontextmanager
import importlib
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from process.provider_directory_admission_seal import (
    AdmissionSealError,
    ProviderDirectoryAdmissionSeal,
)


backfill = importlib.import_module(
    "process.provider_directory_admission_backfill"
)


def _dataset_by_field(**overrides):
    dataset_by_field = {
        "status": "validated",
        "completion_proof_required_version": None,
        "completion_resource_hashes": None,
        "completion_resource_counts": None,
        "raw_metadata_bytes": 1,
        "evidence_run_id": "root-shared",
        "dataset_hash": "a" * 64,
        "resource_count": 1,
        **dict.fromkeys(backfill._SEAL_FIELDS),
    }
    dataset_by_field.update(overrides)
    return dataset_by_field


def test_backfill_rejects_invalid_schema_and_partial_seal(monkeypatch):
    monkeypatch.setenv("HLTHPRT_DB_SCHEMA", "invalid-schema")
    with pytest.raises(AdmissionSealError, match="schema_invalid"):
        backfill._schema_name()

    with pytest.raises(AdmissionSealError, match="partial_seal"):
        backfill._existing_seal_result(
            _dataset_by_field(publication_metadata_sha256="a" * 64),
            "dataset_shared",
        )


@pytest.mark.parametrize(
    ("overrides", "marker"),
    [
        ({"status": "pending"}, "status_invalid"),
        ({"raw_metadata_bytes": 0}, "metadata_size_invalid"),
        ({"evidence_run_id": None}, "parent_identity_invalid"),
    ],
)
def test_backfill_rejects_invalid_legacy_rows(overrides, marker):
    with pytest.raises(AdmissionSealError, match=marker):
        backfill._validated_row_inputs(_dataset_by_field(**overrides))


class _CopyConnection:
    def __init__(self, chunks=(), status="COPY 1"):
        self.chunks = chunks
        self.status = status

    async def copy_from_query(self, *_args, output, **_kwargs):
        for chunk in self.chunks:
            await output(chunk)
        return self.status


@pytest.mark.asyncio
async def test_backfill_rejects_copy_overrun_and_lost_row(monkeypatch, tmp_path):
    monkeypatch.setattr(backfill, "ADMISSION_RAW_METADATA_MAX_BYTES", 1)
    with pytest.raises(AdmissionSealError, match="copy_size_invalid"):
        await backfill._copy_locked_metadata(
            _CopyConnection([b"x" * 130]),
            '"mrf"."provider_directory_endpoint_dataset"',
            {"dataset_id": "dataset", "row_ctid": "(0,1)", "row_xmin": "1"},
            (tmp_path / "oversized.copy").open("w+b"),
        )

    with pytest.raises(AdmissionSealError, match="copy_lost"):
        await backfill._copy_locked_metadata(
            _CopyConnection(status="COPY 0"),
            '"mrf"."provider_directory_endpoint_dataset"',
            {"dataset_id": "dataset", "row_ctid": "(0,1)", "row_xmin": "1"},
            (tmp_path / "lost.copy").open("w+b"),
        )


@pytest.mark.asyncio
async def test_backfill_rejects_lost_update_and_missing_dataset(monkeypatch):
    connection = SimpleNamespace(execute=AsyncMock(return_value="UPDATE 0"))
    seal = ProviderDirectoryAdmissionSeal(
        metadata_summary={},
        metadata_sha256="a" * 64,
        admission_version=1,
        admission_kind="generic",
        proof_sha256="b" * 64,
        resource_types=("Location",),
    )
    with pytest.raises(AdmissionSealError, match="backfill_lost"):
        await backfill._store_seal(
            connection,
            '"mrf"."provider_directory_endpoint_dataset"',
            "dataset",
            {"row_ctid": "(0,1)", "row_xmin": "1"},
            seal,
        )

    monkeypatch.setattr(
        backfill,
        "_fetch_dataset_row",
        AsyncMock(return_value=None),
    )
    with pytest.raises(AdmissionSealError, match="dataset_missing"):
        await backfill._backfill_locked_dataset(connection, "dataset_table", "missing")


@pytest.mark.asyncio
async def test_backfill_rejects_invalid_id_and_uses_default_database(monkeypatch):
    with pytest.raises(AdmissionSealError, match="dataset_id_invalid"):
        await backfill._backfill_provider_directory_admission_seal(" padded")

    connection = SimpleNamespace()

    @asynccontextmanager
    async def transaction(**kwargs):
        assert kwargs == {"isolation": "repeatable_read"}
        yield

    connection.transaction = transaction

    @asynccontextmanager
    async def acquire_driver():
        yield connection

    database = SimpleNamespace(acquire_driver=acquire_driver)
    models = importlib.import_module("db.models")
    monkeypatch.setattr(models, "db", database)
    monkeypatch.setattr(
        backfill,
        "_backfill_locked_dataset",
        AsyncMock(return_value={"status": "sealed"}),
    )

    assert await backfill._backfill_provider_directory_admission_seal(
        "dataset"
    ) == {"status": "sealed"}
