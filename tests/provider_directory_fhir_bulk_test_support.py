# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import contextlib
import dataclasses
import datetime
import hashlib
import importlib
import json
from unittest.mock import AsyncMock

import pytest

from db.models import ProviderDirectoryLocation, ProviderDirectoryPractitioner


importer = importlib.import_module("process.provider_directory_fhir")
TEST_TRANSACTION_TIME = (
    datetime.datetime.now(datetime.UTC)
    .replace(microsecond=0)
    .isoformat()
    .replace("+00:00", "Z")
)


def _source() -> dict:
    return {
        "source_id": "aetna-coverage",
        "api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        "canonical_api_base": importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        "metadata_json": {
            "provider_directory_bulk_export_output_hosts": [
                "storage.googleapis.com"
            ]
        },
    }


def _identity(
    *, owner_run_id: str = "run-coverage", retry_of_run_id: str | None = None
) -> importer.BulkExportCheckpointIdentity:
    return importer.BulkExportCheckpointIdentity(
        checkpoint_id="checkpoint-coverage",
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        resource_type="Practitioner",
        source_scope_hash="scope-coverage",
        strategy_version=importer.BULK_EXPORT_CHECKPOINT_STRATEGY_VERSION,
        acquisition_root_run_id="root-coverage",
        owner_run_id=owner_run_id,
        retry_of_run_id=retry_of_run_id,
        endpoint_id="endpoint-coverage",
        dataset_id="dataset-coverage",
        start_url=(
            f"{importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE}"
            "/$export?_type=Practitioner"
        ),
        start_url_hash="a" * 64,
    )


def _checkpoint(identity: importer.BulkExportCheckpointIdentity) -> dict:
    return {
        "checkpoint_id": identity.checkpoint_id,
        "canonical_api_base": identity.canonical_api_base,
        "resource_type": identity.resource_type,
        "source_scope_hash": identity.source_scope_hash,
        "strategy_version": identity.strategy_version,
        "acquisition_root_run_id": identity.acquisition_root_run_id,
        "owner_run_id": identity.owner_run_id,
        "retry_of_run_id": identity.retry_of_run_id,
        "endpoint_id": identity.endpoint_id,
        "dataset_id": identity.dataset_id,
        "start_url_hash": identity.start_url_hash,
        "status_url_ciphertext": None,
        "status_url_hash": None,
        "state": importer.BULK_EXPORT_CHECKPOINT_ACCEPTED,
    }


def _manifest_payload(**overrides) -> dict:
    payload = {
        "transactionTime": TEST_TRANSACTION_TIME,
        "request": _identity().start_url,
        "requiresAccessToken": False,
        "output": [
            {
                "type": "Practitioner",
                "url": "https://storage.googleapis.com/aetna/part.ndjson?sig=x",
            }
        ],
    }
    payload.update(overrides)
    return payload


class _ChunkedContent:
    def __init__(self, chunks: list[bytes]):
        self.chunks = chunks

    async def iter_chunked(self, _size: int):
        for chunk in self.chunks:
            yield chunk


class _Response:
    def __init__(
        self,
        *,
        status: int = 200,
        headers: dict[str, str] | None = None,
        chunks: list[bytes] | None = None,
        body: bytes = b"{}",
    ):
        self.status = status
        self.headers = headers or {}
        self.content = _ChunkedContent(chunks or [])
        self.body = body

    async def read(self) -> bytes:
        return self.body

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_exc_info):
        return False


class _Session:
    def __init__(self, response_or_error):
        self.response_or_error = response_or_error
        self.calls: list[tuple[str, dict]] = []

    def get(self, url: str, **options):
        self.calls.append((url, options))
        if isinstance(self.response_or_error, BaseException):
            raise self.response_or_error
        return self.response_or_error

@contextlib.asynccontextmanager
async def _client_session():
    yield object()

class _Connection:
    def __init__(self, status_result: int):
        self.status = AsyncMock(return_value=status_result)


@contextlib.asynccontextmanager
async def _acquire(connection):
    yield connection

def _stream_options(*, range_resume_enabled: bool) -> importer.BulkExportStreamOptions:
    return importer.BulkExportStreamOptions(
        model=ProviderDirectoryPractitioner,
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
        polls=0,
        ownership_probe=AsyncMock(),
        range_resume_enabled=range_resume_enabled,
    )

def _checkpoint_context() -> importer.PaginationCheckpointContext:
    return importer.PaginationCheckpointContext(
        canonical_api_base=importer.AETNA_PROVIDER_DIRECTORY_DATA_BASE,
        source_scope_hash="scope-coverage",
        source_ids=("aetna-coverage",),
        owner_run_id="run-coverage",
        acquisition_root_run_id="root-coverage",
        endpoint_id="endpoint-coverage",
        dataset_id="dataset-coverage",
    )


def _fetch_options() -> importer.BulkExportFetchOptions:
    return importer.BulkExportFetchOptions(
        timeout=3,
        run_id="run-coverage",
        row_batch_handler=AsyncMock(return_value=0),
        row_batch_size=1,
        retain_rows=False,
    )
