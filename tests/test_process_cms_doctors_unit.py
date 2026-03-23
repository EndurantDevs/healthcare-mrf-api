# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import importlib
import io
import zipfile
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest


@pytest.fixture
def cms_doctors_module():
    return importlib.import_module("process.cms_doctors")


def _zip_bytes_with_csv(csv_name: str, csv_payload: str) -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_payload)
    return buffer.getvalue()


class _FakeContent:
    def __init__(self, payload: bytes):
        self._payload = payload

    async def iter_chunked(self, _chunk_size):
        yield self._payload


class _FakeResponse:
    def __init__(self, payload: bytes):
        self.content = _FakeContent(payload)

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeClient:
    def __init__(self, payload: bytes):
        self._payload = payload

    def get(self, _url, timeout=0):  # noqa: ARG002
        return _FakeResponse(self._payload)

    async def close(self):
        return None


@pytest.mark.asyncio
async def test_process_data_keeps_multiple_addresses_per_npi(monkeypatch, cms_doctors_module):
    csv_payload = (
        "NPI,Line 1 Street Address,Line 2 Street Address,City,State,Zip Code,Primary specialty\n"
        "1111111111,123 Main St,,Chicago,IL,60654,Internal Medicine\n"
        "1111111111,789 Lake St,,Chicago,IL,60610,Nurse Practitioner\n"
        "1111111111,789 Lake St,,Chicago,IL,60610,Nurse Practitioner\n"
    )
    zip_payload = _zip_bytes_with_csv("doctors.csv", csv_payload)

    pushed_rows = []

    async def _fake_push(rows, _cls):
        pushed_rows.extend(rows)

    monkeypatch.setattr(
        cms_doctors_module,
        "_fetch_doctors_download_url",
        AsyncMock(return_value="https://x/y.zip"),
    )
    monkeypatch.setattr(cms_doctors_module, "ensure_database", AsyncMock())
    monkeypatch.setattr(cms_doctors_module, "push_objects", _fake_push)
    monkeypatch.setitem(
        __import__("sys").modules,
        "aiohttp",
        SimpleNamespace(ClientSession=lambda: _FakeClient(zip_payload)),
    )

    ctx = {"import_date": "20260321", "context": {}}
    await cms_doctors_module.process_data(ctx, {"test_mode": True})

    assert len(pushed_rows) == 2
    assert len({row["address_checksum"] for row in pushed_rows}) == 2
    assert {row["npi"] for row in pushed_rows} == {1111111111}
