# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Original-byte compression must retain the existing native Parquet budgets."""

import asyncio
import gzip
from dataclasses import replace
from io import BytesIO

import pytest

from process.custom_import.capture import CaptureError, capture_stream
from process.custom_import.https_candidate import _bounded_arrow_bytes
from tests.test_custom_import_https_candidate import _request


@pytest.mark.parametrize("compression", ["none", "gzip"])
async def test_original_parquet_preflight_accounts_decoded_arrow_bytes(compression):
    request, payloads = _request()
    stream = replace(request.definition.source_streams[0], compression=compression)
    payload = payloads[stream.stream_id]
    if compression == "gzip":
        payload = gzip.compress(payload, mtime=0)
    sealed = capture_stream(BytesIO(payload), stream, source_snapshot_token="synthetic", limits=request.limits)
    arrow_bytes = await _bounded_arrow_bytes(sealed, request.limits, 0, 0, asyncio.get_running_loop().time() + 10)
    assert arrow_bytes > 0
    assert sealed.payload == payload


async def test_original_gzip_preflight_rejects_changed_decoded_identity():
    request, payloads = _request()
    stream = replace(request.definition.source_streams[0], compression="gzip")
    sealed = capture_stream(
        BytesIO(gzip.compress(payloads[stream.stream_id], mtime=0)),
        stream,
        source_snapshot_token="synthetic",
        limits=request.limits,
    )
    sealed = replace(sealed, manifest=replace(sealed.manifest, decoded_sha256="0" * 64))
    with pytest.raises(CaptureError, match="does not match"):
        await _bounded_arrow_bytes(sealed, request.limits, 0, 0, asyncio.get_running_loop().time() + 10)
