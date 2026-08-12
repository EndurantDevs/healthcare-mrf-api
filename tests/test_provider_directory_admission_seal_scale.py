# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Opt-in production-scale memory gate for raw admission-proof streaming."""

from __future__ import annotations

import json
import os
from pathlib import Path
import resource
import struct
import sys

import pytest

from process.provider_directory_admission_seal import (
    validate_generic_admission_copy,
)
from tests.test_provider_directory_dataset_selection_bounded_db import (
    _large_metadata_by_field,
)


_SHARD_COUNT = 168_275
_MEMORY_LIMIT_BYTES = 2 * 1024 * 1024 * 1024
_COPY_SIGNATURE = b"PGCOPY\n\xff\r\n\x00"


def _peak_rss_bytes() -> int:
    peak = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    return int(peak if sys.platform == "darwin" else peak * 1024)


@pytest.mark.skipif(
    os.getenv("HLTHPRT_PROVIDER_DIRECTORY_ADMISSION_SCALE_TEST") != "1",
    reason="set the explicit admission scale-test gate",
)
def test_168275_shard_copy_revalidates_below_two_gib(tmp_path: Path):
    metadata = _large_metadata_by_field(_SHARD_COUNT)
    proof_payload = json.dumps(metadata, separators=(",", ":")).encode()
    payload_bytes = len(proof_payload)
    assert 100 * 1024 * 1024 < payload_bytes < 256 * 1024 * 1024
    copy_path = tmp_path / "metadata.copy"
    with copy_path.open("wb") as output:
        output.write(_COPY_SIGNATURE)
        output.write(struct.pack("!ii", 0, 0))
        output.write(struct.pack("!h", 1))
        output.write(struct.pack("!i", len(proof_payload)))
        output.write(proof_payload)
        output.write(struct.pack("!h", -1))
    del proof_payload

    receipt = validate_generic_admission_copy(
        copy_path,
        dataset_id="dataset_shared",
        endpoint_id="endpoint_shared",
        evidence_run_id="root-shared",
        dataset_hash="e" * 64,
        resource_count=_SHARD_COUNT,
        scratch_directory=tmp_path,
    )

    assert receipt.proof_sha256 == metadata[
        "provider_directory_content_proof_v1"
    ]["proof_sha256"]
    assert receipt.resource_types == ("Location",)
    assert _peak_rss_bytes() < _MEMORY_LIMIT_BYTES
