# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

import hashlib
import importlib.util
import io
import json
import os
import struct
import subprocess
import sys
import types
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from process.ptg_parts.ptg2_shared_finalize import (
    attach_v3_dictionary_contract,
    attach_v3_source_run_contract,
    write_v3_finalizer_input_manifest,
)
from process.ptg_parts.ptg2_provider_quarantine import (
    provider_identifier_quarantine_payload,
)
from process.ptg_parts.ptg2_source_witness import (
    build_persisted_source_witness,
    decode_persisted_source_witness,
)

from tests.ptg2_scanner_v3_release_support import (
    _AUDIT_CANDIDATE_RECORD,
    _MIB,
    _SERVING_RECORD,
    _STRICT_SCANNER_FRAME_KINDS,
    _SUPPORT_MODULE,
    _built_scanner_binary,
    _decode_by_code_groups,
    _fixture_network_entries,
    _fixture_payload,
    _fixture_provider_references,
    _load_isolated_rust_scanner,
    _load_isolated_shared_blocks,
    _network_rate_fixture,
    _parse_scanner_frames,
    _pg_binary_copy_rows,
    _read_pg_binary_rows,
    _read_uvarint,
    _single_frame,
    _v3_finalizer_test_resource_args,
)
from tests.ptg2_scanner_v3_finalizer_support import (
    _DirectFinalizerFixture,
    _assert_direct_finalizer_audit,
    _assert_direct_finalizer_block_metadata,
    _assert_direct_finalizer_dictionary,
    _assert_direct_finalizer_rejects_tamper,
    _assert_direct_finalizer_serving_rows,
    _assert_direct_finalizer_summary,
    _prepare_direct_finalizer_fixture,
    _run_direct_finalizer,
    _write_direct_finalizer_manifest,
)

def test_direct_v3_finalizer_cli_emits_shared_block_staging_copy(tmp_path):
    """Verify direct v3 finalizer cli emits shared block staging copy."""
    fixture = _prepare_direct_finalizer_fixture(tmp_path)
    completed = _run_direct_finalizer(
        fixture,
        fixture.output_directory,
        check=True,
    )
    frames = _parse_scanner_frames(completed.stdout)
    summary = _single_frame(frames, "v3_finalizer_summary")
    _assert_direct_finalizer_summary(summary, fixture.output_directory)
    _assert_direct_finalizer_audit(summary, fixture.output_directory)
    _assert_direct_finalizer_dictionary(summary, fixture.output_directory)
    _assert_direct_finalizer_block_metadata(summary, fixture.output_directory)
    _assert_direct_finalizer_serving_rows(fixture.output_directory)
    _assert_direct_finalizer_rejects_tamper(fixture, tmp_path)
