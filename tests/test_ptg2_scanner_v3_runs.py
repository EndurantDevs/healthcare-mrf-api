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
from tests.ptg2_scanner_v3_run_support import _run_scanner as _run_scanner_support


def _run_scanner(
    scanner_binary: Path,
    tmp_path: Path,
    label: str,
    **options_by_name: Any,
) -> dict:
    """Preserve the original scanner helper and its patchable fixture hook."""
    resolved_options_by_name = dict(options_by_name)
    if (
        "fixture_payload" not in resolved_options_by_name
        and "input_artifact" not in resolved_options_by_name
    ):
        resolved_options_by_name["fixture_payload"] = _fixture_payload(
            provider_references_first=resolved_options_by_name[
                "provider_references_first"
            ],
            multiple_prices=resolved_options_by_name.get("multiple_prices", False),
            duplicate_first_price=resolved_options_by_name.get(
                "duplicate_first_price",
                False,
            ),
            repeated_rate_occurrences=resolved_options_by_name.get(
                "repeated_rate_occurrences",
                False,
            ),
        )
    return _run_scanner_support(
        scanner_binary,
        tmp_path,
        label,
        **resolved_options_by_name,
    )

@pytest.mark.parametrize(
    ("kind", "input_copy_rows"),
    [
        (
            "price_set_atom_memberships_v3",
            [[struct.pack(">q", 0), struct.pack(">q", 1)]],
        ),
        (
            "price_atoms_v3",
            [
                [
                    struct.pack(">q", 0),
                    b"125.5",
                    struct.pack(">q", 1),
                    None,
                    struct.pack(">q", 2),
                    None,
                    None,
                    None,
                    None,
                ]
            ],
        ),
    ],
)
def test_release_scanner_exposes_only_strict_v3_price_streams(kind, input_copy_rows):
    conversion_process = subprocess.run(
        [
            str(_built_scanner_binary()),
            "--serving-binary-copy-from-key-copy-stdio",
            kind,
            "24",
        ],
        input=_pg_binary_copy_rows(input_copy_rows),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
        timeout=30,
    )

    output_rows = _read_pg_binary_rows(conversion_process.stdout, 10)
    assert len(output_rows) == 1
    assert output_rows[0][2] == kind.encode("ascii")
    summary_line = next(
        line
        for line in conversion_process.stderr.splitlines()
        if line.startswith(b"PTG2_SERVING_BINARY_COPY\t")
    )
    summary = json.loads(summary_line.split(b"\t", 1)[1])
    assert summary["artifact_kind"] == kind
    assert summary["atom_key_bits"] == 24
    assert summary["target_copy_format"] == "postgres_binary_shared_blocks"

    rejected = subprocess.run(
        [
            str(_built_scanner_binary()),
            "--serving-binary-copy-from-key-copy-stdio",
            "by_code",
            "24",
        ],
        input=b"",
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
        timeout=30,
    )
    assert rejected.returncode != 0
