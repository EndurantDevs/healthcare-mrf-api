# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic PostgreSQL binary-COPY fixtures for packed hospital prices."""

from __future__ import annotations

import hashlib
from pathlib import Path
import struct
from types import SimpleNamespace
from typing import Any


def _binary_copy(copy_rows: list[tuple[bytes | None, ...]]) -> bytes:
    payload = bytearray(b"PGCOPY\n\xff\r\n\x00" + struct.pack("!ii", 0, 0))
    for row in copy_rows:
        payload.extend(struct.pack("!h", len(row)))
        for field in row:
            payload.extend(struct.pack("!i", -1 if field is None else len(field)))
            if field is not None:
                payload.extend(field)
    payload.extend(struct.pack("!h", -1))
    return bytes(payload)


def _packed_row(
    version_id: str,
    kind: int,
    block_ordinal: int,
    logical_range: tuple[int, int],
    secondary_range: tuple[int, int],
    page_range: tuple[int, int],
    selector_hashes: tuple[bytes | None, bytes | None],
    block_bytes: bytes,
) -> tuple[bytes | None, ...]:
    logical_first, logical_count = logical_range
    secondary_first, secondary_count = secondary_range
    page_index, page_count = page_range
    key_sha256, parent_sha256 = selector_hashes
    return (
        version_id.encode("ascii"),
        struct.pack("!h", kind),
        struct.pack("!q", block_ordinal),
        struct.pack("!q", logical_first),
        struct.pack("!i", logical_count),
        struct.pack("!q", secondary_first),
        struct.pack("!i", secondary_count),
        struct.pack("!i", page_index),
        struct.pack("!i", page_count),
        key_sha256,
        parent_sha256,
        hashlib.sha256(block_bytes).digest(),
        block_bytes,
    )


def _service_layout(
    service_first: int,
    split_service: bool,
    replayed_services: bool,
) -> tuple[int, int, tuple[tuple[int, int, int, int, int], ...]]:
    assert not (split_service and replayed_services)
    if replayed_services:
        return 3, 3, ((0, 0, 2, 0, 1), (1, 0, 2, 1, 1), (2, 2, 1, 2, 1))
    if split_service:
        return 1, 513, (
            (0, service_first, 1, 0, 512),
            (1, service_first, 1, 512, 1),
        )
    return 1, 1, ((0, service_first, 1, 0, 1),)


def _write_packed_artifacts(
    tmp_path: Path,
    version_id: str,
    blocks_by_kind: dict[str, list[tuple[bytes | None, ...]]],
) -> tuple[Any, ...]:
    artifacts = []
    for kind, block_rows in blocks_by_kind.items():
        content = _binary_copy(block_rows)
        path = tmp_path / f"{version_id[:8]}-{kind}.copy"
        path.write_bytes(content)
        artifacts.append(SimpleNamespace(
            kind=kind, path=path, rows=len(block_rows), bytes=len(content),
            sha256=hashlib.sha256(content).hexdigest(),
        ))
    return tuple(artifacts)


def _packed_root(
    service_count: int,
    charge_count: int,
    service_block_count: int,
    code_selector_block_count: int,
) -> SimpleNamespace:
    return SimpleNamespace(
        service_count=service_count,
        charge_count=charge_count,
        fact_count=1,
        code_selector_key_count=1,
        payer_plan_selector_key_count=1,
        code_selector_ref_count=charge_count,
        payer_plan_selector_ref_count=1,
        service_block_count=service_block_count,
        fact_block_count=1,
        code_selector_page_count=code_selector_block_count,
        payer_plan_selector_page_count=1,
        code_selector_block_count=code_selector_block_count,
        payer_plan_selector_block_count=1,
    )


def _selector_blocks(
    version_id: str,
    charge_count: int,
    mixed_null_code_parent: bool,
) -> list[tuple[bytes | None, ...]]:
    code_hash = hashlib.sha256(b"CPT\x0070551").digest()
    payer_hash = hashlib.sha256(b"Payer\x00Plan").digest()
    code_blocks = (
        [
            _packed_row(
                version_id, 3, 0, (0, 1), (0, 512), (0, 2),
                (code_hash, None), b"HPTSEL code page 0",
            ),
            _packed_row(
                version_id, 3, 1, (0, 1), (512, 1), (1, 2),
                (code_hash, code_hash), b"HPTSEL code page 1",
            ),
        ]
        if mixed_null_code_parent
        else [
            _packed_row(
                version_id, 3, 0, (0, 1), (0, charge_count), (0, 1),
                (code_hash, code_hash), b"HPTSEL code",
            )
        ]
    )
    return [
        *code_blocks,
        _packed_row(
            version_id, 4, 0, (1, 1), (0, 1), (0, 1),
            (payer_hash, payer_hash), b"HPTSEL payer-plan",
        ),
    ]


def _packed_receipt(
    tmp_path: Path,
    version_id: str,
    *,
    service_first: int = 0,
    split_service: bool = False,
    replayed_services: bool = False,
    mixed_null_code_parent: bool = False,
) -> Any:
    """Build one synthetic packed receipt for store integrity tests."""
    service_count, charge_count, service_ranges = _service_layout(
        service_first, split_service, replayed_services
    )
    service_blocks = [
        _packed_row(
            version_id,
            1,
            ordinal,
            (logical_first, logical_count),
            (charge_first, charge_rows),
            (0, 0),
            (None, None),
            f"HPTSERV synthetic {ordinal}".encode(),
        )
        for ordinal, logical_first, logical_count, charge_first, charge_rows
        in service_ranges
    ]
    blocks_by_kind = {
        "service_block": service_blocks,
        "fact_block": [
            _packed_row(
                version_id, 2, 0, (0, 1), (0, 0), (0, 0),
                (None, None), b"HPTFACT synthetic",
            )
        ],
        "selector_page": _selector_blocks(
            version_id, charge_count, mixed_null_code_parent
        ),
    }
    return SimpleNamespace(
        version_id=version_id,
        artifacts=_write_packed_artifacts(tmp_path, version_id, blocks_by_kind),
        root=_packed_root(
            service_count,
            charge_count,
            len(service_blocks),
            1 + int(mixed_null_code_parent),
        ),
    )
