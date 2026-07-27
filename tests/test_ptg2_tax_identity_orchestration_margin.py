# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from tests.ptg2_v4_graph_compiler_test_support import _write_tax_identity


ptg = importlib.import_module("process.ptg")


def _ptg_frame(path: Path) -> dict[str, object]:
    entry = _write_tax_identity(
        path.parent / "source-tax.sidecar",
        shard_id="source-a",
        tax_observations=[
            (bytes.fromhex("10" * 16), 1, bytes.fromhex("11" * 32)),
            (bytes.fromhex("20" * 16), 2, None),
        ],
    )
    source = Path(str(entry["path"]))
    source.replace(path)
    return {
        "path": str(path),
        "bytes": int(entry["byte_count"]),
        "row_count": int(entry["row_count"]),
        "provider_group_count": int(entry["provider_group_count"]),
        "matched_ein_count": int(entry["matched_ein_count"]),
        "missing_count": int(entry["missing_count"]),
        "malformed_count": int(entry["malformed_count"]),
        "unsupported_type_count": int(entry["unsupported_type_count"]),
        "format": entry["record_format"],
        "version": entry["version"],
        "record_bytes": entry["record_bytes"],
        "token_policy_id": entry["token_policy_id"],
        "normalization_contract": entry["normalization_contract"],
        "hmac_contract": entry["hmac_contract"],
        "sha256": entry["sha256"],
        "final": True,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("format", "wrong"),
        ("version", 2),
        ("record_bytes", 64),
        ("normalization_contract", "wrong"),
        ("hmac_contract", "wrong"),
        ("final", False),
        ("token_policy_id", 1),
        ("token_policy_id", "UPPER"),
        ("sha256", "z" * 64),
        ("row_count", True),
        ("missing_count", -1),
    ),
)
def test_orchestrator_rejects_each_scanner_tax_frame_drift(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    path = tmp_path / "orchestrator-tax.sidecar"
    frame = _ptg_frame(path)
    frame[field] = value

    with pytest.raises(RuntimeError, match="evidence is invalid"):
        ptg._validate_tax_identity_summary_frame(path, frame)


def test_orchestrator_requires_exact_scanner_tax_frame_shape(
    tmp_path: Path,
) -> None:
    path = tmp_path / "orchestrator-tax.sidecar"
    frame = _ptg_frame(path)
    frame.pop("final")
    with pytest.raises(RuntimeError, match="omitted"):
        ptg._validate_tax_identity_summary_frame(path, frame)


@pytest.mark.parametrize(
    ("mutation", "message"),
    (
        (lambda frame: frame.update(provider_group_count=1), "counts"),
        (lambda frame: frame.update(missing_count=0), "counts"),
        (lambda frame: frame.update(bytes=int(frame["bytes"]) + 1), "size"),
        (lambda frame: frame.update(sha256="00" * 32), "digest"),
    ),
)
def test_orchestrator_authenticates_tax_artifact_content(
    tmp_path: Path,
    mutation: object,
    message: str,
) -> None:
    path = tmp_path / "orchestrator-tax.sidecar"
    frame = _ptg_frame(path)
    mutation(frame)
    policy, digest, counts = ptg._validate_tax_identity_summary_frame(
        path, frame
    )

    with pytest.raises(RuntimeError, match=message):
        ptg._validate_tax_identity_artifact_content(
            path,
            frame,
            policy,
            digest,
            counts,
        )
