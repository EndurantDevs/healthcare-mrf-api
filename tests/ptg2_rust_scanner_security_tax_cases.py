from __future__ import annotations

import hashlib
import json
import subprocess
from pathlib import Path

import pytest


def _scanner_output_tree(root: Path) -> dict[str, bytes | None]:
    return {
        str(path.relative_to(root)): path.read_bytes() if path.is_file() else None
        for path in sorted(root.rglob("*"))
    }


def _write_scanner_fixture(tmp_path: Path, scanner_support) -> Path:
    artifact = tmp_path / "tiny-rates.json"
    artifact.write_text(
        json.dumps(
            scanner_support._scanner_fixture_payload(procedure_count=1),
            separators=(",", ":"),
        ),
        encoding="utf-8",
    )
    return artifact


def _tax_secret_environment(
    scanner_support,
    artifact: Path,
    output_root: Path,
) -> dict[str, str]:
    environment = scanner_support._scanner_environment(
        1,
        2,
        output_root / "serving-runs",
    )
    environment.update(
        {
            "HLTHPRT_PTG2_RAW_SOURCE_SHA256": hashlib.sha256(
                artifact.read_bytes()
            ).hexdigest(),
            "HLTHPRT_PTG2_PROVIDER_GRAPH_V4": "true",
            "HLTHPRT_PTG2_PROVIDER_GRAPH_V4_FACTORS": "true",
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_SET_COMPONENT_SIDECAR_PATH": str(
                output_root / "set-component.ptg2sc"
            ),
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_COMPONENT_GROUP_SIDECAR_PATH": str(
                output_root / "component-group.ptg2sc"
            ),
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_TAX_IDENTITY_SIDECAR_PATH": str(
                output_root / "tax-identity.ptg2tax"
            ),
            "HLTHPRT_PTG2_MANIFEST_PRICE_ATOM_COPY_PATH": str(
                output_root / "price-atom.copy"
            ),
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_ATOM_COPY_PATH": str(
                output_root / "price-set-atom.copy"
            ),
            "HLTHPRT_PTG2_MANIFEST_PRICE_SET_SUMMARY_COPY_PATH": str(
                output_root / "price-set-summary.copy"
            ),
            "HLTHPRT_PTG2_MANIFEST_PROVIDER_GROUP_MEMBER_COPY_PATH": str(
                output_root / "provider-group-member.copy"
            ),
            "HLTHPRT_PTG2_TIN_TOKEN_POLICY_ID": (
                "ptg-tin-hmac-sha256-v1:security-process"
            ),
        }
    )
    return environment


@pytest.mark.parametrize(
    ("secret_bytes", "expected_error"),
    [
        (None, "PTG TIN token secret file must be configured"),
        (b"q" * 31, "PTG TIN token secret file must contain exactly 32 raw bytes"),
        (b"r" * 33, "PTG TIN token secret file must contain exactly 32 raw bytes"),
    ],
    ids=("absent", "31-bytes", "33-bytes"),
)
def test_v4_tin_secret_preflight_is_artifact_free_and_redacted(
    tmp_path,
    secret_bytes,
    expected_error,
) -> None:
    """Exercise the real scanner process at the secret-file trust boundary."""
    from tests import test_ptg2_scanner_parallelism as scanner_support

    scanner_binary = scanner_support._built_scanner_binary()
    artifact = _write_scanner_fixture(tmp_path, scanner_support)
    output_root = tmp_path / "outputs"
    output_root.mkdir()
    unrelated = output_root / "unrelated.keep"
    unrelated.write_bytes(b"preexisting-unrelated-content")

    environment = _tax_secret_environment(
        scanner_support,
        artifact,
        output_root,
    )
    secret_path = tmp_path / "sensitive-secret-material.bin"
    environment.pop("HLTHPRT_PTG2_TIN_TOKEN_SECRET_FILE", None)
    if secret_bytes is not None:
        secret_path.write_bytes(secret_bytes)
        environment["HLTHPRT_PTG2_TIN_TOKEN_SECRET_FILE"] = str(secret_path)

    output_before = _scanner_output_tree(output_root)
    completed = subprocess.run(
        [str(scanner_binary), "--compact-serving", str(artifact)],
        check=False,
        env=environment,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        timeout=120,
    )

    assert completed.returncode != 0
    diagnostic = completed.stdout + completed.stderr
    assert expected_error.encode("ascii") in diagnostic
    assert str(secret_path).encode() not in diagnostic
    if secret_bytes is not None:
        assert secret_bytes not in diagnostic
    assert _scanner_output_tree(output_root) == output_before
    assert unrelated.read_bytes() == b"preexisting-unrelated-content"
