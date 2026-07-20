from __future__ import annotations

import asyncio
import json
import os
from dataclasses import replace
from pathlib import Path

import pytest

import process.uhc_retained_native as retained_native
from process.uhc_retained_native import (
    consume_native_attestation,
    is_native_verified_source,
    retain_source_native,
)
from process.uhc_retained_types import UHCRetainedAdmissionError
from tests.uhc_retained_registry_test_support import (
    install_fake_native,
    native_summary,
    native_verified_source,
    records_payload,
    write_retained_fixture,
)


@pytest.mark.asyncio
async def test_native_bridge_accepts_exact_contract_and_issues_one_use_proof(
    tmp_path,
    monkeypatch,
):
    fixture, source = await native_verified_source(tmp_path, monkeypatch)

    assert is_native_verified_source(source)
    assert source.raw_artifact.sha256 == fixture["artifact_sha256"]
    assert source.raw_artifact.record_count == 16
    assert len(source.ranges) == 4
    assert source.verifier_build_id == "ptg2_scanner-test-verifier"
    assert source.timings_seconds == (("total", 0.001),)
    assert not source.raw_reused
    assert not source.manifest_reused

    identities = consume_native_attestation(source)
    assert [Path(identity[0]) for identity in identities] == [
        fixture["raw_path"],
        fixture["manifest_path"],
    ]
    assert not is_native_verified_source(source)
    with pytest.raises(UHCRetainedAdmissionError, match="fresh"):
        consume_native_attestation(source)


@pytest.mark.asyncio
async def test_attestation_is_bound_to_every_returned_proof_field(
    tmp_path,
    monkeypatch,
):
    _fixture, source = await native_verified_source(tmp_path, monkeypatch)

    assert not is_native_verified_source(replace(source, verifier_build_id="forged"))
    assert not is_native_verified_source(replace(source, ranges=tuple(reversed(source.ranges))))
    assert not is_native_verified_source(replace(source, attestation=object()))
    assert not is_native_verified_source(object())


@pytest.mark.parametrize(
    "updates",
    [
        {"record_kind": "wrong"},
        {"contract_id": "wrong"},
        {"contract_version": 1},
        {"canonicalization_id": "wrong"},
        {"raw_artifact_sha256": "0" * 64},
        {"raw_artifact_byte_count": 1},
        {"record_count": 15},
        {"range_count": 5},
        {"raw_reused": 1},
        {"manifest_reused": "false"},
        {"producer_build_id": ""},
        {"producer_build_id": "x" * 257},
        {"producer_build_id": "line\nbreak"},
        {"verifier_build_id": ""},
        {"manifest_sha256": "bad"},
        {"manifest_byte_count": True},
        {"timings_seconds": []},
        {"timings_seconds": {"x": -1}},
        {"timings_seconds": {"x": True}},
        {"timings_seconds": {"x": float("inf")}},
        {"timings_seconds": {"x" * 65: 1}},
    ],
)
@pytest.mark.asyncio
async def test_native_summary_mutations_fail_closed(tmp_path, monkeypatch, updates):
    retained_root = tmp_path / "retained"
    fixture = write_retained_fixture(retained_root, records_payload())
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture["source_bytes"])
    install_fake_native(
        tmp_path,
        monkeypatch,
        native_summary(fixture, **updates),
    )

    with pytest.raises(UHCRetainedAdmissionError):
        await retain_source_native(
            source_path=source_path,
            output_root=retained_root,
            expected_sha256=fixture["artifact_sha256"],
            expected_byte_count=fixture["artifact_byte_count"],
            range_count=4,
        )


@pytest.mark.asyncio
async def test_native_summary_requires_exact_shape_and_single_strict_json_value(
    tmp_path,
    monkeypatch,
):
    retained_root = tmp_path / "retained"
    fixture = write_retained_fixture(retained_root, records_payload())
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture["source_bytes"])
    summary = native_summary(fixture)
    invalid_payloads = (
        json.dumps({**summary, "extra": True}).encode(),
        b'{"record_kind":"one","record_kind":"two"}',
        b"[]",
        b"{}{}",
        b"\xff",
    )
    for ordinal, payload in enumerate(invalid_payloads):
        summary_path = tmp_path / f"invalid-summary-{ordinal}.json"
        summary_path.write_bytes(payload)
        binary = tmp_path / f"fake-scanner-{ordinal}"
        binary.write_text(
            "#!/usr/bin/env python3\n"
            "import pathlib, sys\n"
            f"sys.stdout.buffer.write(pathlib.Path({str(summary_path)!r}).read_bytes())\n",
            encoding="utf-8",
        )
        binary.chmod(0o755)
        monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", str(binary))
        with pytest.raises(UHCRetainedAdmissionError):
            await retain_source_native(
                source_path=source_path,
                output_root=retained_root,
                expected_sha256=fixture["artifact_sha256"],
                expected_byte_count=fixture["artifact_byte_count"],
                range_count=4,
            )


@pytest.mark.asyncio
async def test_native_summary_cannot_redirect_retained_files_outside_root(
    tmp_path,
    monkeypatch,
):
    retained_root = tmp_path / "retained"
    fixture = write_retained_fixture(retained_root, records_payload())
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture["source_bytes"])
    outside = tmp_path / "outside.json"
    outside.write_bytes(fixture["source_bytes"])
    install_fake_native(
        tmp_path,
        monkeypatch,
        native_summary(fixture, raw_artifact_path=str(outside)),
    )

    with pytest.raises(UHCRetainedAdmissionError, match="noncanonical"):
        await retain_source_native(
            source_path=source_path,
            output_root=retained_root,
            expected_sha256=fixture["artifact_sha256"],
            expected_byte_count=fixture["artifact_byte_count"],
            range_count=4,
        )


@pytest.mark.asyncio
async def test_native_bridge_detects_swap_while_loading_proof(tmp_path, monkeypatch):
    retained_root = tmp_path / "retained"
    fixture = write_retained_fixture(retained_root, records_payload())
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture["source_bytes"])
    install_fake_native(tmp_path, monkeypatch, native_summary(fixture))
    original_loader = retained_native.load_verified_range_manifest

    def swap_after_load(**kwargs):
        proof = original_loader(**kwargs)
        raw_path = Path(kwargs["raw_path"])
        raw_bytes = raw_path.read_bytes()
        raw_path.write_bytes(raw_bytes)
        return proof

    monkeypatch.setattr(
        retained_native,
        "load_verified_range_manifest",
        swap_after_load,
    )
    with pytest.raises(UHCRetainedAdmissionError, match="changed"):
        await retain_source_native(
            source_path=source_path,
            output_root=retained_root,
            expected_sha256=fixture["artifact_sha256"],
            expected_byte_count=fixture["artifact_byte_count"],
            range_count=4,
        )


@pytest.mark.asyncio
async def test_native_binary_and_inputs_are_mandatory_regular_paths(
    tmp_path,
    monkeypatch,
):
    retained_root = tmp_path / "retained"
    fixture = write_retained_fixture(retained_root, records_payload())
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture["source_bytes"])
    monkeypatch.delenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", raising=False)
    with pytest.raises(UHCRetainedAdmissionError, match="requires"):
        await retain_source_native(
            source_path=source_path,
            output_root=retained_root,
            expected_sha256=fixture["artifact_sha256"],
            expected_byte_count=fixture["artifact_byte_count"],
            range_count=4,
        )

    monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", str(tmp_path / "missing"))
    with pytest.raises(UHCRetainedAdmissionError, match="unavailable"):
        await retain_source_native(
            source_path=source_path,
            output_root=retained_root,
            expected_sha256=fixture["artifact_sha256"],
            expected_byte_count=fixture["artifact_byte_count"],
            range_count=4,
        )

    install_fake_native(tmp_path, monkeypatch, native_summary(fixture))
    source_path.unlink()
    source_path.mkdir()
    with pytest.raises(UHCRetainedAdmissionError, match="invalid"):
        await retain_source_native(
            source_path=source_path,
            output_root=retained_root,
            expected_sha256=fixture["artifact_sha256"],
            expected_byte_count=fixture["artifact_byte_count"],
            range_count=4,
        )

    root_target = tmp_path / "root-target"
    root_target.mkdir()
    symlink_root = tmp_path / "root-link"
    symlink_root.symlink_to(root_target, target_is_directory=True)
    with pytest.raises(UHCRetainedAdmissionError, match="invalid"):
        await retain_source_native(
            source_path=fixture["raw_path"],
            output_root=symlink_root,
            expected_sha256=fixture["artifact_sha256"],
            expected_byte_count=fixture["artifact_byte_count"],
            range_count=4,
        )


def _write_process_script(path: Path, body: str) -> None:
    path.write_text("#!/usr/bin/env python3\n" + body, encoding="utf-8")
    path.chmod(0o755)


@pytest.mark.asyncio
async def test_native_nonzero_oversized_output_and_timeout_fail_closed(
    tmp_path,
    monkeypatch,
):
    retained_root = tmp_path / "retained"
    fixture = write_retained_fixture(retained_root, records_payload())
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture["source_bytes"])

    scripts = (
        ("nonzero", "import sys\nsys.stderr.write('bad')\nraise SystemExit(7)\n", "exit code"),
        (
            "oversized",
            f"import sys\nsys.stdout.write('x' * {retained_native._MAX_STDOUT_BYTES + 1})\n",
            "byte limit",
        ),
        ("timeout", "import time\ntime.sleep(60)\n", "timed out"),
    )
    for name, body, expected_message in scripts:
        binary = tmp_path / name
        _write_process_script(binary, body)
        monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", str(binary))
        if name == "timeout":
            monkeypatch.setattr(retained_native, "_timeout_seconds", lambda: 0.01)
        with pytest.raises(UHCRetainedAdmissionError, match=expected_message):
            await retain_source_native(
                source_path=source_path,
                output_root=retained_root,
                expected_sha256=fixture["artifact_sha256"],
                expected_byte_count=fixture["artifact_byte_count"],
                range_count=4,
            )


@pytest.mark.parametrize("configured", ["bad", "0", "nan", "86401"])
def test_native_timeout_configuration_is_bounded(monkeypatch, configured):
    monkeypatch.setenv("HLTHPRT_UHC_RETAIN_TIMEOUT_SECONDS", configured)
    with pytest.raises(UHCRetainedAdmissionError):
        retained_native._timeout_seconds()


def test_native_timeout_default_and_valid_override(monkeypatch):
    monkeypatch.delenv("HLTHPRT_UHC_RETAIN_TIMEOUT_SECONDS", raising=False)
    assert retained_native._timeout_seconds() == 3600
    monkeypatch.setenv("HLTHPRT_UHC_RETAIN_TIMEOUT_SECONDS", "12.5")
    assert retained_native._timeout_seconds() == 12.5


@pytest.mark.asyncio
async def test_native_cancellation_terminates_and_reaps_worker(tmp_path, monkeypatch):
    retained_root = tmp_path / "retained"
    fixture = write_retained_fixture(retained_root, records_payload())
    source_path = tmp_path / "source.json"
    source_path.write_bytes(fixture["source_bytes"])
    binary = tmp_path / "blocking"
    pid_path = tmp_path / "worker.pid"
    _write_process_script(
        binary,
        "import os, pathlib, time\n"
        f"pathlib.Path({str(pid_path)!r}).write_text(str(os.getpid()))\n"
        "time.sleep(60)\n",
    )
    monkeypatch.setenv("HLTHPRT_PTG2_RUST_SCANNER_BIN", str(binary))
    task = asyncio.create_task(
        retain_source_native(
            source_path=source_path,
            output_root=retained_root,
            expected_sha256=fixture["artifact_sha256"],
            expected_byte_count=fixture["artifact_byte_count"],
            range_count=4,
        )
    )
    for _ in range(100):
        if pid_path.exists():
            break
        await asyncio.sleep(0.01)
    assert pid_path.exists()
    worker_pid = int(pid_path.read_text())
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    with pytest.raises(ProcessLookupError):
        os.kill(worker_pid, 0)
