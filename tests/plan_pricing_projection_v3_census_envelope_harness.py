"""Shared fake-process harness for the projection-v3 census envelope tests."""

import hashlib
import json
import os
from pathlib import Path
import subprocess

from tests import test_plan_pricing_projection_v3_census_envelope as fixture

SCRIPT = fixture.SCRIPT
SOURCE_SHA = fixture.SOURCE_SHA
OWNER = fixture.OWNER
_FAKE_COMMAND = fixture._FAKE_COMMAND
SOURCE_MANIFEST_SHA256 = "e" * 64
HARNESS_MANIFEST_SHA256 = "f" * 64
OVERLAY_BYTES = b"reviewed source overlay"
LARGE_OVERLAY_BYTES = b"x" * 100_000
OVERLAY_SHA256 = hashlib.sha256(OVERLAY_BYTES).hexdigest()


def _kernel_directory(path: Path) -> Path:
    """Return the kernel-visible spelling of an existing test directory."""

    resolved = path.resolve(strict=True)
    descriptor = os.open(resolved, os.O_RDONLY | os.O_DIRECTORY)
    try:
        proc_descriptor = Path(f"/proc/self/fd/{descriptor}")
        return (
            Path(os.path.realpath(proc_descriptor))
            if proc_descriptor.exists()
            else resolved
        )
    finally:
        os.close(descriptor)


def _state_root(parent: Path, name: str = "envelopes") -> Path:
    """Create an exact-mode root beneath the kernel-visible test directory."""

    state_root = _kernel_directory(parent) / name
    state_root.mkdir(mode=0o700)
    state_root.chmod(0o700)
    return state_root


def _arguments(
    state_root: Path,
    repo: Path,
    overlay_sha256: str = OVERLAY_SHA256,
) -> list[str]:
    """Build the reviewed envelope arguments for a fake census run."""

    receipt_path = str(state_root / "run/census-receipt.json")
    child_arguments = [str(repo.parent / "bin/census-child"), "--receipt", receipt_path]
    command_bytes = b"".join(argument.encode() + b"\0" for argument in child_arguments)
    return [
        "--owner-token",
        OWNER,
        "--state-dir",
        str(state_root / "run"),
        "--source-sha",
        SOURCE_SHA,
        "--repo-dir",
        str(repo),
        "--deadline-seconds",
        "900",
        "--census-job",
        "plan-pricing-v3-census-test",
        "--census-configmap",
        "plan-pricing-v3-census-src-test",
        "--census-receipt",
        receipt_path,
        "--runtime-attestation",
        str(state_root / "run/runtime-attestation.json"),
        "--expected-envelope-script-sha256",
        hashlib.sha256(SCRIPT.read_bytes()).hexdigest(),
        "--expected-child-command-sha256",
        hashlib.sha256(command_bytes).hexdigest(),
        "--expected-child-executable-sha256",
        hashlib.sha256(_FAKE_COMMAND.encode()).hexdigest(),
        "--expected-source-manifest-sha256",
        SOURCE_MANIFEST_SHA256,
        "--expected-harness-manifest-sha256",
        HARNESS_MANIFEST_SHA256,
        "--expected-source-overlay-sha256",
        overlay_sha256,
        "--postgresql-tablespace-path",
        str(repo),
        "--minimum-host-available-memory-bytes",
        "1",
        "--minimum-host-swap-free-bytes",
        "1",
        "--minimum-postgresql-tablespace-free-bytes",
        "1",
        "--drain-deployment",
        "control-api",
        "--import-scheduler-deployment",
        "control-scheduler",
        "--import-node-id",
        "plan-node",
        "--import-token-env",
        "TEST_IMPORT_TOKEN",
        "--",
        *child_arguments,
    ]


def _fake_environment(
    tmp_path: Path, **overrides: str
) -> tuple[dict[str, str], Path, Path]:
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    dispatcher = fake_bin / "fake-command"
    dispatcher.write_text(_FAKE_COMMAND, encoding="utf-8")
    dispatcher.chmod(0o755)
    for command in (
        "git",
        "hostname",
        "k3s",
        "setsid",
        "systemctl",
        "systemd-run",
        "timeout",
    ):
        (fake_bin / command).symlink_to(dispatcher)
    sleep_command = fake_bin / "sleep"
    sleep_command.write_text("#!/bin/sh\nexit 0\n", encoding="utf-8")
    sleep_command.chmod(0o755)
    census_child = fake_bin / "census-child"
    census_child.write_text(_FAKE_COMMAND, encoding="utf-8")
    census_child.chmod(0o755)
    fake_state = tmp_path / "fake-state"
    fake_state.mkdir()
    overlay_bytes = (
        LARGE_OVERLAY_BYTES
        if overrides.get("FAKE_LARGE_OVERLAY") == "1"
        else OVERLAY_BYTES
    )
    (fake_state / "source-overlay.tar.gz").write_bytes(overlay_bytes)
    state_root = _state_root(tmp_path)
    checkout = tmp_path / "repo"
    checkout.mkdir()
    arc_helper = checkout / "scripts/research/plan_pricing_projection_v3_census_arc.py"
    arc_helper.parent.mkdir(parents=True)
    arc_helper.write_text("# reviewed ARC helper\n", encoding="utf-8")
    reviewed_arc_helper = fake_state / "reviewed-arc-helper.py"
    reviewed_arc_helper.write_bytes(
        b"# different ARC helper\n"
        if overrides.get("FAKE_ARC_HELPER_MISMATCH") == "1"
        else arc_helper.read_bytes()
    )
    env_by_name = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DENIAL_MARKER": f"hp-pv3-census-deny-{OWNER}",
        "FAKE_CHILD_EXECUTABLE": str(census_child),
        "FAKE_IMPORT_NODE_ID": "plan-node",
        "FAKE_IMPORT_SCHEDULER": "control-scheduler",
        "FAKE_IMPORT_TOKEN_ENV": "TEST_IMPORT_TOKEN",
        "FAKE_OWNER": OWNER,
        "FAKE_REVIEWED_ARC_HELPER": str(reviewed_arc_helper),
        "FAKE_POLICY": f"hp-pv3-census-{OWNER}.healthporta.com",
        "FAKE_QUOTA": f"hp-pv3-census-{OWNER}",
        "FAKE_SOURCE_SHA": SOURCE_SHA,
        "FAKE_SOURCE_MANIFEST_SHA256": SOURCE_MANIFEST_SHA256,
        "FAKE_HARNESS_MANIFEST_SHA256": HARNESS_MANIFEST_SHA256,
        "FAKE_SOURCE_OVERLAY_SHA256": hashlib.sha256(overlay_bytes).hexdigest(),
        "FAKE_STATE_ROOT": str(state_root),
        "FAKE_OUTSIDE_STATE_ROOT": str(tmp_path / "outside-state-root"),
        "FAKE_STATE": str(fake_state),
        "HLTHPRT_PLAN_PRICING_V3_CENSUS_ENVELOPE_RUN": "run",
        "HLTHPRT_PLAN_PRICING_V3_CENSUS_STATE_ROOT": str(state_root),
        **overrides,
    }
    return env_by_name, state_root, checkout


def _receipt(state_root: Path) -> dict:
    return json.loads((state_root / "run/envelope-receipt.json").read_text())


def _run_envelope(
    tmp_path: Path, **overrides: str
) -> tuple[subprocess.CompletedProcess[str], Path]:
    env_by_name, state_root, checkout = _fake_environment(tmp_path, **overrides)
    result = subprocess.run(
        [
            "/bin/bash",
            str(SCRIPT),
            "run",
            *_arguments(
                state_root,
                checkout,
                env_by_name["FAKE_SOURCE_OVERLAY_SHA256"],
            ),
        ],
        env=env_by_name,
        check=False,
        capture_output=True,
        text=True,
        timeout=60,
    )
    return result, state_root
