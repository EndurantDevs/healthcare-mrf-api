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


def _arguments(state_root: Path, repo: Path) -> list[str]:
    receipt_path = str(state_root / "run/census-receipt.json")
    child_arguments = [
        str(repo.parent / "bin/census-child"),
        "--receipt",
        receipt_path,
    ]
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
        "sleep",
        "systemctl",
        "systemd-run",
        "timeout",
    ):
        (fake_bin / command).symlink_to(dispatcher)
    census_child = fake_bin / "census-child"
    census_child.write_text(_FAKE_COMMAND, encoding="utf-8")
    census_child.chmod(0o755)
    fake_state = tmp_path / "fake-state"
    fake_state.mkdir()
    state_root = tmp_path / "envelopes"
    state_root.mkdir()
    checkout = tmp_path / "repo"
    checkout.mkdir()
    env_by_name = {
        **os.environ,
        "PATH": f"{fake_bin}:{os.environ['PATH']}",
        "FAKE_DENIAL_MARKER": f"hp-pv3-census-deny-{OWNER}",
        "FAKE_CHILD_EXECUTABLE": str(census_child),
        "FAKE_IMPORT_NODE_ID": "plan-node",
        "FAKE_IMPORT_SCHEDULER": "control-scheduler",
        "FAKE_IMPORT_TOKEN_ENV": "TEST_IMPORT_TOKEN",
        "FAKE_OWNER": OWNER,
        "FAKE_POLICY": f"hp-pv3-census-{OWNER}.healthporta.com",
        "FAKE_QUOTA": f"hp-pv3-census-{OWNER}",
        "FAKE_SOURCE_SHA": SOURCE_SHA,
        "FAKE_STATE": str(fake_state),
        "HLTHPRT_PLAN_PRICING_V3_CENSUS_ENVELOPE_RUN": "run",
        "HLTHPRT_PLAN_PRICING_V3_CENSUS_STATE_ROOT": str(state_root),
        **overrides,
    }
    return env_by_name, state_root, checkout


def _receipt(state_root) -> dict:
    return json.loads((state_root / "run/envelope-receipt.json").read_text())


def _run_envelope(tmp_path, **overrides) -> tuple[subprocess.CompletedProcess, Path]:
    env_by_name, state_root, checkout = _fake_environment(tmp_path, **overrides)
    result = subprocess.run(
        ["/bin/bash", str(SCRIPT), "run", *_arguments(state_root, checkout)],
        env=env_by_name,
        check=False,
        capture_output=True,
        text=True,
        timeout=30,
    )
    return result, state_root
