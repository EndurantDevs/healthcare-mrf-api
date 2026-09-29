# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Synthetic checks for the standalone pure Snowflake preflight wheel."""

from __future__ import annotations

import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

import process.custom_import.capture as capture
import process.custom_import.capture_limits as capture_limits
import process.custom_import.snowflake_binding as snowflake_binding
import process.custom_import.snowflake_source_binding as snowflake_source_binding
from support.custom_import_preflight.wheel_manifest import CANONICAL_MODULES, WHEEL_INITIALIZERS

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_BUILD_SCRIPT = _REPOSITORY_ROOT / "support" / "custom_import_preflight" / "build_wheel.py"
_CONSUMER_SCRIPT = _REPOSITORY_ROOT / "tests" / "fixtures" / "custom_import_preflight_consumer.py"


@pytest.fixture(scope="module")
def preflight_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    output_directory = tmp_path_factory.mktemp("preflight-wheel")
    subprocess.run(
        [sys.executable, str(_BUILD_SCRIPT), "--output-dir", str(output_directory)],
        check=True,
        cwd=_REPOSITORY_ROOT,
        env={**os.environ, "PIP_NO_INDEX": "1"},
        capture_output=True,
        text=True,
    )
    wheels = tuple(output_directory.glob("custom_import_preflight-*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def test_legacy_exports_keep_the_pure_contract_identity():
    assert capture.CaptureLimits is capture_limits.CaptureLimits
    assert snowflake_source_binding.SnowflakeSourceBinding is snowflake_binding.SnowflakeSourceBinding
    assert snowflake_source_binding.SnowflakeSourceBindingError is snowflake_binding.SnowflakeSourceBindingError


def test_wheel_contains_only_the_declared_process_modules(preflight_wheel: Path):
    with zipfile.ZipFile(preflight_wheel) as archive:
        process_members = frozenset(name for name in archive.namelist() if name.startswith("process/"))
        license_members = [name for name in archive.namelist() if name.endswith(".dist-info/licenses/LICENSE")]
        assert len(license_members) == 1
        assert archive.read(license_members[0]) == (_REPOSITORY_ROOT / "LICENSE").read_bytes()

    assert process_members == frozenset((*WHEEL_INITIALIZERS, *CANONICAL_MODULES))


def test_installed_wheel_runs_preflight_without_engine_dependencies(preflight_wheel: Path, tmp_path: Path):
    consumer_directory = tmp_path / "consumer"
    subprocess.run(
        [
            sys.executable,
            "-m",
            "pip",
            "install",
            "--no-deps",
            "--target",
            str(consumer_directory),
            str(preflight_wheel),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    completed = subprocess.run(
        [sys.executable, "-I", str(_CONSUMER_SCRIPT), str(consumer_directory)],
        check=False,
        cwd=tmp_path,
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
