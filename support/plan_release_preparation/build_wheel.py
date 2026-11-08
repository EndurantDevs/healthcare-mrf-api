# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Build the narrow native preparation library with standard namespace packages."""

from __future__ import annotations

import argparse
import subprocess
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from shutil import copy2
from tempfile import TemporaryDirectory

from wheel_manifest import stage_modules

_REPOSITORY_ROOT = Path(__file__).resolve().parents[2]


def build_wheel(output_directory: Path) -> Path:
    """Build offline with the pinned toolchain into a caller-owned directory."""
    try:
        backend_version = version("setuptools")
    except PackageNotFoundError:
        backend_version = None
    if backend_version != "84.1.0":
        raise RuntimeError("preparation wheel requires setuptools==84.1.0")
    output_directory = output_directory.resolve()
    output_directory.mkdir(parents=True, exist_ok=True)
    if tuple(output_directory.glob("plan_release_preparation-*.whl")):
        raise ValueError("output directory already contains a preparation wheel")
    with TemporaryDirectory(prefix="plan-release-preparation-") as temporary_directory:
        stage_root = Path(temporary_directory)
        stage_modules(_REPOSITORY_ROOT, stage_root)
        copy2(Path(__file__).with_name("pyproject.toml"), stage_root / "pyproject.toml")
        subprocess.run(
            [
                "uv",
                "--no-config",
                "build",
                "--wheel",
                "--python",
                sys.executable,
                "--offline",
                "--no-build-isolation",
                "--out-dir",
                str(output_directory),
                str(stage_root),
            ],
            check=True,
        )
    wheels = tuple(output_directory.glob("plan_release_preparation-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("expected exactly one preparation wheel")
    return wheels[0]


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    print(build_wheel(_arguments().output_dir))
