# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Build the slim, import-safe preflight wheel from the canonical modules."""

from __future__ import annotations

import argparse
import subprocess
import sys
from pathlib import Path
from shutil import copy2
from tempfile import TemporaryDirectory

from wheel_manifest import stage_modules

_PACKAGE_ROOT = Path(__file__).resolve().parents[2]
_PACKAGE_CONFIG = Path(__file__).with_name("pyproject.toml")


def build_wheel(output_directory: Path) -> Path:
    """Build offline with the repository's pinned CI tools into a caller-owned directory."""

    output_directory.mkdir(parents=True, exist_ok=True)
    with TemporaryDirectory(prefix="custom-import-preflight-") as temporary_directory:
        stage_root = Path(temporary_directory)
        stage_modules(_PACKAGE_ROOT, stage_root)
        copy2(_PACKAGE_CONFIG, stage_root / "pyproject.toml")
        copy2(_PACKAGE_ROOT / "LICENSE", stage_root / "LICENSE")
        subprocess.run(
            [
                sys.executable,
                "-m",
                "pip",
                "wheel",
                "--no-deps",
                "--no-build-isolation",
                "--wheel-dir",
                str(output_directory),
                str(stage_root),
            ],
            check=True,
        )
    wheels = tuple(output_directory.glob("custom_import_preflight-*.whl"))
    if len(wheels) != 1:
        raise RuntimeError("expected exactly one preflight wheel")
    return wheels[0]


def _arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", required=True, type=Path)
    return parser.parse_args()


if __name__ == "__main__":
    print(build_wheel(_arguments().output_dir))
