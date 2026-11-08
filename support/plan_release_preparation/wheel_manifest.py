# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""Stage the complete native helper closure without runtime package initializers."""

from __future__ import annotations

import hashlib
import json
import platform
import subprocess
import tomllib
from importlib.metadata import version
from pathlib import Path

CANONICAL_MODULES = (
    "db/migration_expression_adoption.py",
    "db/plan_release_storage.py",
    "process/plan_pricing_admission.py",
    "process/plan_pricing_control.py",
    "process/plan_pricing_control_catalog.py",
    "process/plan_pricing_control_policy.py",
    "process/plan_pricing_no_work.py",
    "process/plan_pricing_retained_inputs.py",
    "process/plan_release_preparation_pin_catalog.py",
    "process/plan_release_preparation_pin_contract.py",
    "process/plan_release_preparation_pin_retirement.py",
    "process/plan_release_preparation_pins.py",
    "process/plan_release_storage_catalog.py",
    "process/plan_release_storage_custody.py",
    "process/ptg_parts/canonical.py",
    "process/ptg_parts/config.py",
    "process/ptg_parts/domain.py",
    "process/ptg_parts/ptg2_lifecycle_lock.py",
    "process/ptg_parts/snapshot_tables.py",
    "process/source_profile_result_pins.py",
)
LIBRARY_MANIFEST = "process/plan_release_preparation_library.json"
LIBRARY_CONTRACT = "plan-release-preparation-library-v1"


def source_identity(source_root: Path) -> dict:
    """Distinguish a local candidate from the exact committed source tree."""
    coordinates = subprocess.check_output(
        ["git", "rev-parse", "HEAD", "HEAD^{tree}"], cwd=source_root, text=True
    ).splitlines()
    status = subprocess.check_output(["git", "status", "--porcelain", "--untracked-files=all"], cwd=source_root)
    return {"commit": coordinates[0], "tree": coordinates[1], "clean": not status}


def stage_modules(source_root: Path, stage_root: Path) -> None:
    """Copy canonical bytes and retain a separate, verifiable library identity."""
    identity = source_identity(source_root)
    sha256_by_path = {}
    for relative_path in (*CANONICAL_MODULES, "LICENSE"):
        contents = (source_root / relative_path).read_bytes()
        target = stage_root / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(contents)
        sha256_by_path[relative_path] = hashlib.sha256(contents).hexdigest()
    project = tomllib.loads(Path(__file__).with_name("pyproject.toml").read_text())["project"]
    manifest_by_field = {
        "contract": LIBRARY_CONTRACT,
        "distribution": {"name": project["name"], "version": project["version"]},
        "source": identity,
        "files": sha256_by_path,
        "build": {
            "python": platform.python_version(),
            "setuptools": version("setuptools"),
            "uv": subprocess.check_output(["uv", "--version"], text=True).split()[1],
        },
    }
    (stage_root / LIBRARY_MANIFEST).write_text(json.dumps(manifest_by_field, sort_keys=True, indent=2) + "\n")
    if source_identity(source_root) != identity:
        raise RuntimeError("source identity changed while staging the library")
