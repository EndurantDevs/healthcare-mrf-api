# Licensed under the HealthPorta Non-Commercial License (see LICENSE).

"""The installed preparation library keeps native contracts without CLI startup."""

from __future__ import annotations

import ast
import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import zipfile
from pathlib import Path

import pytest

from support.plan_release_preparation.wheel_manifest import (
    CANONICAL_MODULES,
    LIBRARY_CONTRACT,
    LIBRARY_MANIFEST,
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_BUILD_SCRIPT = _REPOSITORY_ROOT / "support" / "plan_release_preparation" / "build_wheel.py"
CONSUMER_SCRIPT = """
import asyncio
import hashlib
import importlib
import json
import sys
from importlib.metadata import distribution
from pathlib import Path

library = distribution("plan-release-preparation")
manifest = json.loads(library.locate_file("process/plan_release_preparation_library.json").read_text())
assert manifest["contract"] == "plan-release-preparation-library-v1"
assert manifest["distribution"] == {"name": library.metadata["Name"], "version": library.version}
for namespace in ("process", "process.ptg_parts", "db"):
    assert importlib.import_module(namespace).__spec__.origin is None
for relative_path, expected_hash in manifest["files"].items():
    if not relative_path.endswith(".py"):
        continue
    module = importlib.import_module(relative_path[:-3].replace("/", "."))
    installed_path = Path(library.locate_file(relative_path)).resolve()
    assert Path(module.__file__).resolve() == installed_path
    assert hashlib.sha256(installed_path.read_bytes()).hexdigest() == expected_hash
assert not {"db.connection", "db.models", "click", "arq"}.intersection(sys.modules)

from process.plan_pricing_control_catalog import NATIVE_RUN_COLUMNS
from process.plan_pricing_no_work import RETRY_NATIVE_PREDICATE, require_native_control_shape
from process.ptg_parts.ptg2_lifecycle_lock import acquire_ptg2_source_lifecycle_lock

class NativeContract:
    def __init__(self):
        self.columns = list(NATIVE_RUN_COLUMNS)
        self.statements = []

    async def fetch(self, query, *args):
        assert "pg_attribute" in query
        return [{"attname": name, "type": "timestamp without time zone"} for name in self.columns]

    async def fetchrow(self, query, *args):
        assert "pg_index" in query
        return {"valid": True, "keys": ["retry_of_run_id", "run_id"], "predicate": RETRY_NATIVE_PREDICATE}

    async def execute(self, statement, parameters):
        self.statements.append((str(statement), parameters))

async def verify_native_contracts():
    connection = NativeContract()
    await require_native_control_shape(connection)
    connection.columns.pop()
    try:
        await require_native_control_shape(connection)
    except ValueError as error:
        assert "native_run_columns_changed" in str(error)
    else:
        raise AssertionError("changed native columns were accepted")
    assert await acquire_ptg2_source_lifecycle_lock(connection, source_key="synthetic_source") == "synthetic_source"
    assert len(connection.statements) == 3
    assert connection.statements[0][1] == {"lock_timeout": "500ms", "statement_timeout": "5s"}
    assert "pg_advisory_xact_lock_shared" in connection.statements[1][0]
    assert "pg_advisory_xact_lock(" in connection.statements[2][0]

asyncio.run(verify_native_contracts())
assert not {"db.connection", "db.models", "click", "arq"}.intersection(sys.modules)
print("installed preparation contracts verified")
"""


@pytest.mark.parametrize("backend_version", [None, "84.0.0", "84.1.1"])
def test_builder_refuses_unpinned_backend_before_creating_output(tmp_path, monkeypatch, backend_version):
    monkeypatch.syspath_prepend(str(_BUILD_SCRIPT.parent))
    specification = importlib.util.spec_from_file_location("preparation_build_test", _BUILD_SCRIPT)
    builder = importlib.util.module_from_spec(specification)
    specification.loader.exec_module(builder)

    def installed_version(package_name):
        assert package_name == "setuptools"
        if backend_version is None:
            raise builder.PackageNotFoundError(package_name)
        return backend_version

    def refuse_staging(*_arguments):
        raise AssertionError("backend validation must precede staging")

    monkeypatch.setattr(builder, "version", installed_version, raising=False)
    monkeypatch.setattr(builder, "stage_modules", refuse_staging)
    output_directory = tmp_path / "output"
    with pytest.raises(RuntimeError, match="requires setuptools==84.1.0"):
        builder.build_wheel(output_directory)
    assert not output_directory.exists()


@pytest.fixture(scope="module")
def preparation_wheel(tmp_path_factory: pytest.TempPathFactory) -> Path:
    output_directory = tmp_path_factory.mktemp("preparation-wheel")
    subprocess.run(
        [sys.executable, str(_BUILD_SCRIPT), "--output-dir", str(output_directory)],
        check=True,
        cwd=_REPOSITORY_ROOT,
        env={**os.environ, "UV_NO_INDEX": "1"},
        capture_output=True,
        text=True,
    )
    wheels = tuple(output_directory.glob("plan_release_preparation-*.whl"))
    assert len(wheels) == 1
    return wheels[0]


def test_wheel_preserves_exact_modules_license_and_separate_source_identity(preparation_wheel: Path):
    with zipfile.ZipFile(preparation_wheel) as archive:
        members = archive.namelist()
        assert {name for name in members if name.endswith(".py")} == set(CANONICAL_MODULES)
        assert all(name.startswith(("db/", "process/", "plan_release_preparation-")) for name in members)
        manifest = json.loads(archive.read(LIBRARY_MANIFEST))
        assert manifest["contract"] == LIBRARY_CONTRACT
        assert manifest["distribution"] == {"name": "plan-release-preparation", "version": "0.1.0"}
        assert set(manifest["files"]) == {*CANONICAL_MODULES, "LICENSE"}
        assert set(manifest["source"]) == {"commit", "tree", "clean"}
        assert type(manifest["source"]["clean"]) is bool
        coordinates = subprocess.check_output(
            ["git", "rev-parse", "HEAD", "HEAD^{tree}"], cwd=_REPOSITORY_ROOT, text=True
        ).splitlines()
        assert [manifest["source"]["commit"], manifest["source"]["tree"]] == coordinates
        assert set(manifest["build"]) == {"python", "setuptools", "uv"}
        for relative_path in CANONICAL_MODULES:
            expected = (_REPOSITORY_ROOT / relative_path).read_bytes()
            assert archive.read(relative_path) == expected
            assert manifest["files"][relative_path] == hashlib.sha256(expected).hexdigest()
        license_members = [name for name in members if name.endswith(".dist-info/licenses/LICENSE")]
        assert len(license_members) == 1
        license_bytes = (_REPOSITORY_ROOT / "LICENSE").read_bytes()
        assert archive.read(license_members[0]) == license_bytes
        assert manifest["files"]["LICENSE"] == hashlib.sha256(license_bytes).hexdigest()


@pytest.mark.parametrize("relative_path", CANONICAL_MODULES)
def test_declared_closure_covers_function_local_imports(relative_path):
    modules = {path[:-3].replace("/", ".") for path in CANONICAL_MODULES}
    tree = ast.parse((_REPOSITORY_ROOT / relative_path).read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            dependencies = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            assert node.level == 0, relative_path
            dependencies = [node.module]
            if node.module in {"db", "process", "process.ptg_parts"}:
                dependencies = [node.module + "." + alias.name for alias in node.names]
        else:
            continue
        for dependency in dependencies:
            root = dependency.split(".", 1)[0]
            if root in {"db", "process"}:
                assert dependency in modules, (relative_path, dependency)
            else:
                assert root in sys.stdlib_module_names | {"sqlalchemy", "dateutil"}, (relative_path, dependency)


def test_installed_wheel_runs_outside_the_source_checkout(preparation_wheel: Path, tmp_path: Path):
    environment = tmp_path / "consumer"
    subprocess.run(
        [
            "uv",
            "--no-config",
            "pip",
            "install",
            "--python",
            sys.executable,
            "--no-index",
            "--no-deps",
            "--target",
            str(environment),
            str(preparation_wheel),
        ],
        check=True,
        capture_output=True,
        text=True,
    )
    completed = subprocess.run(
        [sys.executable, "-P", "-s", "-B", "-c", CONSUMER_SCRIPT],
        check=False,
        cwd=tmp_path,
        env={**os.environ, "PYTHONPATH": str(environment)},
        capture_output=True,
        text=True,
    )
    assert completed.returncode == 0, completed.stderr
    assert completed.stdout.strip() == "installed preparation contracts verified"
