"""The public image carries importer dependencies, not deployment tooling."""

import ast
from pathlib import Path
import re
import shlex

import pytest

from scripts.python_locks import LOCK_INPUTS, validate


ROOT = Path(__file__).resolve().parents[1]


def test_runtime_copy_boundary_keeps_production_imports():
    runtime_stage = (ROOT / "Dockerfile").read_text().split("\nFROM ")[-1]
    copies = {
        copied_path
        for line in runtime_stage.splitlines()
        if line.startswith("COPY ") and "--from=" not in line
        for copied_path in shlex.split(line)[1:-1]
    }
    assert {path for path in copies if path.startswith("scripts/")} == {
        "scripts/provider_directory_support_contract.py",
        "scripts/validation/ptg2_v3_source_api_audit.py",
        "scripts/python_locks.py",
    }
    assert "COPY support/ " not in runtime_stage
    assert "requirements-ci" not in runtime_stage
    assert "scripts/ci/" not in runtime_stage
    assert "rm -rf /wheels" in runtime_stage

    # Catch new runtime imports that would otherwise work in a checkout but
    # disappear from the deliberately smaller client image.
    for directory in ("api", "db", "process", "public_evidence", "alembic", "restore"):
        for path in (ROOT / directory).rglob("*.py"):
            for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
                if not isinstance(node, ast.ImportFrom) or not node.module:
                    continue
                if node.module.split(".")[0] not in {"scripts", "support"}:
                    continue
                module = node.module.replace(".", "/")
                required = [module + ".py"] if (ROOT / (module + ".py")).is_file() else [
                    module + "/" + name.name + ".py" for name in node.names
                ]
                assert set(required) <= copies, (path, required)
    assert "support/zip/" in copies


def test_runtime_lock_rejects_stale_inputs_and_excludes_ci_dependencies(tmp_path):
    validate(ROOT)
    for name in {*LOCK_INPUTS, *(name for inputs in LOCK_INPUTS.values() for name in inputs)}:
        (tmp_path / name).write_bytes((ROOT / name).read_bytes())
    (tmp_path / "requirements.txt").write_text("unexpected-package==1\n")
    with pytest.raises(ValueError, match="requirements-runtime.lock is stale"):
        validate(tmp_path)
    names = set(re.findall(r"^([a-z0-9-]+)(?:\[[^]]+\])?==", (ROOT / "requirements-runtime.lock").read_text(), re.M))
    assert not names & {"pytest", "coverage", "pip-audit", "maturin", "uv", "pytest-xdist"}


def test_local_example_has_neutral_database_and_no_shared_operator_token():
    value_by_name = dict(
        line.split("=", 1) for line in (ROOT / ".env.example").read_text().splitlines()
        if line and not line.startswith("#")
    )
    assert value_by_name["HLTHPRT_DB_PORT"] == "5432"
    assert "HLPRT_DB_PORT" not in value_by_name
    assert value_by_name["HLTHPRT_DB_USER"] == "mrf_api"
    assert value_by_name["HLTHPRT_CONTROL_API_TOKEN"] == ""
