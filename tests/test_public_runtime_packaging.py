"""The public image carries importer dependencies, not deployment tooling."""

import ast
from pathlib import Path
import re
import shlex

import pytest

from scripts.python_locks import LOCK_INPUTS, validate


ROOT = Path(__file__).resolve().parents[1]
PUBLIC_OPERATOR_SCRIPTS = {
    "scripts/smoke/formulary_fhir_reviewed_operator.py",
    "scripts/smoke/formulary_fhir_synthetic_canary.py",
    "scripts/smoke/formulary_fhir_synthetic_seed_publisher.py",
    "scripts/smoke/provider_directory_fhir_reviewed_subset_state.py",
    "scripts/smoke/provider_directory_rooted_graph_operator.py",
    "scripts/smoke/provider_directory_terminal_root_retirement.py",
    "scripts/smoke/uhc_flex_practitioner_operator.py",
    "scripts/smoke/uhc_formulary_operator.py",
}
SYNTHETIC_FIXTURES = {
    f"scripts/smoke/fixtures/formulary_fhir/{name}.json"
    for name in ("canary_expected_v1", "coverage_plan", "medication_a", "medication_b")
}


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
        *PUBLIC_OPERATOR_SCRIPTS,
        *SYNTHETIC_FIXTURES,
    }
    assert "COPY support/ " not in runtime_stage
    assert "requirements-ci" not in runtime_stage
    assert "scripts/ci/" not in runtime_stage
    assert "rm -rf /wheels" in runtime_stage

    # Catch new runtime imports that would otherwise work in a checkout but
    # disappear from the deliberately smaller client image.
    runtime_paths = [ROOT / path for path in copies if path.endswith(".py")]
    for directory in ("api", "db", "process", "public_evidence", "alembic", "restore"):
        runtime_paths.extend((ROOT / directory).rglob("*.py"))
    for path in runtime_paths:
        for node in ast.walk(ast.parse(path.read_text(), filename=str(path))):
            if isinstance(node, ast.Import):
                modules = [name.name for name in node.names]
            elif isinstance(node, ast.ImportFrom) and node.module:
                modules = [node.module] if (ROOT / (node.module.replace(".", "/") + ".py")).is_file() else [
                    node.module + "." + name.name for name in node.names
                ]
            else:
                continue
            required_paths = {module.replace(".", "/") + ".py" for module in modules if module.split(".")[0] in {"scripts", "support"}}
            assert required_paths <= copies, (path, required_paths)
    assert "support/zip/" in copies



def test_documented_container_commands_are_packaged():
    runtime_stage = (ROOT / "Dockerfile").read_text().split("\nFROM ")[-1]
    # Container-facing examples are entrypoints too, even when no API module
    # imports them. Checkout-only development commands are not image promises.
    documented_commands = {
        command
        for document in (ROOT / "docs/imports").glob("*.md")
        for command in re.findall(r"/opt/(scripts/[a-zA-Z0-9_./-]+\.py)", document.read_text())
    }
    assert documented_commands
    for command in documented_commands:
        assert f"COPY {command} /opt/{command}" in runtime_stage


def test_runtime_lock_rejects_stale_inputs_and_excludes_ci_dependencies(tmp_path):
    dockerfile = (ROOT / "Dockerfile").read_text()
    assert (
        "python:3.14.7-slim-trixie@sha256:"
        "cad9a2c871761c413caa6fdd6441c783451e740a48aaeba60ae62a8b53525ef6"
    ) in dockerfile
    assert "--require-hashes" in dockerfile
    assert "--only-binary=:all:" in dockerfile
    assert (
        "ghcr.io/astral-sh/uv:0.12.11@sha256:"
        "79c6f4776b851471cc73b7d21d0cc834bb94383c292e83640d27eff512864df7"
    ) in dockerfile
    assert "uv pip check" in dockerfile
    assert "uv pip install" in dockerfile
    assert not re.search(r"(?:python3? -m|&&) pip ", dockerfile)
    assert "python3-pip" not in dockerfile
    validate(ROOT)
    for name in {*LOCK_INPUTS, *(name for inputs in LOCK_INPUTS.values() for name in inputs)}:
        (tmp_path / name).write_bytes((ROOT / name).read_bytes())
    (tmp_path / "requirements.txt").write_text("unexpected-package==1\n")
    with pytest.raises(ValueError, match="requirements-runtime.lock is stale"):
        validate(tmp_path)
    names = set(re.findall(r"^([a-z0-9-]+)(?:\[[^]]+\])?==", (ROOT / "requirements-runtime.lock").read_text(), re.M))
    assert not names & {"pip", "pytest", "coverage", "pip-audit", "maturin", "uv", "pytest-xdist"}


def test_native_extension_supports_python_314_and_newer():
    assert 'requires-python = ">=3.14"' in (ROOT / "support/ptg2_scanner/pyproject.toml").read_text()
    assert 'features = ["abi3-py314"]' in (ROOT / "support/ptg2_scanner/Cargo.toml").read_text()


def test_local_example_has_neutral_database_and_no_shared_operator_token():
    value_by_name = dict(
        line.split("=", 1) for line in (ROOT / ".env.example").read_text().splitlines()
        if line and not line.startswith("#")
    )
    assert value_by_name["HLTHPRT_DB_PORT"] == "5432"
    assert "HLPRT_DB_PORT" not in value_by_name
    assert value_by_name["HLTHPRT_DB_USER"] == "mrf_api"
    assert value_by_name["HLTHPRT_CONTROL_API_TOKEN"] == ""
