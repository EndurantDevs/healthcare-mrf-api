# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static proof that retained-row replay remains explicit and dormant."""

from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys

_EXECUTOR_MODULE = "process.public_evidence_fhir_organization_replay"


def _repository_root() -> Path:
    return Path(__file__).resolve().parents[1]


def test_replay_executor_has_no_database_publication_or_secret_loader() -> None:
    executor_path = (
        _repository_root() / "process" / "public_evidence_fhir_organization_replay.py"
    )
    module_source = executor_path.read_text(encoding="utf-8")
    tree = ast.parse(module_source, filename=str(executor_path))
    imported_modules = {
        node.module
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is not None
    }
    called_names = {
        node.func.id
        for node in ast.walk(tree)
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name)
    }

    assert not imported_modules & {
        "api",
        "asyncpg",
        "db",
        "process.tin_npi_connector_build",
        "process.tin_npi_connector_generation",
        "process.tin_npi_connector_publication",
        "service",
        "sqlalchemy",
    }
    assert not called_names & {
        "connect",
        "execute",
        "load_tin_token_policy",
        "publish",
        "serve",
        "swap",
    }
    assert not any(
        isinstance(node, (ast.AsyncFunctionDef, ast.Await)) for node in ast.walk(tree)
    )


def test_replay_executor_is_not_wired_into_runtime_or_package_init() -> None:
    repository_root = _repository_root()
    production_paths = [repository_root / "main.py"]
    for package_name in ("api", "db", "process", "service"):
        production_paths.extend((repository_root / package_name).rglob("*.py"))
    executor_path = (
        repository_root / "process" / ("public_evidence_fhir_organization_replay.py")
    )
    for path in production_paths:
        if path == executor_path:
            continue
        module_source = path.read_text(encoding="utf-8")
        assert _EXECUTOR_MODULE not in module_source
        assert "replay_fhir_organization_retained_rows" not in module_source
        assert "verify_fhir_organization_replay_result" not in module_source
    package_init = (repository_root / "public_evidence" / "__init__.py").read_text(
        encoding="utf-8"
    )
    assert "source_record_replay" not in package_init


def test_importing_replay_contract_does_not_load_process_packages() -> None:
    script = """
import json
import sys
before = set(sys.modules)
from public_evidence import source_record_replay_contract
introduced = set(sys.modules) - before
forbidden = sorted(
    name for name in introduced
    if name in {"api", "db", "process", "service"}
    or name.startswith(("api.", "db.", "process.", "service."))
)
print(json.dumps(forbidden))
raise SystemExit(bool(forbidden))
"""
    completed = subprocess.run(
        [sys.executable, "-B", "-c", script],
        cwd=_repository_root(),
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "[]"


def test_existing_adapters_and_publication_intents_do_not_consume_replay() -> None:
    repository_root = _repository_root()
    paths = (
        *sorted((repository_root / "public_evidence").glob("adapter_projection_*.py")),
        *sorted(
            (repository_root / "public_evidence").glob("staged_bundle_publication_*.py")
        ),
    )
    for path in paths:
        module_source = path.read_text(encoding="utf-8")
        assert "source_record_replay" not in module_source
        assert "FhirOrganizationReplayResult" not in module_source
