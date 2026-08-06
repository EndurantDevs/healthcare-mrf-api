# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static dormancy proof for normalized public-evidence record modules."""

from __future__ import annotations

import ast
from pathlib import Path
import subprocess
import sys


def _import_roots(node: ast.AST) -> tuple[str, ...]:
    if isinstance(node, ast.Import):
        return tuple(alias.name.split(".", 1)[0] for alias in node.names)
    if isinstance(node, ast.ImportFrom) and node.module:
        return (node.module.split(".", 1)[0],)
    return ()


def _called_name(node: ast.AST) -> str | None:
    if not isinstance(node, ast.Call):
        return None
    if isinstance(node.func, ast.Name):
        return node.func.id
    if isinstance(node.func, ast.Attribute):
        return node.func.attr
    return None


def _record_module_surface(
    path: Path,
) -> tuple[str, frozenset[str], frozenset[str], bool]:
    source = path.read_text(encoding="utf-8")
    nodes = tuple(ast.walk(ast.parse(source, filename=str(path))))
    roots = frozenset(root for node in nodes for root in _import_roots(node))
    calls = frozenset(
        called for node in nodes if (called := _called_name(node)) is not None
    )
    has_async = any(
        isinstance(node, (ast.AsyncFunctionDef, ast.Await)) for node in nodes
    )
    return source, roots, calls, has_async


def test_record_contract_modules_have_no_io_sql_or_execution_surface() -> None:
    package = Path(__file__).resolve().parents[1] / "public_evidence"
    paths = sorted(package.glob("evidence_record_*.py"))
    assert [path.name for path in paths] == [
        "evidence_record_contract.py",
        "evidence_record_policies.py",
        "evidence_record_primitives.py",
        "evidence_record_token_policy.py",
    ]
    forbidden_roots = {
        "api",
        "asyncpg",
        "db",
        "os",
        "pathlib",
        "process",
        "psycopg",
        "requests",
        "socket",
        "sqlalchemy",
        "subprocess",
        "urllib",
    }
    forbidden_calls = {
        "connect",
        "delete",
        "execute",
        "executemany",
        "mkdir",
        "open",
        "publish",
        "remove",
        "rename",
        "replace_file",
        "rmdir",
        "run",
        "swap",
        "unlink",
        "write",
        "write_bytes",
        "write_text",
    }
    allowed_roots = set(sys.stdlib_module_names) | {"__future__", "public_evidence"}
    for path in paths:
        module_source, roots, calls, has_async = _record_module_surface(path)
        assert has_async is False
        assert not roots & forbidden_roots
        assert not roots - allowed_roots
        assert not calls & forbidden_calls
        assert "alter table" not in module_source.casefold()
        assert "select * from" not in module_source.casefold()


def test_importing_record_contract_does_not_load_runtime_packages() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    script = """
import json
import sys
before = set(sys.modules)
from public_evidence import evidence_record_contract
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
        cwd=repository_root,
        check=True,
        capture_output=True,
        text=True,
    )
    assert completed.stdout.strip() == "[]"


def test_runtime_and_container_do_not_wire_record_contract() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    runtime_paths = [repository_root / "main.py"]
    for package_name in ("api", "db", "process", "service"):
        runtime_paths.extend((repository_root / package_name).rglob("*.py"))
    for path in runtime_paths:
        assert "public_evidence.evidence_record" not in path.read_text(encoding="utf-8")
    docker_text = (repository_root / "Dockerfile").read_text(encoding="utf-8")
    assert "evidence_record_contract" not in docker_text


def test_record_contract_contains_no_adapter_or_publisher_entrypoint() -> None:
    package = Path(__file__).resolve().parents[1] / "public_evidence"
    public_functions = set()
    for path in package.glob("evidence_record_*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        public_functions.update(
            node.name
            for node in tree.body
            if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
            and not node.name.startswith("_")
        )
    assert (
        not {
            "execute_adapter",
            "import_source",
            "materialize",
            "publish",
            "serve",
            "swap",
        }
        & public_functions
    )
