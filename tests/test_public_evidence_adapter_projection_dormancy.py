# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static dormancy proof for inventory and adapter-projection modules."""

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


def _module_surface(
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


def _contract_paths() -> list[Path]:
    package = Path(__file__).resolve().parents[1] / "public_evidence"
    return sorted(
        (
            *package.glob("adapter_projection_*.py"),
            *package.glob("source_record_inclusion_*.py"),
            *package.glob("source_record_replay_*.py"),
        )
    )


def test_projection_modules_have_no_io_sql_or_execution_surface() -> None:
    paths = _contract_paths()
    assert [path.name for path in paths] == [
        "adapter_projection_contract.py",
        "adapter_projection_policies.py",
        "source_record_inclusion_contract.py",
        "source_record_inclusion_primitives.py",
        "source_record_replay_contract.py",
        "source_record_replay_primitives.py",
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
        "service",
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
        "serve",
        "swap",
        "unlink",
        "write",
        "write_bytes",
        "write_text",
    }
    allowed_roots = set(sys.stdlib_module_names) | {"__future__", "public_evidence"}
    for path in paths:
        module_source, roots, calls, has_async = _module_surface(path)
        assert has_async is False
        assert not roots & forbidden_roots
        assert not roots - allowed_roots
        assert not calls & forbidden_calls
        assert "alter table" not in module_source.casefold()
        assert "select * from" not in module_source.casefold()


def test_importing_projection_contract_does_not_load_runtime_packages() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    script = """
import json
import sys
before = set(sys.modules)
from public_evidence import adapter_projection_contract
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


def test_runtime_container_and_package_init_do_not_wire_projection() -> None:
    repository_root = Path(__file__).resolve().parents[1]
    explicit_replay_executor = (
        repository_root / "process" / "public_evidence_fhir_organization_replay.py"
    )
    runtime_paths = [repository_root / "main.py"]
    for package_name in ("api", "db", "process", "service"):
        runtime_paths.extend((repository_root / package_name).rglob("*.py"))
    forbidden_fragments = (
        "public_evidence.adapter_projection",
        "public_evidence.source_record_inclusion",
    )
    for path in runtime_paths:
        if path == explicit_replay_executor:
            continue
        module_source = path.read_text(encoding="utf-8")
        assert not any(fragment in module_source for fragment in forbidden_fragments)
    docker_text = (repository_root / "Dockerfile").read_text(encoding="utf-8")
    assert not any(fragment in docker_text for fragment in forbidden_fragments)
    package_init = (repository_root / "public_evidence" / "__init__.py").read_text(
        encoding="utf-8"
    )
    assert not any(
        fragment.rsplit(".", 1)[-1] in package_init for fragment in forbidden_fragments
    )


def test_projection_contract_contains_no_execution_entrypoint() -> None:
    public_functions = set()
    for path in _contract_paths():
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
