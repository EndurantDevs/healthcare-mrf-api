# Licensed under the HealthPorta Non-Commercial License (see LICENSE).
"""Static dormancy proof for staged bundle publication intent modules."""

from __future__ import annotations

import ast
from pathlib import Path
import sys


def _import_roots_for_node(node: ast.AST) -> tuple[str, ...]:
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


def _module_contract_surface(
    path: Path,
) -> tuple[str, frozenset[str], frozenset[str], bool]:
    module_source_text = path.read_text(encoding="utf-8")
    syntax_nodes = tuple(ast.walk(ast.parse(module_source_text, filename=str(path))))
    import_roots = frozenset(
        root
        for syntax_node in syntax_nodes
        for root in _import_roots_for_node(syntax_node)
    )
    called_names = frozenset(
        called_name
        for syntax_node in syntax_nodes
        if (called_name := _called_name(syntax_node)) is not None
    )
    has_async_syntax = any(
        isinstance(syntax_node, (ast.AsyncFunctionDef, ast.Await))
        for syntax_node in syntax_nodes
    )
    return module_source_text, import_roots, called_names, has_async_syntax


def test_contract_modules_have_no_io_sql_or_mutation_execution_surface() -> None:
    package = Path(__file__).resolve().parents[1] / "public_evidence"
    paths = sorted(package.glob("staged_bundle_publication_*.py"))
    forbidden_import_roots = {
        "asyncpg",
        "os",
        "pathlib",
        "psycopg",
        "requests",
        "socket",
        "sqlalchemy",
        "subprocess",
        "urllib",
    }
    forbidden_call_names = {
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
    allowed_import_roots = set(sys.stdlib_module_names) | {
        "__future__",
        "public_evidence",
    }
    for path in paths:
        module_source_text, import_roots, called_names, has_async_syntax = (
            _module_contract_surface(path)
        )
        assert has_async_syntax is False
        assert "alter table" not in module_source_text.casefold()
        assert not import_roots & forbidden_import_roots
        assert not called_names & forbidden_call_names
        assert not import_roots - allowed_import_roots
