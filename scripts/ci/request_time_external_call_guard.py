#!/usr/bin/env python3
"""Block request-time external network clients in API endpoint modules."""

from __future__ import annotations

import ast
import sys
from pathlib import Path


ENDPOINT_DIR = Path("api/endpoint")
BLOCKED_IMPORTS = {
    "aiohttp",
    "httpx",
    "requests",
    "urllib.request",
}
BLOCKED_IMPORT_ROOTS = {
    "aiohttp",
    "httpx",
    "requests",
}
BLOCKED_CALL_PREFIXES = {
    "aiohttp",
    "httpx",
    "requests",
    "urllib.request",
}


def dotted_name(node: ast.AST) -> str | None:
    """Return the dotted reference represented by an AST node."""
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        parent = dotted_name(node.value)
        return f"{parent}.{node.attr}" if parent else node.attr
    return None


def check_file(path: Path) -> list[str]:
    """Find request-time external call violations in one module."""
    errors: list[str] = []
    tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    imported_blocked_refs: set[str] = set()

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = alias.name
                if name in BLOCKED_IMPORTS or name.split(".")[0] in BLOCKED_IMPORT_ROOTS:
                    errors.append(f"{path}: blocked network import {name}")
                    imported_blocked_refs.add(alias.asname or name)
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ""
            if module in BLOCKED_IMPORTS or module.split(".")[0] in BLOCKED_IMPORT_ROOTS:
                errors.append(f"{path}: blocked network import from {module}")
                imported_blocked_refs.update(alias.asname or alias.name for alias in node.names)

    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        root = dotted_name(node.func)
        if not root:
            continue
        if any(root == prefix or root.startswith(f"{prefix}.") for prefix in BLOCKED_CALL_PREFIXES):
            errors.append(f"{path}:{node.lineno}: blocked request-time network call {root}")
            continue
        if any(root == ref or root.startswith(f"{ref}.") for ref in imported_blocked_refs):
            errors.append(f"{path}:{node.lineno}: blocked request-time network call {root}")

    return errors


def main() -> int:
    """Check every API endpoint module for external calls."""
    errors: list[str] = []
    for path in sorted(ENDPOINT_DIR.glob("*.py")):
        errors.extend(check_file(path))
    if errors:
        print("Request-time external call guard failed:")
        for error in errors:
            print(f"- {error}")
        return 1
    print("Request-time external call guard passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
