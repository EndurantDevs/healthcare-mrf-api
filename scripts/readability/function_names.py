"""Cross-function naming checks that require lexical-scope context."""

from __future__ import annotations

import ast
import re
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path

from .config import split_name_tokens
from .model import Issue

_IRREGULAR_SINGULARS = {
    "analyses": "analysis",
    "buses": "bus",
    "children": "child",
    "ids": "id",
    "indices": "index",
    "matrices": "matrix",
    "people": "person",
    "statuses": "status",
}
_SINGULAR_EXCEPTIONS = {
    "address",
    "analysis",
    "business",
    "class",
    "kubernetes",
    "news",
    "process",
    "series",
    "species",
    "status",
    "success",
}


def confusable_function_name_issues(
    repo_root: Path,
    paths: list[Path],
    exceptions: set[str] | None = None,
) -> list[Issue]:
    """Report functions in one logical module that differ only by plurality."""

    collector = _FunctionNameCollector()
    for path in paths:
        relative = path.relative_to(repo_root).as_posix()
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"))
        except SyntaxError:
            continue
        collector.collect(relative, tree)
    issues: list[Issue] = []
    for (namespace, scope, normalized_tokens), definitions in sorted(collector.definitions_by_key.items()):
        raw_token_sets = {definition.tokens for definition in definitions}
        if len(raw_token_sets) < 2:
            continue
        function_names = sorted({definition.name for definition in definitions})
        scope_name = ".".join(scope) or "<module>"
        normalized_name = "_".join(normalized_tokens)
        definition_paths = sorted({definition.path for definition in definitions})
        name_key = "|".join(function_names)
        identifier = f"confusable_function_name:{namespace}:{scope_name}:{normalized_name}:{name_key}"
        if identifier in (exceptions or set()):
            continue
        representative = min(definitions, key=lambda definition: (definition.path, definition.line, definition.name))
        issues.append(
            Issue(
                "confusable_function_names",
                identifier,
                representative.path,
                {
                    "line": representative.line,
                    "name": " / ".join(function_names),
                    "functions": function_names,
                    "paths": definition_paths,
                    "scope": scope_name,
                    "reason": "names_differ_only_by_singular_plural_tokens",
                },
            )
        )
    return issues


@dataclass(frozen=True)
class _FunctionDefinition:
    path: str
    name: str
    line: int
    tokens: tuple[str, ...]


class _FunctionNameCollector(ast.NodeVisitor):
    def __init__(self) -> None:
        self.namespace = ""
        self.relative = ""
        self.scope: list[str] = []
        self.definitions_by_key: dict[
            tuple[str, tuple[str, ...], tuple[str, ...]],
            list[_FunctionDefinition],
        ] = defaultdict(list)

    def collect(self, relative: str, tree: ast.AST) -> None:
        """Collect definitions from one physical module."""

        self.relative = relative
        self.namespace = _logical_module_namespace(relative)
        self.scope = []
        self.visit(tree)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Collect methods under their owning class scope."""

        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Collect one synchronous function and visit its nested scope."""

        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Collect one asynchronous function and visit its nested scope."""

        self._visit_function(node)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        tokens = tuple(split_name_tokens(node.name))
        normalized_tokens = tuple(_singularize_token(token) for token in tokens)
        key = (self.namespace, tuple(self.scope), normalized_tokens)
        self.definitions_by_key[key].append(
            _FunctionDefinition(self.relative, node.name, node.lineno, tokens)
        )
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()


def _singularize_token(token: str) -> str:
    if token in _IRREGULAR_SINGULARS:
        return _IRREGULAR_SINGULARS[token]
    if token in _SINGULAR_EXCEPTIONS:
        return token
    if len(token) > 4 and token.endswith("ies"):
        return f"{token[:-3]}y"
    if len(token) > 4 and token.endswith(("ches", "shes", "sses", "xes", "zes")):
        return token[:-2]
    if len(token) > 3 and token.endswith("s") and not token.endswith(("is", "ss", "us")):
        return token[:-1]
    return token


def _logical_module_namespace(relative: str) -> str:
    module_path = relative.removesuffix(".py")
    return re.sub(r"_part_\d+$", "", module_path)
