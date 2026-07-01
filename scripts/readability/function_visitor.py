"""AST visitor for function and class readability checks."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any

from .ast_helpers import (is_bool_expression, is_stub_body, is_test_function, iter_without_nested_scopes,
                          max_nesting_depth, parameter_count)
from .config import (DEFAULT_AMBIGUOUS_FUNCTION_NAMES, DEFAULT_BOOLEAN_PREFIXES, has_boolean_prefix,
                     is_boolean_method_name, name_list, name_prefixes, split_name_tokens, threshold)
from .local_scope import LocalScopeChecker
from .model import Issue


class FunctionVisitor(ast.NodeVisitor):
    def __init__(self, repo_root: Path, path: Path, config: dict[str, Any]) -> None:
        self.repo_root = repo_root
        self.path = path
        self.config = config
        self.issues: list[Issue] = []
        self.scope: list[str] = []
        self.interface_scope_depth = 0
        self.local_scope_checker = LocalScopeChecker(config)

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Check class-level naming rules before walking nested definitions."""
        self._check_class_name(node)
        is_interface_class = _is_interface_class(node)
        if is_interface_class:
            self.interface_scope_depth += 1
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()
        if is_interface_class:
            self.interface_scope_depth -= 1

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Check synchronous functions with the shared function rules."""
        self._visit_function(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Check asynchronous functions with the shared function rules."""
        self._visit_function(node)

    def _visit_function(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        relative = self.path.relative_to(self.repo_root).as_posix()
        qualified_name = ".".join([*self.scope, node.name])
        function_lines = node.end_lineno - node.lineno + 1 if node.end_lineno else 0
        self._check_function_size(node, qualified_name, relative, function_lines)
        self._check_function_name(node, qualified_name, function_lines)
        self._check_function_contract(node, qualified_name, function_lines)
        self.issues.extend(
            self.local_scope_checker.collect_issues(
                relative,
                qualified_name,
                node,
                function_lines,
                allow_stub_body=self._should_allow_stub_body(node),
            )
        )
        self.scope.append(node.name)
        self.generic_visit(node)
        self.scope.pop()

    def _check_function_size(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        relative: str,
        function_lines: int,
    ) -> None:
        max_function_lines = threshold(self.config, "max_function_lines", 60)
        if function_lines > max_function_lines:
            self.issues.append(
                Issue(
                    "long_functions",
                    f"long_function:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "lines": function_lines,
                        "limit": max_function_lines,
                    },
                )
            )
        max_depth = max((max_nesting_depth(child) for child in node.body), default=0)
        max_allowed_depth = threshold(self.config, "max_nesting_depth", 4)
        if max_depth > max_allowed_depth:
            self.issues.append(
                Issue(
                    "deep_nesting",
                    f"deep_nesting:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "depth": max_depth,
                        "limit": max_allowed_depth,
                    },
                )
            )

    def _should_allow_stub_body(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        if self.interface_scope_depth > 0:
            return True
        if not is_stub_body(node.body):
            return False
        return any(_decorator_name(decorator) == "abstractmethod" for decorator in node.decorator_list)

    def _check_class_name(self, node: ast.ClassDef) -> None:
        relative = self.path.relative_to(self.repo_root).as_posix()
        class_name = ".".join([*self.scope, node.name])
        tokens = split_name_tokens(node.name)
        max_tokens = threshold(self.config, "max_class_name_tokens", 6)
        generic_names = name_list(
            self.config,
            "ambiguous_class_names",
            {"Base", "Data", "Handler", "Helper", "Manager", "Processor", "Service", "Wrapper"},
        )
        if node.name in generic_names:
            self.issues.append(
                Issue(
                    "class_name_shape",
                    f"class_name_shape:{relative}:{class_name}:generic",
                    relative,
                    {"class": class_name, "line": node.lineno, "name": node.name, "reason": "generic"},
                )
            )
        if len(tokens) > max_tokens:
            self.issues.append(
                Issue(
                    "class_name_shape",
                    f"class_name_shape:{relative}:{class_name}:too_many_tokens",
                    relative,
                    {
                        "class": class_name,
                        "line": node.lineno,
                        "name": node.name,
                        "tokens": len(tokens),
                        "limit": max_tokens,
                        "reason": "too_many_tokens",
                    },
                )
            )

    def _check_function_name(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        function_lines: int,
    ) -> None:
        """Report vague names, overlong names, and boolean predicate mismatches."""
        relative = self.path.relative_to(self.repo_root).as_posix()
        self._check_ambiguous_function_name(node, qualified_name, function_lines, relative)
        self._check_function_name_length(node, qualified_name, relative)
        self._check_boolean_function_name(node, qualified_name, relative)

    def _check_ambiguous_function_name(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        function_lines: int,
        relative: str,
    ) -> None:
        name = node.name.strip("_")
        ambiguous_names = name_list(
            self.config,
            "ambiguous_function_names",
            DEFAULT_AMBIGUOUS_FUNCTION_NAMES,
        )
        min_generic_lines = threshold(self.config, "min_generic_function_lines", 30)
        if name in ambiguous_names and function_lines >= min_generic_lines:
            self.issues.append(
                Issue(
                    "ambiguous_function_names",
                    f"ambiguous_function_name:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "name": node.name,
                        "lines": function_lines,
                        "limit": min_generic_lines,
                    },
                )
            )

    def _check_function_name_length(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        relative: str,
    ) -> None:
        tokens = split_name_tokens(node.name)
        threshold_name = "max_test_function_name_tokens" if is_test_function(relative, node.name) else "max_function_name_tokens"
        default_limit = 12 if threshold_name == "max_test_function_name_tokens" else 6
        max_tokens = threshold(self.config, threshold_name, default_limit)
        if len(tokens) > max_tokens:
            self.issues.append(
                Issue(
                    "function_name_shape",
                    f"function_name_shape:{relative}:{qualified_name}:too_many_tokens",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "name": node.name,
                        "tokens": len(tokens),
                        "limit": max_tokens,
                        "reason": "too_many_tokens",
                    },
                )
            )

    def _check_boolean_function_name(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        relative: str,
    ) -> None:
        if self._has_only_boolean_returns(node):
            if node.name.startswith("__") and node.name.endswith("__"):
                return
            if is_boolean_method_name(node.name):
                return
            boolean_prefixes = name_prefixes(self.config, "boolean_prefixes", DEFAULT_BOOLEAN_PREFIXES)
            if not has_boolean_prefix(node.name, boolean_prefixes):
                self.issues.append(
                    Issue(
                        "boolean_name_mismatch",
                        f"boolean_name_mismatch:{relative}:{qualified_name}:function",
                        relative,
                        {
                            "function": qualified_name,
                            "line": node.lineno,
                            "name": node.name,
                            "reason": "boolean_function_without_predicate_name",
                        },
                    )
                )

    def _check_function_contract(
        self,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        qualified_name: str,
        function_lines: int,
    ) -> None:
        relative = self.path.relative_to(self.repo_root).as_posix()
        min_docstring_lines = threshold(self.config, "min_docstring_function_lines", 60)
        is_public_function = not node.name.startswith("_") and not relative.startswith("tests/")
        if (is_public_function or function_lines >= min_docstring_lines) and ast.get_docstring(node) is None:
            self.issues.append(
                Issue(
                    "missing_contract_docstrings",
                    f"missing_contract_docstring:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "lines": function_lines,
                        "limit": min_docstring_lines,
                        "public": is_public_function,
                    },
                )
            )
        max_parameters = threshold(self.config, "max_parameters", 8)
        count = parameter_count(node)
        if count > max_parameters:
            self.issues.append(
                Issue(
                    "too_many_parameters",
                    f"too_many_parameters:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "parameters": count,
                        "limit": max_parameters,
                    },
                )
            )

    def _has_only_boolean_returns(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> bool:
        """Return true when a function annotation or all returns are boolean."""
        if isinstance(node.returns, ast.Name) and node.returns.id == "bool":
            return True
        returns = [
            child.value
            for child in iter_without_nested_scopes(node)
            if isinstance(child, ast.Return) and child.value is not None
        ]
        return bool(returns) and all(is_bool_expression(value) for value in returns)


def _is_interface_class(node: ast.ClassDef) -> bool:
    return any(_base_name(base) in {"ABC", "Protocol"} for base in node.bases)


def _base_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Subscript):
        return _base_name(node.value)
    return ""


def _decorator_name(node: ast.AST) -> str:
    if isinstance(node, ast.Name):
        return node.id
    if isinstance(node, ast.Attribute):
        return node.attr
    if isinstance(node, ast.Call):
        return _decorator_name(node.func)
    return ""
