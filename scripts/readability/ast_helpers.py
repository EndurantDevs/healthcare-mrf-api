"""AST helpers shared by readability rule visitors."""

from __future__ import annotations

import ast
import builtins
from typing import Iterable

from .config import DEFAULT_BOOLEAN_PREFIXES, has_boolean_prefix, is_boolean_method_name

NESTING_NODES = (
    ast.If,
    ast.For,
    ast.AsyncFor,
    ast.While,
    ast.With,
    ast.AsyncWith,
    ast.Try,
    ast.Match,
)
SCOPE_NODES = (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef, ast.Lambda)
BUILTIN_NAMES = set(dir(builtins))


def is_bool_expression(node: ast.AST) -> bool:
    """Return true for expressions that are very likely boolean values."""
    if isinstance(node, ast.Constant) and isinstance(node.value, bool):
        return True
    if isinstance(node, ast.Compare):
        return True
    if isinstance(node, ast.BoolOp):
        return all(is_bool_expression(value) for value in node.values)
    if isinstance(node, ast.UnaryOp) and isinstance(node.op, ast.Not):
        return True
    if isinstance(node, ast.Call):
        if isinstance(node.func, ast.Attribute):
            if node.func.attr in {"empty", "full"} and (node.args or node.keywords):
                return False
            return is_boolean_method_name(node.func.attr)
        if isinstance(node.func, ast.Name):
            return has_boolean_prefix(node.func.id, DEFAULT_BOOLEAN_PREFIXES)
    return False


def is_boolean_or_none_literal(node: ast.AST) -> bool:
    """Return true for literal bool and None nodes."""
    return isinstance(node, ast.Constant) and (isinstance(node.value, bool) or node.value is None)


def is_ellipsis_expr(node: ast.AST) -> bool:
    """Return true for a bare ellipsis expression."""
    return isinstance(node, ast.Expr) and isinstance(node.value, ast.Constant) and node.value.value is Ellipsis


def is_stub_body(statements: list[ast.stmt]) -> bool:
    """Return true when a function body is only an interface stub marker."""
    return len(statements) == 1 and (isinstance(statements[0], ast.Pass) or is_ellipsis_expr(statements[0]))


def collection_expression_kind(node: ast.AST) -> str | None:
    """Return the collection kind implied by a literal, comprehension, or constructor."""
    if isinstance(node, (ast.List, ast.ListComp, ast.GeneratorExp)):
        return "list"
    if isinstance(node, (ast.Set, ast.SetComp)):
        return "set"
    if isinstance(node, (ast.Dict, ast.DictComp)):
        return "dict"
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        if node.func.id in {"list", "tuple"}:
            return "list"
        if node.func.id == "set":
            return "set"
        if node.func.id == "dict":
            return "dict"
    return None


def iter_local_nodes(node: ast.FunctionDef | ast.AsyncFunctionDef) -> Iterable[ast.AST]:
    """Yield nodes inside a function without walking nested scopes."""
    for child in node.body:
        if isinstance(child, SCOPE_NODES):
            continue
        yield from iter_without_nested_scopes(child)


def iter_without_nested_scopes(node: ast.AST) -> Iterable[ast.AST]:
    """Yield a node tree while skipping nested functions, classes, and lambdas."""
    yield node
    for child in ast.iter_child_nodes(node):
        if isinstance(child, SCOPE_NODES):
            continue
        yield from iter_without_nested_scopes(child)


def target_names(node: ast.AST) -> list[str]:
    """Return assignment target names from simple and unpacked targets."""
    if isinstance(node, ast.Name):
        return [node.id]
    if isinstance(node, (ast.Tuple, ast.List)):
        names: list[str] = []
        for element in node.elts:
            names.extend(target_names(element))
        return names
    return []


def is_test_function(relative: str, name: str) -> bool:
    """Return true for pytest-style test functions."""
    return relative.startswith("tests/") and name.startswith("test_")


def max_nesting_depth(node: ast.AST, depth: int = 0) -> int:
    """Return maximum control-flow nesting beneath a node."""
    if isinstance(node, SCOPE_NODES):
        return depth
    current_depth = depth + 1 if isinstance(node, NESTING_NODES) else depth
    max_depth = current_depth
    for child in ast.iter_child_nodes(node):
        max_depth = max(max_depth, max_nesting_depth(child, current_depth))
    return max_depth


def parameter_count(node: ast.FunctionDef | ast.AsyncFunctionDef) -> int:
    """Return the total number of positional, keyword, vararg, and kwarg parameters."""
    args = node.args
    return (
        len(args.posonlyargs)
        + len(args.args)
        + len(args.kwonlyargs)
        + (1 if args.vararg else 0)
        + (1 if args.kwarg else 0)
    )


def iter_args(node: ast.FunctionDef | ast.AsyncFunctionDef) -> Iterable[ast.arg]:
    """Yield every argument node in a function signature."""
    args = node.args
    yield from args.posonlyargs
    yield from args.args
    yield from args.kwonlyargs
    if args.vararg:
        yield args.vararg
    if args.kwarg:
        yield args.kwarg
