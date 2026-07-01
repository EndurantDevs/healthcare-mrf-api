"""Function-local readability checks."""

from __future__ import annotations

import ast
from dataclasses import dataclass
from typing import Any

from .ast_helpers import (BUILTIN_NAMES, collection_expression_kind, is_bool_expression, is_boolean_or_none_literal,
                          is_ellipsis_expr, is_stub_body, iter_args, iter_local_nodes, target_names)
from .config import (DEFAULT_ALLOWED_SHORT_NAMES, DEFAULT_ALWAYS_BAD_SHORT_NAMES, DEFAULT_AMBIGUOUS_VARIABLE_NAMES,
                     DEFAULT_BOOLEAN_PREFIXES, DEFAULT_COLLECTION_SINGULAR_EXCEPTIONS, DEFAULT_DICT_NAME_MARKERS,
                     has_boolean_prefix, name_list, name_prefixes, threshold)
from .model import Issue


@dataclass(frozen=True)
class LocalNameRules:
    ambiguous_names: set[str]
    allowed_short_names: set[str]
    always_bad_short_names: set[str]
    min_vague_scope_lines: int
    max_short_scope_lines: int


class LocalScopeChecker:
    def __init__(self, config: dict[str, Any]) -> None:
        self.config = config
        self.issues: list[Issue] = []

    def collect_issues(
        self,
        relative: str,
        qualified_name: str,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        function_lines: int,
        *,
        allow_stub_body: bool = False,
    ) -> list[Issue]:
        """Return readability findings for local variables and statements."""
        self.issues = []
        assigned_names: set[str] = {arg.arg for arg in iter_args(node)}
        local_name_rules = self._build_local_name_rules()
        boolean_prefixes = name_prefixes(self.config, "boolean_prefixes", DEFAULT_BOOLEAN_PREFIXES)

        if not (allow_stub_body and is_stub_body(node.body)):
            for local_node in iter_local_nodes(node):
                self._check_local_node(relative, qualified_name, local_node, assigned_names, boolean_prefixes)

        self._check_local_count(relative, qualified_name, node, assigned_names)
        for name in sorted(assigned_names):
            self._check_local_name(
                relative,
                qualified_name,
                node,
                name,
                function_lines,
                local_name_rules,
            )
        return list(self.issues)

    def _build_local_name_rules(self) -> LocalNameRules:
        return LocalNameRules(
            ambiguous_names=name_list(
                self.config,
                "ambiguous_variable_names",
                DEFAULT_AMBIGUOUS_VARIABLE_NAMES,
            ),
            allowed_short_names=name_list(self.config, "allowed_short_names", DEFAULT_ALLOWED_SHORT_NAMES),
            always_bad_short_names=name_list(
                self.config,
                "always_bad_short_names",
                DEFAULT_ALWAYS_BAD_SHORT_NAMES,
            ),
            min_vague_scope_lines=threshold(self.config, "min_ambiguous_variable_scope_lines", 30),
            max_short_scope_lines=threshold(self.config, "max_single_letter_scope_lines", 15),
        )

    def _check_local_node(
        self,
        relative: str,
        qualified_name: str,
        local_node: ast.AST,
        assigned_names: set[str],
        boolean_prefixes: tuple[str, ...],
    ) -> None:
        if isinstance(local_node, ast.Name) and isinstance(local_node.ctx, ast.Store):
            assigned_names.add(local_node.id)
            return
        if isinstance(local_node, ast.Global):
            self._add_global_state_issue(relative, qualified_name, local_node, "global")
            return
        if isinstance(local_node, ast.Nonlocal):
            self._add_global_state_issue(relative, qualified_name, local_node, "nonlocal")
            return
        if isinstance(local_node, ast.Pass):
            self._add_pass_placeholder_issue(relative, qualified_name, local_node, "pass")
            return
        if is_ellipsis_expr(local_node):
            self._add_pass_placeholder_issue(relative, qualified_name, local_node, "ellipsis")
            return
        if isinstance(local_node, ast.Compare):
            self._check_literal_boolean_compare(relative, qualified_name, local_node)
            return
        if isinstance(local_node, (ast.Assign, ast.AnnAssign, ast.NamedExpr)):
            self._check_assignment_names(relative, qualified_name, local_node, boolean_prefixes)

    def _check_local_count(
        self,
        relative: str,
        qualified_name: str,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        assigned_names: set[str],
    ) -> None:
        max_locals = threshold(self.config, "max_locals", 25)
        if len(assigned_names) > max_locals:
            self.issues.append(
                Issue(
                    "too_many_locals",
                    f"too_many_locals:{relative}:{qualified_name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "locals": len(assigned_names),
                        "limit": max_locals,
                    },
                )
            )

    def _check_local_name(
        self,
        relative: str,
        qualified_name: str,
        node: ast.FunctionDef | ast.AsyncFunctionDef,
        name: str,
        function_lines: int,
        local_name_rules: LocalNameRules,
    ) -> None:
        if name in BUILTIN_NAMES and not (name.startswith("__") and name.endswith("__")):
            self.issues.append(
                Issue(
                    "builtin_shadowing",
                    f"builtin_shadowing:{relative}:{qualified_name}:{name}",
                    relative,
                    {"function": qualified_name, "line": node.lineno, "name": name},
                )
            )
        if name in local_name_rules.always_bad_short_names or (
            len(name) == 1
            and name not in local_name_rules.allowed_short_names
            and function_lines > local_name_rules.max_short_scope_lines
        ):
            self.issues.append(
                Issue(
                    "single_letter_names",
                    f"single_letter_name:{relative}:{qualified_name}:{name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "name": name,
                        "lines": function_lines,
                        "limit": local_name_rules.max_short_scope_lines,
                    },
                )
            )
        if function_lines >= local_name_rules.min_vague_scope_lines and name in local_name_rules.ambiguous_names:
            self.issues.append(
                Issue(
                    "ambiguous_variable_names",
                    f"ambiguous_variable_name:{relative}:{qualified_name}:{name}",
                    relative,
                    {
                        "function": qualified_name,
                        "line": node.lineno,
                        "name": name,
                        "lines": function_lines,
                        "limit": local_name_rules.min_vague_scope_lines,
                    },
                )
            )

    def _check_assignment_names(
        self,
        relative: str,
        qualified_name: str,
        node: ast.Assign | ast.AnnAssign | ast.NamedExpr,
        boolean_prefixes: tuple[str, ...],
    ) -> None:
        assigned_names, assigned_value = self._assignment_parts(node)
        if assigned_value is None:
            return
        collection_kind = collection_expression_kind(assigned_value)
        for name in assigned_names:
            if is_bool_expression(assigned_value) and not has_boolean_prefix(name, boolean_prefixes):
                self.issues.append(
                    Issue(
                        "boolean_name_mismatch",
                        f"boolean_name_mismatch:{relative}:{qualified_name}:{name}:assignment",
                        relative,
                        {
                            "function": qualified_name,
                            "line": getattr(node, "lineno", 1),
                            "name": name,
                            "reason": "boolean_assignment_without_predicate_name",
                        },
                    )
                )
            if collection_kind:
                self._check_collection_name(relative, qualified_name, name, collection_kind, getattr(node, "lineno", 1))

    def _assignment_parts(
        self,
        node: ast.Assign | ast.AnnAssign | ast.NamedExpr,
    ) -> tuple[list[str], ast.AST | None]:
        if isinstance(node, ast.Assign):
            names = [name for assignment_target in node.targets for name in target_names(assignment_target)]
            return names, node.value
        if isinstance(node, ast.AnnAssign):
            return target_names(node.target), node.value
        return target_names(node.target), node.value

    def _check_collection_name(
        self,
        relative: str,
        qualified_name: str,
        name: str,
        collection_kind: str,
        line: int,
    ) -> None:
        singular_exceptions = name_list(
            self.config,
            "collection_singular_exceptions",
            DEFAULT_COLLECTION_SINGULAR_EXCEPTIONS,
        )
        dict_markers = name_prefixes(self.config, "dict_name_markers", DEFAULT_DICT_NAME_MARKERS)
        normalized = name.strip("_")
        if normalized in singular_exceptions:
            return
        if collection_kind == "dict":
            if any(marker in normalized for marker in dict_markers):
                return
        elif normalized.endswith(("s", "_list", "_set", "_ids", "_rows")):
            return
        self.issues.append(
            Issue(
                "collection_name_mismatch",
                f"collection_name_mismatch:{relative}:{qualified_name}:{name}",
                relative,
                {
                    "function": qualified_name,
                    "line": line,
                    "name": name,
                    "collection": collection_kind,
                },
            )
        )

    def _check_literal_boolean_compare(
        self,
        relative: str,
        qualified_name: str,
        node: ast.Compare,
    ) -> None:
        values = [node.left, *node.comparators]
        if not any(is_boolean_or_none_literal(value) for value in values):
            return
        if not any(isinstance(operator, (ast.Eq, ast.NotEq)) for operator in node.ops):
            return
        self.issues.append(
            Issue(
                "boolean_name_mismatch",
                f"boolean_literal_compare:{relative}:{qualified_name}:{node.lineno}:{node.col_offset}",
                relative,
                {
                    "function": qualified_name,
                    "line": node.lineno,
                    "reason": "literal_boolean_or_none_comparison",
                },
            )
        )

    def _add_global_state_issue(
        self,
        relative: str,
        qualified_name: str,
        node: ast.Global | ast.Nonlocal,
        kind: str,
    ) -> None:
        names = ",".join(node.names)
        self.issues.append(
            Issue(
                "global_state_usage",
                f"global_state_usage:{relative}:{qualified_name}:{kind}:{node.lineno}:{names}",
                relative,
                {"function": qualified_name, "line": node.lineno, "kind": kind, "names": node.names},
            )
        )

    def _add_pass_placeholder_issue(
        self,
        relative: str,
        qualified_name: str,
        node: ast.AST,
        kind: str,
    ) -> None:
        self.issues.append(
            Issue(
                "pass_placeholders",
                f"pass_placeholder:{relative}:{qualified_name}:{kind}:{getattr(node, 'lineno', 1)}",
                relative,
                {"function": qualified_name, "line": getattr(node, "lineno", 1), "kind": kind},
            )
        )
