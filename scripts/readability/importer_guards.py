"""AST-based importer architecture guards for custody, re-hash, and paged-copy rules."""

from __future__ import annotations

import ast
import re
from typing import Any

from .config import is_matching_path_pattern, readability_options
from .model import Issue

_CUSTODY_RULES = (
    ("physical_filenode_check", re.compile(r"\b(?:relfilenode|pg_relation_filenode)\b", re.IGNORECASE), True),
    (
        "row_security_custody_check",
        re.compile(r"\b(?:row_security_active|relrowsecurity|relforcerowsecurity)\b", re.IGNORECASE),
        True,
    ),
    (
        "owner_or_acl_custody_check",
        re.compile(r"\b(?:relowner|relacl|nspowner|nspacl|pg_has_role)\b", re.IGNORECASE),
        True,
    ),
    ("catalog_hook_custody_check", re.compile(r"\b(?:pg_trigger|pg_rewrite|pg_inherits)\b", re.IGNORECASE), False),
    (
        "forbidden_seal_or_multiset_verifier",
        re.compile(
            r"\b(?:verify_independent_source_contributions|verify_registry_composition_rows|"
            r"validate_observed_projection|_table_digest|post_seal_audit)\b"
        ),
        True,
    ),
)
_REHASH_PATTERN = re.compile(r"\b(?:sha256|hashlib|hexdigest)\b")
_INSERT_PATTERN = re.compile(r"\bINSERT\s+INTO\b", re.IGNORECASE)
_SELECT_PATTERN = re.compile(r"\bSELECT\b", re.IGNORECASE)
_ORDER_PATTERN = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)
_OVER_PATTERN = re.compile(r"\bOVER\s*\(", re.IGNORECASE)
_LIMIT_PARAM_PATTERN = re.compile(r"\bLIMIT\s+\$", re.IGNORECASE)
_LIMIT_KEYWORD_PATTERN = re.compile(r"\bLIMIT\b", re.IGNORECASE)
_PAGED_STATE_PATTERN = re.compile(r"\b(?:cursor_json|running_bytes)\b")
_FAST_IDENT_PATTERN = re.compile(
    r"filenode|row_security|relrowsecurity|relforcerowsecurity|"
    r"relowner|relacl|nspowner|nspacl|pg_has_role|"
    r"verify_independent_source_contributions|verify_registry_composition_rows|"
    r"validate_observed_projection|_table_digest|post_seal_audit|"
    r"sha256|hashlib|hexdigest|cursor_json|running_bytes",
    re.IGNORECASE,
)
_FAST_STRING_PATTERN = re.compile(
    r"filenode|row_security|relrowsecurity|relforcerowsecurity|"
    r"relowner|relacl|nspowner|nspacl|pg_has_role|"
    r"pg_trigger|pg_rewrite|pg_inherits|"
    r"verify_independent_source_contributions|verify_registry_composition_rows|"
    r"validate_observed_projection|_table_digest|post_seal_audit|"
    r"sha256|hashlib|hexdigest|cursor_json|running_bytes|INSERT|LIMIT",
    re.IGNORECASE,
)
_INSERT_WORD_PATTERN = re.compile(r"\binsert\b", re.IGNORECASE)


def _is_guarded_path(relative: str, config: dict[str, Any]) -> bool:
    options = readability_options(config)
    roots = options.get("importer_guard_roots", [])
    if not roots:
        return False
    in_root = any(relative == root or relative.startswith(f"{root.rstrip('/')}/") for root in roots)
    if not in_root:
        return False
    excludes = options.get("importer_guard_exclude_globs", [])
    return not is_matching_path_pattern(relative, excludes)


def _joined_string_text(expression: ast.AST, memo_by_node: dict[int, str | None]) -> str | None:
    node_id = id(expression)
    if node_id in memo_by_node:
        return memo_by_node[node_id]
    text = _resolve_string_text(expression, memo_by_node)
    memo_by_node[node_id] = text
    return text


def _resolve_string_text(expression: ast.AST, memo_by_node: dict[int, str | None]) -> str | None:
    if isinstance(expression, ast.Constant) and isinstance(expression.value, str):
        return expression.value
    if isinstance(expression, ast.FormattedValue):
        return ""
    if isinstance(expression, ast.JoinedStr):
        parts = [_joined_string_text(part, memo_by_node) or "" for part in expression.values]
        return "".join(parts)
    if isinstance(expression, ast.BinOp) and isinstance(expression.op, (ast.Add, ast.Mod)):
        left_text = _joined_string_text(expression.left, memo_by_node)
        right_text = _joined_string_text(expression.right, memo_by_node)
        if left_text is None and right_text is None:
            return None
        return (left_text or "") + (right_text or "")
    if (
        isinstance(expression, ast.Call)
        and isinstance(expression.func, ast.Attribute)
        and expression.func.attr == "format"
    ):
        return _joined_string_text(expression.func.value, memo_by_node)
    return None


def _is_paged_copy_sql(sql_text: str) -> bool:
    if not _INSERT_PATTERN.search(sql_text):
        return False
    if "running_bytes" in sql_text.lower():
        return True
    has_limit = _LIMIT_PARAM_PATTERN.search(sql_text) is not None
    has_window_limit = _OVER_PATTERN.search(sql_text) is not None and has_limit
    has_ordered_limit = _SELECT_PATTERN.search(sql_text) is not None and _ORDER_PATTERN.search(sql_text) is not None
    return has_window_limit or (has_ordered_limit and has_limit)


class _ScopeGuardCollector(ast.NodeVisitor):
    """Single-pass AST collector grouped by enclosing module/class/function scope."""

    def __init__(self) -> None:
        self.class_stack: list[str] = []
        self.scope_stack: list[dict[str, Any]] = [self._new_scope("<module>", 1, is_function=False)]
        self.completed_scopes: list[dict[str, Any]] = []
        self.loop_flags_stack: list[list[bool]] = []
        self.memo_by_node: dict[int, str | None] = {}

    def _new_scope(self, scope_name: str, line: int, *, is_function: bool) -> dict[str, Any]:
        return {
            "scope_name": scope_name,
            "line": line,
            "is_function": is_function,
            "custody_rules": set(),
            "has_copy_hash_sink": False,
            "has_rehash": False,
            "has_insert": False,
            "sql_candidates": [],
            "has_paged_loop": False,
        }

    def _record_identifier(self, token: str) -> None:
        if not _FAST_IDENT_PATTERN.search(token):
            return
        current = self.scope_stack[-1]
        for rule_name, pattern, check_identifiers in _CUSTODY_RULES:
            if check_identifiers and pattern.search(token):
                current["custody_rules"].add(rule_name)
        if _REHASH_PATTERN.search(token):
            current["has_rehash"] = True
        if self.loop_flags_stack and _PAGED_STATE_PATTERN.search(token):
            self.loop_flags_stack[-1][1] = True

    def _record_string(self, text: str) -> None:
        if not _FAST_STRING_PATTERN.search(text):
            return
        current = self.scope_stack[-1]
        for rule_name, pattern, _ in _CUSTODY_RULES:
            if pattern.search(text):
                current["custody_rules"].add(rule_name)
        if _REHASH_PATTERN.search(text):
            current["has_rehash"] = True
        if _INSERT_WORD_PATTERN.search(text):
            current["has_insert"] = True
        if self.loop_flags_stack:
            if _LIMIT_KEYWORD_PATTERN.search(text):
                self.loop_flags_stack[-1][0] = True
            if _PAGED_STATE_PATTERN.search(text):
                self.loop_flags_stack[-1][1] = True
            if _INSERT_PATTERN.search(text):
                self.loop_flags_stack[-1][2] = True

    def visit_ClassDef(self, node: ast.ClassDef) -> None:
        """Track enclosing class names while visiting class body nodes."""
        self.class_stack.append(node.name)
        self.generic_visit(node)
        self.class_stack.pop()

    def _visit_callable(self, node: ast.FunctionDef | ast.AsyncFunctionDef) -> None:
        prefix = ".".join([*self.class_stack, node.name]) if self.class_stack else node.name
        scope = self._new_scope(prefix, node.lineno, is_function=True)
        for rule_name, pattern, check_identifiers in _CUSTODY_RULES:
            if check_identifiers and pattern.search(node.name):
                scope["custody_rules"].add(rule_name)
        self.scope_stack.append(scope)
        enclosing_loop_flags = self.loop_flags_stack
        self.loop_flags_stack = []
        try:
            self.generic_visit(node)
        finally:
            self.loop_flags_stack = enclosing_loop_flags
            self.scope_stack.pop()
        self.completed_scopes.append(scope)

    def visit_FunctionDef(self, node: ast.FunctionDef) -> None:
        """Analyze synchronous function scopes for importer guard patterns."""
        self._visit_callable(node)

    def visit_AsyncFunctionDef(self, node: ast.AsyncFunctionDef) -> None:
        """Analyze async function scopes for importer guard patterns."""
        self._visit_callable(node)

    def visit_Name(self, node: ast.Name) -> None:
        """Record identifier usage against custody, re-hash, and loop-state rules."""
        self._record_identifier(node.id)

    def visit_Attribute(self, node: ast.Attribute) -> None:
        """Record attribute use against the scope's importer guard patterns."""
        self._record_identifier(node.attr)
        self.generic_visit(node)

    def visit_Call(self, node: ast.Call) -> None:
        """Collect string formatting and COPY output wired directly into a digest."""
        function_name = node.func.id if isinstance(node.func, ast.Name) else getattr(node.func, "attr", None)
        if function_name == "copy_from_query" and any(
            keyword.arg == "output" and isinstance(keyword.value, ast.Attribute) and keyword.value.attr == "update"
            for keyword in node.keywords
        ):
            self.scope_stack[-1]["has_copy_hash_sink"] = True
        if isinstance(node.func, ast.Attribute) and node.func.attr == "format":
            self.scope_stack[-1]["sql_candidates"].append(node)
        self.generic_visit(node)

    def visit_BinOp(self, node: ast.BinOp) -> None:
        """Collect concatenated or %-formatted SQL expressions in the current scope."""
        if isinstance(node.op, (ast.Add, ast.Mod)):
            self.scope_stack[-1]["sql_candidates"].append(node)
        self.generic_visit(node)

    def visit_JoinedStr(self, node: ast.JoinedStr) -> None:
        """Collect f-string expressions as SQL candidates in the current scope."""
        self.scope_stack[-1]["sql_candidates"].append(node)
        self.generic_visit(node)

    def visit_Constant(self, node: ast.Constant) -> None:
        """Inspect string literals for SQL verbs, custody terms, and paging hints."""
        if isinstance(node.value, str):
            self._record_string(node.value)
            if _INSERT_WORD_PATTERN.search(node.value):
                self.scope_stack[-1]["sql_candidates"].append(node)

    def _visit_loop(self, node: ast.While | ast.For | ast.AsyncFor) -> None:
        """Flag paged SQL writes inside a cursor-driven loop."""
        self.loop_flags_stack.append([False, False, False])
        self.generic_visit(node)
        loop_flags = self.loop_flags_stack.pop()
        if self.loop_flags_stack:
            enclosing_flags = self.loop_flags_stack[-1]
            enclosing_flags[:] = [outer or inner for outer, inner in zip(enclosing_flags, loop_flags, strict=True)]
        if all(loop_flags):
            self.scope_stack[-1]["has_paged_loop"] = True

    visit_While = _visit_loop
    visit_For = _visit_loop
    visit_AsyncFor = _visit_loop


def collect_file_importer_guard_issues(
    relative: str,
    tree: ast.Module,
    config: dict[str, Any],
) -> list[Issue]:
    """Collect importer custody, re-hash, and paged-copy issues for one parsed file."""
    if not _is_guarded_path(relative, config):
        return []
    collector = _ScopeGuardCollector()
    collector.visit(tree)
    scopes = [collector.scope_stack[0], *collector.completed_scopes]
    issues: list[Issue] = []
    for scope in scopes:
        scope_name, line = scope["scope_name"], scope["line"]
        custody_rules = set(scope["custody_rules"])
        if scope["is_function"] and scope["has_copy_hash_sink"] and scope["has_rehash"]:
            custody_rules.add("copy_from_query_rehash")
        for rule_name in sorted(custody_rules):
            issues.append(
                Issue(
                    "importer_custody_checks",
                    f"importer_custody:{relative}:{scope_name}:{rule_name}",
                    relative,
                    {"line": line, "function": scope_name, "reason": rule_name},
                )
            )
        has_paged_sql = scope["has_insert"] and any(
            (text := _joined_string_text(expr, collector.memo_by_node)) is not None and _is_paged_copy_sql(text)
            for expr in scope["sql_candidates"]
        )
        for rule_name, present in (
            ("paged_copy_forward_sql", has_paged_sql),
            ("python_paged_copy_loop", scope["is_function"] and scope["has_paged_loop"]),
        ):
            if present:
                issues.append(
                    Issue(
                        "importer_paged_copy_loops",
                        f"importer_paged_copy:{relative}:{scope_name}:{rule_name}",
                        relative,
                        {"line": line, "function": scope_name, "reason": rule_name},
                    )
                )
    return issues
