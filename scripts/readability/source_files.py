"""Source file discovery and per-file readability checks."""

from __future__ import annotations

import ast
import hashlib
import re
import subprocess
from pathlib import Path
from typing import Any

from .config import (DEFAULT_COMMENT_NOISE_PATTERNS, compile_suppression_patterns, is_matching_path_pattern,
                     readability_options, threshold)
from .function_visitor import FunctionVisitor
from .function_names import confusable_function_name_issues
from .model import DEFAULT_ISSUE_CATEGORIES, Issue


def collect_issues(
    repo_root: Path,
    config: dict[str, Any],
    base_revision: str | None = None,
) -> dict[str, list[Issue]]:
    """Collect readability findings grouped by rule category."""
    patterns = compile_suppression_patterns(config)
    issues_by_category: dict[str, list[Issue]] = {category: [] for category in DEFAULT_ISSUE_CATEGORIES}
    source_files = _iter_source_files(repo_root, config)
    for path in source_files:
        split_issue = _split_module_name_issue(repo_root, path)
        if split_issue:
            issues_by_category[split_issue.category].append(split_issue)
        for issue in _analyze_file(repo_root, path, config):
            issues_by_category[issue.category].append(issue)
        issues_by_category["inline_suppressions"].extend(_find_inline_suppressions(repo_root, path, patterns))
        if path.suffix == ".py":
            issues_by_category["comment_noise"].extend(_find_comment_noise(repo_root, path, config))
    python_paths = [path for path in source_files if path.suffix == ".py"]
    name_exceptions = set(readability_options(config).get("confusable_function_name_exceptions", []))
    issues_by_category["confusable_function_names"].extend(
        confusable_function_name_issues(repo_root, python_paths, name_exceptions)
    )
    if base_revision:
        issues_by_category["huge_file_growth"].extend(
            _huge_file_growth_issues(repo_root, source_files, config, base_revision)
        )
    return {category: sorted(values, key=lambda issue: issue.identifier) for category, values in issues_by_category.items()}


def _iter_source_files(repo_root: Path, config: dict[str, Any]) -> list[Path]:
    roots = config.get("source_roots", [])
    exclude_globs = config.get("exclude_globs", [])
    include_suffixes = tuple(config.get("include_suffixes", [".py"]))
    files: list[Path] = []
    for source_root in roots:
        root_path = repo_root / source_root
        candidates = _candidate_files(root_path)
        for path in candidates:
            relative = path.relative_to(repo_root).as_posix()
            if not path.name.endswith(include_suffixes):
                continue
            if is_matching_path_pattern(relative, exclude_globs):
                continue
            files.append(path)
    return sorted(set(files))


def _candidate_files(root_path: Path) -> list[Path]:
    if root_path.is_file():
        return [root_path]
    if root_path.is_dir():
        return [path for path in root_path.rglob("*") if path.is_file()]
    return []


def _line_count(path: Path) -> int:
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for _ in handle)


def _is_file_length_path(relative: str, config: dict[str, Any]) -> bool:
    roots = readability_options(config).get("file_length_roots", config.get("source_roots", []))
    return any(relative == root or relative.startswith(f"{root.rstrip('/')}/") for root in roots)


def _split_module_name_issue(repo_root: Path, path: Path) -> Issue | None:
    relative = path.relative_to(repo_root).as_posix()
    if re.search(r"_part_\d+\.(?:py|ts|tsx)$", relative):
        return Issue("split_module_name", f"split_module_name:{relative}", relative, {"line": 1})
    if path.suffix == ".rs" and re.search(r"_[ab]\.rs$", path.name):
        sibling_suffix = "_b.rs" if path.name.endswith("_a.rs") else "_a.rs"
        sibling = path.with_name(path.name[:-5] + sibling_suffix)
        if sibling.is_file():
            return Issue("split_module_name", f"split_module_name:{relative}", relative, {"line": 1})
    if path.suffix == ".rs":
        for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
            match = re.search(r'include!\s*\(\s*"((?:.*/)?tests/[^"]*_[ab]\.rs)"', line)
            if match:
                return Issue(
                    "split_module_name",
                    f"split_module_name:{relative}:include:{match.group(1)}",
                    relative,
                    {"line": line_number, "name": "split test include"},
                )
    return None


def _suppression_fingerprint(path: str, pattern_name: str, line: str) -> str:
    normalized = " ".join(line.strip().split())
    digest = hashlib.sha1(f"{path}:{pattern_name}:{normalized}".encode("utf-8")).hexdigest()[:12]
    return f"inline_suppression:{path}:{pattern_name}:{digest}"


def _find_inline_suppressions(
    repo_root: Path,
    path: Path,
    patterns: list[tuple[str, re.Pattern[str]]],
) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    issues: list[Issue] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            for pattern_name, pattern in patterns:
                if pattern.search(line):
                    issues.append(
                        Issue(
                            "inline_suppressions",
                            _suppression_fingerprint(relative, pattern_name, line),
                            relative,
                            {
                                "line": line_number,
                                "pattern": pattern_name,
                                "text": line.strip(),
                            },
                        )
                    )
    return issues


def _find_comment_noise(repo_root: Path, path: Path, config: dict[str, Any]) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    patterns = [
        re.compile(pattern, re.IGNORECASE)
        for pattern in readability_options(config).get("comment_noise_patterns", DEFAULT_COMMENT_NOISE_PATTERNS)
    ]
    issues: list[Issue] = []
    with path.open("r", encoding="utf-8") as handle:
        for line_number, line in enumerate(handle, start=1):
            issue = _comment_noise_issue(relative, line_number, line, patterns)
            if issue:
                issues.append(issue)
    return issues


def _comment_noise_issue(
    relative: str,
    line_number: int,
    line: str,
    patterns: list[re.Pattern[str]],
) -> Issue | None:
    stripped = line.strip()
    if not stripped.startswith("#"):
        return None
    comment = stripped.lstrip("#").strip()
    if not comment or comment.startswith(("!", "-", "Licensed", "Copyright")):
        return None
    if not any(pattern.search(comment) for pattern in patterns):
        return None
    digest = hashlib.sha1(f"{relative}:{line_number}:{comment}".encode("utf-8")).hexdigest()[:12]
    return Issue(
        "comment_noise",
        f"comment_noise:{relative}:{digest}",
        relative,
        {"line": line_number, "text": comment},
    )


def _analyze_file(repo_root: Path, path: Path, config: dict[str, Any]) -> list[Issue]:
    relative = path.relative_to(repo_root).as_posix()
    issues = _file_size_issues(relative, path, config) if _is_file_length_path(relative, config) else []
    if path.suffix != ".py":
        return issues
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"))
    except SyntaxError as exc:
        return [
            Issue(
                "syntax_errors",
                f"syntax_error:{relative}:{exc.lineno}:{exc.offset}",
                relative,
                {"line": exc.lineno, "offset": exc.offset, "message": exc.msg},
            )
        ]
    visitor = FunctionVisitor(repo_root, path, config)
    visitor.visit(tree)
    issues.extend(visitor.issues)
    issues.extend(_module_attribute_injection_issues(relative, tree, config))
    return issues


def _module_attribute_injection_issues(
    relative: str,
    tree: ast.AST,
    config: dict[str, Any],
) -> list[Issue]:
    sys_aliases = {
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
        if alias.name == "sys"
    }
    module_aliases = {
        alias.asname or alias.name.split(".", 1)[0]
        for node in ast.walk(tree)
        if isinstance(node, ast.Import)
        for alias in node.names
    }
    module_aliases.update(
        alias.asname or alias.name
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module is None
        for alias in node.names
    )
    allowlisted_names = set(
        readability_options(config).get("module_attribute_injection_allowlist", [])
    )
    issues: list[Issue] = []
    for node in ast.walk(tree):
        issue = _assignment_injection_issue(
            relative, node, module_aliases, allowlisted_names
        )
        if issue:
            issues.append(issue)
        issues.extend(_namespace_copy_loop_issues(relative, node, allowlisted_names))
        issue = _sys_modules_update_issue(relative, node, sys_aliases, allowlisted_names)
        if issue:
            issues.append(issue)
    return issues


def _injection_issue(
    relative: str,
    node: ast.AST,
    kind: str,
    name: str,
    allowlisted_names: set[str],
) -> Issue | None:
    if f"{relative}:{name}" in allowlisted_names:
        return None
    return Issue(
        "module_attribute_injection",
        f"module_attribute_injection:{relative}:{kind}:{name}",
        relative,
        {"line": getattr(node, "lineno", 1), "name": name},
    )


def _assignment_injection_issue(
    relative: str,
    node: ast.AST,
    module_aliases: set[str],
    allowlisted_names: set[str],
) -> Issue | None:
    if not isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
        return None
    assignment_targets = node.targets if isinstance(node, ast.Assign) else [node.target]
    for assignment_target in assignment_targets:
        if isinstance(assignment_target, ast.Attribute):
            if assignment_target.attr == "__module__":
                return _injection_issue(
                    relative,
                    node,
                    "module_rewrite",
                    ast.unparse(assignment_target.value),
                    allowlisted_names,
                )
            if isinstance(assignment_target.value, ast.Name) and (
                assignment_target.value.id in module_aliases
                or re.fullmatch(r"_\w*facade", assignment_target.value.id)
            ):
                return _injection_issue(
                    relative,
                    node,
                    "facade_assignment",
                    ast.unparse(assignment_target),
                    allowlisted_names,
                )
    return None


def _namespace_copy_loop_issues(
    relative: str,
    node: ast.AST,
    allowlisted_names: set[str],
) -> list[Issue]:
    if not isinstance(node, (ast.For, ast.AsyncFor)):
        return []
    referenced_names = {
        child.id for child in ast.walk(node.iter) if isinstance(child, ast.Name)
    }
    blocked_names = {
        "_SPLIT_IMPLEMENTATION_MODULES",
        "_STORE_MIXIN_MODULES",
        "_SQL_STORE_MIXIN_MODULES",
    }
    return [
        issue
        for name in sorted(referenced_names & blocked_names)
        for issue in [
            _injection_issue(
                relative, node, "namespace_copy_loop", name, allowlisted_names
            )
        ]
        if issue
    ]


def _sys_modules_update_issue(
    relative: str,
    node: ast.AST,
    sys_aliases: set[str],
    allowlisted_names: set[str],
) -> Issue | None:
    if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
        return None
    namespace = node.func.value
    is_sys_modules_update = (
        node.func.attr == "update"
        and isinstance(namespace, ast.Attribute)
        and namespace.attr == "__dict__"
        and isinstance(namespace.value, ast.Subscript)
        and isinstance(namespace.value.value, ast.Attribute)
        and namespace.value.value.attr == "modules"
        and isinstance(namespace.value.value.value, ast.Name)
        and namespace.value.value.value.id in sys_aliases
    )
    if not is_sys_modules_update:
        return None
    return _injection_issue(
        relative,
        node,
        "sys_modules_update",
        "sys.modules.__dict__.update",
        allowlisted_names,
    )


def _huge_file_git_issue(name: str, path: str = ".") -> Issue:
    return Issue(
        "huge_file_growth",
        f"huge_file_growth:git:{name}:{path}",
        path,
        {"line": 1, "name": name},
    )


def _renamed_base_path_by_current(
    repo_root: Path,
    base_revision: str,
) -> tuple[dict[str, str], Issue | None]:
    completed = subprocess.run(
        [
            "git",
            "diff",
            "--name-status",
            "-z",
            "--find-renames=1%",
            f"{base_revision}..HEAD",
            "--",
        ],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if completed.returncode:
        return {}, _huge_file_git_issue("base diff unavailable")
    fields = completed.stdout.split(b"\0")
    renamed_base_path_by_current: dict[str, str] = {}
    field_index = 0
    while field_index < len(fields) and fields[field_index]:
        status = fields[field_index].decode("utf-8")
        field_index += 1
        if "\t" in status:
            status, base_path = status.split("\t", 1)
        else:
            base_path = fields[field_index].decode("utf-8")
            field_index += 1
        if not status.startswith(("R", "C")):
            continue
        current_path = fields[field_index].decode("utf-8")
        field_index += 1
        if status.startswith("R"):
            renamed_base_path_by_current[current_path] = base_path
    return renamed_base_path_by_current, None


def _base_file_lines(
    repo_root: Path,
    base_revision: str,
    relative: str,
) -> tuple[int | None, Issue | None]:
    listing = subprocess.run(
        ["git", "ls-tree", "-z", "--name-only", base_revision, "--", relative],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if listing.returncode:
        return None, _huge_file_git_issue("base tree unavailable", relative)
    if not listing.stdout:
        return None, None
    completed = subprocess.run(
        ["git", "show", f"{base_revision}:{relative}"],
        cwd=repo_root,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if completed.returncode:
        return None, _huge_file_git_issue("base file unavailable", relative)
    return len(completed.stdout.splitlines()), None


def _huge_file_growth_issues(
    repo_root: Path,
    source_files: list[Path],
    config: dict[str, Any],
    base_revision: str,
) -> list[Issue]:
    threshold_lines = threshold(config, "huge_file_lines", 5000)
    verify_base = subprocess.run(
        ["git", "rev-parse", "--verify", f"{base_revision}^{{commit}}"],
        cwd=repo_root,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        check=False,
    )
    if verify_base.returncode:
        return [_huge_file_git_issue("base revision unavailable")]
    renamed_base_path_by_current, rename_error = _renamed_base_path_by_current(
        repo_root, base_revision
    )
    if rename_error:
        return [rename_error]
    issues: list[Issue] = []
    for path in source_files:
        relative = path.relative_to(repo_root).as_posix()
        current_lines = _line_count(path)
        if current_lines <= threshold_lines or not _is_file_length_path(relative, config):
            continue
        base_relative = renamed_base_path_by_current.get(relative, relative)
        base_lines, lookup_error = _base_file_lines(
            repo_root,
            base_revision,
            base_relative,
        )
        if lookup_error:
            issues.append(lookup_error)
            continue
        if base_lines is None:
            continue
        if base_lines > threshold_lines and current_lines > base_lines:
            issues.append(
                Issue(
                    "huge_file_growth",
                    f"huge_file_growth:{relative}",
                    relative,
                    {"line": 1, "lines": current_lines, "limit": base_lines},
                )
            )
    return issues


_RUST_CFG_TEST = re.compile(
    r"^\s*#\[\s*cfg\s*\(\s*(?:test|all\s*\(\s*test\s*,[^)]*\))\s*\)\s*\]\s*(.*)$"
)
_RUST_ATTRIBUTE = re.compile(r"^\s*#\[.*\]\s*$")
_RUST_MODULE = re.compile(r"^\s*(?:pub(?:\s*\([^)]*\))?\s+)?mod\s+[A-Za-z_]\w*\b")


def _rust_brace_deltas(lines: list[str]) -> list[int]:
    """Count structural braces while ignoring Rust comments and literals."""
    deltas: list[int] = []
    block_comment_depth = 0
    string_terminator: str | None = None
    escaped = False
    raw_string = False
    for line in lines:
        delta = 0
        index = 0
        while index < len(line):
            if string_terminator is not None:
                if raw_string:
                    end_index = line.find(string_terminator, index)
                    if end_index < 0:
                        index = len(line)
                        continue
                    index = end_index + len(string_terminator)
                    string_terminator = None
                    raw_string = False
                    continue
                character = line[index]
                index += 1
                if escaped:
                    escaped = False
                elif character == "\\":
                    escaped = True
                elif character == string_terminator:
                    string_terminator = None
                continue
            if block_comment_depth:
                if line.startswith("/*", index):
                    block_comment_depth += 1
                    index += 2
                elif line.startswith("*/", index):
                    block_comment_depth -= 1
                    index += 2
                else:
                    index += 1
                continue
            if line.startswith("//", index):
                break
            if line.startswith("/*", index):
                block_comment_depth = 1
                index += 2
                continue
            raw_match = re.match(r"(?:br|cr|r)(#*)\"", line[index:])
            if raw_match:
                hashes = raw_match.group(1)
                string_terminator = f'\"{hashes}'
                raw_string = True
                index += raw_match.end()
                continue
            if line.startswith(('b\"', 'c\"'), index):
                string_terminator = '"'
                raw_string = False
                escaped = False
                index += 2
                continue
            if line[index] == '"':
                string_terminator = '"'
                raw_string = False
                escaped = False
                index += 1
                continue
            character_match = re.match(r"(?:b)?'(?:\\.|[^'\\])+'", line[index:])
            if character_match:
                index += character_match.end()
                continue
            if line[index] == "{":
                delta += 1
            elif line[index] == "}":
                delta -= 1
            index += 1
        deltas.append(delta)
    return deltas


def _rust_test_module_ranges(lines: list[str]) -> list[tuple[int, int]]:
    brace_deltas = _rust_brace_deltas(lines)
    ranges: list[tuple[int, int]] = []
    line_index = 0
    while line_index < len(lines):
        cfg_match = _RUST_CFG_TEST.match(lines[line_index])
        if not cfg_match:
            line_index += 1
            continue
        declaration_index = line_index
        declaration = cfg_match.group(1)
        if not declaration:
            declaration_index += 1
            while declaration_index < len(lines) and _RUST_ATTRIBUTE.match(lines[declaration_index]):
                declaration_index += 1
            if declaration_index >= len(lines):
                break
            declaration = lines[declaration_index]
        if not _RUST_MODULE.match(declaration):
            line_index += 1
            continue
        declaration_end = declaration_index
        while declaration_end < len(lines) and not re.search(r"[;{]", lines[declaration_end]):
            declaration_end += 1
        if declaration_end >= len(lines):
            line_index += 1
            continue
        if ";" in lines[declaration_end] and "{" not in lines[declaration_end]:
            ranges.append((line_index, declaration_end))
            line_index = declaration_end + 1
            continue
        depth = sum(brace_deltas[declaration_index : declaration_end + 1])
        module_end = declaration_end
        while depth > 0 and module_end + 1 < len(lines):
            module_end += 1
            depth += brace_deltas[module_end]
        ranges.append((line_index, module_end))
        line_index = module_end + 1
    return ranges


def _rust_non_test_line_count(path: Path) -> int:
    lines = path.read_text(encoding="utf-8").splitlines()
    excluded_lines = sum(
        end - start + 1 for start, end in _rust_test_module_ranges(lines)
    )
    return len(lines) - excluded_lines


def _file_size_issues(relative: str, path: Path, config: dict[str, Any]) -> list[Issue]:
    if path.suffix == ".rs":
        file_lines = _rust_non_test_line_count(path)
        max_file_lines = threshold(config, "max_rust_file_lines", 800)
    else:
        file_lines = _line_count(path)
        max_file_lines = threshold(config, "max_file_lines", 1500)
    if file_lines <= max_file_lines:
        return []
    return [
        Issue(
            "long_files",
            f"long_file:{relative}",
            relative,
            {"lines": file_lines, "limit": max_file_lines},
        )
    ]
